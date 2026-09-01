use crate::compat::{AtomicUsize, Ordering};

/// Tracks work moving between MPMC-owned queues.
///
/// Publishers increment `generation` before leaving `in_flight`. A receiver
/// can therefore prove that an empty scan was stable, or conservatively report
/// a transient empty result while work is moving.
pub(crate) struct PublicationTracker {
    generation: AtomicUsize,
    in_flight: AtomicUsize,
}

impl PublicationTracker {
    pub(crate) fn new() -> Self {
        Self {
            generation: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> usize {
        self.generation.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn begin(&self) -> Publication<'_> {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        Publication { tracker: self }
    }

    #[inline]
    pub(crate) fn changed_since(&self, snapshot: usize) -> bool {
        self.generation.load(Ordering::Acquire) != snapshot
    }

    #[inline]
    pub(crate) fn is_in_flight(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub(crate) fn notify_change(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }
}

pub(crate) struct Publication<'a> {
    tracker: &'a PublicationTracker,
}

impl Drop for Publication<'_> {
    #[inline]
    fn drop(&mut self) {
        self.tracker.notify_change();
        self.tracker.in_flight.fetch_sub(1, Ordering::Release);
    }
}

#[cfg(all(test, loom, target_pointer_width = "64"))]
mod loom_tests {
    use super::PublicationTracker;
    use crate::compat::{Arc, AtomicBool, Ordering};

    #[test]
    fn completed_publication_changes_snapshot() {
        loom::model(|| {
            let tracker = Arc::new(PublicationTracker::new());
            let snapshot = tracker.snapshot();
            let publisher_tracker = tracker.clone();
            let publisher = loom::thread::spawn(move || {
                let _publication = publisher_tracker.begin();
            });

            publisher.join().unwrap();
            assert!(tracker.changed_since(snapshot));
            assert!(!tracker.is_in_flight());
        });
    }

    #[test]
    fn active_publication_prevents_stable_empty_scan() {
        loom::model(|| {
            let tracker = Arc::new(PublicationTracker::new());
            let active = Arc::new(AtomicBool::new(false));
            let release = Arc::new(AtomicBool::new(false));
            let publisher_tracker = tracker.clone();
            let publisher_active = active.clone();
            let publisher_release = release.clone();
            let publisher = loom::thread::spawn(move || {
                let _publication = publisher_tracker.begin();
                publisher_active.store(true, Ordering::Release);
                while !publisher_release.load(Ordering::Acquire) {
                    loom::thread::yield_now();
                }
            });

            while !active.load(Ordering::Acquire) {
                loom::thread::yield_now();
            }
            let snapshot = tracker.snapshot();
            assert!(tracker.is_in_flight());
            release.store(true, Ordering::Release);
            publisher.join().unwrap();
            assert!(tracker.changed_since(snapshot));
        });
    }

    #[test]
    fn multiple_publishers_leave_visible_generation() {
        loom::model(|| {
            let tracker = Arc::new(PublicationTracker::new());
            let snapshot = tracker.snapshot();
            let first_tracker = tracker.clone();
            let second_tracker = tracker.clone();
            let first = loom::thread::spawn(move || {
                let _publication = first_tracker.begin();
                loom::thread::yield_now();
            });
            let second = loom::thread::spawn(move || {
                let _publication = second_tracker.begin();
                loom::thread::yield_now();
            });

            first.join().unwrap();
            second.join().unwrap();
            assert!(tracker.changed_since(snapshot));
            assert!(!tracker.is_in_flight());
        });
    }
}
