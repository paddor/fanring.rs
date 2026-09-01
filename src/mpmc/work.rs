//! Receiver-local staging and batch stealing for the MPMC channel.
//!
//! Each receiver owns one bounded queue. Competing receivers may pop from it
//! and move a small batch into their own queue. The shared unbounded queue only
//! preserves staged values when a receiver is dropped.

use concurrent_queue::ConcurrentQueue;

use crate::compat::Arc;

const STEAL_BATCH_LIMIT: usize = 8;

/// Cloneable handle to a lock-free queue of staged receiver work.
pub(super) struct WorkQueue<T> {
    queue: Arc<ConcurrentQueue<T>>,
}

impl<T> Clone for WorkQueue<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

impl<T> WorkQueue<T> {
    /// Create a bounded queue for one receiver's prefetched values.
    pub(super) fn bounded(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ConcurrentQueue::bounded(capacity)),
        }
    }

    /// Create the shared unbounded queue used during receiver removal.
    pub(super) fn unbounded() -> Self {
        Self {
            queue: Arc::new(ConcurrentQueue::unbounded()),
        }
    }

    /// Append one staged value.
    ///
    /// Bounded queues rely on the invariant that a receiver stages at most one
    /// prefetch batch while its queue is empty.
    #[inline]
    pub(super) fn push(&self, value: T) {
        if self.queue.push(value).is_err() {
            unreachable!("work queue capacity matches the maximum staged batch");
        }
    }

    /// Remove one staged value, or return `None` when the queue is empty.
    #[inline]
    pub(super) fn pop(&self) -> Option<T> {
        self.queue.pop().ok()
    }

    /// Steal one value and move part of the remaining work to `destination`.
    ///
    /// The returned value satisfies the current receive. At most seven more
    /// values move to the thief, bounding work performed by one steal.
    pub(super) fn steal_batch_into(&self, destination: &Self) -> Option<T> {
        let value = self.pop()?;
        let transfer = self.queue.len().div_ceil(2).min(STEAL_BATCH_LIMIT - 1);
        for _ in 0..transfer {
            let Some(stolen) = self.pop() else {
                break;
            };
            destination.push(stolen);
        }
        Some(value)
    }

    /// Move every currently reachable value to `destination`.
    ///
    /// Returns whether at least one value moved.
    pub(super) fn drain_into(&self, destination: &Self) -> bool {
        let mut moved = false;
        while let Some(value) = self.pop() {
            destination.push(value);
            moved = true;
        }
        moved
    }

    /// Return an instantaneous queue-length estimate.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }
}

impl<T> std::fmt::Debug for WorkQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkQueue")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::WorkQueue;

    #[test]
    fn steal_returns_one_and_moves_half_the_remainder() {
        let source = WorkQueue::bounded(8);
        let destination = WorkQueue::bounded(8);
        for value in 0..7 {
            source.push(value);
        }

        assert_eq!(source.steal_batch_into(&destination), Some(0));
        assert_eq!(source.len(), 3);
        assert_eq!(destination.len(), 3);

        let mut values = Vec::new();
        while let Some(value) = source.pop() {
            values.push(value);
        }
        while let Some(value) = destination.pop() {
            values.push(value);
        }
        values.sort_unstable();
        assert_eq!(values, (1..7).collect::<Vec<_>>());
    }

    #[test]
    fn steal_batch_is_capped_at_eight_values() {
        let source = WorkQueue::bounded(64);
        let destination = WorkQueue::bounded(64);
        for value in 0..64 {
            source.push(value);
        }

        assert_eq!(source.steal_batch_into(&destination), Some(0));
        assert_eq!(source.len(), 56);
        assert_eq!(destination.len(), 7);
    }

    #[test]
    fn drain_moves_every_value() {
        let source = WorkQueue::bounded(4);
        let destination = WorkQueue::unbounded();
        for value in 0..4 {
            source.push(value);
        }

        assert!(source.drain_into(&destination));
        assert_eq!(source.len(), 0);
        assert_eq!(destination.len(), 4);
        assert!(!source.drain_into(&destination));
    }
}

#[cfg(all(test, loom, target_pointer_width = "64"))]
mod loom_tests {
    use loom::thread;

    use super::WorkQueue;

    #[test]
    fn concurrent_steals_preserve_every_value() {
        loom::model(|| {
            let source = WorkQueue::bounded(2);
            source.push(1);
            source.push(2);

            let first_source = source.clone();
            let first = thread::spawn(move || drain_steal(&first_source));
            let second_source = source.clone();
            let second = thread::spawn(move || drain_steal(&second_source));

            let mut values = first.join().unwrap();
            values.extend(second.join().unwrap());
            while let Some(value) = source.pop() {
                values.push(value);
            }
            values.sort_unstable();
            assert_eq!(values, [1, 2]);
        });
    }

    #[test]
    fn drain_racing_steal_preserves_every_value() {
        loom::model(|| {
            let source = WorkQueue::bounded(2);
            source.push(1);
            source.push(2);

            let orphaned = WorkQueue::unbounded();
            let drain_source = source.clone();
            let drain_orphaned = orphaned.clone();
            let drain = thread::spawn(move || {
                drain_source.drain_into(&drain_orphaned);
            });
            let steal_source = source.clone();
            let steal = thread::spawn(move || drain_steal(&steal_source));

            drain.join().unwrap();
            let mut values = steal.join().unwrap();
            while let Some(value) = source.pop() {
                values.push(value);
            }
            while let Some(value) = orphaned.pop() {
                values.push(value);
            }
            values.sort_unstable();
            assert_eq!(values, [1, 2]);
        });
    }

    fn drain_steal(source: &WorkQueue<usize>) -> Vec<usize> {
        let destination = WorkQueue::bounded(2);
        let mut values = source
            .steal_batch_into(&destination)
            .into_iter()
            .collect::<Vec<_>>();
        while let Some(value) = destination.pop() {
            values.push(value);
        }
        values
    }
}
