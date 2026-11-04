//! Readiness protocols of layers built on the ring, modeled with plain
//! atomics.

use loom::thread;

#[test]
fn upper_layer_lwm_reactivation_rechecks_stale_flag() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    loom::model(|| {
        const CAPACITY: usize = 4;
        const LOW_WATER_MARK: usize = CAPACITY / 2;

        let len = Arc::new(AtomicUsize::new(CAPACITY));
        let above_lwm = Arc::new(AtomicBool::new(false));

        let sender_len = len.clone();
        let sender_above_lwm = above_lwm.clone();
        let sender = thread::spawn(move || {
            if sender_len.load(Ordering::Acquire) >= CAPACITY {
                thread::yield_now();
                sender_above_lwm.store(true, Ordering::Release);
            }
        });

        let consumer_len = len.clone();
        let consumer_above_lwm = above_lwm.clone();
        let consumer = thread::spawn(move || {
            consumer_len.store(LOW_WATER_MARK, Ordering::Release);
            let _ = consumer_above_lwm.swap(false, Ordering::AcqRel);
        });

        sender.join().unwrap();
        consumer.join().unwrap();

        let len = len.load(Ordering::Acquire);
        let above_lwm = above_lwm.load(Ordering::Acquire);
        let can_reactivate = !above_lwm || len <= LOW_WATER_MARK;
        assert!(
            can_reactivate,
            "stale high-water flag must not hide a pipe below low-water mark"
        );
    });
}

#[test]
fn upper_layer_ready_survives_cancelled_waiter() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    struct Signal {
        pending: AtomicBool,
        permit: AtomicBool,
    }

    impl Signal {
        fn mark(&self) {
            if !self.pending.swap(true, Ordering::Release) {
                self.permit.store(true, Ordering::Release);
            }
        }

        fn cancel_waiter_after_poll(&self) {
            let _ = self.permit.swap(false, Ordering::AcqRel);
        }

        fn ready_after_enable(&self) -> bool {
            self.pending.load(Ordering::Acquire) || self.permit.swap(false, Ordering::AcqRel)
        }
    }

    loom::model(|| {
        let signal = Arc::new(Signal {
            pending: AtomicBool::new(false),
            permit: AtomicBool::new(false),
        });

        let producer_signal = signal.clone();
        let producer = thread::spawn(move || producer_signal.mark());

        let cancelled_signal = signal.clone();
        let cancelled = thread::spawn(move || cancelled_signal.cancel_waiter_after_poll());

        producer.join().unwrap();
        cancelled.join().unwrap();

        assert!(
            signal.ready_after_enable(),
            "pending flag must preserve readiness after a permit is consumed"
        );
    });
}

#[test]
fn upper_layer_blocking_wait_uses_stateful_generation() {
    use loom::sync::atomic::{AtomicBool, Ordering};
    use loom::sync::{Arc, Condvar, Mutex};

    struct BlockingSpace {
        generation: Mutex<u64>,
        changed: Condvar,
    }

    impl BlockingSpace {
        fn notify(&self) {
            let mut generation = self.generation.lock().unwrap();
            *generation = generation.wrapping_add(1);
            drop(generation);
            self.changed.notify_all();
        }

        fn wait_until_not_full(&self, full: &AtomicBool) {
            while full.load(Ordering::Acquire) {
                let mut generation = self.generation.lock().unwrap();
                if !full.load(Ordering::Acquire) {
                    return;
                }
                let seen = *generation;
                while seen == *generation && full.load(Ordering::Acquire) {
                    generation = self.changed.wait(generation).unwrap();
                }
            }
        }
    }

    loom::model(|| {
        let space = Arc::new(BlockingSpace {
            generation: Mutex::new(0),
            changed: Condvar::new(),
        });
        let full = Arc::new(AtomicBool::new(true));
        let finished = Arc::new(AtomicBool::new(false));

        let waiter_space = space.clone();
        let waiter_full = full.clone();
        let waiter_finished = finished.clone();
        let waiter = thread::spawn(move || {
            waiter_space.wait_until_not_full(&waiter_full);
            waiter_finished.store(true, Ordering::Release);
        });

        let notifier_space = space.clone();
        let notifier_full = full.clone();
        let notifier = thread::spawn(move || {
            thread::yield_now();
            notifier_full.store(false, Ordering::Release);
            notifier_space.notify();
        });

        waiter.join().unwrap();
        notifier.join().unwrap();

        assert!(finished.load(Ordering::Acquire));
    });
}
