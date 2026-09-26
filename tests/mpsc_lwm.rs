#![cfg(not(loom))]

use fanring::mpsc::{TryRecvError, channel_with_policy};
use fanring::teardown::{Coordinated, Deferred, Teardown};

// Keep several fairness bursts per LWM batch under Miri without interpreting
// every large-ring permutation used by the native tests.
const LARGE_CAPACITY: usize = if cfg!(miri) { 512 } else { 8192 };

#[test]
fn full_lane_returns_credits_at_capacity_scaled_boundary() {
    fn check<P: Teardown>() {
        for requested in [1, 2, 3, 8, 64, 128, 256, LARGE_CAPACITY] {
            for receive in 0..3 {
                let (mut tx, mut rx) = channel_with_policy::<_, P>(requested);
                let capacity = tx.capacity();
                let credits = capacity.div_ceil(2).max(64).min(capacity);
                for value in 0..capacity {
                    tx.try_send(value).unwrap();
                }
                for expected in 0..credits {
                    assert!(tx.is_full(), "capacity {capacity}, received {expected}");
                    let value = match receive {
                        0 => rx.try_recv().unwrap(),
                        1 => rx.recv().unwrap(),
                        _ => rx.try_recv_fair().unwrap(),
                    };
                    assert_eq!(value, expected);
                }
                for value in capacity..capacity + credits {
                    tx.try_send(value).unwrap();
                }
                assert!(tx.is_full());
                for expected in credits..capacity + credits {
                    assert_eq!(rx.try_recv(), Ok(expected));
                }
                assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
            }
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn partial_prefetch_windows_accumulate_credits_across_fairness_rotations() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(512);
        for value in 0..256 {
            tx.try_send(value).unwrap();
            assert_eq!(rx.try_recv(), Ok(value));
        }
        // A full LWM batch spans four fairness bursts and 256 short windows.
        for value in 256..768 {
            tx.try_send(value).unwrap();
        }
        assert!(tx.is_full());
        for expected in 256..768 {
            assert_eq!(rx.try_recv(), Ok(expected));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn large_credit_batch_does_not_delay_a_quiet_lane() {
    fn check<P: Teardown>() {
        for fair in [false, true] {
            let (mut hot, mut rx) = channel_with_policy::<_, P>(512);
            // Include unused lanes across ready pages. Keep them registered.
            let idle: Vec<_> = (0..if cfg!(miri) { 8 } else { 129 })
                .map(|_| hot.try_clone().unwrap())
                .collect();
            for value in 0..512 {
                hot.try_send((0, value)).unwrap();
            }
            assert_eq!(rx.try_recv(), Ok((0, 0)));
            let mut quiet = hot.try_clone().unwrap();
            quiet.try_send((1, 0)).unwrap();
            let mut found = false;
            // Ordinary receives may need one readiness-poll interval followed
            // by one burst before serving a newly discovered lane. Fair
            // receives discover it immediately, then rotate after one item.
            for _ in 0..if fair { 2 } else { 128 } {
                let value = if fair {
                    rx.try_recv_fair().unwrap()
                } else {
                    rx.try_recv().unwrap()
                };
                if value == (1, 0) {
                    found = true;
                    break;
                }
            }
            assert!(found, "credit batching must not change the fairness bound");
            assert!(hot.is_full(), "fairness must not force a credit release");
            drop(idle);
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[cfg(feature = "async")]
mod asynchronous {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn producer_wakes_at_lwm_not_each_fairness_burst() {
        fn check<P: Teardown>() {
            for capacity in [1_usize, 2, 4, 16, 256, LARGE_CAPACITY] {
                let (mut tx, mut rx) = channel_with_policy::<_, P>(capacity);
                for value in 0..capacity {
                    tx.try_send(value).unwrap();
                }
                let wakes = Arc::new(Wakes::default());
                let waker = Waker::from(wakes.clone());
                let mut cx = Context::from_waker(&waker);
                assert_eq!(tx.poll_ready(&mut cx), Poll::Pending);
                for expected in 0..capacity.div_ceil(2).max(64).min(capacity) {
                    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
                    assert_eq!(rx.try_recv_fair(), Ok(expected));
                }
                assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
                assert_eq!(tx.poll_ready(&mut cx), Poll::Ready(Ok(())));
            }
        }
        check::<Deferred>();
        check::<Coordinated>();
    }

    #[test]
    fn forced_partial_flush_wakes_once_and_resets_credit_batch() {
        fn check<P: Teardown>() {
            let (mut tx, mut rx) = channel_with_policy::<_, P>(256);
            for value in 0..256 {
                tx.try_send(value).unwrap();
            }
            let wakes = Arc::new(Wakes::default());
            let waker = Waker::from(wakes.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(tx.poll_ready(&mut cx).is_pending());
            assert_eq!(rx.try_recv(), Ok(0));
            assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
            rx.release_consumed();
            assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
            assert!(tx.poll_ready(&mut cx).is_ready());
            tx.try_send(256).unwrap();
            assert!(tx.poll_ready(&mut cx).is_pending());
            rx.release_consumed();
            assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
            for value in 1..=128 {
                assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
                assert_eq!(rx.try_recv(), Ok(value));
            }
            assert_eq!(wakes.0.load(Ordering::Relaxed), 2);
            assert!(tx.poll_ready(&mut cx).is_ready());
        }
        check::<Deferred>();
        check::<Coordinated>();
    }
}
