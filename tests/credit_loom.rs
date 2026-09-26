#![cfg(all(loom, target_pointer_width = "64"))]

//! Credit publication and parking, through the real MPSC endpoints.
//!
//! Keep endpoints alive across joins: teardown wakes must not rescue a lost
//! capacity notification. Loom scales the minimum release batch from 64 to 2;
//! four-slot rings exercise the same half-ring LWM without huge state spaces.
//! These are bounded end-to-end models, not proofs over arbitrary executions.

use fanring::mpsc::{Receiver, Sender, channel_with_policy};
use fanring::teardown::Teardown;

fn model(check: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    if std::env::var_os("LOOM_MAX_BRANCHES").is_none() {
        builder.max_branches = 10_000;
    }
    if std::env::var_os("LOOM_MAX_PERMUTATIONS").is_none() {
        builder.max_permutations = Some(10_000);
    }
    if std::env::var_os("LOOM_MAX_PREEMPTIONS").is_none() {
        builder.preemption_bound = Some(2);
    }
    builder.check(check);
}

fn full<P: Teardown>() -> (Sender<usize, P>, Receiver<usize, P>) {
    let (mut tx, rx) = channel_with_policy(4);
    for value in 0..4 {
        tx.try_send(value).unwrap();
    }
    (tx, rx)
}

#[cfg(feature = "async")]
mod asynchronous {
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    use loom::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    pub(super) struct Wakes(AtomicUsize);

    impl Wakes {
        pub(super) fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn waker() -> (Arc<Wakes>, Waker) {
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        (wakes, waker)
    }
}

macro_rules! credit_models {
    ($module:ident, $policy:ty) => {
        mod $module {
            use super::{full, model};
            use fanring::mpsc::{RecvError, SendError, SendTimeoutError, TryRecvError};
            use loom::thread;
            use std::time::Duration;

            #[test]
            fn blocked_sender_resumes_at_lwm_with_unread_values() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    let sender = thread::spawn(move || {
                        tx.send(4).unwrap();
                        tx
                    });
                    assert_eq!(rx.recv(), Ok(0));
                    assert_eq!(rx.recv(), Ok(1));
                    let tx = sender.join().unwrap();
                    for expected in 2..5 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                    drop(tx);
                });
            }

            #[test]
            fn fair_receive_returns_lwm_credits() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    let sender = thread::spawn(move || {
                        tx.send(4).unwrap();
                        tx
                    });
                    assert_eq!(rx.try_recv_fair(), Ok(0));
                    assert_eq!(rx.try_recv_fair(), Ok(1));
                    let tx = sender.join().unwrap();
                    for expected in 2..5 {
                        assert_eq!(rx.try_recv_fair(), Ok(expected));
                    }
                    drop(tx);
                });
            }

            #[test]
            fn partial_flush_races_blocking_registration() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    let sender = thread::spawn(move || {
                        tx.send(4).unwrap();
                        tx
                    });
                    assert_eq!(rx.recv(), Ok(0));
                    rx.release_consumed();
                    let mut tx = sender.join().unwrap();
                    assert!(tx.is_full());
                    for expected in 1..5 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                });
            }

            #[test]
            fn partial_bulk_return_races_blocking_registration() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    let sender = thread::spawn(move || {
                        tx.send(4).unwrap();
                        tx
                    });
                    let mut out = Vec::new();
                    assert_eq!(rx.recv_batch_into(&mut out, 1), Ok(1));
                    assert_eq!(out, [0]);
                    let mut tx = sender.join().unwrap();
                    assert!(tx.is_full());
                    for expected in 1..5 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                });
            }

            #[test]
            fn zero_limit_releases_partial_credits() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    assert_eq!(rx.try_recv(), Ok(0));
                    let sender = thread::spawn(move || {
                        tx.send(4).unwrap();
                        tx
                    });
                    let mut out = vec![99];
                    assert_eq!(rx.try_recv_batch_into(&mut out, 0), Ok(0));
                    assert_eq!(out, [99]);
                    let mut tx = sender.join().unwrap();
                    assert!(tx.is_full());
                    assert_eq!(rx.try_recv(), Ok(1));
                });
            }

            #[test]
            fn admission_stop_releases_partial_credits_not_rejected_value() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    let sender = thread::spawn(move || {
                        tx.send(4).unwrap();
                        tx
                    });
                    let mut out = Vec::new();
                    assert_eq!(
                        rx.try_recv_batch_into_while(&mut out, 4, |&value| value == 0),
                        Ok(1)
                    );
                    assert_eq!(out, [0]);
                    let mut tx = sender.join().unwrap();
                    assert!(tx.is_full());
                    assert_eq!(rx.try_recv(), Ok(1));
                });
            }

            #[test]
            fn empty_partial_window_racing_refill_cannot_strand_sender() {
                model(|| {
                    let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, $policy>(4);
                    tx.try_send(0).unwrap();
                    assert_eq!(rx.try_recv(), Ok(0));
                    let sender = thread::spawn(move || {
                        for value in 1..=4 {
                            tx.send(value).unwrap();
                        }
                        tx
                    });
                    // Empty must flush one partial credit. If refill wins,
                    // receiving the next value instead reaches the LWM.
                    let next = match rx.try_recv() {
                        Err(TryRecvError::Empty) => 1,
                        Ok(1) => 2,
                        other => panic!("unexpected receive {other:?}"),
                    };
                    let tx = sender.join().unwrap();
                    for expected in next..=4 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                    drop(tx);
                });
            }

            #[test]
            fn timeout_racing_lwm_preserves_value_and_registration() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    let sender = thread::spawn(move || {
                        let result = tx.send_timeout(4, Duration::ZERO);
                        (tx, result)
                    });
                    assert_eq!(rx.recv(), Ok(0));
                    assert_eq!(rx.recv(), Ok(1));
                    let (mut tx, result) = sender.join().unwrap();
                    match result {
                        Ok(()) => {}
                        Err(SendTimeoutError::Timeout(value)) => tx.send(value).unwrap(),
                        other => panic!("unexpected send {other:?}"),
                    }
                    for expected in 2..5 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                });
            }

            #[test]
            fn receiver_drop_bypasses_lwm_after_partial_consumption() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    assert_eq!(rx.try_recv(), Ok(0));
                    let sender = thread::spawn(move || {
                        let result = tx.send(4);
                        (tx, result)
                    });
                    drop(rx);
                    let (mut tx, result) = sender.join().unwrap();
                    // Deferred yring close releases the consumed head. An
                    // overlapping send may reuse that slot before observing
                    // close. Coordinated teardown keeps it reserved.
                    if <$policy as fanring::teardown::Teardown>::COORDINATED {
                        assert_eq!(result, Err(SendError(4)));
                    }
                    assert_eq!(tx.send(5), Err(SendError(5)));
                });
            }

            #[test]
            fn sender_drop_preserves_partially_consumed_window() {
                model(|| {
                    let (tx, mut rx) = full::<$policy>();
                    assert_eq!(rx.try_recv(), Ok(0));
                    let sender = thread::spawn(move || drop(tx));
                    for expected in 1..4 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                    assert_eq!(rx.recv(), Err(RecvError));
                    sender.join().unwrap();
                });
            }

            #[test]
            fn successive_lwm_batches_reuse_slots_without_losing_wakes() {
                model(|| {
                    let (mut tx, mut rx) = full::<$policy>();
                    let sender = thread::spawn(move || {
                        for value in 4..8 {
                            tx.send(value).unwrap();
                        }
                        tx
                    });
                    for expected in 0..4 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                    let tx = sender.join().unwrap();
                    for expected in 4..8 {
                        assert_eq!(rx.recv(), Ok(expected));
                    }
                    drop(tx);
                });
            }

            #[cfg(feature = "async")]
            mod asynchronous {
                use super::{full, model, thread};
                use crate::asynchronous::waker;
                use std::future::Future;
                use std::pin::pin;
                use std::task::{Context, Poll};

                #[test]
                fn lwm_racing_registration_is_ready_or_woken() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        assert_eq!(rx.try_recv(), Ok(0));
                        let (wakes, waker) = waker();
                        let receiver = thread::spawn(move || {
                            assert_eq!(rx.try_recv(), Ok(1));
                            rx
                        });
                        let result = tx.poll_ready(&mut Context::from_waker(&waker));
                        let mut rx = receiver.join().unwrap();
                        if result.is_pending() {
                            assert!(wakes.count() > 0);
                        }
                        assert_eq!(
                            tx.poll_ready(&mut Context::from_waker(&waker)),
                            Poll::Ready(Ok(()))
                        );
                        tx.try_send(4).unwrap();
                        tx.try_send(5).unwrap();
                        assert!(tx.is_full());
                        for expected in 2..6 {
                            assert_eq!(rx.try_recv(), Ok(expected));
                        }
                    });
                }

                #[test]
                fn partial_flush_racing_registration_is_ready_or_woken() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        let (wakes, waker) = waker();
                        let receiver = thread::spawn(move || {
                            assert_eq!(rx.try_recv(), Ok(0));
                            rx.release_consumed();
                            rx
                        });
                        let result = tx.poll_ready(&mut Context::from_waker(&waker));
                        let rx = receiver.join().unwrap();
                        if result.is_pending() {
                            assert!(wakes.count() > 0);
                        }
                        tx.try_send(4).unwrap();
                        assert!(tx.is_full());
                        drop(rx);
                    });
                }

                #[test]
                fn cancel_and_reregister_racing_lwm_keeps_new_waiter_live() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        let (_, first) = waker();
                        let (wakes, second) = waker();
                        assert!(tx.poll_ready(&mut Context::from_waker(&first)).is_pending());
                        let receiver = thread::spawn(move || {
                            assert_eq!(rx.try_recv_fair(), Ok(0));
                            assert_eq!(rx.try_recv_fair(), Ok(1));
                            rx
                        });
                        tx.cancel_send_wait();
                        let result = tx.poll_ready(&mut Context::from_waker(&second));
                        let rx = receiver.join().unwrap();
                        if result.is_pending() {
                            assert!(wakes.count() > 0);
                        }
                        assert_eq!(
                            tx.poll_ready(&mut Context::from_waker(&second)),
                            Poll::Ready(Ok(()))
                        );
                        drop(rx);
                    });
                }

                #[test]
                fn replace_registered_waker_racing_lwm_keeps_progress() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        let (_, first) = waker();
                        let (wakes, second) = waker();
                        assert!(tx.poll_ready(&mut Context::from_waker(&first)).is_pending());
                        let receiver = thread::spawn(move || {
                            assert_eq!(rx.recv(), Ok(0));
                            assert_eq!(rx.recv(), Ok(1));
                            rx
                        });
                        let result = tx.poll_ready(&mut Context::from_waker(&second));
                        let rx = receiver.join().unwrap();
                        if result.is_pending() {
                            assert!(wakes.count() > 0);
                        }
                        assert_eq!(
                            tx.poll_ready(&mut Context::from_waker(&second)),
                            Poll::Ready(Ok(()))
                        );
                        drop(rx);
                    });
                }

                #[test]
                fn single_async_receive_keeps_immediate_credit_contract() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        let (wakes, sender_waker) = waker();
                        let (_, receiver_waker) = waker();
                        let receiver = thread::spawn(move || {
                            assert_eq!(
                                rx.poll_recv(&mut Context::from_waker(&receiver_waker)),
                                Poll::Ready(Ok(0))
                            );
                            rx
                        });
                        let result = tx.poll_ready(&mut Context::from_waker(&sender_waker));
                        let rx = receiver.join().unwrap();
                        if result.is_pending() {
                            assert!(wakes.count() > 0);
                        }
                        tx.try_send(4).unwrap();
                        assert!(tx.is_full());
                        drop(rx);
                    });
                }

                #[test]
                fn partial_async_bulk_return_wakes_sender() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        let (wakes, sender_waker) = waker();
                        let (_, receiver_waker) = waker();
                        let receiver = thread::spawn(move || {
                            let mut out = Vec::new();
                            {
                                let mut receive = pin!(rx.recv_batch_into_async(&mut out, 1));
                                assert_eq!(
                                    receive
                                        .as_mut()
                                        .poll(&mut Context::from_waker(&receiver_waker)),
                                    Poll::Ready(Ok(1))
                                );
                            }
                            assert_eq!(out, [0]);
                            rx
                        });
                        let result = tx.poll_ready(&mut Context::from_waker(&sender_waker));
                        let rx = receiver.join().unwrap();
                        if result.is_pending() {
                            assert!(wakes.count() > 0);
                        }
                        tx.try_send(4).unwrap();
                        assert!(tx.is_full());
                        drop(rx);
                    });
                }

                #[test]
                fn drop_after_partial_consumption_races_async_registration() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        assert_eq!(rx.try_recv(), Ok(0));
                        let (wakes, waker) = waker();
                        let receiver = thread::spawn(move || drop(rx));
                        let result = tx.poll_ready(&mut Context::from_waker(&waker));
                        receiver.join().unwrap();
                        if result.is_pending() {
                            assert!(wakes.count() > 0);
                        }
                        assert!(matches!(
                            tx.poll_ready(&mut Context::from_waker(&waker)),
                            Poll::Ready(Err(_))
                        ));
                    });
                }

                #[test]
                fn canceled_pending_send_reserves_no_credit() {
                    model(|| {
                        let (mut tx, mut rx) = full::<$policy>();
                        let (_, waker) = waker();
                        {
                            let mut send = pin!(tx.send_async(99));
                            assert!(
                                send.as_mut()
                                    .poll(&mut Context::from_waker(&waker))
                                    .is_pending()
                            );
                        }
                        let receiver = thread::spawn(move || {
                            assert_eq!(rx.try_recv(), Ok(0));
                            assert_eq!(rx.try_recv(), Ok(1));
                            rx
                        });
                        let result = tx.poll_ready(&mut Context::from_waker(&waker));
                        let mut rx = receiver.join().unwrap();
                        assert!(result.is_pending() || result == Poll::Ready(Ok(())));
                        tx.cancel_send_wait();
                        tx.try_send(4).unwrap();
                        tx.try_send(5).unwrap();
                        assert!(tx.is_full());
                        for expected in 2..6 {
                            assert_eq!(rx.try_recv(), Ok(expected));
                        }
                    });
                }
            }
        }
    };
}

credit_models!(deferred, fanring::teardown::Deferred);
credit_models!(coordinated, fanring::teardown::Coordinated);

#[test]
fn deferred_full_publication_and_partial_empty_flush_make_progress() {
    model(|| {
        let (mut tx, mut rx) = fanring::mpsc::channel(4);
        tx.try_send(0).unwrap();
        assert_eq!(rx.try_recv(), Ok(0));
        let sender = loom::thread::spawn(move || {
            for value in 1..4 {
                tx.try_send_deferred(value).unwrap();
            }
            // Full publishes earlier deferred values. If the receiver already
            // released its partial credit, this send succeeds unpublished.
            let result = tx.try_send_deferred(4);
            tx.flush();
            if let Err(fanring::mpsc::TrySendError::Full(value)) = result {
                tx.send(value).unwrap();
            } else {
                assert_eq!(result, Ok(()));
            }
            tx
        });
        for expected in 1..5 {
            assert_eq!(rx.recv(), Ok(expected));
        }
        let tx = sender.join().unwrap();
        assert_eq!(rx.try_recv(), Err(fanring::mpsc::TryRecvError::Empty));
        drop(tx);
    });
}
