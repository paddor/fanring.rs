#![cfg(all(loom, target_pointer_width = "64"))]

//! Receiver-owned lane control, publication, and producer capacity waits.
//! Keep endpoints alive across joins so teardown cannot rescue a missed wake.
//! End-to-end exploration is bounded; see DEVELOPMENT.md for limits.

use fanring::mpsc::{RecvError, TryRecvError, TrySendError, channel};
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

#[derive(Debug)]
struct Counted(Arc<loom::sync::atomic::AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

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

#[test]
fn mixed_capacity_lanes_release_and_resume_independently() {
    model(|| {
        let (mut small, mut rx) = channel(1);
        let mut large = small.try_register_with_capacity(4).unwrap();
        let lane = large.lane();
        rx.pause(&lane).unwrap();
        let sender = thread::spawn(move || {
            for value in 0..4 {
                large.try_send(value).unwrap();
            }
            large
        });
        small.try_send(99).unwrap();
        // The concurrent publisher may have claimed an empty ready page but
        // not published its group bit yet. Retry that transient empty probe.
        loop {
            match rx.try_recv_fair() {
                Ok(value) => {
                    assert_eq!(value, 99);
                    break;
                }
                Err(TryRecvError::Empty) => thread::yield_now(),
                Err(TryRecvError::Disconnected) => panic!("live lanes disconnected"),
            }
        }
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
        let mut large = sender.join().unwrap();
        assert_eq!(large.capacity(), 4);
        assert!(large.is_full());
        assert_eq!(rx.try_recv_from(&lane), Ok(0));
        rx.resume(&lane).unwrap();
        for value in 1..4 {
            assert_eq!(rx.try_recv_fair(), Ok(value));
        }
        rx.release_consumed();
        large.try_send(4).unwrap();
        assert_eq!(rx.try_recv_fair(), Ok(4));
    });
}

#[test]
fn externally_signaled_tagged_poll_cannot_strand_an_unpaused_lane() {
    use loom::sync::atomic::fence;
    model(|| {
        let (mut paused, mut rx) = channel(2);
        let mut live = paused.try_register().unwrap();
        let lane = live.lane();
        rx.pause(&paused.lane()).unwrap();
        paused.try_send_unsignaled(9).unwrap();
        let sleeping = Arc::new(AtomicBool::new(false));
        let sender = {
            let sleeping = sleeping.clone();
            thread::spawn(move || {
                live.try_send_unsignaled(1).unwrap();
                fence(Ordering::SeqCst);
                let woke = sleeping.load(Ordering::Acquire);
                (live, woke)
            })
        };
        rx.poll_all_lanes();
        let mut received = rx.with_lane_ids().try_recv_fair().ok();
        if received.is_none() {
            sleeping.store(true, Ordering::Release);
            fence(Ordering::SeqCst);
            rx.poll_all_lanes();
            received = rx.with_lane_ids().try_recv_fair().ok();
        }
        let (_live, woke) = sender.join().unwrap();
        assert!(received.is_some() || woke, "unsignaled value stranded");
        rx.poll_all_lanes();
        let received = received.or_else(|| rx.with_lane_ids().try_recv_fair().ok());
        assert_eq!(received, Some((lane, 1)));
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
        assert_eq!(rx.try_recv_from(&paused.lane()), Ok(9));
    });
}

#[test]
fn tagged_receive_racing_send_identifies_the_paused_lane() {
    model(|| {
        let (mut tx, mut rx) = channel(2);
        let expected = tx.lane();
        let sender = thread::spawn(move || {
            tx.try_send(1).unwrap();
            tx.try_send(2).unwrap();
        });
        let (lane, value) = rx.with_lane_ids().recv().unwrap();
        assert_eq!(lane, expected);
        assert_eq!(value, 1);
        rx.pause(&lane).unwrap();
        assert_eq!(rx.with_lane_ids().try_recv_fair(), Err(TryRecvError::Empty));
        sender.join().unwrap();
        assert_eq!(rx.try_recv_from(&lane), Ok(2));
        assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Disconnected));
        assert_eq!(rx.with_lane_ids().recv(), Err(RecvError));
    });
}

#[test]
fn paused_lane_control_rejects_concurrently_created_foreign_channel() {
    model(|| {
        let creator = thread::spawn(|| {
            let (tx, _rx) = channel::<usize>(1);
            tx.lane()
        });
        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();
        let own = tx.lane();
        let foreign = creator.join().unwrap();
        assert_ne!(own, foreign);
        assert_eq!(rx.pause(&foreign), Err(RecvError));
        assert_eq!(rx.with_lane_ids().try_recv(), Ok((own, 1)));
    });
}

#[test]
fn paused_lane_publication_resumes_without_lost_values() {
    model(|| {
        let (mut tx, mut rx) = channel(2);
        let lane = tx.lane();
        rx.pause(&lane).unwrap();
        let sender = thread::spawn(move || {
            tx.try_send(1).unwrap();
            tx.try_send(2).unwrap();
            tx
        });
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
        rx.resume(&lane).unwrap();
        let tx = sender.join().unwrap();
        assert_eq!(rx.try_recv_fair(), Ok(1));
        rx.pause(&lane).unwrap();
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
        assert_eq!(rx.try_recv_from(&lane), Ok(2));
        drop(tx);
        assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Disconnected));
    });
}

#[test]
fn paused_lane_close_racing_send_cannot_select_replacement() {
    model(|| {
        let (mut tx, mut rx) =
            fanring::mpsc::channel_with_policy::<_, fanring::teardown::Coordinated>(2);
        let lane = tx.lane();
        rx.pause(&lane).unwrap();
        let sender = thread::spawn(move || {
            let result = tx.try_send(1);
            assert!(matches!(
                result,
                Ok(()) | Err(TrySendError::Disconnected(1))
            ));
            tx
        });
        rx.close_lane(&lane).unwrap();
        let tx = sender.join().unwrap();
        assert!(tx.is_disconnected());
        let mut replacement = tx.try_register_bounded(1).unwrap();
        replacement.try_send(2).unwrap();
        assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Disconnected));
        assert_eq!(rx.resume(&lane), Err(RecvError));
        assert_eq!(rx.try_recv_fair(), Ok(2));
    });
}

#[test]
fn empty_paused_lane_racing_final_sender_drop_reports_disconnect() {
    model(|| {
        let (mut tx, mut rx) = channel(2);
        let lane = tx.lane();
        tx.try_send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        rx.pause(&lane).unwrap();
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
        let sender = thread::spawn(move || drop(tx));
        assert!(matches!(
            rx.try_recv_fair(),
            Err(TryRecvError::Empty | TryRecvError::Disconnected)
        ));
        sender.join().unwrap();
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Disconnected));
    });
}

#[cfg(feature = "async")]
mod asynchronous {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Wake, Waker};

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

macro_rules! lane_models {
    ($module:ident, $policy:ty) => {
        mod $module {
            use super::model;
            use fanring::mpsc::{
                RecvError, Sender, TryRecvError, TrySendError, channel_with_policy,
            };
            use loom::thread;

            fn full_paused(
                capacity: usize,
            ) -> (
                Sender<usize, $policy>,
                fanring::mpsc::Receiver<usize, $policy>,
            ) {
                let (mut tx, mut rx) = channel_with_policy(capacity);
                for value in 0..capacity {
                    tx.try_send(value).unwrap();
                }
                rx.pause(&tx.lane()).unwrap();
                (tx, rx)
            }

            #[test]
            fn close_racing_send_rejects_old_lane_after_reuse() {
                model(|| {
                    let (mut tx, mut rx) = channel_with_policy::<_, $policy>(2);
                    let old = tx.lane();
                    let sender = thread::spawn(move || {
                        assert!(matches!(
                            tx.try_send(1),
                            Ok(()) | Err(TrySendError::Disconnected(1))
                        ));
                        tx
                    });
                    rx.close_lane(&old).unwrap();
                    let mut tx = sender.join().unwrap();
                    assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));
                    assert_eq!(
                        tx.try_send_unsignaled(3),
                        Err(TrySendError::Disconnected(3))
                    );
                    let mut replacement = tx.try_register_with_capacity(4).unwrap();
                    replacement.try_send(4).unwrap();
                    assert_eq!(rx.close_lane(&old), Err(RecvError));
                    assert_eq!(rx.with_lane_ids().recv(), Ok((replacement.lane(), 4)));
                });
            }

            #[test]
            fn targeted_lwm_release_wakes_blocked_paused_sender() {
                model(|| {
                    let (mut tx, mut rx) = full_paused(4);
                    let lane = tx.lane();
                    let sender = thread::spawn(move || {
                        tx.send(4).unwrap();
                        tx
                    });
                    assert_eq!(rx.try_recv_from(&lane), Ok(0));
                    assert_eq!(rx.try_recv_from(&lane), Ok(1));
                    let _tx = sender.join().unwrap();
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                    for expected in 2..5 {
                        assert_eq!(rx.try_recv_from(&lane), Ok(expected));
                    }
                });
            }

            #[test]
            fn close_wakes_blocked_sender_without_closing_receiver() {
                model(|| {
                    let (mut tx, mut rx) = full_paused(1);
                    let lane = tx.lane();
                    let sender = thread::spawn(move || {
                        assert_eq!(tx.send(1), Err(fanring::mpsc::SendError(1)));
                        tx
                    });
                    rx.close_lane(&lane).unwrap();
                    let tx = sender.join().unwrap();
                    let mut replacement = tx.try_register_bounded(1).unwrap();
                    replacement.try_send(2).unwrap();
                    assert_eq!(rx.recv(), Ok(2));
                });
            }

            #[test]
            fn paused_full_sender_does_not_block_other_lanes() {
                model(|| {
                    let (mut slow, mut rx) = full_paused(1);
                    let lane = slow.lane();
                    let mut fast = slow.try_register_with_capacity(2).unwrap();
                    let blocked = thread::spawn(move || {
                        slow.send(1).unwrap();
                        slow
                    });
                    fast.try_send(9).unwrap();
                    assert_eq!(rx.with_lane_ids().recv(), Ok((fast.lane(), 9)));
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                    rx.resume(&lane).unwrap();
                    assert_eq!(rx.with_lane_ids().recv(), Ok((lane, 0)));
                    let _slow = blocked.join().unwrap();
                    assert_eq!(rx.with_lane_ids().recv(), Ok((lane, 1)));
                });
            }

            #[test]
            fn targeted_drain_racing_last_sender_drop_preserves_values() {
                model(|| {
                    let (mut tx, mut rx) = channel_with_policy::<_, $policy>(2);
                    let lane = tx.lane();
                    rx.pause(&lane).unwrap();
                    let sender = thread::spawn(move || {
                        tx.try_send(0).unwrap();
                        tx.try_send(1).unwrap();
                    });
                    let first = rx.try_recv_from(&lane);
                    assert!(matches!(first, Ok(0) | Err(TryRecvError::Empty)));
                    sender.join().unwrap();
                    if first.is_err() {
                        assert_eq!(rx.try_recv_from(&lane), Ok(0));
                    }
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                    assert_eq!(rx.try_recv_from(&lane), Ok(1));
                    assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Disconnected));
                    assert_eq!(rx.resume(&lane), Err(RecvError));
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
                });
            }

            #[test]
            fn old_sender_drop_cannot_pause_or_close_replacement() {
                model(|| {
                    let (tx, mut rx) = channel_with_policy::<usize, $policy>(1);
                    let old = tx.lane();
                    rx.pause(&old).unwrap();
                    rx.close_lane(&old).unwrap();
                    let sender = thread::spawn(move || {
                        let mut replacement = tx.try_register_with_capacity(2).unwrap();
                        replacement.try_send(1).unwrap();
                        drop(tx);
                        replacement
                    });
                    assert_eq!(rx.pause(&old), Err(RecvError));
                    assert_eq!(rx.resume(&old), Err(RecvError));
                    assert_eq!(rx.close_lane(&old), Err(RecvError));
                    let mut replacement = sender.join().unwrap();
                    assert_eq!(rx.with_lane_ids().recv(), Ok((replacement.lane(), 1)));
                    replacement.try_send(2).unwrap();
                    assert_eq!(rx.with_lane_ids().recv(), Ok((replacement.lane(), 2)));
                });
            }

            #[test]
            fn retired_signal_races_replacement_publication() {
                model(|| {
                    let (root, mut rx) = channel_with_policy::<_, $policy>(1);
                    let mut old_sender = root.try_register().unwrap();
                    let old = old_sender.lane();
                    let slot = old_sender.lane_id();
                    rx.pause(&old).unwrap();
                    let retired = thread::spawn(move || {
                        assert!(matches!(
                            old_sender.try_send(1),
                            Ok(()) | Err(TrySendError::Disconnected(1))
                        ));
                        old_sender
                    });
                    rx.close_lane(&old).unwrap();
                    let mut replacement = root.try_register_bounded(2).unwrap();
                    assert_eq!(replacement.lane_id(), slot);
                    let new = replacement.lane();
                    let publisher = thread::spawn(move || {
                        replacement.try_send(2).unwrap();
                        replacement
                    });
                    assert_eq!(rx.try_recv_from(&old), Err(TryRecvError::Disconnected));
                    assert_eq!(rx.with_lane_ids().recv(), Ok((new, 2)));
                    // Neither sender's drop can supply a missing publication wake.
                    let _retired = retired.join().unwrap();
                    let _replacement = publisher.join().unwrap();
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                });
            }

            #[test]
            fn close_racing_publication_drops_values_once() {
                use super::Counted;
                use fanring::teardown::Teardown;
                use loom::sync::{
                    Arc,
                    atomic::{AtomicUsize, Ordering},
                };
                model(|| {
                    let first = Arc::new(AtomicUsize::new(0));
                    let second = Arc::new(AtomicUsize::new(0));
                    let (mut tx, mut rx) = channel_with_policy::<_, $policy>(2);
                    tx.try_send(Counted(first.clone())).unwrap();
                    let lane = tx.lane();
                    rx.pause(&lane).unwrap();
                    let value = Counted(second.clone());
                    let sender = thread::spawn(move || {
                        let accepted = tx.try_send(value).is_ok();
                        (tx, accepted)
                    });
                    rx.close_lane(&lane).unwrap();
                    let (tx, accepted) = sender.join().unwrap();
                    assert_eq!(
                        first.load(Ordering::Relaxed),
                        usize::from(<$policy>::COORDINATED)
                    );
                    assert_eq!(
                        second.load(Ordering::Relaxed),
                        usize::from(<$policy>::COORDINATED || !accepted)
                    );
                    drop(tx);
                    assert_eq!(first.load(Ordering::Relaxed), 1);
                    assert_eq!(second.load(Ordering::Relaxed), 1);
                });
            }

            #[test]
            fn custom_capacity_registration_races_receiver_drop() {
                use super::Counted;
                use fanring::teardown::Teardown;
                use loom::sync::{
                    Arc,
                    atomic::{AtomicUsize, Ordering},
                };
                model(|| {
                    let drops = Arc::new(AtomicUsize::new(0));
                    let value = Counted(drops.clone());
                    let (tx, rx) = channel_with_policy::<_, $policy>(1);
                    let registrar = thread::spawn(move || {
                        let child = match tx.try_register_with_capacity(3) {
                            Ok(mut child) => {
                                assert_eq!(child.capacity(), 4);
                                drop(child.try_send(value));
                                Some(child)
                            }
                            Err(_) => {
                                drop(value);
                                None
                            }
                        };
                        (tx, child)
                    });
                    drop(rx);
                    let (tx, child) = registrar.join().unwrap();
                    assert!(tx.is_disconnected());
                    if let Some(child) = &child {
                        assert!(child.is_disconnected());
                    }
                    if <$policy>::COORDINATED {
                        assert_eq!(drops.load(Ordering::Relaxed), 1);
                    }
                    drop((tx, child));
                    assert_eq!(drops.load(Ordering::Relaxed), 1);
                });
            }

            #[test]
            fn paused_pages_do_not_hide_new_group_publication() {
                model(|| {
                    // Loom has two lanes per page and two pages per group.
                    let (mut root, mut rx) = channel_with_policy::<_, $policy>(1);
                    root.try_send_unsignaled(9).unwrap();
                    let mut idle = vec![root];
                    for _ in 0..3 {
                        idle.push(idle[0].try_register().unwrap());
                    }
                    for sender in &idle {
                        rx.pause(&sender.lane()).unwrap();
                    }
                    let mut live = idle[0].try_register_with_capacity(2).unwrap();
                    let lane = live.lane();
                    let sender = thread::spawn(move || {
                        live.try_send(7).unwrap();
                        live
                    });
                    assert_eq!(rx.with_lane_ids().recv(), Ok((lane, 7)));
                    let _live = sender.join().unwrap();
                    rx.poll_all_lanes();
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                    assert_eq!(rx.try_recv_from(&idle[0].lane()), Ok(9));
                });
            }

            #[cfg(feature = "async")]
            #[test]
            fn targeted_partial_release_races_capacity_registration() {
                use super::asynchronous::waker;
                use std::task::{Context, Poll};
                model(|| {
                    let (mut tx, mut rx) = full_paused(4);
                    let lane = tx.lane();
                    let (wakes, waker) = waker();
                    let mut cx = Context::from_waker(&waker);
                    let receiver = thread::spawn(move || {
                        assert_eq!(rx.try_recv_from(&lane), Ok(0));
                        rx.release_consumed();
                        rx
                    });
                    let result = tx.poll_ready(&mut cx);
                    let mut rx = receiver.join().unwrap();
                    if result.is_pending() {
                        assert!(wakes.count() > 0);
                    }
                    assert_eq!(tx.poll_ready(&mut cx), Poll::Ready(Ok(())));
                    tx.try_send(4).unwrap();
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
                    for expected in 1..5 {
                        assert_eq!(rx.try_recv_from(&lane), Ok(expected));
                    }
                });
            }

            #[cfg(feature = "async")]
            #[test]
            fn close_races_capacity_registration() {
                use super::asynchronous::waker;
                use std::task::{Context, Poll};
                model(|| {
                    let (mut tx, mut rx) = full_paused(1);
                    let lane = tx.lane();
                    let (wakes, waker) = waker();
                    let mut cx = Context::from_waker(&waker);
                    let receiver = thread::spawn(move || {
                        rx.close_lane(&lane).unwrap();
                        rx
                    });
                    let result = tx.poll_ready(&mut cx);
                    let _rx = receiver.join().unwrap();
                    if result.is_pending() {
                        assert!(wakes.count() > 0);
                    }
                    assert_eq!(
                        tx.poll_ready(&mut cx),
                        Poll::Ready(Err(fanring::mpsc::SendError(())))
                    );
                });
            }

            #[cfg(feature = "async")]
            #[test]
            fn tagged_receive_registration_races_live_publisher() {
                use super::asynchronous::waker;
                use std::task::{Context, Poll};
                model(|| {
                    let (mut tx, mut rx) = channel_with_policy::<_, $policy>(1);
                    let lane = tx.lane();
                    let (wakes, waker) = waker();
                    let mut cx = Context::from_waker(&waker);
                    let sender = thread::spawn(move || {
                        tx.try_send(7).unwrap();
                        tx
                    });
                    let result = rx.with_lane_ids().poll_recv(&mut cx);
                    let _tx = sender.join().unwrap();
                    if result.is_pending() {
                        assert!(wakes.count() > 0);
                        assert_eq!(
                            rx.with_lane_ids().poll_recv(&mut cx),
                            Poll::Ready(Ok((lane, 7)))
                        );
                    } else {
                        assert_eq!(result, Poll::Ready(Ok((lane, 7))));
                    }
                });
            }

            #[cfg(feature = "async")]
            #[test]
            fn resume_notifies_tagged_waiter_after_paused_publication() {
                use super::asynchronous::waker;
                use std::task::{Context, Poll};
                model(|| {
                    let (mut tx, mut rx) = channel_with_policy::<_, $policy>(1);
                    let lane = tx.lane();
                    rx.pause(&lane).unwrap();
                    let (wakes, waker) = waker();
                    let mut cx = Context::from_waker(&waker);
                    let sender = thread::spawn(move || {
                        tx.try_send(7).unwrap();
                        tx
                    });
                    assert_eq!(rx.with_lane_ids().poll_recv(&mut cx), Poll::Pending);
                    let _tx = sender.join().unwrap();
                    // Consume the publication wake without allowing a drain.
                    assert_eq!(rx.with_lane_ids().poll_recv(&mut cx), Poll::Pending);
                    let before = wakes.count();
                    rx.resume(&lane).unwrap();
                    assert!(wakes.count() > before);
                    assert_eq!(
                        rx.with_lane_ids().poll_recv(&mut cx),
                        Poll::Ready(Ok((lane, 7)))
                    );
                });
            }

            #[cfg(feature = "async")]
            #[test]
            fn tagged_wait_cancellation_preserves_paused_value_and_replaces_waker() {
                use super::asynchronous::waker;
                use std::future::Future;
                use std::task::{Context, Poll};
                model(|| {
                    let (mut tx, mut rx) = channel_with_policy::<_, $policy>(1);
                    let lane = tx.lane();
                    rx.pause(&lane).unwrap();
                    let (old_wakes, old_waker) = waker();
                    let (new_wakes, new_waker) = waker();
                    let mut old_cx = Context::from_waker(&old_waker);
                    {
                        let mut view = rx.with_lane_ids();
                        let mut future = std::pin::pin!(view.recv_async());
                        assert!(future.as_mut().poll(&mut old_cx).is_pending());
                    }
                    let old_count = old_wakes.count();
                    let sender = thread::spawn(move || {
                        tx.try_send(7).unwrap();
                        tx
                    });
                    let mut new_cx = Context::from_waker(&new_waker);
                    assert_eq!(rx.with_lane_ids().poll_recv(&mut new_cx), Poll::Pending);
                    let _tx = sender.join().unwrap();
                    assert_eq!(old_wakes.count(), old_count);
                    assert_eq!(rx.with_lane_ids().poll_recv(&mut new_cx), Poll::Pending);
                    let before = new_wakes.count();
                    rx.resume(&lane).unwrap();
                    assert!(new_wakes.count() > before);
                    assert_eq!(
                        rx.with_lane_ids().poll_recv(&mut new_cx),
                        Poll::Ready(Ok((lane, 7)))
                    );
                });
            }
        }
    };
}

lane_models!(deferred, fanring::teardown::Deferred);
lane_models!(coordinated, fanring::teardown::Coordinated);

#[test]
fn paused_lane_final_drop_publishes_deferred_value() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        let lane = tx.lane();
        rx.pause(&lane).unwrap();
        let sender = thread::spawn(move || {
            tx.try_send_deferred(7).unwrap();
        });
        let value = rx.try_recv_from(&lane);
        assert!(matches!(value, Ok(7) | Err(TryRecvError::Empty)));
        sender.join().unwrap();
        if value.is_err() {
            assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
            assert_eq!(rx.try_recv_from(&lane), Ok(7));
        }
        assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Disconnected));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    });
}
