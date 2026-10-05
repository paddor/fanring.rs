#![cfg(not(loom))]

use fanring::mpsc::{RecvError, TryRecvError, TrySendError, channel};

#[test]
fn readiness_poll_finds_unsignaled_values_without_resuming_paused_lanes() {
    let (mut a, mut rx) = channel(4);
    let mut b = a.try_register().unwrap();
    a.try_send_unsignaled(1).unwrap();
    a.try_send_unsignaled(2).unwrap();
    b.try_send_unsignaled(11).unwrap();
    b.try_send_unsignaled(12).unwrap();
    rx.poll_all_lanes();
    assert_eq!(rx.with_lane_ids().try_recv_fair(), Ok((a.lane(), 1)));
    rx.pause(&a.lane()).unwrap();
    // Repeated polling queues each active source only once.
    rx.poll_all_lanes();
    rx.poll_all_lanes();
    assert_eq!(rx.with_lane_ids().try_recv_fair(), Ok((b.lane(), 11)));
    assert_eq!(rx.with_lane_ids().try_recv_fair(), Ok((b.lane(), 12)));
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    assert_eq!(rx.try_recv_from(&a.lane()), Ok(2));
    a.try_send_unsignaled(3).unwrap();
    rx.poll_all_lanes();
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    rx.resume(&a.lane()).unwrap();
    assert_eq!(rx.with_lane_ids().try_recv_fair(), Ok((a.lane(), 3)));
}

#[test]
fn mixed_capacity_lanes_keep_independent_bounds_and_pause_state() {
    fn check<P: fanring::teardown::Teardown>() {
        let (mut small, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
        let mut large = small.try_register_with_capacity(5).unwrap();
        let mut ordinary = large.try_register().unwrap();
        assert_eq!(small.capacity(), 2);
        assert_eq!(large.capacity(), 8);
        assert_eq!(ordinary.capacity(), 2);
        for value in 0..8 {
            large.try_send(value).unwrap();
        }
        assert!(matches!(large.try_send(8), Err(TrySendError::Full(8))));
        small.try_send(20).unwrap();
        ordinary.try_send(30).unwrap();
        rx.pause(&large.lane()).unwrap();
        assert_eq!(rx.try_recv_fair(), Ok(20));
        assert_eq!(rx.try_recv_fair(), Ok(30));
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
        assert_eq!(rx.try_recv_from(&large.lane()), Ok(0));
        rx.resume(&large.lane()).unwrap();
        for value in 1..8 {
            assert_eq!(rx.try_recv(), Ok(value));
        }
        rx.release_consumed();
        for value in 0..8 {
            large.try_send(value).unwrap();
        }
        assert!(large.is_full());
        let old = large.lane();
        rx.close_lane(&old).unwrap();
        let mut replacement = small.try_register_with_capacity(3).unwrap();
        assert_eq!(replacement.capacity(), 4);
        assert_ne!(replacement.lane(), old);
        assert_eq!(rx.try_recv_from(&old), Err(TryRecvError::Disconnected));
        replacement.try_send(99).unwrap();
        assert_eq!(rx.try_recv(), Ok(99));
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn custom_capacity_registration_validates_before_allocation_and_reports_disconnect() {
    let (sender, receiver) = channel::<u8>(2);
    for capacity in [0, fanring::mpsc::MAX_CAPACITY_PER_SENDER + 1] {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                sender.try_register_with_capacity(capacity)
            }))
            .is_err()
        );
        assert_eq!(sender.registered_lanes(), 1);
    }
    drop(receiver);
    assert!(matches!(
        sender.try_register_with_capacity(8),
        Err(fanring::mpsc::TryRegisterError::Disconnected)
    ));
}

#[test]
fn returned_ids_distinguish_identical_values_and_control_the_popped_lane() {
    let (mut tx0, mut rx) = channel(4);
    let mut tx1 = tx0.try_clone().unwrap();
    for _ in 0..3 {
        tx0.try_send(7).unwrap();
        tx1.try_send(7).unwrap();
    }
    let (lane, value) = rx.with_lane_ids().try_recv_fair().unwrap();
    assert_eq!(value, 7);
    let other = if lane == tx0.lane() {
        tx1.lane()
    } else {
        assert_eq!(lane, tx1.lane());
        tx0.lane()
    };
    rx.pause(&lane).unwrap();
    for _ in 0..3 {
        assert_eq!(rx.with_lane_ids().try_recv_fair(), Ok((other, 7)));
    }
    assert_eq!(rx.with_lane_ids().try_recv(), Err(TryRecvError::Empty));
    assert_eq!(rx.try_recv_from(&lane), Ok(7));
    assert_eq!(rx.with_lane_ids().try_recv(), Err(TryRecvError::Empty));
    rx.resume(&lane).unwrap();
    assert_eq!(rx.with_lane_ids().try_recv(), Ok((lane, 7)));
}

#[test]
fn tagged_and_plain_pops_share_cached_windows_and_maintenance_boundaries() {
    fn check<P: fanring::teardown::Teardown>() {
        let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(256);
        let lane = tx.lane();
        for value in 0..200 {
            tx.try_send(Box::new(value)).unwrap();
        }
        for expected in 0..200 {
            if expected % 3 == 0 {
                assert_eq!(rx.try_recv(), Ok(Box::new(expected)));
            } else {
                let result = if expected % 3 == 1 {
                    rx.with_lane_ids().try_recv()
                } else {
                    rx.with_lane_ids().try_recv_fair()
                };
                assert_eq!(result, Ok((lane, Box::new(expected))));
            }
        }
        drop(tx);
        assert_eq!(
            rx.with_lane_ids().try_recv(),
            Err(TryRecvError::Disconnected)
        );
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn returned_id_cannot_pause_replacement_after_its_source_disconnects() {
    let (mut tx, mut rx) = channel(2);
    tx.try_send(1).unwrap();
    let (old, _) = rx.with_lane_ids().try_recv().unwrap();
    rx.close_lane(&old).unwrap();
    let mut replacement = tx.try_register_bounded(1).unwrap();
    replacement.try_send(2).unwrap();
    let (new, value) = rx.with_lane_ids().try_recv_fair().unwrap();
    assert_eq!(new, replacement.lane());
    assert_ne!(new, old);
    assert_eq!(value, 2);
    assert_eq!(rx.pause(&old), Err(RecvError));
    assert_eq!(rx.resume(&old), Err(RecvError));
    assert_eq!(rx.try_recv_from(&old), Err(TryRecvError::Disconnected));
    rx.pause(&new).unwrap();
    replacement.try_send(3).unwrap();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(rx.try_recv_from(&new), Ok(3));
}

#[test]
fn copied_ids_from_dropped_channels_cannot_select_a_new_channel() {
    let ids = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    (0..32)
                        .map(|_| {
                            let (tx, _rx) = channel::<()>(1);
                            tx.lane()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        ids.len()
    );
    let (mut tx, mut rx) = channel(1);
    tx.try_send(9).unwrap();
    for old in ids {
        assert_eq!(rx.pause(&old), Err(RecvError));
        assert_eq!(rx.try_recv_from(&old), Err(TryRecvError::Disconnected));
    }
    assert_eq!(rx.with_lane_ids().try_recv(), Ok((tx.lane(), 9)));
}

#[test]
fn tagged_blocking_and_timed_receives_preserve_lane_and_disconnect() {
    use std::time::{Duration, Instant};
    let (mut tx, mut rx) = channel(2);
    let lane = tx.lane();
    assert_eq!(
        rx.with_lane_ids().recv_timeout(Duration::ZERO),
        Err(fanring::mpsc::RecvTimeoutError::Timeout)
    );
    let sender = std::thread::spawn(move || {
        for value in 0..6 {
            tx.send(value).unwrap();
        }
    });
    for value in 0..6 {
        let result = match value % 3 {
            0 => rx.with_lane_ids().recv(),
            1 => rx
                .with_lane_ids()
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| RecvError),
            _ => rx
                .with_lane_ids()
                .recv_deadline(Instant::now() + Duration::from_secs(5))
                .map_err(|_| RecvError),
        };
        assert_eq!(result, Ok((lane, value)));
        rx.release_consumed();
    }
    sender.join().unwrap();
    assert_eq!(rx.with_lane_ids().recv(), Err(RecvError));
}

#[cfg(feature = "async")]
#[test]
fn tagged_async_cancellation_leaves_value_and_source_available() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let (mut tx, mut rx) = channel(2);
    let lane = tx.lane();
    let mut cx = Context::from_waker(Waker::noop());
    let mut view = rx.with_lane_ids();
    {
        let mut pending = std::pin::pin!(view.recv_async());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
    }
    tx.try_send(1).unwrap();
    assert_eq!(
        futures_lite::future::block_on(view.recv_async()),
        Ok((lane, 1))
    );
    tx.try_send(2).unwrap();
    assert_eq!(view.poll_recv(&mut cx), Poll::Ready(Ok((lane, 2))));
    drop(tx);
    assert_eq!(view.poll_recv(&mut cx), Poll::Ready(Err(RecvError)));
}

#[test]
fn paused_lane_stays_full_while_other_lanes_progress() {
    let (mut slow, mut rx) = channel(4);
    let mut fast = slow.try_clone().unwrap();
    let lane = slow.lane();
    for value in 0..4 {
        slow.try_send(value).unwrap();
    }
    assert_eq!(rx.try_recv_fair(), Ok(0));
    rx.pause(&lane).unwrap();
    rx.release_consumed();
    slow.try_send(4).unwrap();
    for value in 100..110 {
        fast.try_send(value).unwrap();
        assert_eq!(rx.try_recv_fair(), Ok(value));
        rx.release_consumed();
    }
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(slow.try_send(5), Err(TrySendError::Full(5)));
    rx.resume(&lane).unwrap();
    // Resumption schedules drainage. Only consumption returns ring capacity.
    assert_eq!(slow.try_send(5), Err(TrySendError::Full(5)));
    for value in 1..5 {
        assert_eq!(rx.try_recv_fair(), Ok(value));
    }
}

#[test]
fn pause_covers_bulk_scan_iter_and_cached_fast_paths() {
    let (mut tx, mut rx) = channel(128);
    let lane = tx.lane();
    for value in 0..80 {
        tx.try_send(value).unwrap();
    }
    assert_eq!(rx.try_recv(), Ok(0));
    rx.pause(&lane).unwrap();
    let mut out = Vec::new();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    assert_eq!(
        rx.try_recv_batch_into(&mut out, 80),
        Err(TryRecvError::Empty)
    );
    assert_eq!(
        rx.try_recv_scan_into_while(&mut out, 80, |_| true),
        Err(TryRecvError::Empty)
    );
    assert_eq!(rx.try_iter().next(), None);
    assert!(out.is_empty());
    rx.resume(&lane).unwrap();
    assert_eq!(rx.try_recv_batch_into(&mut out, 80), Ok(79));
    assert_eq!(out, (1..80).collect::<Vec<_>>());
}

#[test]
fn targeting_paused_lane_preserves_fifo_and_pause() {
    let (mut tx, mut rx) = channel(4);
    let lane = tx.lane();
    rx.pause(&lane).unwrap();
    rx.pause(&lane).unwrap();
    assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Empty));
    tx.try_send(1).unwrap();
    tx.try_send_unsignaled(2).unwrap();
    assert_eq!(rx.try_recv_from(&lane), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(rx.try_recv_from(&lane), Ok(2));
    tx.try_send(3).unwrap();
    rx.resume(&lane).unwrap();
    rx.resume(&lane).unwrap();
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn targeted_unsignaled_reads_keep_remaining_values_in_rotation() {
    let (mut tx, mut rx) = channel(4);
    let lane = tx.lane();
    tx.try_send_unsignaled(1).unwrap();
    tx.try_send_unsignaled(2).unwrap();
    assert_eq!(rx.try_recv_from(&lane), Ok(1));
    assert_eq!(rx.try_recv_fair(), Ok(2));
    assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Empty));
    tx.try_send(3).unwrap();
    assert_eq!(rx.try_recv(), Ok(3));
}

#[test]
fn ids_reject_other_channels_and_reused_slots() {
    let (tx, mut rx) = channel::<usize>(2);
    let (foreign, _) = channel::<usize>(2);
    let old = tx.lane();
    assert_ne!(old, foreign.lane());
    assert_eq!(rx.pause(&foreign.lane()), Err(RecvError));
    assert_eq!(
        rx.try_recv_from(&foreign.lane()),
        Err(TryRecvError::Disconnected)
    );
    rx.close_lane(&old).unwrap();
    let mut replacement = tx.try_register_bounded(1).unwrap();
    assert_eq!(replacement.lane_id(), tx.lane_id());
    assert_ne!(replacement.lane(), old);
    assert_eq!(rx.pause(&old), Err(RecvError));
    assert_eq!(rx.resume(&old), Err(RecvError));
    assert_eq!(rx.close_lane(&old), Err(RecvError));
    assert_eq!(rx.try_recv_from(&old), Err(TryRecvError::Disconnected));
    replacement.try_send(7).unwrap();
    assert_eq!(rx.try_recv_fair(), Ok(7));
}

#[test]
fn targeted_disconnect_drains_and_retires_paused_lane() {
    let (registrar, mut rx) = channel(2);
    let mut tx = registrar.try_clone().unwrap();
    let lane = tx.lane();
    rx.pause(&lane).unwrap();
    tx.try_send(1).unwrap();
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(rx.try_recv_from(&lane), Ok(1));
    assert_eq!(rx.try_recv_from(&lane), Err(TryRecvError::Disconnected));
    let replacement = registrar.try_register_bounded(2).unwrap();
    assert_ne!(replacement.lane(), lane);
}

#[test]
fn empty_paused_lanes_do_not_hide_final_disconnect() {
    let (mut tx, mut rx) = channel(2);
    let lane = tx.lane();
    tx.try_send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    rx.pause(&lane).unwrap();
    // Consume any remaining readiness bit while the sender is alive.
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(tx);
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Disconnected));
}

#[test]
fn paused_lanes_retire_independently_before_final_disconnect() {
    let (mut tx0, mut rx) = channel(2);
    let mut tx1 = tx0.try_clone().unwrap();
    let lane0 = tx0.lane();
    let lane1 = tx1.lane();
    tx0.try_send(1).unwrap();
    tx1.try_send(2).unwrap();
    rx.pause(&lane0).unwrap();
    rx.pause(&lane1).unwrap();
    drop(tx0);
    drop(tx1);

    assert_eq!(rx.try_recv_from(&lane0), Ok(1));
    assert_eq!(rx.try_recv_from(&lane0), Err(TryRecvError::Disconnected));
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    rx.resume(&lane1).unwrap();
    assert_eq!(rx.try_recv_fair(), Ok(2));
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Disconnected));
}

#[test]
fn coordinated_close_reclaims_paused_values_with_sender_alive() {
    use fanring::teardown::Coordinated;
    use std::sync::Arc;
    let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, Coordinated>(2);
    let value = Arc::new(());
    let weak = Arc::downgrade(&value);
    tx.try_send(value).unwrap();
    let lane = tx.lane();
    rx.pause(&lane).unwrap();
    rx.close_lane(&lane).unwrap();
    assert!(weak.upgrade().is_none());
    assert!(tx.is_disconnected());
    assert!(matches!(
        tx.try_send(Arc::new(())),
        Err(TrySendError::Disconnected(_))
    ));
}
