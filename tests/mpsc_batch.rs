use fanring::mpsc::{RecvError, TryRecvError, TrySendError, channel_with_policy};
use fanring::teardown::{Coordinated, Deferred, Teardown};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[test]
fn release_consumed_preserves_unread_values_and_is_idempotent() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        rx.release_consumed();
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));
        assert_eq!(rx.try_recv(), Ok(0));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));

        rx.release_consumed();
        rx.release_consumed();
        tx.try_send(4).unwrap();
        tx.try_send(5).unwrap();
        assert_eq!(tx.try_send(6), Err(TrySendError::Full(6)));
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), [2, 3, 4, 5]);
        rx.release_consumed();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn release_consumed_covers_all_lanes_without_resetting_rotation() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<_, P>(128);
        let mut tx1 = tx0.try_clone().unwrap();
        for value in 0..128 {
            tx0.try_send((0, value)).unwrap();
            tx1.try_send((1, value)).unwrap();
        }
        assert_eq!(rx.recv(), Ok((0, 0)));
        rx.release_consumed();
        tx0.try_send((0, 128)).unwrap();

        // Offset lane 0's release boundary from its rotation boundary so both
        // lanes hold partial release batches when lane 1 becomes active.
        for value in 1..64 {
            assert_eq!(rx.try_recv(), Ok((0, value)));
        }
        assert_eq!(rx.recv(), Ok((1, 0)));
        assert_eq!(tx0.try_send((0, 129)), Err(TrySendError::Full((0, 129))));
        assert_eq!(tx1.try_send((1, 128)), Err(TrySendError::Full((1, 128))));
        rx.release_consumed();
        for value in 129..192 {
            tx0.try_send((0, value)).unwrap();
        }
        tx1.try_send((1, 128)).unwrap();
        assert_eq!(tx0.try_send((0, 192)), Err(TrySendError::Full((0, 192))));
        assert_eq!(tx1.try_send((1, 129)), Err(TrySendError::Full((1, 129))));

        drop((tx0, tx1));
        let mut next = [64, 1];
        for (lane, value) in rx {
            assert_eq!(value, next[lane]);
            next[lane] += 1;
        }
        assert_eq!(next, [192, 129]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_appends_bounded_values_and_releases_earlier_receives() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(rx.recv(), Ok(0));
        let mut output = Vec::with_capacity(3);
        output.push(-1);
        let allocation = output.as_ptr();
        let capacity = output.capacity();
        assert_eq!(rx.recv_batch_into(&mut output, 1), Ok(1));
        assert_eq!(output, [-1, 1]);
        assert_eq!(output.as_ptr(), allocation);
        assert_eq!(output.capacity(), capacity);
        tx.try_send(4).unwrap();
        tx.try_send(5).unwrap();
        assert_eq!(tx.try_send(6), Err(TrySendError::Full(6)));

        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 2), Ok(2));
        assert_eq!(output, [2, 3]);
        assert_eq!(output.as_ptr(), allocation);
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 3), Ok(2));
        assert_eq!(output, [4, 5]);
        assert_eq!(output.capacity(), capacity);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_waits_only_for_first_value() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut output = Vec::with_capacity(4);
            started_tx.send(()).unwrap();
            let result = rx.recv_batch_into(&mut output, 4);
            done_tx.send((result, output)).unwrap();
        });
        started_rx.recv().unwrap();
        tx.send(7).unwrap();
        // Keep the sender connected until the batch returns. Disconnect on
        // failure as well, so an implementation waiting for more can exit.
        let result = done_rx.recv_timeout(Duration::from_secs(5));
        drop(tx);
        receiver.join().unwrap();
        assert_eq!(result.unwrap(), (Ok(1), vec![7]));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_zero_limit_releases_without_receiving() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        let mut output = vec![9];
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(rx.recv(), Ok(0));
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        assert_eq!(output, [9]);
        tx.try_send(4).unwrap();
        drop(tx);
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        // A large limit must not cause an up-front allocation for that limit.
        assert_eq!(rx.recv_batch_into(&mut output, usize::MAX), Ok(4));
        assert_eq!(output, [9, 1, 2, 3, 4]);
        assert_eq!(rx.recv_batch_into(&mut output, 1), Err(RecvError));
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        assert_eq!(output, [9, 1, 2, 3, 4]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_disconnect_preserves_partial_batch_and_owned_values() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        tx.send(Box::new(1)).unwrap();
        tx.send(Box::new(2)).unwrap();
        drop(tx);
        let mut output = vec![Box::new(0)];
        assert_eq!(rx.recv_batch_into(&mut output, 4), Ok(2));
        assert_eq!(
            output.iter().map(|value| **value).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(rx.recv_batch_into(&mut output, 4), Err(RecvError));
        drop(rx);
        assert_eq!(
            output.iter().map(|value| **value).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_reclaims_exact_capacity_across_wraparound_and_release_boundaries() {
    fn check<P: Teardown>() {
        for requested_capacity in [1, 3, 64, 65] {
            let (mut tx, mut rx) = channel_with_policy::<_, P>(requested_capacity);
            let capacity = rx.capacity_per_sender();
            for value in 0..capacity {
                tx.try_send(value).unwrap();
            }
            let mut sent = capacity;
            let mut received = 0;
            let mut output = Vec::with_capacity(capacity);
            // Cross the 64-item release boundary and repeatedly reuse physical
            // slots, including capacities rounded upward by yring.
            for limit in [1, 3, 63, 64, 65, 129] {
                let count = capacity.min(limit);
                assert_eq!(rx.recv_batch_into(&mut output, limit), Ok(count));
                assert_eq!(output, (received..received + count).collect::<Vec<_>>());
                received += count;
                output.clear();

                for value in sent..sent + count {
                    tx.try_send(value).unwrap();
                }
                sent += count;
                assert_eq!(tx.try_send(sent), Err(TrySendError::Full(sent)));
            }
            drop(tx);
            assert_eq!(rx.recv_batch_into(&mut output, capacity + 1), Ok(capacity));
            assert_eq!(output, (received..sent).collect::<Vec<_>>());
            assert_eq!(rx.recv_batch_into(&mut output, 1), Err(RecvError));
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_retires_and_reuses_lane_without_releasing_new_unread_slots() {
    fn check<P: Teardown>() {
        let (root, mut rx) = channel_with_policy::<_, P>(4);
        let mut old = root.try_clone().unwrap();
        let slot = old.lane_id();
        for value in 0..4 {
            old.try_send(value).unwrap();
        }
        let mut output = Vec::with_capacity(4);
        assert_eq!(rx.recv_batch_into(&mut output, 2), Ok(2));
        assert_eq!(output, [0, 1]);
        drop(old);
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 4), Ok(2));
        assert_eq!(output, [2, 3]);
        // The empty poll retires this lane, leaving a hole in the receiver's
        // lane table. Releasing again must tolerate that hole.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        rx.release_consumed();

        let mut new = root.try_clone().unwrap();
        assert_eq!(new.lane_id(), slot);
        for value in 4..8 {
            new.try_send(value).unwrap();
        }
        // The reused slot is registered but has not been imported or read yet.
        rx.release_consumed();
        assert_eq!(new.try_send(8), Err(TrySendError::Full(8)));
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 1), Ok(1));
        assert_eq!(output, [4]);
        new.try_send(8).unwrap();
        assert_eq!(new.try_send(9), Err(TrySendError::Full(9)));
        drop((root, new));
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 5), Ok(4));
        assert_eq!(output, [5, 6, 7, 8]);
        rx.release_consumed();
        assert_eq!(rx.recv_batch_into(&mut output, 1), Err(RecvError));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[derive(Debug)]
struct Counted {
    id: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        assert_eq!(self.drops[self.id].fetch_add(1, Ordering::Relaxed), 0);
    }
}

#[test]
fn released_slot_reuse_and_receiver_drop_preserve_output_ownership() {
    fn check<P: Teardown>() {
        for explicit_release in [false, true] {
            let drops = Arc::new((0..6).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            let value = |id| Counted {
                id,
                drops: drops.clone(),
            };
            let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
            for id in 0..4 {
                tx.try_send(value(id)).unwrap();
            }
            let mut output = Vec::with_capacity(2);
            if explicit_release {
                output.push(rx.recv().unwrap());
                output.push(rx.recv().unwrap());
                rx.release_consumed();
            } else {
                assert_eq!(rx.recv_batch_into(&mut output, 2), Ok(2));
            }
            tx.try_send(value(4)).unwrap();
            tx.try_send(value(5)).unwrap();
            assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 0));

            drop(rx);
            for count in &drops[2..] {
                assert_eq!(count.load(Ordering::Relaxed), usize::from(P::COORDINATED));
            }
            drop(tx);
            for count in &drops[2..] {
                assert_eq!(count.load(Ordering::Relaxed), 1);
            }
            assert_eq!(
                output.iter().map(|value| value.id).collect::<Vec<_>>(),
                [0, 1]
            );
            assert_eq!(drops[0].load(Ordering::Relaxed), 0);
            assert_eq!(drops[1].load(Ordering::Relaxed), 0);
            drop(output);
            assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 1));
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn deferred_sends_publish_on_flush_full_and_drop() {
    let (mut tx, mut rx) = fanring::mpsc::channel(2);
    tx.try_send_deferred(0).unwrap();
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    tx.flush();
    assert_eq!(rx.try_recv_fair(), Ok(0));
    rx.release_consumed();
    tx.try_send_deferred(1).unwrap();
    tx.try_send_deferred(2).unwrap();
    assert_eq!(tx.try_send_deferred(3), Err(TrySendError::Full(3)));
    assert_eq!(rx.try_recv_fair(), Ok(1));
    assert_eq!(rx.try_recv_fair(), Ok(2));
    rx.release_consumed();
    tx.try_send_deferred(3).unwrap();
    drop(tx);
    assert_eq!(rx.try_recv_fair(), Ok(3));
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Disconnected));
}

#[test]
fn deferred_values_remain_owned_across_receiver_drop() {
    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, rx) = fanring::mpsc::channel(2);
    assert!(tx.try_send_deferred(Tracked(drops.clone())).is_ok());
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let returned = tx.try_send_deferred(Tracked(drops.clone())).err().unwrap();
    assert!(matches!(returned, TrySendError::Disconnected(_)));
    drop(returned);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(tx);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn try_recv_batch_reports_empty_disconnect_and_releases_slots() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        let mut output = vec![9];
        assert_eq!(
            rx.try_recv_batch_into(&mut output, 4),
            Err(TryRecvError::Empty)
        );
        assert_eq!(rx.try_recv_batch_into(&mut output, 0), Ok(0));
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));
        assert_eq!(rx.try_recv_batch_into(&mut output, 2), Ok(2));
        assert_eq!(output, [9, 0, 1]);
        // Consumed slots are published before returning.
        tx.try_send(4).unwrap();
        tx.try_send(5).unwrap();
        assert_eq!(tx.try_send(6), Err(TrySendError::Full(6)));
        assert_eq!(rx.try_recv_batch_into(&mut output, 0), Ok(0));
        assert_eq!(rx.try_recv_batch_into(&mut output, 10), Ok(4));
        assert_eq!(output, [9, 0, 1, 2, 3, 4, 5]);
        assert_eq!(
            rx.try_recv_batch_into(&mut output, 1),
            Err(TryRecvError::Empty)
        );
        tx.try_send(6).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv_batch_into(&mut output, 5), Ok(1));
        assert_eq!(
            rx.try_recv_batch_into(&mut output, 1),
            Err(TryRecvError::Disconnected)
        );
        assert_eq!(rx.try_recv_batch_into(&mut output, 0), Ok(0));
        assert_eq!(output, [9, 0, 1, 2, 3, 4, 5, 6]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn try_recv_batch_follows_single_receive_schedule() {
    // Drive two identical channels with the same sends, lane drops, and
    // registrations. One side receives with repeated try_recv, the other with
    // bulk receives of the same limit. The delivered sequences must match
    // exactly, including cross-lane rotation, readiness polls, and slot reuse.
    fn check<P: Teardown>() {
        for limit in [1, 3, 64, 65, 100, 1024] {
            let (root_a, mut rx_a) = channel_with_policy::<_, P>(128);
            let (root_b, mut rx_b) = channel_with_policy::<_, P>(128);
            let mut txs_a = vec![Some(root_a)];
            let mut txs_b = vec![Some(root_b)];
            for _ in 1..4 {
                let a = txs_a[0].as_ref().unwrap().try_clone().unwrap();
                let b = txs_b[0].as_ref().unwrap().try_clone().unwrap();
                txs_a.push(Some(a));
                txs_b.push(Some(b));
            }
            let mut next = [0usize; 5];
            let mut sequence_a = Vec::new();
            let mut sequence_b = Vec::new();
            let mut batch_a = Vec::with_capacity(limit);
            let mut batch_b = Vec::with_capacity(limit);
            let rounds = if cfg!(miri) { 16 } else { 40 };
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            for round in 0..rounds {
                for lane in 0..txs_a.len() {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    let count = ((state >> 33) % 90) as usize;
                    let (Some(a), Some(b)) = (txs_a[lane].as_mut(), txs_b[lane].as_mut()) else {
                        continue;
                    };
                    for _ in 0..count {
                        let value = (lane, next[lane]);
                        let sent_a = a.try_send(value).is_ok();
                        let sent_b = b.try_send(value).is_ok();
                        assert_eq!(sent_a, sent_b, "limit {limit} round {round}");
                        if !sent_a {
                            break;
                        }
                        next[lane] += 1;
                    }
                }
                if round == 10 {
                    txs_a[1] = None;
                    txs_b[1] = None;
                }
                if round == 14 {
                    let a = txs_a[0].as_ref().unwrap().try_clone().unwrap();
                    let b = txs_b[0].as_ref().unwrap().try_clone().unwrap();
                    assert_eq!(a.lane_id(), b.lane_id());
                    txs_a.push(Some(a));
                    txs_b.push(Some(b));
                }
                for _ in 0..2 {
                    let mut single = 0;
                    while single < limit {
                        match rx_a.try_recv() {
                            Ok(value) => {
                                batch_a.push(value);
                                single += 1;
                            }
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => panic!("senders alive"),
                        }
                    }
                    rx_a.release_consumed();
                    let bulk = match rx_b.try_recv_batch_into(&mut batch_b, limit) {
                        Ok(count) => count,
                        Err(TryRecvError::Empty) => 0,
                        Err(TryRecvError::Disconnected) => panic!("senders alive"),
                    };
                    assert_eq!(single, bulk, "limit {limit} round {round}");
                    assert_eq!(batch_a, batch_b, "limit {limit} round {round}");
                    sequence_a.append(&mut batch_a);
                    sequence_b.append(&mut batch_b);
                }
            }
            drop(txs_a);
            drop(txs_b);
            let before = sequence_a.len();
            sequence_a.extend(rx_a.try_iter());
            let remaining = sequence_a.len() - before;
            assert_eq!(
                rx_b.try_recv_batch_into(&mut sequence_b, usize::MAX),
                if remaining == 0 {
                    Err(TryRecvError::Disconnected)
                } else {
                    Ok(remaining)
                },
                "limit {limit}"
            );
            assert_eq!(sequence_a, sequence_b, "limit {limit}");
            assert_eq!(
                sequence_a.len(),
                next.iter().sum::<usize>(),
                "limit {limit}"
            );
            assert_eq!(
                rx_b.try_recv_batch_into(&mut sequence_b, 1),
                Err(TryRecvError::Disconnected)
            );
            assert_eq!(rx_a.try_recv(), Err(TryRecvError::Disconnected));
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn try_recv_batch_reclaims_exact_capacity_across_wraparound_and_release_boundaries() {
    fn check<P: Teardown>() {
        for requested_capacity in [1, 3, 64, 65, 200] {
            let (mut tx, mut rx) = channel_with_policy::<_, P>(requested_capacity);
            let capacity = rx.capacity_per_sender();
            for value in 0..capacity {
                tx.try_send(value).unwrap();
            }
            let mut sent = capacity;
            let mut received = 0;
            let mut output = Vec::with_capacity(capacity);
            for limit in [1, 3, 63, 64, 65, 129, 1024] {
                let count = capacity.min(limit);
                assert_eq!(rx.try_recv_batch_into(&mut output, limit), Ok(count));
                assert_eq!(output, (received..received + count).collect::<Vec<_>>());
                received += count;
                output.clear();

                for value in sent..sent + count {
                    tx.try_send(value).unwrap();
                }
                sent += count;
                assert_eq!(tx.try_send(sent), Err(TrySendError::Full(sent)));
            }
            drop(tx);
            assert_eq!(
                rx.try_recv_batch_into(&mut output, capacity + 1),
                Ok(capacity)
            );
            assert_eq!(output, (received..sent).collect::<Vec<_>>());
            assert_eq!(
                rx.try_recv_batch_into(&mut output, 1),
                Err(TryRecvError::Disconnected)
            );
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn bulk_receives_move_owned_values_exactly_once() {
    fn check<P: Teardown>() {
        let drops = Arc::new((0..8).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let value = |id| Counted {
            id,
            drops: drops.clone(),
        };
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        for id in 0..4 {
            tx.try_send(value(id)).unwrap();
        }
        let mut output = Vec::with_capacity(1);
        assert_eq!(rx.try_recv_batch_into(&mut output, 3), Ok(3));
        for id in 4..7 {
            tx.try_send(value(id)).unwrap();
        }
        assert_eq!(rx.recv_batch_into(&mut output, 8), Ok(4));
        tx.try_send(value(7)).unwrap();
        assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 0));
        assert_eq!(
            output.iter().map(|value| value.id).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 5, 6]
        );
        drop(rx);
        drop(tx);
        assert_eq!(drops[7].load(Ordering::Relaxed), 1);
        assert!(
            drops[..7]
                .iter()
                .all(|count| count.load(Ordering::Relaxed) == 0)
        );
        drop(output);
        assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn bulk_receives_preserve_per_sender_fifo_across_threads() {
    fn check<P: Teardown>() {
        let per_sender = if cfg!(miri) { 300 } else { 20_000 };
        let (root, mut rx) = channel_with_policy::<_, P>(64);
        let threads: Vec<_> = (0..4)
            .map(|sender| {
                let mut tx = root.try_clone().unwrap();
                std::thread::spawn(move || {
                    for sequence in 0..per_sender {
                        tx.send((sender, sequence)).unwrap();
                    }
                })
            })
            .collect();
        drop(root);
        let mut next = [0usize; 4];
        let mut batch = Vec::with_capacity(1024);
        let mut total = 0;
        loop {
            match rx.try_recv_batch_into(&mut batch, 1024) {
                Ok(count) => {
                    assert!((1..=1024).contains(&count));
                    total += count;
                    for (sender, sequence) in batch.drain(..) {
                        assert_eq!(sequence, next[sender]);
                        next[sender] += 1;
                    }
                }
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        assert_eq!(total, 4 * per_sender);
        assert_eq!(next, [per_sender; 4]);
        for thread in threads {
            thread.join().unwrap();
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn try_recv_batch_while_stops_at_first_rejected_value_and_keeps_lane_turn() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<_, P>(4);
        let mut tx1 = tx0.try_clone().unwrap();
        for value in 0..4 {
            tx0.try_send((0, value)).unwrap();
            tx1.try_send((1, value)).unwrap();
        }
        let mut batch = Vec::with_capacity(16);
        assert_eq!(
            rx.try_recv_batch_into_while(&mut batch, 8, |(_, value)| *value < 2),
            Ok(2)
        );
        assert_eq!(batch, [(0, 0), (0, 1)]);
        // Consumed slots are released before returning.
        tx0.try_send((0, 4)).unwrap();
        tx0.try_send((0, 5)).unwrap();
        assert_eq!(tx0.try_send((0, 6)), Err(TrySendError::Full((0, 6))));
        // The rejected value stays at the front of its lane and the lane keeps
        // its turn, so the plain bulk receive continues there.
        assert_eq!(rx.try_recv_batch_into(&mut batch, 8), Ok(8));
        assert_eq!(
            batch,
            [
                (0, 0),
                (0, 1),
                (0, 2),
                (0, 3),
                (0, 4),
                (0, 5),
                (1, 0),
                (1, 1),
                (1, 2),
                (1, 3)
            ]
        );
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn try_recv_batch_while_reports_rejection_empty_and_disconnect() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(2);
        let mut batch = Vec::new();
        assert_eq!(
            rx.try_recv_batch_into_while(&mut batch, 1, |_| true),
            Err(TryRecvError::Empty)
        );
        tx.try_send(7).unwrap();
        assert_eq!(
            rx.try_recv_batch_into_while(&mut batch, 1, |_| false),
            Ok(0)
        );
        assert_eq!(
            rx.try_recv_batch_into_while(&mut batch, 0, |_| panic!("zero limit admits nothing")),
            Ok(0)
        );
        assert_eq!(rx.try_recv_batch_into_while(&mut batch, 1, |_| true), Ok(1));
        assert_eq!(batch, [7]);
        assert_eq!(
            rx.try_recv_batch_into_while(&mut batch, 1, |_| true),
            Err(TryRecvError::Empty)
        );
        tx.try_send(8).unwrap();
        drop(tx);
        assert_eq!(
            rx.try_recv_batch_into_while(&mut batch, 1, |_| false),
            Ok(0)
        );
        assert_eq!(rx.try_recv_batch_into_while(&mut batch, 1, |_| true), Ok(1));
        assert_eq!(
            rx.try_recv_batch_into_while(&mut batch, 1, |_| true),
            Err(TryRecvError::Disconnected)
        );
        assert_eq!(rx.try_recv_batch_into_while(&mut batch, 0, |_| true), Ok(0));
        assert_eq!(batch, [7, 8]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn try_recv_batch_while_follows_single_receive_schedule_under_budget() {
    // Same sends into two identical channels. Side A receives one value at a
    // time and stops once a cost budget is spent. Side B uses the bulk drain
    // with that budget as its admission predicate. Both accept exactly the
    // same prefix, so the delivered sequences must match, including rotation,
    // readiness polls, and slot reuse.
    fn cost(value: &(usize, usize)) -> usize {
        1 + value.1 % 7
    }
    fn check<P: Teardown>() {
        for budget in [1usize, 5, 64, 300, 5_000] {
            let (root_a, mut rx_a) = channel_with_policy::<_, P>(128);
            let (root_b, mut rx_b) = channel_with_policy::<_, P>(128);
            let mut txs_a = vec![root_a];
            let mut txs_b = vec![root_b];
            for _ in 1..4 {
                let a = txs_a[0].try_clone().unwrap();
                let b = txs_b[0].try_clone().unwrap();
                txs_a.push(a);
                txs_b.push(b);
            }
            let mut next = [0usize; 4];
            let mut batch_a = Vec::new();
            let mut batch_b = Vec::new();
            let mut sequence_a = Vec::new();
            let mut sequence_b = Vec::new();
            let rounds = if cfg!(miri) { 12 } else { 40 };
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            for round in 0..rounds {
                for lane in 0..4 {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    let count = ((state >> 33) % 90) as usize;
                    for _ in 0..count {
                        let value = (lane, next[lane]);
                        let sent_a = txs_a[lane].try_send(value).is_ok();
                        let sent_b = txs_b[lane].try_send(value).is_ok();
                        assert_eq!(sent_a, sent_b, "budget {budget} round {round}");
                        if !sent_a {
                            break;
                        }
                        next[lane] += 1;
                    }
                }
                for _ in 0..2 {
                    let mut left = budget;
                    let mut single = 0;
                    while left != 0 {
                        match rx_a.try_recv() {
                            Ok(value) => {
                                left = left.saturating_sub(cost(&value));
                                batch_a.push(value);
                                single += 1;
                            }
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => panic!("senders alive"),
                        }
                    }
                    rx_a.release_consumed();
                    let mut left = budget;
                    let bulk =
                        match rx_b.try_recv_batch_into_while(&mut batch_b, usize::MAX, |value| {
                            if left == 0 {
                                return false;
                            }
                            left = left.saturating_sub(cost(value));
                            true
                        }) {
                            Ok(count) => count,
                            Err(TryRecvError::Empty) => 0,
                            Err(TryRecvError::Disconnected) => panic!("senders alive"),
                        };
                    assert_eq!(single, bulk, "budget {budget} round {round}");
                    assert_eq!(batch_a, batch_b, "budget {budget} round {round}");
                    sequence_a.append(&mut batch_a);
                    sequence_b.append(&mut batch_b);
                }
            }
            drop(txs_a);
            drop(txs_b);
            let before = sequence_a.len();
            sequence_a.extend(rx_a.try_iter());
            let remaining = sequence_a.len() - before;
            assert_eq!(
                rx_b.try_recv_batch_into_while(&mut sequence_b, usize::MAX, |_| true),
                if remaining == 0 {
                    Err(TryRecvError::Disconnected)
                } else {
                    Ok(remaining)
                },
                "budget {budget}"
            );
            assert_eq!(sequence_a, sequence_b, "budget {budget}");
            assert_eq!(
                sequence_a.len(),
                next.iter().sum::<usize>(),
                "budget {budget}"
            );
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn budgeted_bulk_receives_preserve_per_sender_fifo_across_threads() {
    fn check<P: Teardown>() {
        let per_sender = if cfg!(miri) { 300 } else { 20_000 };
        let (root, mut rx) = channel_with_policy::<_, P>(64);
        let threads: Vec<_> = (0..4)
            .map(|sender| {
                let mut tx = root.try_clone().unwrap();
                std::thread::spawn(move || {
                    for sequence in 0..per_sender {
                        tx.send((sender, sequence)).unwrap();
                    }
                })
            })
            .collect();
        drop(root);
        let mut next = [0usize; 4];
        let mut batch = Vec::with_capacity(1024);
        let mut total = 0;
        loop {
            let mut budget = 100usize;
            let result = rx.try_recv_batch_into_while(&mut batch, 1024, |_| {
                if budget == 0 {
                    return false;
                }
                budget -= 1;
                true
            });
            match result {
                Ok(count) => {
                    assert!((1..=100).contains(&count));
                    total += count;
                    for (sender, sequence) in batch.drain(..) {
                        assert_eq!(sequence, next[sender]);
                        next[sender] += 1;
                    }
                }
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        assert_eq!(total, 4 * per_sender);
        assert_eq!(next, [per_sender; 4]);
        for thread in threads {
            thread.join().unwrap();
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn is_full_follows_lane_capacity_and_slot_release() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        let mut other = tx.try_clone().unwrap();
        assert!(!tx.is_full());
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        assert!(tx.is_full());
        // Lanes are independent.
        assert!(!other.is_full());
        other.try_send(10).unwrap();
        // Consumed slots become visible to the sender once released.
        assert_eq!(rx.try_recv(), Ok(0));
        rx.release_consumed();
        assert!(!tx.is_full());
        tx.try_send(4).unwrap();
        assert!(tx.is_full());
        let mut batch = Vec::new();
        assert_eq!(rx.try_recv_batch_into(&mut batch, 8), Ok(5));
        assert!(!tx.is_full());
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn scan_receives_unsignaled_values_in_per_sender_order() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<_, P>(8);
        let mut tx1 = tx0.try_clone().unwrap();
        for value in 0..4 {
            tx0.try_send_unsignaled((0, value)).unwrap();
            tx1.try_send_unsignaled((1, value)).unwrap();
        }
        let mut batch = Vec::with_capacity(8);
        assert_eq!(rx.try_recv_scan_into_while(&mut batch, 8, |_| true), Ok(8));
        for sender in 0..2 {
            let values: Vec<_> = batch
                .iter()
                .filter(|(from, _)| *from == sender)
                .map(|(_, value)| *value)
                .collect();
            assert_eq!(values, [0, 1, 2, 3]);
        }
        assert_eq!(
            rx.try_recv_scan_into_while(&mut batch, 8, |_| true),
            Err(TryRecvError::Empty)
        );
        drop((tx0, tx1));
        assert_eq!(
            rx.try_recv_scan_into_while(&mut batch, 8, |_| true),
            Err(TryRecvError::Disconnected)
        );
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn scan_visits_lanes_outside_the_current_rotation() {
    let (mut tx0, mut rx) = channel_with_policy::<usize, Deferred>(128);
    let mut tx1 = tx0.try_clone().unwrap();
    for value in 0..100 {
        tx0.try_send(value).unwrap();
    }
    let mut batch = Vec::with_capacity(128);
    // A limited receive leaves the first lane queued with values left.
    assert_eq!(rx.try_recv_batch_into(&mut batch, 10), Ok(10));
    tx1.try_send_unsignaled(1000).unwrap();
    assert_eq!(
        rx.try_recv_scan_into_while(&mut batch, 128, |_| true),
        Ok(91)
    );
    assert!(batch.contains(&1000));
    assert_eq!(
        batch.iter().filter(|&&value| value < 100).count(),
        100,
        "every signaled value arrives once"
    );
}

#[test]
fn scan_admission_keeps_rejected_value_queued() {
    let (mut tx, mut rx) = channel_with_policy::<usize, Deferred>(8);
    for value in [10, 20, 30] {
        tx.try_send_unsignaled(value).unwrap();
    }
    let mut batch = Vec::with_capacity(8);
    let mut budget = 25;
    let admitted = rx.try_recv_scan_into_while(&mut batch, 8, |value| {
        if *value > budget {
            return false;
        }
        budget -= *value;
        true
    });
    assert_eq!(admitted, Ok(1));
    assert_eq!(rx.try_recv_scan_into_while(&mut batch, 8, |_| false), Ok(0));
    assert_eq!(rx.try_recv_scan_into_while(&mut batch, 8, |_| true), Ok(2));
    assert_eq!(batch, [10, 20, 30]);
}

#[test]
fn unsignaled_send_reports_full_and_disconnected() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<usize, P>(2);
        tx.try_send_unsignaled(0).unwrap();
        tx.try_send_unsignaled(1).unwrap();
        assert_eq!(tx.try_send_unsignaled(2), Err(TrySendError::Full(2)));
        let mut batch = Vec::with_capacity(2);
        assert_eq!(rx.try_recv_scan_into_while(&mut batch, 2, |_| true), Ok(2));
        tx.try_send_unsignaled(2).unwrap();
        drop(rx);
        assert_eq!(
            tx.try_send_unsignaled(3),
            Err(TrySendError::Disconnected(3))
        );
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn scan_then_signaled_send_keeps_each_lane_queued_once() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<u32, P>(16);
        tx0.try_send_unsignaled(1).unwrap();
        tx0.try_send_unsignaled(2).unwrap();
        let mut out = Vec::new();
        // The scan queues lane 0 and leaves it queued with a value left.
        assert_eq!(rx.try_recv_scan_into_while(&mut out, 1, |_| true), Ok(1));
        // A signaled send on the still-queued lane indexes it again.
        for value in 10..14 {
            tx0.try_send(value).unwrap();
        }
        let mut tx1 = tx0.try_clone().expect("receiver alive");
        for value in 20..23 {
            tx1.try_send(value).unwrap();
        }
        let mut order = Vec::new();
        while let Ok(value) = rx.try_recv_fair() {
            order.push(value);
        }
        assert_eq!(order, [2, 20, 10, 21, 11, 22, 12, 13]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn short_scan_without_rejection_observed_every_lane_empty() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<u32, P>(16);
        let mut tx1 = tx0.try_clone().expect("receiver alive");
        tx0.try_send_unsignaled(1).unwrap();
        tx1.try_send(20).unwrap();
        tx1.try_send_unsignaled(21).unwrap();
        let mut out = Vec::new();
        assert_eq!(rx.try_recv_scan_into_while(&mut out, 8, |_| true), Ok(3));
        out.sort_unstable();
        assert_eq!(out, [1, 20, 21]);
        // Nothing is left behind in any lane, signaled or not.
        assert_eq!(
            rx.try_recv_scan_into_while(&mut out, 8, |_| true),
            Err(TryRecvError::Empty)
        );
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn panicking_admission_leaves_the_window_queued() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<Box<u32>, P>(8);
        for value in 0..4 {
            tx.try_send(Box::new(value)).unwrap();
        }
        let mut out = Vec::new();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rx.try_recv_batch_into_while(&mut out, 4, |value| {
                assert!(**value != 2, "admission panic");
                true
            })
        }));
        assert!(panicked.is_err());
        assert!(
            out.is_empty(),
            "nothing moves before the window is admitted"
        );
        assert_eq!(rx.try_recv_batch_into(&mut out, 4), Ok(4));
        let values: Vec<u32> = out.iter().map(|value| **value).collect();
        assert_eq!(values, [0, 1, 2, 3]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}
