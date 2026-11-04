//! Nonblocking bulk receives with `try_recv_batch_into`.

use crate::support::{
    Counted, LaneValue, MirroredLanes, MirroredReceives, count_while_senders_alive,
    drain_after_disconnect,
};
use fanring::mpsc::{Receiver, TryRecvError, TrySendError, channel_with_policy};
use fanring::teardown::{Coordinated, Deferred, Teardown};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
            follow_single_receive_schedule::<P>(limit);
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

/// Runs the mirrored schedule once, with every pass capped at `limit`.
fn follow_single_receive_schedule<P: Teardown>(limit: usize) {
    let (mut lanes, mut rx_a, mut rx_b) = MirroredLanes::<P>::open();
    let mut received = MirroredReceives::with_batch_capacity(limit);
    let rounds = if cfg!(miri) { 16 } else { 40 };
    for round in 0..rounds {
        let context = format!("limit {limit} round {round}");
        lanes.send_round(&context);
        if round == 10 {
            lanes.drop_lane(1);
        }
        if round == 14 {
            lanes.register_lane();
        }
        for _ in 0..2 {
            let single = try_recv_up_to(&mut rx_a, &mut received.single_batch, limit);
            rx_a.release_consumed();
            let bulk = count_while_senders_alive(
                rx_b.try_recv_batch_into(&mut received.bulk_batch, limit),
            );
            received.commit(single, bulk, &context);
        }
    }
    let sent = lanes.disconnect();
    let (mut sequence_a, mut sequence_b) = received.into_sequences();
    let expected = drain_after_disconnect(&mut rx_a, &mut sequence_a);
    assert_eq!(
        rx_b.try_recv_batch_into(&mut sequence_b, usize::MAX),
        expected,
        "limit {limit}"
    );
    assert_eq!(sequence_a, sequence_b, "limit {limit}");
    assert_eq!(sequence_a.len(), sent, "limit {limit}");
    assert_eq!(
        rx_b.try_recv_batch_into(&mut sequence_b, 1),
        Err(TryRecvError::Disconnected)
    );
    assert_eq!(rx_a.try_recv(), Err(TryRecvError::Disconnected));
}

/// Receives up to `limit` values, one `try_recv` at a time.
fn try_recv_up_to<P: Teardown>(
    rx: &mut Receiver<LaneValue, P>,
    batch: &mut Vec<LaneValue>,
    limit: usize,
) -> usize {
    let mut single = 0;
    while single < limit {
        match rx.try_recv() {
            Ok(value) => {
                batch.push(value);
                single += 1;
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => panic!("senders alive"),
        }
    }
    single
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
