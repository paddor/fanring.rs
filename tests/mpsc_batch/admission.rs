//! Bulk receives with an admission predicate
//! (`try_recv_batch_into_while`).

use crate::support::{
    LaneValue, MirroredLanes, MirroredReceives, count_while_senders_alive, drain_after_disconnect,
};
use fanring::mpsc::{Receiver, TryRecvError, TrySendError, channel_with_policy};
use fanring::teardown::{Coordinated, Deferred, Teardown};

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
    fn check<P: Teardown>() {
        for budget in [1usize, 5, 64, 300, 5_000] {
            follow_single_receive_schedule_under_budget::<P>(budget);
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

/// Budget cost of one value in the budgeted schedule.
fn cost(value: &LaneValue) -> usize {
    1 + value.1 % 7
}

/// Runs the mirrored schedule once, with every pass spending `budget`.
fn follow_single_receive_schedule_under_budget<P: Teardown>(budget: usize) {
    let (mut lanes, mut rx_a, mut rx_b) = MirroredLanes::<P>::open();
    let mut received = MirroredReceives::with_batch_capacity(0);
    let rounds = if cfg!(miri) { 12 } else { 40 };
    for round in 0..rounds {
        let context = format!("budget {budget} round {round}");
        lanes.send_round(&context);
        for _ in 0..2 {
            let single = try_recv_within_budget(&mut rx_a, &mut received.single_batch, budget);
            rx_a.release_consumed();
            let bulk = try_recv_batch_within_budget(&mut rx_b, &mut received.bulk_batch, budget);
            received.commit(single, bulk, &context);
        }
    }
    let sent = lanes.disconnect();
    let (mut sequence_a, mut sequence_b) = received.into_sequences();
    let expected = drain_after_disconnect(&mut rx_a, &mut sequence_a);
    assert_eq!(
        rx_b.try_recv_batch_into_while(&mut sequence_b, usize::MAX, |_| true),
        expected,
        "budget {budget}"
    );
    assert_eq!(sequence_a, sequence_b, "budget {budget}");
    assert_eq!(sequence_a.len(), sent, "budget {budget}");
}

/// Receives one `try_recv` at a time until `budget` is spent.
fn try_recv_within_budget<P: Teardown>(
    rx: &mut Receiver<LaneValue, P>,
    batch: &mut Vec<LaneValue>,
    budget: usize,
) -> usize {
    let mut left = budget;
    let mut single = 0;
    while left != 0 {
        match rx.try_recv() {
            Ok(value) => {
                left = left.saturating_sub(cost(&value));
                batch.push(value);
                single += 1;
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => panic!("senders alive"),
        }
    }
    single
}

/// Receives in bulk, admitting values until `budget` is spent.
fn try_recv_batch_within_budget<P: Teardown>(
    rx: &mut Receiver<LaneValue, P>,
    batch: &mut Vec<LaneValue>,
    budget: usize,
) -> usize {
    let mut left = budget;
    count_while_senders_alive(rx.try_recv_batch_into_while(batch, usize::MAX, |value| {
        if left == 0 {
            return false;
        }
        left = left.saturating_sub(cost(value));
        true
    }))
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
