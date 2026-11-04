//! Ready lane requeue: busy lanes yield their turn, full ready pages
//! requeue, and requeued lanes reuse slots.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fanring::mpmc::{Receiver, RecvTimeoutError, TryRecvError, channel};

use crate::support::DISCONNECT_WAIT_TIMEOUT;

#[test]
fn busy_lane_cannot_hide_ready_lane() {
    let (mut busy, mut rx) = channel(128);
    let mut sparse = busy.try_clone().unwrap();
    for value in 0..128 {
        busy.try_send((0, value)).unwrap();
    }
    sparse.try_send((1, 0)).unwrap();

    assert!((0..=64).any(|_| rx.try_recv().unwrap().0 == 1));
}

#[test]
fn full_ready_page_requeues_without_overflow_or_loss() {
    #[cfg(miri)]
    const SENDERS: usize = 4;
    #[cfg(not(miri))]
    const SENDERS: usize = 64;
    #[cfg(miri)]
    const RECEIVER_COUNTS: &[usize] = &[1, 2];
    #[cfg(not(miri))]
    const RECEIVER_COUNTS: &[usize] = &[1, 2, 8, 64];
    const MESSAGES_PER_LANE: usize = 65;

    for &receiver_count in RECEIVER_COUNTS {
        let total = SENDERS * MESSAGES_PER_LANE;
        let seen = Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let (tx0, rx0) = channel(128);
        let mut senders = vec![tx0];
        for _ in 1..SENDERS {
            senders.push(senders[0].try_clone().unwrap());
        }
        let mut receivers = vec![rx0];
        for _ in 1..receiver_count {
            receivers.push(receivers[0].clone());
        }

        for (sender, tx) in senders.iter_mut().enumerate() {
            for sequence in 0..MESSAGES_PER_LANE {
                tx.try_send(sender * MESSAGES_PER_LANE + sequence).unwrap();
            }
        }
        drop(senders);

        std::thread::scope(|scope| {
            for mut rx in receivers {
                let seen = seen.clone();
                scope.spawn(move || {
                    loop {
                        match rx.recv_timeout(DISCONNECT_WAIT_TIMEOUT) {
                            Ok(value) => {
                                assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0)
                            }
                            Err(RecvTimeoutError::Disconnected) => break,
                            Err(RecvTimeoutError::Timeout) => {
                                panic!("ready lane became unreachable")
                            }
                        }
                    }
                });
            }
        });

        assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    }
}

#[test]
fn requeued_lanes_reuse_slots_across_receiver_contention() {
    #[cfg(miri)]
    const SENDERS: usize = 4;
    #[cfg(not(miri))]
    const SENDERS: usize = 64;
    #[cfg(miri)]
    const RECEIVERS: usize = 2;
    #[cfg(not(miri))]
    const RECEIVERS: usize = 8;
    #[cfg(miri)]
    const ROUNDS: usize = 2;
    #[cfg(not(miri))]
    const ROUNDS: usize = 8;
    const MESSAGES_PER_LANE: usize = 65;

    let (mut root, rx0) = channel(128);
    let mut receivers = vec![rx0];
    for _ in 1..RECEIVERS {
        receivers.push(receivers[0].clone());
    }
    let mut expected_slots = None;

    for round in 0..ROUNDS {
        let mut dynamic = (1..SENDERS)
            .map(|_| root.try_clone().unwrap())
            .collect::<Vec<_>>();
        let mut slots = dynamic
            .iter()
            .map(fanring::mpmc::Sender::lane_id)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        if let Some(expected) = &expected_slots {
            assert_eq!(&slots, expected);
        } else {
            expected_slots = Some(slots);
        }

        for sequence in 0..MESSAGES_PER_LANE {
            root.try_send(sequence).unwrap();
        }
        for (sender, tx) in dynamic.iter_mut().enumerate() {
            for sequence in 0..MESSAGES_PER_LANE {
                tx.try_send((sender + 1) * MESSAGES_PER_LANE + sequence)
                    .unwrap();
            }
        }
        drop(dynamic);

        receivers = receive_round_exactly_once(receivers, SENDERS * MESSAGES_PER_LANE, round);
    }
}

/// Polls every receiver on its own thread until `total` values have arrived,
/// checks that each arrived exactly once, and returns the receivers.
fn receive_round_exactly_once(
    receivers: Vec<Receiver<usize>>,
    total: usize,
    round: usize,
) -> Vec<Receiver<usize>> {
    let seen = Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let received = Arc::new(AtomicUsize::new(0));
    let receivers = std::thread::scope(|scope| {
        let handles = receivers
            .into_iter()
            .map(|mut rx| {
                let seen = seen.clone();
                let received = received.clone();
                scope.spawn(move || {
                    let deadline = std::time::Instant::now() + DISCONNECT_WAIT_TIMEOUT;
                    while received.load(Ordering::Acquire) != total {
                        match rx.try_recv() {
                            Ok(value) => {
                                assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                                received.fetch_add(1, Ordering::Release);
                            }
                            Err(TryRecvError::Empty) => {
                                assert!(
                                    std::time::Instant::now() < deadline,
                                    "requeued lane became unreachable in round {round}"
                                );
                                std::thread::yield_now();
                            }
                            Err(TryRecvError::Disconnected) => {
                                panic!("root sender remains connected")
                            }
                        }
                    }
                    rx
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    assert_eq!(received.load(Ordering::Relaxed), total);
    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    receivers
}
