use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use fanring::mpmc::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, channel,
};

#[cfg(miri)]
const MESSAGES_PER_SENDER: usize = 16;
#[cfg(not(miri))]
const MESSAGES_PER_SENDER: usize = 5_000;

#[cfg(miri)]
const RACE_ROUNDS: usize = 2;
#[cfg(not(miri))]
const RACE_ROUNDS: usize = 100;

#[cfg(miri)]
const PUBLICATION_RACE_ROUNDS: usize = 2;
#[cfg(not(miri))]
const PUBLICATION_RACE_ROUNDS: usize = 20_000;

#[cfg(miri)]
const CHURN_MESSAGES: usize = 128;
#[cfg(not(miri))]
const CHURN_MESSAGES: usize = 20_000;

#[cfg(miri)]
const STATE_MACHINE_STEPS: usize = 512;
#[cfg(not(miri))]
const STATE_MACHINE_STEPS: usize = 100_000;

#[cfg(miri)]
const DISCONNECT_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(not(miri))]
const DISCONNECT_WAIT_TIMEOUT: Duration = Duration::from_secs(1);

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct TrackedValue {
    id: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for TrackedValue {
    fn drop(&mut self) {
        self.drops[self.id].fetch_add(1, Ordering::Relaxed);
    }
}

fn next_random(state: &mut u64) -> usize {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state as usize
}

#[test]
fn many_senders_many_receivers_deliver_exactly_once() {
    let sender_count = 4;
    let receiver_count = 4;
    let total = sender_count * MESSAGES_PER_SENDER;
    let seen = Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let (tx0, rx0) = channel(64);
    let mut senders = vec![tx0];
    for _ in 1..sender_count {
        senders.push(senders[0].try_clone().unwrap());
    }
    let mut receivers = vec![rx0];
    for _ in 1..receiver_count {
        receivers.push(receivers[0].clone());
    }

    std::thread::scope(|scope| {
        for (sender_id, mut tx) in senders.into_iter().enumerate() {
            scope.spawn(move || {
                for seq in 0..MESSAGES_PER_SENDER {
                    tx.send(sender_id * MESSAGES_PER_SENDER + seq).unwrap();
                }
            });
        }
        for mut rx in receivers {
            let seen = seen.clone();
            scope.spawn(move || {
                while let Ok(value) = rx.recv() {
                    assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                }
            });
        }
    });

    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
}

#[test]
fn dynamic_sender_churn_during_receiver_contention_delivers_exactly_once() {
    #[cfg(miri)]
    const PRODUCERS: usize = 2;
    #[cfg(not(miri))]
    const PRODUCERS: usize = 8;
    #[cfg(miri)]
    const RECEIVERS: usize = 2;
    #[cfg(not(miri))]
    const RECEIVERS: usize = 8;
    #[cfg(miri)]
    const ROUNDS: usize = 2;
    #[cfg(not(miri))]
    const ROUNDS: usize = 100;
    const MESSAGES_PER_LANE: usize = 65;

    let total = PRODUCERS * ROUNDS * MESSAGES_PER_LANE;
    let seen = Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let (tx0, rx0) = channel(128);
    let mut parents = vec![tx0];
    for _ in 1..PRODUCERS {
        parents.push(parents[0].try_clone().unwrap());
    }
    let mut receivers = vec![rx0];
    for _ in 1..RECEIVERS {
        receivers.push(receivers[0].clone());
    }
    let start = Arc::new(Barrier::new(PRODUCERS + RECEIVERS));

    std::thread::scope(|scope| {
        for (producer, parent) in parents.into_iter().enumerate() {
            let start = start.clone();
            scope.spawn(move || {
                for round in 0..ROUNDS {
                    let mut tx = parent.try_clone().unwrap();
                    let base = (producer * ROUNDS + round) * MESSAGES_PER_LANE;
                    for sequence in 0..MESSAGES_PER_LANE {
                        tx.send(base + sequence).unwrap();
                    }
                    if round == 0 {
                        start.wait();
                    }
                }
            });
        }
        for mut rx in receivers {
            let start = start.clone();
            let seen = seen.clone();
            scope.spawn(move || {
                start.wait();
                loop {
                    match rx.recv_timeout(DISCONNECT_WAIT_TIMEOUT) {
                        Ok(value) => assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0),
                        Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => panic!("dynamic lane became unreachable"),
                    }
                }
            });
        }
    });

    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
}

#[test]
fn non_copy_items_drop_once_across_receiver_handoff() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx0) = channel(128);
    let rx1 = rx0.clone();
    for _ in 0..65 {
        tx.try_send(DropCount(drops.clone()))
            .unwrap_or_else(|_| unreachable!());
    }

    drop(rx0.try_recv().unwrap());
    drop(rx0);
    drop(rx1);
    drop(tx);

    assert_eq!(drops.load(Ordering::Relaxed), 65);
}

#[test]
fn stalled_receiver_does_not_strand_its_sender_lane() {
    let (mut tx, mut stalled) = channel(64);
    let mut worker = stalled.clone();
    for value in 0..64 {
        tx.try_send(value).unwrap();
    }
    assert_eq!(stalled.try_recv(), Ok(0));
    drop(tx);

    for expected in 1..64 {
        assert_eq!(worker.recv(), Ok(expected));
    }
    assert_eq!(worker.recv(), Err(RecvError));
}

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

        let total = SENDERS * MESSAGES_PER_LANE;
        let seen = Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let received = Arc::new(AtomicUsize::new(0));
        receivers = std::thread::scope(|scope| {
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
    }
}

#[test]
fn blocked_sender_wakes_after_receive_releases_space() {
    let (mut tx, mut rx) = channel(1);
    tx.try_send(1).unwrap();

    std::thread::scope(|scope| {
        let sender = scope.spawn(move || tx.send(2));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
        assert_eq!(rx.recv(), Ok(2));
    });
}

#[test]
fn blocked_sender_wakes_when_last_receiver_drops() {
    let (mut tx, rx) = channel(1);
    tx.try_send(1).unwrap();

    std::thread::scope(|scope| {
        let sender = scope.spawn(move || tx.send(2));
        drop(rx);
        assert_eq!(sender.join().unwrap(), Err(SendError(2)));
    });
}

#[test]
fn parked_receivers_all_observe_disconnect() {
    let receiver_count = 8;
    let (tx, rx0) = channel::<u8>(1);
    let mut receivers = vec![rx0];
    for _ in 1..receiver_count {
        receivers.push(receivers[0].clone());
    }

    std::thread::scope(|scope| {
        let handles = receivers
            .into_iter()
            .map(|mut rx| scope.spawn(move || rx.recv()))
            .collect::<Vec<_>>();
        drop(tx);
        for handle in handles {
            assert_eq!(handle.join().unwrap(), Err(RecvError));
        }
    });
}

#[test]
fn buffered_last_sender_drop_wakes_every_receiver() {
    const RECEIVERS: usize = 8;

    for _ in 0..RACE_ROUNDS {
        let (mut tx, rx0) = channel(1);
        tx.try_send(7).unwrap();
        let mut receivers = vec![rx0];
        for _ in 1..RECEIVERS {
            receivers.push(receivers[0].clone());
        }
        let barrier = Arc::new(Barrier::new(RECEIVERS + 1));

        let results = std::thread::scope(|scope| {
            let handles = receivers
                .into_iter()
                .map(|mut rx| {
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        rx.recv_timeout(DISCONNECT_WAIT_TIMEOUT)
                    })
                })
                .collect::<Vec<_>>();
            barrier.wait();
            drop(tx);
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert_eq!(results.iter().filter(|result| result == &&Ok(7)).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| result == &&Err(RecvTimeoutError::Disconnected))
                .count(),
            RECEIVERS - 1
        );
    }
}

#[test]
fn many_receivers_share_every_value_exactly_once() {
    #[cfg(miri)]
    const RECEIVERS: usize = 4;
    #[cfg(not(miri))]
    const RECEIVERS: usize = 32;
    #[cfg(miri)]
    const MESSAGES: usize = 64;
    #[cfg(not(miri))]
    const MESSAGES: usize = 8_192;

    let seen = Arc::new(
        (0..MESSAGES)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let (mut tx, rx0) = channel(64);
    let mut receivers = vec![rx0];
    for _ in 1..RECEIVERS {
        receivers.push(receivers[0].clone());
    }

    std::thread::scope(|scope| {
        scope.spawn(move || {
            for value in 0..MESSAGES {
                tx.send(value).unwrap();
            }
        });
        for mut rx in receivers {
            let seen = seen.clone();
            scope.spawn(move || {
                while let Ok(value) = rx.recv() {
                    assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                }
            });
        }
    });

    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
}

#[test]
fn receiver_drop_racing_steal_preserves_every_value() {
    for _ in 0..RACE_ROUNDS {
        let drops = Arc::new((0..64).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let (mut tx, mut rx0) = channel(64);
        let rx1 = rx0.clone();
        for value in 0..64 {
            tx.try_send(TrackedValue {
                id: value,
                drops: drops.clone(),
            })
            .unwrap_or_else(|_| unreachable!());
        }
        let first = rx0.try_recv().unwrap();
        assert_eq!(first.id, 0);
        drop(tx);

        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = barrier.clone();
        let mut values = std::thread::scope(|scope| {
            let worker = scope.spawn(move || {
                worker_barrier.wait();
                rx1.into_iter().collect::<Vec<_>>()
            });
            barrier.wait();
            drop(rx0);
            worker.join().unwrap()
        });
        values.push(first);
        values.sort_unstable_by_key(|value| value.id);
        assert_eq!(
            values.iter().map(|value| value.id).collect::<Vec<_>>(),
            (0..64).collect::<Vec<_>>()
        );
        drop(values);
        assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    }
}

#[test]
fn batch_publication_never_reports_premature_disconnect() {
    for _ in 0..PUBLICATION_RACE_ROUNDS {
        let (mut tx, mut rx0) = channel(64);
        let mut rx1 = rx0.clone();
        for value in 0..64 {
            tx.try_send(value).unwrap();
        }
        drop(tx);

        let barrier = Arc::new(Barrier::new(3));
        let (mut rx0, first0, mut rx1, first1) = std::thread::scope(|scope| {
            let barrier0 = barrier.clone();
            let worker0 = scope.spawn(move || {
                barrier0.wait();
                let result = rx0.try_recv();
                (rx0, result)
            });
            let barrier1 = barrier.clone();
            let worker1 = scope.spawn(move || {
                barrier1.wait();
                let result = rx1.try_recv();
                (rx1, result)
            });
            barrier.wait();
            let (rx0, first0) = worker0.join().unwrap();
            let (rx1, first1) = worker1.join().unwrap();
            (rx0, first0, rx1, first1)
        });

        let mut terminal_seen = matches!(first0, Err(TryRecvError::Disconnected))
            || matches!(first1, Err(TryRecvError::Disconnected));
        let mut values = [first0, first1]
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        loop {
            let mut progress = false;
            for receiver in [&mut rx0, &mut rx1] {
                match receiver.try_recv() {
                    Ok(value) => {
                        assert!(!terminal_seen, "values appeared after disconnect");
                        values.push(value);
                        progress = true;
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => terminal_seen = true,
                }
            }
            if values.len() == 64 {
                break;
            }
            assert!(progress, "buffered values became unreachable");
        }

        values.sort_unstable();
        assert_eq!(values, (0..64).collect::<Vec<_>>());
        assert_eq!(rx0.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(rx1.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn batch_publication_with_live_sender_never_reports_disconnect() {
    for _ in 0..RACE_ROUNDS {
        let (mut tx, mut rx0) = channel(64);
        let mut rx1 = rx0.clone();
        for value in 0..64 {
            tx.try_send(value).unwrap();
        }

        let barrier = Arc::new(Barrier::new(3));
        let (first0, first1) = std::thread::scope(|scope| {
            let barrier0 = barrier.clone();
            let worker0 = scope.spawn(move || {
                barrier0.wait();
                rx0.try_recv()
            });
            let barrier1 = barrier.clone();
            let worker1 = scope.spawn(move || {
                barrier1.wait();
                rx1.try_recv()
            });
            barrier.wait();
            (worker0.join().unwrap(), worker1.join().unwrap())
        });

        assert_ne!(first0, Err(TryRecvError::Disconnected));
        assert_ne!(first1, Err(TryRecvError::Disconnected));
        drop(tx);
    }
}

#[test]
fn receiver_clone_drop_churn_delivers_exactly_once() {
    const CHURN_WORKERS: usize = 2;
    const RECEIVES_PER_CLONE: usize = 8;

    let seen = Arc::new(
        (0..CHURN_MESSAGES)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let (mut tx, mut stable) = channel(64);
    let churn_sources = (0..CHURN_WORKERS)
        .map(|_| stable.clone())
        .collect::<Vec<_>>();

    std::thread::scope(|scope| {
        scope.spawn(move || {
            for value in 0..CHURN_MESSAGES {
                tx.send(value).unwrap();
            }
        });

        for source in churn_sources {
            let seen = seen.clone();
            scope.spawn(move || {
                loop {
                    let mut short_lived = source.clone();
                    for _ in 0..RECEIVES_PER_CLONE {
                        let Ok(value) = short_lived.recv() else {
                            return;
                        };
                        assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                    }
                    drop(short_lived);
                    std::thread::yield_now();
                }
            });
        }

        while let Ok(value) = stable.recv() {
            assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
        }
    });

    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
}

#[test]
fn randomized_endpoint_churn_preserves_exact_reachability() {
    let (mut root, rx0) = channel(128);
    let mut pending = BTreeSet::new();
    for value in 0..65 {
        root.try_send(value).unwrap();
        pending.insert(value);
    }

    let mut senders = vec![root];
    let mut receivers = vec![rx0];
    let mut next_value = 65;
    let mut random = 0xd1b5_4a32_d192_ed03;

    for _ in 0..STATE_MACHINE_STEPS {
        match next_random(&mut random) % 8 {
            0 if senders.len() < 8 => {
                let source = next_random(&mut random) % senders.len();
                let tx = senders[source].try_clone().unwrap();
                senders.push(tx);
            }
            1 if senders.len() > 1 => {
                let sender = next_random(&mut random) % senders.len();
                drop(senders.swap_remove(sender));
            }
            2 if receivers.len() < 8 => {
                let source = next_random(&mut random) % receivers.len();
                let rx = receivers[source].clone();
                receivers.push(rx);
            }
            3 if receivers.len() > 1 => {
                let receiver = next_random(&mut random) % receivers.len();
                drop(receivers.swap_remove(receiver));
            }
            4 | 5 => {
                let sender = next_random(&mut random) % senders.len();
                match senders[sender].try_send(next_value) {
                    Ok(()) => {
                        assert!(pending.insert(next_value));
                        next_value += 1;
                    }
                    Err(fanring::mpmc::TrySendError::Full(returned)) => {
                        assert_eq!(returned, next_value);
                    }
                    Err(fanring::mpmc::TrySendError::Disconnected(_)) => {
                        panic!("at least one receiver remains alive")
                    }
                }
            }
            _ => {
                let receiver = next_random(&mut random) % receivers.len();
                match receivers[receiver].try_recv() {
                    Ok(value) => assert!(pending.remove(&value), "duplicate value {value}"),
                    Err(TryRecvError::Empty) if !pending.is_empty() => {
                        let mut reached = false;
                        for _ in 0..128 {
                            let receiver = next_random(&mut random) % receivers.len();
                            match receivers[receiver].try_recv() {
                                Ok(value) => {
                                    assert!(pending.remove(&value), "duplicate value {value}");
                                    reached = true;
                                    break;
                                }
                                Err(TryRecvError::Empty) => std::thread::yield_now(),
                                Err(TryRecvError::Disconnected) => {
                                    panic!("{0} values remain unconsumed", pending.len())
                                }
                            }
                        }
                        assert!(reached, "{0} values remain unreachable", pending.len());
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {
                        panic!("at least one sender remains alive")
                    }
                }
            }
        }
    }

    drop(senders);
    let mut empty_polls = 0;
    while !pending.is_empty() {
        let mut progress = false;
        for rx in &mut receivers {
            match rx.try_recv() {
                Ok(value) => {
                    assert!(pending.remove(&value), "duplicate value {value}");
                    progress = true;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) if !pending.is_empty() => {
                    panic!("{0} values remain unreachable", pending.len())
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }
        if progress {
            empty_polls = 0;
        } else {
            empty_polls += 1;
            assert!(
                empty_polls < 1_000,
                "{0} values remain unreachable",
                pending.len()
            );
            std::thread::yield_now();
        }
    }

    for rx in &mut receivers {
        loop {
            match rx.try_recv() {
                Ok(value) => panic!("unexpected value {value}"),
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}

#[test]
fn sender_slots_reuse_across_ready_page_boundaries() {
    const DYNAMIC_SENDERS: usize = 129;
    const ROUNDS: usize = 4;

    let (root, mut rx) = channel(2);
    let mut expected_slots = None;

    for round in 0..ROUNDS {
        let mut senders = (0..DYNAMIC_SENDERS)
            .map(|_| root.try_clone().unwrap())
            .collect::<Vec<_>>();
        let mut slots = senders
            .iter()
            .map(fanring::mpmc::Sender::lane_id)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        assert!(slots.last().copied().unwrap() >= 129);
        if let Some(expected) = &expected_slots {
            assert_eq!(&slots, expected);
        } else {
            expected_slots = Some(slots);
        }

        for (sender, tx) in senders.iter_mut().enumerate() {
            tx.send(round * DYNAMIC_SENDERS + sender).unwrap();
        }
        drop(senders);

        let mut values = (0..DYNAMIC_SENDERS)
            .map(|_| rx.recv().unwrap())
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(
            values,
            (round * DYNAMIC_SENDERS..(round + 1) * DYNAMIC_SENDERS).collect::<Vec<_>>()
        );
    }
}

#[test]
fn sparse_lane_on_later_ready_page_is_not_starved() {
    let (mut busy, mut rx) = channel(256);
    let mut senders = (0..128)
        .map(|_| busy.try_clone().unwrap())
        .collect::<Vec<_>>();
    let sparse = senders.last_mut().unwrap();

    for value in 0..256 {
        busy.try_send((0, value)).unwrap();
    }
    sparse.try_send((1, 0)).unwrap();

    let sparse_position = (0..=128)
        .position(|_| rx.try_recv().unwrap().0 == 1)
        .expect("later ready page must be activated");
    assert!(sparse_position <= 128);
}

#[cfg(not(miri))]
#[test]
fn blocked_receiver_finds_sender_added_in_second_ready_group() {
    const FIRST_GROUP_LANES: usize = 64 * 64;

    let (root, mut rx) = channel(1);
    let mut existing = Vec::with_capacity(FIRST_GROUP_LANES);
    existing.push(root);
    for _ in 1..FIRST_GROUP_LANES {
        existing.push(existing[0].try_clone().unwrap());
    }
    assert_eq!(existing.last().unwrap().lane_id(), FIRST_GROUP_LANES - 1);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    let barrier = Arc::new(Barrier::new(2));
    let receiver_barrier = barrier.clone();
    let receiver = std::thread::spawn(move || {
        receiver_barrier.wait();
        let result = rx.recv();
        (rx, result)
    });
    barrier.wait();

    let mut newest = existing[0].try_clone().unwrap();
    assert_eq!(newest.lane_id(), FIRST_GROUP_LANES);
    newest.send(42).unwrap();

    let (rx, result) = receiver.join().unwrap();
    assert_eq!(result, Ok(42));
    drop(rx);
    drop(newest);
    drop(existing);
}

#[cfg(not(miri))]
#[test]
fn busy_first_group_cannot_starve_sparse_second_group() {
    const SECOND_GROUP_SLOT: usize = 64 * 64;

    let (mut busy, mut rx) = channel(256);
    let mut other_senders = (0..SECOND_GROUP_SLOT)
        .map(|_| busy.try_clone().unwrap())
        .collect::<Vec<_>>();
    let sparse = other_senders.last_mut().unwrap();
    assert_eq!(sparse.lane_id(), SECOND_GROUP_SLOT);

    for sequence in 0..256 {
        busy.try_send((0, sequence)).unwrap();
    }
    assert_eq!(rx.try_recv(), Ok((0, 0)));
    sparse.try_send((1, 0)).unwrap();

    let sparse_position = (0..=128)
        .position(|_| rx.try_recv().unwrap().0 == 1)
        .expect("second ready group must be claimed");
    assert!(sparse_position <= 128);
}

#[cfg(not(miri))]
#[test]
fn sender_slots_reuse_across_ready_group_boundary() {
    const DYNAMIC_SENDERS: usize = 64 * 64;
    const ROUNDS: usize = 2;

    let (root, mut rx) = channel(1);
    let mut expected_slots = None;

    for round in 0..ROUNDS {
        let mut senders = (0..DYNAMIC_SENDERS)
            .map(|_| root.try_clone().unwrap())
            .collect::<Vec<_>>();
        let mut slots = senders
            .iter()
            .map(fanring::mpmc::Sender::lane_id)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        assert_eq!(slots.first(), Some(&1));
        assert_eq!(slots.last(), Some(&DYNAMIC_SENDERS));
        if let Some(expected) = &expected_slots {
            assert_eq!(&slots, expected);
        } else {
            expected_slots = Some(slots);
        }

        for (sender, tx) in senders.iter_mut().enumerate() {
            tx.try_send(round * DYNAMIC_SENDERS + sender).unwrap();
        }
        drop(senders);

        let mut values = (0..DYNAMIC_SENDERS)
            .map(|_| rx.recv().unwrap())
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(
            values,
            (round * DYNAMIC_SENDERS..(round + 1) * DYNAMIC_SENDERS).collect::<Vec<_>>()
        );
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
}

#[test]
fn timeout_races_preserve_data_and_recover_space() {
    let timeout = Duration::from_micros(50);

    for value in 0..RACE_ROUNDS {
        let (mut tx, mut rx) = channel(1);
        std::thread::scope(|scope| {
            let sender = scope.spawn(move || tx.send(value));
            let result = rx.recv_timeout(timeout);
            assert_eq!(sender.join().unwrap(), Ok(()));
            match result {
                Ok(received) => assert_eq!(received, value),
                Err(RecvTimeoutError::Timeout) => assert_eq!(rx.recv(), Ok(value)),
                Err(RecvTimeoutError::Disconnected) => panic!("message was sent"),
            }
        });

        let (mut tx, mut rx) = channel(1);
        tx.try_send(usize::MAX).unwrap();
        std::thread::scope(|scope| {
            let receiver = scope.spawn(move || {
                assert_eq!(rx.recv(), Ok(usize::MAX));
                rx
            });
            let result = tx.send_timeout(value, timeout);
            let mut rx = receiver.join().unwrap();
            match result {
                Ok(()) => assert_eq!(rx.recv(), Ok(value)),
                Err(SendTimeoutError::Timeout(returned)) => {
                    assert_eq!(returned, value);
                    tx.send(returned).unwrap();
                    assert_eq!(rx.recv(), Ok(value));
                }
                Err(SendTimeoutError::Disconnected(_)) => panic!("receiver stayed alive"),
            }
        });

        let (tx, mut rx) = channel::<usize>(1);
        std::thread::scope(|scope| {
            let dropper = scope.spawn(move || drop(tx));
            let result = rx.recv_timeout(timeout);
            dropper.join().unwrap();
            match result {
                Err(RecvTimeoutError::Disconnected) => {}
                Err(RecvTimeoutError::Timeout) => {
                    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
                }
                Ok(_) => panic!("no message was sent"),
            }
        });
    }
}
