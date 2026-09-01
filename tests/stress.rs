use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use fanring::mpsc::{RecvTimeoutError, SendTimeoutError, TryRecvError, TrySendError, channel};

#[cfg(miri)]
const MESSAGES_PER_SENDER: usize = 16;
#[cfg(not(miri))]
const MESSAGES_PER_SENDER: usize = 2_000;

#[cfg(miri)]
const STATE_MACHINE_STEPS: usize = 512;
#[cfg(not(miri))]
const STATE_MACHINE_STEPS: usize = 50_000;

#[cfg(miri)]
const RACE_ROUNDS: usize = 2;
#[cfg(not(miri))]
const RACE_ROUNDS: usize = 1_000;

fn send_until_ok<T: Copy>(tx: &mut fanring::mpsc::Sender<T>, mut value: T) {
    loop {
        match tx.try_send(value) {
            Ok(()) => return,
            Err(TrySendError::Full(returned)) => {
                value = returned;
                std::thread::yield_now();
            }
            Err(TrySendError::Disconnected(_)) => panic!("receiver dropped"),
        }
    }
}

fn next_random(state: &mut u64) -> usize {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state as usize
}

#[test]
fn all_64_sender_bits_deliver() {
    let senders = 64;
    let (tx0, mut rx) = channel(8);
    let mut txs = vec![tx0];
    for _ in 1..senders {
        let tx = txs[0].try_clone().expect("sender slot available");
        txs.push(tx);
    }

    std::thread::scope(|scope| {
        for (sender_id, mut tx) in txs.into_iter().enumerate() {
            scope.spawn(move || {
                for seq in 0..MESSAGES_PER_SENDER {
                    let value = (u64::try_from(sender_id).expect("sender id fits u64") << 32)
                        | u64::try_from(seq).expect("sequence fits u64");
                    send_until_ok(&mut tx, value);
                }
            });
        }

        let mut seen = vec![0usize; senders];
        let total = senders * MESSAGES_PER_SENDER;
        let mut received = 0usize;

        while received < total {
            match rx.try_recv() {
                Ok(value) => {
                    let sender_id = usize::try_from(value >> 32).expect("sender id fits usize");
                    let seq =
                        usize::try_from(value & u64::from(u32::MAX)).expect("sequence fits usize");
                    assert_eq!(seq, seen[sender_id]);
                    seen[sender_id] += 1;
                    received += 1;
                }
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => panic!("senders disconnected early"),
            }
        }

        assert_eq!(seen, vec![MESSAGES_PER_SENDER; senders]);
    });
}

#[test]
fn sender_drop_preserves_buffered_items_until_drained() {
    let (mut tx, mut rx) = channel(8);
    for i in 0..8 {
        tx.try_send(i).unwrap();
    }
    drop(tx);

    for i in 0..8 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn receiver_drop_turns_full_into_disconnected() {
    let (mut tx, rx) = channel(2);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));

    drop(rx);
    assert_eq!(tx.try_send(4), Err(TrySendError::Disconnected(4)));
}

#[test]
fn drops_remaining_items_once() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct Counted(Arc<()>);

    impl Drop for Counted {
        fn drop(&mut self) {
            let _ = &self.0;
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    DROPS.store(0, Ordering::Relaxed);

    let token = Arc::new(());
    let (mut tx0, rx) = channel(8);
    let mut tx1 = tx0.try_clone().unwrap();

    for _ in 0..8 {
        tx0.try_send(Counted(token.clone())).unwrap();
        tx1.try_send(Counted(token.clone())).unwrap();
    }

    drop(rx);
    drop(tx0);
    drop(tx1);

    assert_eq!(DROPS.load(Ordering::Relaxed), 16);
}

#[test]
fn sparse_sender_set_does_not_require_low_shards() {
    let (tx0, mut rx) = channel(4);
    let tx1 = tx0.try_clone().unwrap();
    let mut tx2 = tx1.try_clone().unwrap();
    drop(tx0);
    drop(tx1);

    tx2.try_send(42).unwrap();
    assert_eq!(rx.try_recv(), Ok(42));
}

#[test]
fn repeated_empty_ready_races_do_not_lose_messages() {
    let (mut tx, mut rx) = channel(4);

    std::thread::scope(|scope| {
        scope.spawn(|| {
            for i in 0..MESSAGES_PER_SENDER {
                send_until_ok(&mut tx, i);
                std::thread::yield_now();
            }
        });

        for expected in 0..MESSAGES_PER_SENDER {
            loop {
                match rx.try_recv() {
                    Ok(value) => {
                        assert_eq!(value, expected);
                        break;
                    }
                    Err(TryRecvError::Empty) => std::thread::yield_now(),
                    Err(TryRecvError::Disconnected) => panic!("sender disconnected early"),
                }
            }
        }
    });
}

#[test]
fn randomized_sender_churn_matches_per_sender_fifo_model() {
    struct ModelSender {
        id: usize,
        next_sequence: usize,
        tx: fanring::mpsc::Sender<(usize, usize)>,
    }

    let (root, mut rx) = channel(4);
    let mut senders = vec![ModelSender {
        id: 0,
        next_sequence: 0,
        tx: root,
    }];
    let mut expected = vec![VecDeque::new()];
    let mut expected_count = 0;
    let mut random = 0x9e37_79b9_7f4a_7c15;

    for _ in 0..STATE_MACHINE_STEPS {
        match next_random(&mut random) % 5 {
            0 if senders.len() < 16 => {
                let source = next_random(&mut random) % senders.len();
                let tx = senders[source].tx.try_clone().unwrap();
                let id = expected.len();
                expected.push(VecDeque::new());
                senders.push(ModelSender {
                    id,
                    next_sequence: 0,
                    tx,
                });
            }
            1 if senders.len() > 1 => {
                let sender = next_random(&mut random) % senders.len();
                drop(senders.swap_remove(sender));
            }
            2 | 3 => {
                let sender = next_random(&mut random) % senders.len();
                let model = &mut senders[sender];
                let value = (model.id, model.next_sequence);
                match model.tx.try_send(value) {
                    Ok(()) => {
                        expected[model.id].push_back(model.next_sequence);
                        expected_count += 1;
                        model.next_sequence += 1;
                    }
                    Err(TrySendError::Full(returned)) => assert_eq!(returned, value),
                    Err(TrySendError::Disconnected(_)) => panic!("receiver remains alive"),
                }
            }
            _ => match rx.try_recv() {
                Ok((sender, sequence)) => {
                    assert_eq!(expected[sender].pop_front(), Some(sequence));
                    expected_count -= 1;
                }
                Err(TryRecvError::Empty) => assert_eq!(expected_count, 0),
                Err(TryRecvError::Disconnected) => panic!("at least one sender remains alive"),
            },
        }
    }

    drop(senders);
    loop {
        match rx.try_recv() {
            Ok((sender, sequence)) => {
                assert_eq!(expected[sender].pop_front(), Some(sequence));
                expected_count -= 1;
            }
            Err(TryRecvError::Empty) => std::thread::yield_now(),
            Err(TryRecvError::Disconnected) => break,
        }
    }
    assert_eq!(expected_count, 0);
    assert!(expected.iter().all(VecDeque::is_empty));
}

#[test]
fn capacity_one_blocking_ping_pong_does_not_lose_wakeups() {
    let (mut tx, mut rx) = channel(1);

    std::thread::scope(|scope| {
        scope.spawn(move || {
            for value in 0..MESSAGES_PER_SENDER {
                tx.send(value).unwrap();
            }
        });

        for expected in 0..MESSAGES_PER_SENDER {
            assert_eq!(rx.recv(), Ok(expected));
        }
    });
}

#[test]
fn capacity_eight_blocking_stream_does_not_stall() {
    let (mut tx, mut rx) = channel(8);

    std::thread::scope(|scope| {
        scope.spawn(move || {
            for value in 0..MESSAGES_PER_SENDER * 10 {
                tx.send(value).unwrap();
            }
        });

        for expected in 0..MESSAGES_PER_SENDER * 10 {
            assert_eq!(rx.recv(), Ok(expected));
        }
    });
}

#[test]
fn four_small_blocking_lanes_do_not_lose_readiness() {
    let senders = 4;
    let (tx0, mut rx) = channel(2);
    let mut txs = vec![tx0];
    for _ in 1..senders {
        txs.push(txs[0].try_clone().unwrap());
    }

    std::thread::scope(|scope| {
        for (sender_id, mut tx) in txs.into_iter().enumerate() {
            scope.spawn(move || {
                for seq in 0..MESSAGES_PER_SENDER * 10 {
                    tx.send((sender_id, seq)).unwrap();
                }
            });
        }

        let mut seen = vec![0usize; senders];
        for _ in 0..senders * MESSAGES_PER_SENDER * 10 {
            let (sender_id, seq) = rx.recv().unwrap();
            assert_eq!(seq, seen[sender_id]);
            seen[sender_id] += 1;
        }
        assert_eq!(seen, vec![MESSAGES_PER_SENDER * 10; senders]);
    });
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
            .map(fanring::mpsc::Sender::lane_id)
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
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
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
        .expect("second ready group must be polled");
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
            .map(fanring::mpsc::Sender::lane_id)
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
fn dynamic_sender_churn_during_receive_delivers_exactly_once() {
    #[cfg(miri)]
    const PRODUCERS: usize = 2;
    #[cfg(not(miri))]
    const PRODUCERS: usize = 8;
    #[cfg(miri)]
    const ROUNDS: usize = 2;
    #[cfg(not(miri))]
    const ROUNDS: usize = 10_000;
    const MESSAGES_PER_LANE: usize = 4;

    let total = PRODUCERS * ROUNDS * MESSAGES_PER_LANE;
    let (tx0, mut rx) = channel(8);
    let mut parents = vec![tx0];
    for _ in 1..PRODUCERS {
        parents.push(parents[0].try_clone().unwrap());
    }
    let start = Arc::new(Barrier::new(PRODUCERS + 1));

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

        start.wait();
        let mut seen = vec![false; total];
        for received in 0..total {
            let value = match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(value) => value,
                Err(error) => {
                    let missing = seen
                        .iter()
                        .enumerate()
                        .filter_map(|(value, seen)| (!seen).then_some(value))
                        .collect::<Vec<_>>();
                    panic!(
                        "receive {received}/{total} failed: {error}; live senders: {0}; missing: {missing:?}",
                        rx.sender_count()
                    );
                }
            };
            assert!(!seen[value], "duplicate value {value}");
            seen[value] = true;
        }
        assert!(seen.into_iter().all(|value| value));
    });

    assert_eq!(rx.recv(), Err(fanring::mpsc::RecvError));
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
