use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fanring::mpsc::{TryRecvError, TrySendError, channel};

#[cfg(miri)]
const MESSAGES_PER_SENDER: usize = 16;
#[cfg(not(miri))]
const MESSAGES_PER_SENDER: usize = 2_000;

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
                    let value = ((sender_id as u64) << 32) | seq as u64;
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
                    let sender_id = (value >> 32) as usize;
                    let seq = (value & u32::MAX as u64) as usize;
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
        let mut slots = senders.iter().map(|tx| tx.shard()).collect::<Vec<_>>();
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
