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
const CHURN_MESSAGES: usize = 128;
#[cfg(not(miri))]
const CHURN_MESSAGES: usize = 20_000;

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
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
fn non_copy_items_drop_once_across_receiver_handoff() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx0) = channel(64);
    let rx1 = rx0.clone();
    for _ in 0..64 {
        tx.try_send(DropCount(drops.clone()))
            .unwrap_or_else(|_| unreachable!());
    }

    drop(rx0.try_recv().unwrap());
    drop(rx0);
    drop(rx1);
    drop(tx);

    assert_eq!(drops.load(Ordering::Relaxed), 64);
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
fn receiver_drop_racing_steal_preserves_every_value() {
    for _ in 0..RACE_ROUNDS {
        let (mut tx, mut rx0) = channel(64);
        let rx1 = rx0.clone();
        for value in 0..64 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(rx0.try_recv(), Ok(0));
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
        values.push(0);
        values.sort_unstable();
        assert_eq!(values, (0..64).collect::<Vec<_>>());
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
