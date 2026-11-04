//! Exactly-once delivery with many senders, many receivers, and endpoint
//! churn.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use fanring::mpmc::{RecvTimeoutError, channel};

use crate::support::DISCONNECT_WAIT_TIMEOUT;

#[cfg(miri)]
const MESSAGES_PER_SENDER: usize = 16;
#[cfg(not(miri))]
const MESSAGES_PER_SENDER: usize = 5_000;

#[cfg(miri)]
const CHURN_MESSAGES: usize = 128;
#[cfg(not(miri))]
const CHURN_MESSAGES: usize = 20_000;

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
