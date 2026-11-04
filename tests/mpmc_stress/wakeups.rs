//! Blocking wakeups, disconnect notification, and timeout races.

use std::sync::{Arc, Barrier};
use std::time::Duration;

use fanring::mpmc::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, channel,
};

use crate::support::{DISCONNECT_WAIT_TIMEOUT, RACE_ROUNDS};

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
