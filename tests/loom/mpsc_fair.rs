//! MPSC fair receives racing registration and deferred publication.

use crate::support::model;
use fanring::mpsc::{TryRecvError, channel};

#[test]
fn fair_receive_registration_racing_drain() {
    model(|| {
        let (mut tx, mut rx) = channel(2);
        tx.try_send(0).unwrap();
        let sender = loom::thread::spawn(move || {
            let mut other = tx.try_clone().unwrap();
            other.try_send(1).unwrap();
            tx.try_send(2).unwrap();
        });
        let mut values = Vec::new();
        loop {
            match rx.try_recv_fair() {
                Ok(value) => values.push(value),
                Err(TryRecvError::Empty) => loom::thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
            rx.release_consumed();
        }
        sender.join().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(
            values
                .iter()
                .filter(|&&v| v != 1)
                .copied()
                .collect::<Vec<_>>(),
            [0, 2]
        );
        values.sort_unstable();
        assert_eq!(values, [0, 1, 2]);
    });
}

#[test]
fn deferred_publish_racing_fair_receive() {
    model(|| {
        let (mut tx, mut rx) = channel(2);
        let sender = loom::thread::spawn(move || {
            tx.try_send_deferred(0).unwrap();
            tx.try_send_deferred(1).unwrap();
            tx.flush();
        });
        let mut next = 0;
        loop {
            match rx.try_recv_fair() {
                Ok(value) => {
                    assert_eq!(value, next);
                    next += 1;
                }
                Err(TryRecvError::Empty) => loom::thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        sender.join().unwrap();
        assert_eq!(next, 2);
    });
}
