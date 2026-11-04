//! MPSC blocking send and receive wakeups for data, space, and disconnect.

use crate::support::model;
use fanring::mpsc::{RecvError, SendError, channel};
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

#[test]
fn blocking_recv_does_not_lose_data_wakeup() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        let sender = thread::spawn(move || tx.send(1));

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
    });
}

#[test]
fn blocking_send_does_not_lose_space_wakeup() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || tx.send(2));

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
        assert_eq!(rx.recv(), Ok(2));
    });
}

#[test]
fn mpsc_release_consumed_does_not_lose_space_wakeup() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            tx.try_send(0).unwrap();
            tx.try_send(1).unwrap();
            let sender = thread::spawn(move || {
                tx.send(2).unwrap();
                tx
            });

            assert_eq!(rx.recv(), Ok(0));
            rx.release_consumed();
            let tx = sender.join().unwrap();
            assert_eq!(rx.recv(), Ok(1));
            assert_eq!(rx.recv(), Ok(2));
            drop(tx);
            assert_eq!(rx.recv(), Err(RecvError));
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn blocking_send_recv_do_not_deadlock_while_sender_stays_alive() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let sender_done = done.clone();
        let sender = thread::spawn(move || {
            tx.send(2).unwrap();
            while !sender_done.load(Ordering::Acquire) {
                thread::yield_now();
            }
        });

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        done.store(true, Ordering::Release);
        sender.join().unwrap();
    });
}

#[test]
fn blocking_recv_wakes_on_sender_disconnect() {
    model(|| {
        let (tx, mut rx) = channel::<u8>(1);
        let sender = thread::spawn(move || drop(tx));

        assert_eq!(rx.recv(), Err(RecvError));
        sender.join().unwrap();
    });
}

#[test]
fn blocking_send_wakes_on_receiver_disconnect() {
    model(|| {
        let (mut tx, rx) = channel(1);
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || tx.send(2));

        drop(rx);
        assert_eq!(sender.join().unwrap(), Err(SendError(2)));
    });
}
