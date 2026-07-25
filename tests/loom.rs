#![cfg(all(loom, target_pointer_width = "64"))]

use fanring::{RecvError, SendError, TryRegisterError, channel};
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

#[test]
fn send_recv_ready_race_does_not_lose_message() {
    loom::model(|| {
        let (mut tx, mut rx) = channel(1, 1);
        tx.try_send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));

        let sent = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let sent_for_sender = sent.clone();
        let done_for_sender = done.clone();

        let sender = thread::spawn(move || {
            tx.try_send(2).unwrap();
            sent_for_sender.store(true, Ordering::Release);
            while !done_for_sender.load(Ordering::Acquire) {
                thread::yield_now();
            }
        });

        let mut received = false;
        for _ in 0..2 {
            match rx.try_recv() {
                Ok(2) => {
                    received = true;
                    break;
                }
                Ok(other) => panic!("unexpected value {other}"),
                Err(RecvError::Empty) => thread::yield_now(),
                Err(RecvError::Disconnected) => panic!("sender still alive"),
            }
        }

        while !sent.load(Ordering::Acquire) {
            thread::yield_now();
        }

        if !received {
            assert_eq!(rx.try_recv(), Ok(2));
        }

        done.store(true, Ordering::Release);
        sender.join().unwrap();
    });
}

#[test]
fn receiver_drop_makes_sender_disconnect() {
    loom::model(|| {
        let (mut tx, rx) = channel(1, 1);
        tx.try_send(1).unwrap();

        let sender = thread::spawn(move || {
            let mut value = 2;
            loop {
                match tx.try_send(value) {
                    Ok(()) => return,
                    Err(SendError::Full(returned)) => {
                        value = returned;
                        thread::yield_now();
                    }
                    Err(SendError::Disconnected(returned)) => {
                        assert_eq!(returned, 2);
                        return;
                    }
                }
            }
        });

        drop(rx);
        sender.join().unwrap();
    });
}

#[test]
fn second_shard_ready_race_does_not_lose_message() {
    loom::model(|| {
        let (mut tx0, mut rx) = channel(2, 1);
        let mut tx1 = tx0.try_clone().unwrap();

        tx0.try_send(10).unwrap();
        assert_eq!(rx.try_recv(), Ok(10));

        let sender1 = thread::spawn(move || {
            tx1.try_send(20).unwrap();
        });

        let mut received = false;
        match rx.try_recv() {
            Ok(20) => received = true,
            Ok(other) => panic!("unexpected value {other}"),
            Err(RecvError::Empty) => thread::yield_now(),
            Err(RecvError::Disconnected) => panic!("senders still alive"),
        }

        sender1.join().unwrap();

        if !received {
            assert_eq!(rx.try_recv(), Ok(20));
        }
        drop(tx0);
        assert_eq!(rx.try_recv(), Err(RecvError::Disconnected));
    });
}

#[test]
fn register_race_with_receiver_drop_reports_disconnected() {
    loom::model(|| {
        let (mut tx, rx) = channel::<u8>(2, 1);

        let dropper = thread::spawn(move || {
            thread::yield_now();
            drop(rx);
        });

        let registered = tx.try_register();
        dropper.join().unwrap();

        match registered {
            Ok(mut tx1) => {
                assert_eq!(tx1.try_send(1), Err(SendError::Disconnected(1)));
            }
            Err(TryRegisterError::Disconnected) => {}
            Err(TryRegisterError::NoSenderSlot) => panic!("sender slot should be available"),
        }

        assert_eq!(tx.try_send(2), Err(SendError::Disconnected(2)));
    });
}

#[test]
fn sender_drop_during_receiver_drain_preserves_buffered_items() {
    loom::model(|| {
        let (mut tx, mut rx) = channel(1, 2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        let sender = thread::spawn(move || {
            thread::yield_now();
            drop(tx);
        });

        assert_eq!(rx.try_recv(), Ok(1));
        sender.join().unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(RecvError::Disconnected));
    });
}
