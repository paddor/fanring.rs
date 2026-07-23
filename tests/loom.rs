#![cfg(all(loom, target_pointer_width = "64"))]

use fanring::{RecvError, SendError, channel};
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
