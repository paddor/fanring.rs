#![cfg(all(feature = "async", loom, target_pointer_width = "64"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use fanring::mpsc;

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn publication_and_final_drop_racing_registration_cannot_lose_receive() {
    loom::model(|| {
        let (mut tx, mut rx) = mpsc::channel(1);
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let sender = loom::thread::spawn(move || {
            tx.try_send(7).unwrap();
        });
        let result = rx.poll_recv(&mut cx);
        sender.join().unwrap();
        if result.is_pending() {
            assert!(wakes.0.load(Ordering::Relaxed) > 0);
            assert_eq!(rx.poll_recv(&mut cx), Poll::Ready(Ok(7)));
        } else {
            assert_eq!(result, Poll::Ready(Ok(7)));
        }
        assert_eq!(rx.poll_recv(&mut cx), Poll::Ready(Err(mpsc::RecvError)));
    });
}

#[test]
fn capacity_release_racing_registration_cannot_lose_sender() {
    loom::model(|| {
        let (mut tx, mut rx) = mpsc::channel(1);
        tx.try_send(1).unwrap();
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let receiver = loom::thread::spawn(move || {
            assert_eq!(rx.try_recv(), Ok(1));
            rx.release_consumed();
            rx
        });
        let result = tx.poll_ready(&mut cx);
        let mut rx = receiver.join().unwrap();
        if result.is_pending() {
            assert!(wakes.0.load(Ordering::Relaxed) > 0);
        }
        assert_eq!(tx.poll_ready(&mut cx), Poll::Ready(Ok(())));
        tx.try_send(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
    });
}

#[test]
fn receiver_drop_racing_registration_cannot_leave_sender_asleep() {
    loom::model(|| {
        let (mut tx, rx) = mpsc::channel(1);
        tx.try_send(1).unwrap();
        let wakes = Arc::new(Wakes::default());
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let receiver = loom::thread::spawn(move || drop(rx));
        let result = tx.poll_ready(&mut cx);
        receiver.join().unwrap();
        if result.is_pending() {
            assert!(wakes.0.load(Ordering::Relaxed) > 0);
        }
        assert!(matches!(tx.poll_ready(&mut cx), Poll::Ready(Err(_))));
    });
}
