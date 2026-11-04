//! MPSC bulk receives racing sends, registration, and endpoint drops.

use crate::support::{DropCount, model};
use fanring::mpsc::{RecvError, TryRecvError, TrySendError, channel};
use loom::sync::Arc;
use loom::sync::atomic::Ordering;
use loom::thread;

#[test]
fn mpsc_recv_batch_releases_capacity_before_returning() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            tx.try_send(0).unwrap();
            tx.try_send(1).unwrap();
            let sender = thread::spawn(move || {
                tx.send(2).unwrap();
                tx
            });

            let mut output = Vec::with_capacity(1);
            assert_eq!(rx.recv_batch_into(&mut output, 1), Ok(1));
            assert_eq!(output, [0]);
            let tx = sender.join().unwrap();
            assert_eq!(rx.recv(), Ok(1));
            assert_eq!(rx.recv(), Ok(2));
            drop(tx);
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn mpsc_recv_batch_waits_only_for_first_value() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            let receiver = thread::spawn(move || {
                let mut output = Vec::with_capacity(2);
                assert_eq!(rx.recv_batch_into(&mut output, 2), Ok(1));
                assert_eq!(output, [7]);
                rx
            });
            tx.send(7).unwrap();
            // Join before disconnecting. Waiting for a second value would
            // deadlock, while returning before the first would fail above.
            let mut rx = receiver.join().unwrap();
            tx.try_send(8).unwrap();
            tx.try_send(9).unwrap();
            assert_eq!(tx.try_send(10), Err(TrySendError::Full(10)));
            drop(tx);
            assert_eq!(rx.iter().collect::<Vec<_>>(), [8, 9]);
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn mpsc_recv_batch_wakes_on_last_sender_drop() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            let sender = thread::spawn(move || drop(tx));
            let mut output = vec![7];
            assert_eq!(rx.recv_batch_into(&mut output, 2), Err(RecvError));
            assert_eq!(output, [7]);
            sender.join().unwrap();
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn mpsc_recv_batch_slot_refill_racing_receiver_drop_preserves_ownership() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            let first = Arc::new(loom::sync::atomic::AtomicUsize::new(0));
            let unread = Arc::new(loom::sync::atomic::AtomicUsize::new(0));
            let refill = Arc::new(loom::sync::atomic::AtomicUsize::new(0));
            tx.try_send(DropCount(first.clone())).unwrap();
            tx.try_send(DropCount(unread.clone())).unwrap();
            let value = DropCount(refill.clone());
            let sender = thread::spawn(move || {
                let sent = match tx.send(value) {
                    Ok(()) => true,
                    Err(error) => {
                        drop(error);
                        false
                    }
                };
                (tx, sent)
            });

            let mut output = Vec::with_capacity(1);
            assert_eq!(rx.recv_batch_into(&mut output, 1), Ok(1));
            // The sender can reuse the released slot while the receiver closes
            // a window still containing the unread value.
            drop(rx);
            let (tx, sent) = sender.join().unwrap();
            assert_eq!(first.load(Ordering::Relaxed), 0);
            assert_eq!(unread.load(Ordering::Relaxed), usize::from(P::COORDINATED));
            assert_eq!(
                refill.load(Ordering::Relaxed),
                usize::from(P::COORDINATED || !sent)
            );
            drop(tx);
            assert_eq!(first.load(Ordering::Relaxed), 0);
            assert_eq!(unread.load(Ordering::Relaxed), 1);
            assert_eq!(refill.load(Ordering::Relaxed), 1);
            drop(output);
            assert_eq!(first.load(Ordering::Relaxed), 1);
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn mpsc_try_recv_batch_racing_send_preserves_values() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            tx.try_send(0).unwrap();
            let sender = thread::spawn(move || {
                tx.try_send(1).unwrap();
                tx
            });

            let mut output = Vec::with_capacity(2);
            assert!(matches!(rx.try_recv_batch_into(&mut output, 2), Ok(1 | 2)));
            let tx = sender.join().unwrap();
            if output.len() < 2 {
                assert_eq!(rx.try_recv_batch_into(&mut output, 2), Ok(1));
            }
            assert_eq!(output, [0, 1]);
            assert_eq!(
                rx.try_recv_batch_into(&mut output, 2),
                Err(TryRecvError::Empty)
            );
            drop(tx);
            assert_eq!(
                rx.try_recv_batch_into(&mut output, 2),
                Err(TryRecvError::Disconnected)
            );
            assert_eq!(output, [0, 1]);
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn mpsc_try_recv_batch_racing_registration_collects_new_lane() {
    model(|| {
        let (mut root, mut rx) = channel::<usize>(2);
        root.try_send(0).unwrap();
        let registrar = thread::spawn(move || {
            let mut child = root.try_clone().unwrap();
            child.try_send(1).unwrap();
            (root, child)
        });

        let mut output = Vec::with_capacity(2);
        let first = rx.try_recv_batch_into(&mut output, 2);
        assert!(matches!(first, Ok(1 | 2)));
        let senders = registrar.join().unwrap();
        if output.len() < 2 {
            assert_eq!(rx.try_recv_batch_into(&mut output, 2), Ok(1));
        }
        output.sort_unstable();
        assert_eq!(output, [0, 1]);
        drop(senders);
        assert_eq!(
            rx.try_recv_batch_into(&mut output, 1),
            Err(TryRecvError::Disconnected)
        );
    });
}

#[test]
fn mpsc_recv_batch_bulk_release_wakes_blocked_sender() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            tx.try_send(0).unwrap();
            tx.try_send(1).unwrap();
            let sender = thread::spawn(move || {
                tx.send(2).unwrap();
                tx.send(3).unwrap();
                tx
            });

            let mut output = Vec::with_capacity(4);
            while output.len() < 4 {
                assert!(matches!(rx.recv_batch_into(&mut output, 4), Ok(1..=4)));
            }
            assert_eq!(output, [0, 1, 2, 3]);
            let tx = sender.join().unwrap();
            drop(tx);
            assert_eq!(rx.recv_batch_into(&mut output, 1), Err(RecvError));
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}

#[test]
fn mpsc_try_recv_batch_while_racing_send_keeps_rejected_value() {
    fn check<P: fanring::teardown::Teardown>() {
        model(|| {
            let (mut tx, mut rx) = fanring::mpsc::channel_with_policy::<_, P>(2);
            tx.try_send(0).unwrap();
            let sender = thread::spawn(move || {
                tx.try_send(1).unwrap();
                tx
            });

            // Value 1 may or may not be visible yet. Either way only 0 is admitted.
            let mut output = Vec::with_capacity(2);
            assert_eq!(
                rx.try_recv_batch_into_while(&mut output, 2, |value| *value == 0),
                Ok(1)
            );
            let tx = sender.join().unwrap();
            assert_eq!(
                rx.try_recv_batch_into_while(&mut output, 2, |value| *value == 0),
                Ok(0)
            );
            assert_eq!(rx.try_recv_batch_into(&mut output, 2), Ok(1));
            assert_eq!(output, [0, 1]);
            drop(tx);
            assert_eq!(
                rx.try_recv_batch_into_while(&mut output, 2, |_| true),
                Err(TryRecvError::Disconnected)
            );
        });
    }
    check::<fanring::teardown::Deferred>();
    check::<fanring::teardown::Coordinated>();
}
