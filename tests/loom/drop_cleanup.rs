//! Payload cleanup when an endpoint closes during a send or a registration,
//! for both channel kinds.

macro_rules! drop_cleanup_models {
    ($module:ident, $kind:ident) => {
        mod $module {
            use crate::support::{DropCount, model};
            use fanring::teardown::Coordinated;
            fn channel<T>(
                capacity: usize,
            ) -> (
                fanring::$kind::Sender<T, Coordinated>,
                fanring::$kind::Receiver<T, Coordinated>,
            ) {
                fanring::$kind::channel_with_policy(capacity)
            }
            use loom::sync::Arc;
            use loom::sync::atomic::{AtomicUsize, Ordering};
            use loom::thread;

            #[test]
            fn deferred_close_racing_send_retains_then_drops_exactly_once() {
                model(|| {
                    let first = Arc::new(AtomicUsize::new(0));
                    let second = Arc::new(AtomicUsize::new(0));
                    let (mut tx, rx) = fanring::$kind::channel(2);
                    tx.try_send(DropCount(first.clone())).unwrap();
                    let value = DropCount(second.clone());
                    let sender = thread::spawn(move || {
                        drop(tx.try_send(value));
                        tx
                    });
                    drop(rx);
                    let tx = sender.join().unwrap();
                    assert_eq!(first.load(Ordering::Relaxed), 0);
                    assert!(second.load(Ordering::Relaxed) <= 1);
                    assert!(tx.is_disconnected());
                    drop(tx);
                    assert_eq!(first.load(Ordering::Relaxed), 1);
                    assert_eq!(second.load(Ordering::Relaxed), 1);
                });
            }

            #[test]
            fn receiver_drop_racing_send_releases_payloads_with_sender_alive() {
                model(|| {
                    let first = Arc::new(AtomicUsize::new(0));
                    let second = Arc::new(AtomicUsize::new(0));
                    let (mut tx, rx) = channel(2);
                    tx.try_send(DropCount(first.clone())).unwrap();
                    let value = DropCount(second.clone());
                    let sender = thread::spawn(move || {
                        drop(tx.try_send(value));
                        tx
                    });
                    drop(rx);
                    let tx = sender.join().unwrap();
                    assert_eq!(first.load(Ordering::Relaxed), 1);
                    assert_eq!(second.load(Ordering::Relaxed), 1);
                    drop(tx);
                    assert_eq!(first.load(Ordering::Relaxed), 1);
                    assert_eq!(second.load(Ordering::Relaxed), 1);
                });
            }

            #[test]
            fn receiver_drop_racing_registration_releases_payload_with_senders_alive() {
                model(|| {
                    let drops = Arc::new(AtomicUsize::new(0));
                    let (root, rx) = channel(1);
                    let value = DropCount(drops.clone());
                    let registrar = thread::spawn(move || {
                        let mut child = root.try_clone();
                        if let Some(tx) = &mut child {
                            drop(tx.try_send(value));
                        } else {
                            drop(value);
                        }
                        (root, child)
                    });
                    drop(rx);
                    let senders = registrar.join().unwrap();
                    assert_eq!(drops.load(Ordering::Relaxed), 1);
                    drop(senders);
                    assert_eq!(drops.load(Ordering::Relaxed), 1);
                });
            }
        }
    };
}

drop_cleanup_models!(mpsc_cleanup, mpsc);
drop_cleanup_models!(mpmc_cleanup, mpmc);
