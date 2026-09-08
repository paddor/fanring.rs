use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

#[cfg(miri)]
const RACE_ROUNDS: usize = 2;
#[cfg(not(miri))]
const RACE_ROUNDS: usize = 256;

#[derive(Debug)]
struct Counted {
    id: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops[self.id].fetch_add(1, Ordering::Relaxed);
    }
}

fn counters(count: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect())
}

fn counted(drops: &Arc<Vec<AtomicUsize>>, id: usize) -> Counted {
    Counted {
        id,
        drops: drops.clone(),
    }
}

fn assert_drops(drops: &[AtomicUsize], expected: usize) {
    for (id, count) in drops.iter().enumerate() {
        assert_eq!(count.load(Ordering::Relaxed), expected, "payload {id}");
    }
}

macro_rules! receiver_drop_tests {
    ($kind:ident) => {
        mod $kind {
            use super::*;
            use fanring::teardown::Coordinated;
            use fanring::$kind::channel_with_policy;
            fn channel<T>(
                capacity: usize,
            ) -> (
                fanring::$kind::Sender<T, Coordinated>,
                fanring::$kind::Receiver<T, Coordinated>,
            ) {
                channel_with_policy(capacity)
            }

            #[test]
            fn default_defers_each_lane_until_its_sender_drops() {
                for register in [false, true] {
                    let drops = counters(2);
                    let (mut tx, mut rx) = fanring::$kind::channel(4);
                    let mut cloned = tx.try_clone().unwrap();
                    if register {
                        assert!(rx.try_recv().is_err());
                    }
                    tx.try_send(counted(&drops, 0)).unwrap();
                    cloned.try_send(counted(&drops, 1)).unwrap();
                    drop(rx);
                    assert!(tx.is_disconnected());
                    assert!(cloned.is_disconnected());
                    assert_drops(&drops, 0);
                    drop(cloned);
                    assert_eq!(drops[0].load(Ordering::Relaxed), 0);
                    assert_eq!(drops[1].load(Ordering::Relaxed), 1);
                    drop(tx);
                    assert_drops(&drops, 1);
                }
            }

            #[test]
            fn both_policies_preserve_operations_and_sender_disconnect() {
                fn check<P: fanring::teardown::Teardown>() {
                    let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
                    let mut cloned = tx.try_clone().unwrap();
                    tx.send_timeout(1, std::time::Duration::ZERO).unwrap();
                    cloned.send(2).unwrap();
                    drop((tx, cloned));
                    let mut values: Vec<_> = rx.iter().collect();
                    values.sort_unstable();
                    assert_eq!(values, [1, 2]);
                    assert!(rx.try_iter().next().is_none());
                    assert!(rx.into_iter().next().is_none());
                }
                check::<fanring::teardown::Deferred>();
                check::<Coordinated>();
            }

            #[test]
            fn queued_reply_disconnects_while_sender_lives() {
                let (mut tx, rx) = channel(4);
                let (reply_tx, reply_rx) = std::sync::mpsc::channel::<()>();
                tx.try_send(reply_tx).unwrap();
                drop(rx);
                assert!(tx.is_disconnected());
                assert_eq!(
                    reply_rx.try_recv(),
                    Err(std::sync::mpsc::TryRecvError::Disconnected)
                );
                drop(tx);
            }

            #[test]
            fn initial_and_cloned_lanes_drop_unread_values() {
                for register in [false, true] {
                    let drops = counters(8);
                    let (mut tx0, mut rx) = channel(4);
                    let mut tx1 = tx0.try_clone().unwrap();
                    if register {
                        assert!(rx.try_recv().is_err());
                    }
                    for id in 0..4 {
                        tx0.try_send(counted(&drops, id)).unwrap();
                        tx1.try_send(counted(&drops, id + 4)).unwrap();
                    }
                    drop(rx);
                    assert_drops(&drops, 1);
                    assert!(tx0.is_disconnected());
                    assert!(tx1.is_disconnected());
                    drop((tx0, tx1));
                    assert_drops(&drops, 1);
                }
            }

            #[test]
            fn partial_batch_drops_only_unread_values() {
                let drops = counters(128);
                let (mut tx, mut rx) = channel(128);
                for id in 0..128 {
                    tx.try_send(counted(&drops, id)).unwrap();
                }
                let received: Vec<_> = (0..3).map(|_| rx.try_recv().unwrap()).collect();
                drop(rx);
                for (id, count) in drops.iter().enumerate() {
                    let expected = usize::from(!received.iter().any(|value| value.id == id));
                    assert_eq!(count.load(Ordering::Relaxed), expected, "payload {id}");
                }
                drop(received);
                assert_drops(&drops, 1);
                drop(tx);
                assert_drops(&drops, 1);
            }

            #[test]
            fn send_racing_close_drops_values_before_sender_drops() {
                for _ in 0..RACE_ROUNDS {
                    let drops = counters(2);
                    let (mut tx, rx) = channel(2);
                    tx.try_send(counted(&drops, 0)).unwrap();
                    let barrier = Arc::new(Barrier::new(2));
                    let sender_barrier = barrier.clone();
                    let value = counted(&drops, 1);
                    let sender = std::thread::spawn(move || {
                        sender_barrier.wait();
                        drop(tx.try_send(value));
                        tx
                    });
                    barrier.wait();
                    drop(rx);
                    let tx = sender.join().unwrap();
                    assert_drops(&drops, 1);
                    assert!(tx.is_disconnected());
                    drop(tx);
                    assert_drops(&drops, 1);
                }
            }

            #[test]
            fn registration_and_send_racing_close_release_values() {
                for _ in 0..RACE_ROUNDS {
                    let drops = counters(1);
                    let (root, rx) = channel(2);
                    let barrier = Arc::new(Barrier::new(2));
                    let sender_barrier = barrier.clone();
                    let value = counted(&drops, 0);
                    let registrar = std::thread::spawn(move || {
                        sender_barrier.wait();
                        let mut child = root.try_clone();
                        if let Some(tx) = &mut child {
                            drop(tx.try_send(value));
                        } else {
                            drop(value);
                        }
                        (root, child)
                    });
                    barrier.wait();
                    drop(rx);
                    let senders = registrar.join().unwrap();
                    assert_drops(&drops, 1);
                    drop(senders);
                    assert_drops(&drops, 1);
                }
            }
        }
    };
}

receiver_drop_tests!(mpsc);
receiver_drop_tests!(mpmc);

#[test]
fn nonlast_mpmc_receiver_preserves_private_and_shared_batches() {
    for clone_before_prefetch in [false, true] {
        let drops = counters(128);
        let (mut tx, mut rx0) =
            fanring::mpmc::channel_with_policy::<_, fanring::teardown::Coordinated>(128);
        for id in 0..128 {
            tx.try_send(counted(&drops, id)).unwrap();
        }
        let rx1 = clone_before_prefetch.then(|| rx0.clone());
        let received = rx0.try_recv().unwrap();
        let mut rx1 = rx1.unwrap_or_else(|| rx0.clone());
        drop(rx0);
        assert_drops(&drops, 0);
        assert!(!tx.is_disconnected());
        // Orphaned work and the partially consumed lane must remain readable.
        let mut received = vec![received];
        received.extend((0..127).map(|_| rx1.try_recv().unwrap()));
        assert_drops(&drops, 0);
        drop(rx1);
        assert!(tx.is_disconnected());
        assert_drops(&drops, 0);
        drop(received);
        assert_drops(&drops, 1);
        drop(tx);
        assert_drops(&drops, 1);
    }
}

#[test]
fn last_mpmc_receiver_drops_orphaned_work_and_requeued_lane() {
    let drops = counters(128);
    let (mut tx, mut rx0) =
        fanring::mpmc::channel_with_policy::<_, fanring::teardown::Coordinated>(128);
    let rx1 = rx0.clone();
    for id in 0..128 {
        tx.try_send(counted(&drops, id)).unwrap();
    }
    drop(rx0.try_recv().unwrap());
    drop(rx0);
    drop(rx1);
    assert_drops(&drops, 1);
    assert!(tx.is_disconnected());
    drop(tx);
    assert_drops(&drops, 1);
}
