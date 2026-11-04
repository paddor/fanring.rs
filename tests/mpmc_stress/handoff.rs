//! Work handoff between receivers: prefetched batches, steals, and staged
//! work publication.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use fanring::mpmc::{RecvError, TryRecvError, channel};

use crate::support::{DISCONNECT_WAIT_TIMEOUT, RACE_ROUNDS};

#[cfg(miri)]
const PUBLICATION_RACE_ROUNDS: usize = 2;
#[cfg(not(miri))]
const PUBLICATION_RACE_ROUNDS: usize = 20_000;

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct TrackedValue {
    id: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for TrackedValue {
    fn drop(&mut self) {
        self.drops[self.id].fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn non_copy_items_drop_once_across_receiver_handoff() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx0) = channel(128);
    let rx1 = rx0.clone();
    for _ in 0..65 {
        tx.try_send(DropCount(drops.clone()))
            .unwrap_or_else(|_| unreachable!());
    }

    drop(rx0.try_recv().unwrap());
    drop(rx0);
    drop(rx1);
    drop(tx);

    assert_eq!(drops.load(Ordering::Relaxed), 65);
}

#[test]
fn stalled_receiver_does_not_strand_its_sender_lane() {
    let (mut tx, mut stalled) = channel(64);
    let mut worker = stalled.clone();
    for value in 0..64 {
        tx.try_send(value).unwrap();
    }
    assert_eq!(stalled.try_recv(), Ok(0));
    drop(tx);

    for expected in 1..64 {
        assert_eq!(worker.recv(), Ok(expected));
    }
    assert_eq!(worker.recv(), Err(RecvError));
}

#[test]
fn one_prefetched_batch_reaches_every_waiting_receiver() {
    #[cfg(miri)]
    const RECEIVERS: usize = 2;
    #[cfg(not(miri))]
    const RECEIVERS: usize = 8;

    let (mut tx, rx0) = channel(RECEIVERS);
    let mut receivers = vec![rx0];
    for _ in 1..RECEIVERS {
        receivers.push(receivers[0].clone());
    }
    let barrier = Arc::new(Barrier::new(RECEIVERS + 1));

    let mut values = std::thread::scope(|scope| {
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
        for value in 0..RECEIVERS {
            tx.try_send(value).unwrap();
        }

        handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>()
    });

    values.sort_unstable();
    assert_eq!(values, (0..RECEIVERS).collect::<Vec<_>>());
}

#[test]
fn receiver_drop_racing_steal_preserves_every_value() {
    for _ in 0..RACE_ROUNDS {
        let drops = Arc::new((0..64).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let (mut tx, mut rx0) = channel(64);
        let rx1 = rx0.clone();
        for value in 0..64 {
            tx.try_send(TrackedValue {
                id: value,
                drops: drops.clone(),
            })
            .unwrap_or_else(|_| unreachable!());
        }
        let first = rx0.try_recv().unwrap();
        assert_eq!(first.id, 0);
        drop(tx);

        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = barrier.clone();
        let mut values = std::thread::scope(|scope| {
            let worker = scope.spawn(move || {
                worker_barrier.wait();
                rx1.into_iter().collect::<Vec<_>>()
            });
            barrier.wait();
            drop(rx0);
            worker.join().unwrap()
        });
        values.push(first);
        values.sort_unstable_by_key(|value| value.id);
        assert_eq!(
            values.iter().map(|value| value.id).collect::<Vec<_>>(),
            (0..64).collect::<Vec<_>>()
        );
        drop(values);
        assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    }
}

#[test]
fn staged_work_publication_never_reports_premature_disconnect() {
    for _ in 0..PUBLICATION_RACE_ROUNDS {
        let (mut tx, mut rx0) = channel(64);
        let mut rx1 = rx0.clone();
        for value in 0..64 {
            tx.try_send(value).unwrap();
        }
        drop(tx);

        let barrier = Arc::new(Barrier::new(3));
        let (mut rx0, first0, mut rx1, first1) = std::thread::scope(|scope| {
            let barrier0 = barrier.clone();
            let worker0 = scope.spawn(move || {
                barrier0.wait();
                let result = rx0.try_recv();
                (rx0, result)
            });
            let barrier1 = barrier.clone();
            let worker1 = scope.spawn(move || {
                barrier1.wait();
                let result = rx1.try_recv();
                (rx1, result)
            });
            barrier.wait();
            let (rx0, first0) = worker0.join().unwrap();
            let (rx1, first1) = worker1.join().unwrap();
            (rx0, first0, rx1, first1)
        });

        let mut terminal_seen = matches!(first0, Err(TryRecvError::Disconnected))
            || matches!(first1, Err(TryRecvError::Disconnected));
        let mut values = [first0, first1]
            .into_iter()
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        loop {
            let mut progress = false;
            for receiver in [&mut rx0, &mut rx1] {
                match receiver.try_recv() {
                    Ok(value) => {
                        assert!(!terminal_seen, "values appeared after disconnect");
                        values.push(value);
                        progress = true;
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => terminal_seen = true,
                }
            }
            if values.len() == 64 {
                break;
            }
            assert!(progress, "buffered values became unreachable");
        }

        values.sort_unstable();
        assert_eq!(values, (0..64).collect::<Vec<_>>());
        assert_eq!(rx0.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(rx1.try_recv(), Err(TryRecvError::Disconnected));
    }
}

#[test]
fn staged_work_publication_with_live_sender_never_reports_disconnect() {
    for _ in 0..RACE_ROUNDS {
        let (mut tx, mut rx0) = channel(64);
        let mut rx1 = rx0.clone();
        for value in 0..64 {
            tx.try_send(value).unwrap();
        }

        let barrier = Arc::new(Barrier::new(3));
        let (first0, first1) = std::thread::scope(|scope| {
            let barrier0 = barrier.clone();
            let worker0 = scope.spawn(move || {
                barrier0.wait();
                rx0.try_recv()
            });
            let barrier1 = barrier.clone();
            let worker1 = scope.spawn(move || {
                barrier1.wait();
                rx1.try_recv()
            });
            barrier.wait();
            (worker0.join().unwrap(), worker1.join().unwrap())
        });

        assert_ne!(first0, Err(TryRecvError::Disconnected));
        assert_ne!(first1, Err(TryRecvError::Disconnected));
        drop(tx);
    }
}
