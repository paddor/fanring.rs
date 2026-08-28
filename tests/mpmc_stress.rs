use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fanring::mpmc::{RecvError, SendError, channel};

#[cfg(miri)]
const MESSAGES_PER_SENDER: usize = 16;
#[cfg(not(miri))]
const MESSAGES_PER_SENDER: usize = 5_000;

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn many_senders_many_receivers_deliver_exactly_once() {
    let sender_count = 4;
    let receiver_count = 4;
    let total = sender_count * MESSAGES_PER_SENDER;
    let seen = Arc::new((0..total).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let (tx0, rx0) = channel(64);
    let mut senders = vec![tx0];
    for _ in 1..sender_count {
        senders.push(senders[0].try_clone().unwrap());
    }
    let mut receivers = vec![rx0];
    for _ in 1..receiver_count {
        receivers.push(receivers[0].clone());
    }

    std::thread::scope(|scope| {
        for (sender_id, mut tx) in senders.into_iter().enumerate() {
            scope.spawn(move || {
                for seq in 0..MESSAGES_PER_SENDER {
                    tx.send(sender_id * MESSAGES_PER_SENDER + seq).unwrap();
                }
            });
        }
        for mut rx in receivers {
            let seen = seen.clone();
            scope.spawn(move || {
                while let Ok(value) = rx.recv() {
                    assert_eq!(seen[value].fetch_add(1, Ordering::Relaxed), 0);
                }
            });
        }
    });

    assert!(seen.iter().all(|count| count.load(Ordering::Relaxed) == 1));
}

#[test]
fn non_copy_items_drop_once_across_receiver_handoff() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx0) = channel(64);
    let rx1 = rx0.clone();
    for _ in 0..64 {
        tx.try_send(DropCount(drops.clone()))
            .unwrap_or_else(|_| unreachable!());
    }

    drop(rx0.try_recv().unwrap());
    drop(rx0);
    drop(rx1);
    drop(tx);

    assert_eq!(drops.load(Ordering::Relaxed), 64);
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
fn busy_lane_cannot_hide_ready_lane() {
    let (mut busy, mut rx) = channel(128);
    let mut sparse = busy.try_clone().unwrap();
    for value in 0..128 {
        busy.try_send((0, value)).unwrap();
    }
    sparse.try_send((1, 0)).unwrap();

    assert!((0..=64).any(|_| rx.try_recv().unwrap().0 == 1));
}

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
