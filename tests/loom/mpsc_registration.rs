//! MPSC sender registration racing receives, receiver drop, topology
//! growth, and lane reuse.

use crate::support::model;
use fanring::mpsc::{RecvError, TryRecvError, TryRegisterError, TrySendError, channel};
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

#[test]
fn registration_racing_ready_page_drain_does_not_lose_new_lane() {
    model(|| {
        let (mut root, mut rx) = channel(1);
        root.try_send(1).unwrap();

        let registrar = thread::spawn(move || {
            let mut child = root.try_clone().unwrap();
            child.try_send(2).unwrap();
            drop(child);
            drop(root);
        });

        let mut values = [rx.recv().unwrap(), rx.recv().unwrap()];
        registrar.join().unwrap();
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
        assert_eq!(rx.recv(), Err(RecvError));
    });
}

#[test]
fn registration_racing_group_drain_does_not_lose_new_page() {
    model(|| {
        let (mut root, mut rx) = channel(1);
        let filler = root.try_clone().unwrap();
        root.try_send(1).unwrap();

        let registrar = thread::spawn(move || {
            let mut child = root.try_clone().unwrap();
            assert_eq!(child.lane_id(), 2);
            child.try_send(2).unwrap();
            drop(child);
            drop(filler);
            drop(root);
        });

        let mut values = [rx.recv().unwrap(), rx.recv().unwrap()];
        registrar.join().unwrap();
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
        assert_eq!(rx.recv(), Err(RecvError));
    });
}

#[test]
fn register_race_with_receiver_drop_reports_disconnected() {
    model(|| {
        let (mut tx, rx) = channel::<u8>(1);

        let dropper = thread::spawn(move || {
            thread::yield_now();
            drop(rx);
        });

        let registered = tx.try_register();
        dropper.join().unwrap();

        match registered {
            Ok(mut tx1) => {
                assert_eq!(tx1.try_send(1), Err(TrySendError::Disconnected(1)));
            }
            Err(TryRegisterError::Disconnected) => {}
        }

        assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));
    });
}

#[test]
fn receiver_refreshes_topology_after_new_ready_group_is_added() {
    model(|| {
        let (root, mut rx) = channel(1);
        let existing = (0..3)
            .map(|_| root.try_clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        let started = Arc::new(AtomicBool::new(false));
        let receiver_started = started.clone();
        let receiver = thread::spawn(move || {
            receiver_started.store(true, Ordering::Release);
            let result = rx.recv();
            (rx, result)
        });
        while !started.load(Ordering::Acquire) {
            thread::yield_now();
        }

        let mut newest = root.try_clone().unwrap();
        assert_eq!(newest.lane_id(), 4);
        newest.send(42).unwrap();
        drop(newest);
        drop(existing);
        drop(root);

        let (mut rx, result) = receiver.join().unwrap();
        assert_eq!(result, Ok(42));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    });
}

#[test]
fn reused_mpsc_lane_ignores_stale_generation_state() {
    model(|| {
        let (root, mut rx) = channel(1);
        let mut retired = root.try_clone().unwrap();
        let reused_slot = retired.lane_id();
        retired.try_send(1).unwrap();
        drop(retired);

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        let mut reused = root.try_clone().unwrap();
        assert_eq!(reused.lane_id(), reused_slot);
        let sender = thread::spawn(move || {
            reused.try_send(2).unwrap();
            drop(reused);
        });

        assert_eq!(rx.recv(), Ok(2));
        sender.join().unwrap();
        drop(root);
        assert_eq!(rx.recv(), Err(RecvError));
    });
}

#[test]
fn empty_short_lived_mpsc_lane_cannot_delay_disconnect() {
    model(|| {
        let (root, mut rx) = channel::<usize>(1);
        let registrar = thread::spawn(move || {
            let child = root.try_clone().unwrap();
            drop(child);
            drop(root);
        });

        assert_eq!(rx.recv(), Err(RecvError));
        registrar.join().unwrap();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    });
}
