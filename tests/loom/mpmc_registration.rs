//! MPMC sender registration racing receiver drop, topology growth, and
//! lane reuse.

use crate::support::model;
use fanring::mpmc;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

#[test]
fn reused_mpmc_lane_ignores_stale_requeue_state() {
    model(|| {
        let (root, mut rx) = mpmc::channel(4);
        let mut retired = root.try_clone().unwrap();
        let reused_slot = retired.lane_id();
        for value in 0..3 {
            retired.try_send(value).unwrap();
        }
        drop(retired);

        assert_eq!(rx.recv(), Ok(0));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(mpmc::TryRecvError::Empty));

        let mut reused = root.try_clone().unwrap();
        assert_eq!(reused.lane_id(), reused_slot);
        let sender = thread::spawn(move || {
            reused.try_send(3).unwrap();
            drop(reused);
        });

        assert_eq!(rx.recv(), Ok(3));
        sender.join().unwrap();
        drop(root);
        assert_eq!(rx.recv(), Err(mpmc::RecvError));
    });
}

#[test]
fn empty_short_lived_mpmc_lane_cannot_delay_disconnect() {
    model(|| {
        let (root, mut rx) = mpmc::channel::<usize>(1);
        let registrar = thread::spawn(move || {
            let child = root.try_clone().unwrap();
            drop(child);
            drop(root);
        });

        assert_eq!(rx.recv(), Err(mpmc::RecvError));
        registrar.join().unwrap();
        assert_eq!(rx.try_recv(), Err(mpmc::TryRecvError::Disconnected));
    });
}

#[test]
fn mpmc_register_race_with_last_receiver_drop_reports_disconnected() {
    model(|| {
        let (mut tx, rx0) = mpmc::channel::<u8>(1);
        let rx1 = rx0.clone();
        drop(rx0);

        let dropper = thread::spawn(move || {
            thread::yield_now();
            drop(rx1);
        });
        let registered = tx.try_register();
        dropper.join().unwrap();

        match registered {
            Ok(mut tx1) => {
                assert_eq!(tx1.try_send(1), Err(mpmc::TrySendError::Disconnected(1)));
            }
            Err(mpmc::TryRegisterError::Disconnected) => {}
        }
        assert_eq!(tx.try_send(2), Err(mpmc::TrySendError::Disconnected(2)));
    });
}

#[test]
fn mpmc_receiver_refreshes_topology_after_new_ready_group_is_added() {
    model(|| {
        let (root, mut rx) = mpmc::channel(1);
        let existing = (0..3)
            .map(|_| root.try_clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rx.try_recv(), Err(mpmc::TryRecvError::Empty));

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
        assert_eq!(rx.recv(), Err(mpmc::RecvError));
    });
}
