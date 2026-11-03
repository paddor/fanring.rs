use super::{MultiWaitCell, Park, WAITING, WaitCell};
use crate::compat::{Ordering, lock};
use std::sync::mpsc::sync_channel;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn notify_does_not_wait_for_a_registered_recheck() {
    let cell = WaitCell::new();
    let (done_tx, done_rx) = sync_channel(1);
    std::thread::scope(|scope| {
        let registration = cell.prepare();
        let notifier = &cell;
        scope.spawn(move || {
            notifier.notify();
            done_tx.send(()).unwrap();
        });
        let finished = done_rx.recv_timeout(TIMEOUT).is_ok();
        let timed_out = registration.wait_timeout(TIMEOUT);
        assert!(finished, "notify waited for the registered recheck");
        assert!(!timed_out, "wait missed a notify that raced its recheck");
    });
}

#[test]
fn multi_notify_one_does_not_wait_for_a_registered_recheck() {
    let cell = MultiWaitCell::new();
    let (done_tx, done_rx) = sync_channel(1);
    std::thread::scope(|scope| {
        let registration = cell.prepare();
        let notifier = &cell;
        scope.spawn(move || {
            notifier.notify_one();
            done_tx.send(()).unwrap();
        });
        let finished = done_rx.recv_timeout(TIMEOUT).is_ok();
        let timed_out = registration.wait_timeout(TIMEOUT);
        assert!(finished, "notify_one waited for the registered recheck");
        assert!(
            !timed_out,
            "wait missed a notify_one that raced its recheck"
        );
    });
}

#[test]
fn multi_notify_all_does_not_wait_for_a_registered_recheck() {
    let cell = MultiWaitCell::new();
    let (done_tx, done_rx) = sync_channel(1);
    std::thread::scope(|scope| {
        let registration = cell.prepare();
        let notifier = &cell;
        scope.spawn(move || {
            notifier.notify_all();
            done_tx.send(()).unwrap();
        });
        let finished = done_rx.recv_timeout(TIMEOUT).is_ok();
        let timed_out = registration.wait_timeout(TIMEOUT);
        assert!(finished, "notify_all waited for the registered recheck");
        assert!(
            !timed_out,
            "wait missed a notify_all that raced its recheck"
        );
    });
}

#[test]
fn finished_registrations_leave_no_waiter_behind() {
    let cell = WaitCell::new();
    drop(cell.prepare());
    cell.prepare().wait_timeout(Duration::ZERO);
    assert_eq!(cell.state.load(Ordering::Acquire) & WAITING, 0);

    let cell = MultiWaitCell::new();
    drop(cell.prepare());
    cell.prepare().wait_timeout(Duration::ZERO);
    assert_eq!(*lock(&cell.mutex), 0);
    assert_eq!(cell.state.0.load(Ordering::Acquire) & WAITING, 0);
}
