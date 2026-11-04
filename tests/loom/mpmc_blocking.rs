//! MPMC blocking send and receive wakeups for data, space, and disconnect.

use crate::support::model;
use fanring::mpmc;
use loom::thread;

#[test]
fn mpmc_two_blocked_receivers_wake_for_distinct_messages() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(1);
        let mut rx1 = rx0.clone();

        let first = thread::spawn(move || rx0.recv());
        let second = thread::spawn(move || rx1.recv());
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);

        let mut values = [
            first.join().unwrap().unwrap(),
            second.join().unwrap().unwrap(),
        ];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
    });
}

#[test]
fn mpmc_disconnect_wakes_receiver() {
    model(|| {
        let (tx, mut rx) = mpmc::channel::<u8>(1);
        let receiver = thread::spawn(move || rx.recv());

        drop(tx);
        assert_eq!(receiver.join().unwrap(), Err(mpmc::RecvError));
    });
}

#[test]
fn mpmc_buffered_last_sender_drop_wakes_all_receivers() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(1);
        let mut rx1 = rx0.clone();
        tx.try_send(7).unwrap();

        let first = thread::spawn(move || rx0.recv());
        let second = thread::spawn(move || rx1.recv());
        drop(tx);

        let results = [first.join().unwrap(), second.join().unwrap()];
        assert_eq!(results.iter().filter(|result| result == &&Ok(7)).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| result == &&Err(mpmc::RecvError))
                .count(),
            1
        );
    });
}

#[test]
fn mpmc_blocking_send_does_not_lose_space_wakeup() {
    model(|| {
        let (mut tx, mut rx) = mpmc::channel(1);
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || tx.send(2));

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
        assert_eq!(rx.recv(), Ok(2));
    });
}

#[test]
fn mpmc_blocking_send_wakes_on_last_receiver_drop() {
    model(|| {
        let (mut tx, rx0) = mpmc::channel(1);
        let rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || tx.send(2));

        drop(rx0);
        drop(rx1);
        assert_eq!(sender.join().unwrap(), Err(mpmc::SendError(2)));
    });
}

#[test]
fn mpmc_blocked_sender_survives_nonfinal_receiver_drop() {
    model(|| {
        let (mut tx, rx0) = mpmc::channel(1);
        let mut rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || tx.send(2));

        drop(rx0);
        assert_eq!(rx1.recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
        assert_eq!(rx1.recv(), Ok(2));
        assert_eq!(rx1.recv(), Err(mpmc::RecvError));
    });
}
