//! MPMC work handoff between receivers: distinct takes, batch publication,
//! requeued lanes, and private prefetch staging.

use crate::support::model;
use fanring::mpmc;
use loom::thread;

#[test]
fn mpmc_two_receivers_take_distinct_messages() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        let mut rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        let first = thread::spawn(move || rx0.recv().unwrap());
        let second = thread::spawn(move || rx1.recv().unwrap());
        let mut values = [first.join().unwrap(), second.join().unwrap()];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
    });
}

#[test]
fn mpmc_batch_publication_after_disconnect_preserves_values() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        let mut rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);

        let first = thread::spawn(move || rx0.recv().unwrap());
        let second = thread::spawn(move || rx1.recv().unwrap());
        let mut values = [first.join().unwrap(), second.join().unwrap()];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
    });
}

#[test]
fn mpmc_multiple_batch_publishers_preserve_values() {
    model(|| {
        let (mut tx0, mut rx0) = mpmc::channel(2);
        let mut tx1 = tx0.try_clone().unwrap();
        let mut rx1 = rx0.clone();
        tx0.try_send(1).unwrap();
        tx0.try_send(2).unwrap();
        tx1.try_send(3).unwrap();
        tx1.try_send(4).unwrap();
        drop(tx0);
        drop(tx1);

        let first = thread::spawn(move || [rx0.recv().unwrap(), rx0.recv().unwrap()]);
        let second = thread::spawn(move || [rx1.recv().unwrap(), rx1.recv().unwrap()]);
        let mut values = [first.join().unwrap(), second.join().unwrap()].concat();
        values.sort_unstable();
        assert_eq!(values, [1, 2, 3, 4]);
    });
}

#[test]
fn mpmc_requeued_lane_and_staged_work_remain_reachable() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(4);
        let mut rx1 = rx0.clone();
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        drop(tx);

        let second = thread::spawn(move || {
            let value = rx1.recv().unwrap();
            (rx1, value)
        });
        let first_value = rx0.recv().unwrap();
        let (mut rx1, second_value) = second.join().unwrap();
        let mut values = vec![first_value, second_value];

        while values.len() != 4 {
            for receiver in [&mut rx0, &mut rx1] {
                match receiver.try_recv() {
                    Ok(value) => values.push(value),
                    Err(mpmc::TryRecvError::Empty) => thread::yield_now(),
                    Err(mpmc::TryRecvError::Disconnected) => {}
                }
            }
        }

        values.sort_unstable();
        assert_eq!(values, [0, 1, 2, 3]);
        assert_eq!(rx0.try_recv(), Err(mpmc::TryRecvError::Disconnected));
        assert_eq!(rx1.try_recv(), Err(mpmc::TryRecvError::Disconnected));
    });
}

#[test]
fn mpmc_receiver_drop_preserves_prefetched_work() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert_eq!(rx0.try_recv(), Ok(1));

        let mut rx1 = rx0.clone();
        drop(rx0);
        assert_eq!(rx1.recv(), Ok(2));
    });
}

#[test]
fn mpmc_clone_makes_private_prefetch_competitive() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx0.recv(), Ok(1));

        let mut rx1 = rx0.clone();
        let first = thread::spawn(move || rx0.recv());
        let second = thread::spawn(move || rx1.recv());
        let results = [first.join().unwrap(), second.join().unwrap()];

        assert_eq!(results.iter().filter(|result| result == &&Ok(2)).count(), 1);
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
fn mpmc_owner_drop_racing_private_prefetch_steal_preserves_work() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx0.recv(), Ok(1));

        let mut rx1 = rx0.clone();
        let thief = thread::spawn(move || rx1.recv());
        thread::yield_now();
        drop(rx0);

        assert_eq!(thief.join().unwrap(), Ok(2));
    });
}

#[test]
fn mpmc_last_competitor_drop_racing_prefetch_preserves_work() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        let rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);

        let dropper = thread::spawn(move || drop(rx1));
        assert_eq!(rx0.recv(), Ok(1));
        dropper.join().unwrap();

        let mut rx2 = rx0.clone();
        drop(rx0);
        assert_eq!(rx2.recv(), Ok(2));
        assert_eq!(rx2.try_recv(), Err(mpmc::TryRecvError::Disconnected));
    });
}

#[test]
fn mpmc_shared_to_private_staging_transition_preserves_work() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        let rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert_eq!(rx0.recv(), Ok(1));

        let dropper = thread::spawn(move || drop(rx1));
        dropper.join().unwrap();
        assert_eq!(rx0.recv(), Ok(2));

        tx.try_send(3).unwrap();
        tx.try_send(4).unwrap();
        drop(tx);
        assert_eq!(rx0.recv(), Ok(3));

        let mut rx2 = rx0.clone();
        drop(rx0);
        assert_eq!(rx2.recv(), Ok(4));
        assert_eq!(rx2.try_recv(), Err(mpmc::TryRecvError::Disconnected));
    });
}

#[test]
fn mpmc_unregister_register_race_preserves_staged_work() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        let rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx0.recv(), Ok(1));

        let dropper = thread::spawn(move || drop(rx1));
        let mut rx2 = rx0.clone();
        dropper.join().unwrap();
        drop(rx0);

        assert_eq!(rx2.recv(), Ok(2));
        assert_eq!(rx2.try_recv(), Err(mpmc::TryRecvError::Disconnected));
    });
}

#[test]
fn mpmc_owner_drop_and_two_thieves_preserve_shared_work() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        let mut rx1 = rx0.clone();
        let mut rx2 = rx0.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx0.recv(), Ok(1));

        let first = thread::spawn(move || rx1.recv());
        let second = thread::spawn(move || rx2.recv());
        thread::yield_now();
        drop(rx0);
        let results = [first.join().unwrap(), second.join().unwrap()];

        assert_eq!(results.iter().filter(|result| result == &&Ok(2)).count(), 1);
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
fn mpmc_receiver_drop_racing_steal_preserves_prefetched_work() {
    model(|| {
        let (mut tx, mut rx0) = mpmc::channel(2);
        let mut rx1 = rx0.clone();
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert_eq!(rx0.try_recv(), Ok(1));
        drop(tx);

        let receiver = thread::spawn(move || rx1.recv());
        thread::yield_now();
        drop(rx0);
        assert_eq!(receiver.join().unwrap(), Ok(2));
    });
}
