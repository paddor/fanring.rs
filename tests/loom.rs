#![cfg(all(loom, target_pointer_width = "64"))]

use fanring::mpmc;
use fanring::mpsc::{RecvError, SendError, TryRecvError, TryRegisterError, TrySendError, channel};
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

fn model(check: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    if std::env::var_os("LOOM_MAX_BRANCHES").is_none() {
        builder.max_branches = 10_000;
    }
    if std::env::var_os("LOOM_MAX_PERMUTATIONS").is_none() {
        builder.max_permutations = Some(10_000);
    }
    if std::env::var_os("LOOM_MAX_PREEMPTIONS").is_none() {
        builder.preemption_bound = Some(2);
    }
    builder.check(check);
}

#[test]
fn send_recv_ready_race_does_not_lose_message() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));

        let sent = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let sent_for_sender = sent.clone();
        let done_for_sender = done.clone();

        let sender = thread::spawn(move || {
            tx.try_send(2).unwrap();
            sent_for_sender.store(true, Ordering::Release);
            while !done_for_sender.load(Ordering::Acquire) {
                thread::yield_now();
            }
        });

        let mut received = false;
        for _ in 0..2 {
            match rx.try_recv() {
                Ok(2) => {
                    received = true;
                    break;
                }
                Ok(other) => panic!("unexpected value {other}"),
                Err(TryRecvError::Empty) => thread::yield_now(),
                Err(TryRecvError::Disconnected) => panic!("sender still alive"),
            }
        }

        while !sent.load(Ordering::Acquire) {
            thread::yield_now();
        }

        if !received {
            assert_eq!(rx.try_recv(), Ok(2));
        }

        done.store(true, Ordering::Release);
        sender.join().unwrap();
    });
}

#[test]
fn receiver_drop_makes_sender_disconnect() {
    model(|| {
        let (mut tx, rx) = channel(1);
        tx.try_send(1).unwrap();

        let sender = thread::spawn(move || {
            let mut value = 2;
            loop {
                match tx.try_send(value) {
                    Ok(()) => return,
                    Err(TrySendError::Full(returned)) => {
                        value = returned;
                        thread::yield_now();
                    }
                    Err(TrySendError::Disconnected(returned)) => {
                        assert_eq!(returned, 2);
                        return;
                    }
                }
            }
        });

        drop(rx);
        sender.join().unwrap();
    });
}

#[test]
fn second_lane_ready_race_does_not_lose_message() {
    model(|| {
        let (mut tx0, mut rx) = channel(1);
        let mut tx1 = tx0.try_clone().unwrap();

        tx0.try_send(10).unwrap();
        assert_eq!(rx.try_recv(), Ok(10));

        let sender1 = thread::spawn(move || {
            tx1.try_send(20).unwrap();
        });

        let mut received = false;
        match rx.try_recv() {
            Ok(20) => received = true,
            Ok(other) => panic!("unexpected value {other}"),
            Err(TryRecvError::Empty) => thread::yield_now(),
            Err(TryRecvError::Disconnected) => panic!("senders still alive"),
        }

        sender1.join().unwrap();

        if !received {
            assert_eq!(rx.try_recv(), Ok(20));
        }
        drop(tx0);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    });
}

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
fn sender_drop_during_receiver_drain_preserves_buffered_items() {
    model(|| {
        let (mut tx, mut rx) = channel(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        let sender = thread::spawn(move || {
            thread::yield_now();
            drop(tx);
        });

        assert_eq!(rx.try_recv(), Ok(1));
        sender.join().unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
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

#[test]
fn blocking_recv_does_not_lose_data_wakeup() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        let sender = thread::spawn(move || tx.send(1));

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
    });
}

#[test]
fn blocking_send_does_not_lose_space_wakeup() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || tx.send(2));

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(sender.join().unwrap(), Ok(()));
        assert_eq!(rx.recv(), Ok(2));
    });
}

#[test]
fn blocking_send_recv_do_not_deadlock_while_sender_stays_alive() {
    model(|| {
        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let sender_done = done.clone();
        let sender = thread::spawn(move || {
            tx.send(2).unwrap();
            while !sender_done.load(Ordering::Acquire) {
                thread::yield_now();
            }
        });

        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        done.store(true, Ordering::Release);
        sender.join().unwrap();
    });
}

#[test]
fn blocking_recv_wakes_on_sender_disconnect() {
    model(|| {
        let (tx, mut rx) = channel::<u8>(1);
        let sender = thread::spawn(move || drop(tx));

        assert_eq!(rx.recv(), Err(RecvError));
        sender.join().unwrap();
    });
}

#[test]
fn blocking_send_wakes_on_receiver_disconnect() {
    model(|| {
        let (mut tx, rx) = channel(1);
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || tx.send(2));

        drop(rx);
        assert_eq!(sender.join().unwrap(), Err(SendError(2)));
    });
}

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
