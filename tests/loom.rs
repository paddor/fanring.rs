#![cfg(all(loom, target_pointer_width = "64"))]

use fanring::mpmc;
use fanring::mpsc::{RecvError, SendError, TryRecvError, TryRegisterError, TrySendError, channel};
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;

fn model(check: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.max_branches = 10_000;
    builder.max_permutations = Some(10_000);
    builder.preemption_bound = Some(2);
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
fn mpmc_disconnect_wakes_receiver() {
    model(|| {
        let (tx, mut rx) = mpmc::channel::<u8>(1);
        let receiver = thread::spawn(move || rx.recv());

        drop(tx);
        assert_eq!(receiver.join().unwrap(), Err(mpmc::RecvError));
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
