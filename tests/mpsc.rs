use std::time::{Duration, Instant};

use fanring::mpsc::{
    ChannelError, MAX_CAPACITY_PER_SENDER, RecvError, RecvTimeoutError, SendError,
    SendTimeoutError, TryRecvError, TryRegisterError, TrySendError, channel, try_channel,
};

#[test]
fn basic_send_recv() {
    let (mut tx, mut rx) = channel(4);

    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn blocking_send_waits_for_lane_capacity() {
    let (mut tx, mut rx) = channel(1);
    tx.try_send(1).unwrap();

    std::thread::scope(|scope| {
        let sender = scope.spawn(move || tx.send(2));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(sender.join().unwrap(), Ok(()));
    });
}

#[test]
fn blocking_recv_wakes_for_data_and_disconnect() {
    let (mut tx, mut rx) = channel(1);
    std::thread::scope(|scope| {
        scope.spawn(move || tx.send(7).unwrap());
        assert_eq!(rx.recv(), Ok(7));
    });

    assert_eq!(rx.recv(), Err(RecvError));
}

#[test]
fn blocking_send_wakes_when_receiver_drops() {
    let (mut tx, rx) = channel(1);
    tx.try_send(1).unwrap();

    std::thread::scope(|scope| {
        let sender = scope.spawn(move || tx.send(2));
        drop(rx);
        assert_eq!(sender.join().unwrap(), Err(SendError(2)));
    });
}

#[test]
fn blocking_timeouts_report_unsatisfied_operation() {
    let (mut tx, mut rx) = channel(1);
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(1)),
        Err(RecvTimeoutError::Timeout)
    );

    tx.try_send(1).unwrap();
    assert_eq!(
        tx.send_timeout(2, Duration::from_millis(1)),
        Err(SendTimeoutError::Timeout(2))
    );
    drop(rx);
    assert_eq!(
        tx.send_timeout(3, Duration::ZERO),
        Err(SendTimeoutError::Disconnected(3))
    );
}

#[test]
fn blocking_timeouts_succeed_after_wakeup() {
    let (mut tx, mut rx) = channel(1);
    std::thread::scope(|scope| {
        scope.spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            tx.send(1).unwrap();
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)), Ok(1));
    });

    let (mut tx, mut rx) = channel(1);
    tx.try_send(1).unwrap();
    std::thread::scope(|scope| {
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
        scope.spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            assert_eq!(rx.recv(), Ok(1));
            done_rx.recv().unwrap();
        });
        assert_eq!(tx.send_timeout(2, Duration::from_secs(1)), Ok(()));
        done_tx.send(()).unwrap();
    });
}

#[test]
fn deadlines_report_unsatisfied_operation() {
    let (mut tx, mut rx) = channel(1);
    assert_eq!(
        rx.recv_deadline(Instant::now()),
        Err(RecvTimeoutError::Timeout)
    );

    tx.try_send(1).unwrap();
    assert_eq!(
        tx.send_deadline(2, Instant::now()),
        Err(SendTimeoutError::Timeout(2))
    );
}

#[test]
fn endpoint_counts_and_identity_are_exposed() {
    let (tx0, rx0) = channel::<u8>(1);
    let tx1 = tx0.try_clone().unwrap();
    let (other_tx, other_rx) = channel::<u8>(1);

    assert!(tx0.same_channel(&tx1));
    assert!(!tx0.same_channel(&other_tx));
    assert!(rx0.same_channel(&rx0));
    assert!(!rx0.same_channel(&other_rx));
    assert_eq!(tx0.sender_count(), 2);
    assert_eq!(rx0.sender_count(), 2);
    assert_eq!(tx0.receiver_count(), 1);
    assert_eq!(rx0.receiver_count(), 1);

    drop(tx1);
    assert_eq!(tx0.sender_count(), 1);
    drop(rx0);
    assert_eq!(tx0.receiver_count(), 0);
    assert!(tx0.is_disconnected());
}

#[test]
fn receiver_iterators_cover_blocking_and_ready_values() {
    let (mut tx, mut rx) = channel(4);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(rx.try_iter().collect::<Vec<_>>(), [1, 2]);

    tx.try_send(3).unwrap();
    tx.try_send(4).unwrap();
    drop(tx);
    assert_eq!(rx.iter().collect::<Vec<_>>(), [3, 4]);

    let (mut tx, rx) = channel(1);
    tx.try_send(5).unwrap();
    drop(tx);
    assert_eq!(rx.into_iter().collect::<Vec<_>>(), [5]);
}

#[test]
fn try_channel_validates_config() {
    assert_eq!(
        try_channel::<u8>(0).unwrap_err(),
        ChannelError::ZeroCapacity
    );
    assert_eq!(
        try_channel::<u8>(MAX_CAPACITY_PER_SENDER + 1).unwrap_err(),
        ChannelError::CapacityTooLarge {
            requested: MAX_CAPACITY_PER_SENDER + 1,
            max: MAX_CAPACITY_PER_SENDER,
        }
    );
}

#[test]
fn capacity_reports_yring_rounding() {
    let (tx, rx) = try_channel::<u8>(3).unwrap();

    assert_eq!(tx.capacity(), 4);
    assert_eq!(rx.capacity_per_sender(), 4);
}

#[test]
fn try_register_reports_failure_cause() {
    let (tx, rx) = channel::<u8>(1);
    assert!(tx.try_register().is_ok());
    drop(rx);
    assert_eq!(
        tx.try_register().unwrap_err(),
        TryRegisterError::Disconnected
    );
    assert!(tx.try_clone().is_none());
}

#[test]
fn public_errors_implement_std_error() {
    fn assert_error<E: std::error::Error>() {}

    assert_error::<ChannelError>();
    assert_error::<TryRegisterError>();
    assert_error::<SendError<u8>>();
    assert_error::<TrySendError<u8>>();
    assert_error::<SendTimeoutError<u8>>();
    assert_error::<RecvError>();
    assert_error::<TryRecvError>();
    assert_error::<RecvTimeoutError>();

    assert_eq!(
        ChannelError::ZeroCapacity.to_string(),
        "capacity_per_sender must be > 0"
    );
    assert_eq!(TrySendError::Full(1).to_string(), "sender ring is full");
    assert_eq!(TryRecvError::Empty.to_string(), "channel is empty");
}

#[test]
fn per_sender_fifo_relaxed_across_senders() {
    let (mut tx0, mut rx) = channel(8);
    let mut tx1 = tx0.try_clone().unwrap();

    for i in 0..4 {
        tx0.try_send((0, i)).unwrap();
        tx1.try_send((1, i)).unwrap();
    }

    let mut seen = [0, 0];
    for _ in 0..8 {
        let (sender, seq) = rx.try_recv().unwrap();
        assert_eq!(seq, seen[sender]);
        seen[sender] += 1;
    }
    assert_eq!(seen, [4, 4]);
}

#[test]
fn sender_registration_has_no_fixed_limit() {
    let (tx0, mut rx) = channel::<usize>(4);
    let mut senders = Vec::new();
    for _ in 0..129 {
        senders.push(tx0.try_clone().expect("dynamic sender lane"));
    }

    for (i, tx) in senders.iter_mut().enumerate() {
        tx.try_send(i).unwrap();
    }

    let mut seen = vec![false; senders.len()];
    for _ in 0..senders.len() {
        seen[rx.try_recv().unwrap()] = true;
    }
    assert!(seen.into_iter().all(|value| value));
}

#[test]
fn full_is_per_sender() {
    let (mut tx0, mut rx) = channel(2);
    let mut tx1 = tx0.try_clone().unwrap();

    tx0.try_send(1).unwrap();
    tx0.try_send(2).unwrap();
    assert_eq!(tx0.try_send(3), Err(TrySendError::Full(3)));

    tx1.try_send(10).unwrap();

    let mut values = [
        rx.try_recv().unwrap(),
        rx.try_recv().unwrap(),
        rx.try_recv().unwrap(),
    ];
    values.sort();
    assert_eq!(values, [1, 2, 10]);
}

#[test]
fn drained_sender_lane_is_reused() {
    let (tx0, mut rx) = channel::<u8>(2);
    let old_slot = {
        let tx1 = tx0.try_clone().unwrap();
        tx1.lane_id()
    };

    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    let mut tx2 = tx0.try_clone().unwrap();
    assert_eq!(tx2.lane_id(), old_slot);
    tx2.try_send(7).unwrap();
    assert_eq!(rx.try_recv(), Ok(7));
}

#[test]
fn receive_releases_capacity_when_prefetched_batch_drains() {
    let (mut tx, mut rx) = channel(4);

    for i in 0..4 {
        tx.try_send(i).unwrap();
    }

    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
    tx.try_send(4).unwrap();
    assert_eq!(rx.try_recv(), Ok(4));
}

#[test]
fn receive_releases_capacity_at_local_batch_limit() {
    let (mut tx, mut rx) = channel(128);

    for i in 0..128 {
        tx.try_send(i).unwrap();
    }

    for i in 0..63 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(tx.try_send(128), Err(TrySendError::Full(128)));

    assert_eq!(rx.try_recv(), Ok(63));
    tx.try_send(128).unwrap();

    for i in 64..128 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
    assert_eq!(rx.try_recv(), Ok(128));
}

#[test]
fn empty_lane_releases_partial_credit_batch() {
    let (mut tx, mut rx) = channel(8);
    tx.try_send(0).unwrap();
    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    for value in 1..=8 {
        tx.try_send(value).unwrap();
    }
}

#[test]
fn disconnects_after_draining() {
    let (mut tx, mut rx) = channel(4);
    tx.try_send(7).unwrap();
    drop(tx);

    assert_eq!(rx.try_recv(), Ok(7));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn send_fails_after_receiver_drop() {
    let (mut tx, rx) = channel(4);
    drop(rx);

    assert_eq!(tx.try_send(7), Err(TrySendError::Disconnected(7)));
}

#[test]
fn cross_thread_many_senders() {
    let threads = 4;
    let per_thread = 1_000;
    let (tx0, mut rx) = channel(64);
    let mut senders = vec![tx0];
    for _ in 1..threads {
        let tx = senders[0].try_clone().unwrap();
        senders.push(tx);
    }

    std::thread::scope(|scope| {
        for (sender_id, mut tx) in senders.into_iter().enumerate() {
            scope.spawn(move || {
                for seq in 0..per_thread {
                    let mut value = (sender_id, seq);
                    loop {
                        match tx.try_send(value) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                value = returned;
                                std::thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => panic!("receiver dropped"),
                        }
                    }
                }
            });
        }

        let mut seen = vec![0usize; threads];
        let mut received = 0;
        while received < threads * per_thread {
            match rx.try_recv() {
                Ok((sender_id, seq)) => {
                    assert_eq!(seq, seen[sender_id]);
                    seen[sender_id] += 1;
                    received += 1;
                }
                Err(TryRecvError::Empty) => std::thread::yield_now(),
                Err(TryRecvError::Disconnected) => panic!("senders disconnected early"),
            }
        }
        assert_eq!(seen, vec![per_thread; threads]);
    });
}
