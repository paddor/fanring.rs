use std::time::Instant;

use fanring::mpmc::{
    RecvError, RecvTimeoutError, SendTimeoutError, TryRecvError, TrySendError, channel,
};

#[test]
fn cloned_receivers_distribute_messages() {
    let (mut tx, mut rx0) = channel(8);
    let mut rx1 = rx0.clone();
    for value in 0..8 {
        tx.try_send(value).unwrap();
    }

    let mut values = Vec::new();
    for _ in 0..4 {
        values.push(rx0.try_recv().unwrap());
        values.push(rx1.try_recv().unwrap());
    }
    values.sort_unstable();
    assert_eq!(values, (0..8).collect::<Vec<_>>());
}

#[test]
fn cloned_receiver_steals_prefetched_work() {
    let (mut tx, mut rx0) = channel(4);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(rx0.try_recv(), Ok(1));

    let mut rx1 = rx0.clone();
    assert_eq!(rx1.try_recv(), Ok(2));
}

#[test]
fn receiver_drop_preserves_prefetched_work() {
    let (mut tx, mut rx0) = channel(4);
    let mut rx1 = rx0.clone();
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(rx0.try_recv(), Ok(1));
    drop(rx0);
    assert_eq!(rx1.recv(), Ok(2));
}

#[test]
fn last_receiver_drop_disconnects_senders() {
    let (mut tx, rx0) = channel(1);
    let rx1 = rx0.clone();
    drop(rx0);
    assert_eq!(tx.try_send(1), Ok(()));
    drop(rx1);
    assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));
}

#[test]
fn blocking_receivers_wake_for_distinct_messages() {
    let (mut tx, rx0) = channel(2);
    let rx1 = rx0.clone();
    std::thread::scope(|scope| {
        let a = scope.spawn(move || {
            let mut rx = rx0;
            rx.recv()
        });
        let b = scope.spawn(move || {
            let mut rx = rx1;
            rx.recv()
        });
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let mut values = [a.join().unwrap().unwrap(), b.join().unwrap().unwrap()];
        values.sort_unstable();
        assert_eq!(values, [1, 2]);
    });
}

#[test]
fn dynamic_senders_share_receivers() {
    let (mut tx0, mut rx0) = channel(2);
    let mut tx1 = tx0.try_clone().unwrap();
    let mut rx1 = rx0.clone();
    tx0.try_send(1).unwrap();
    tx1.try_send(2).unwrap();
    let mut values = [rx0.try_recv().unwrap(), rx1.try_recv().unwrap()];
    values.sort_unstable();
    assert_eq!(values, [1, 2]);
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
    let rx1 = rx0.clone();
    let (other_tx, other_rx) = channel::<u8>(1);

    assert!(tx0.same_channel(&tx1));
    assert!(!tx0.same_channel(&other_tx));
    assert!(rx0.same_channel(&rx1));
    assert!(!rx0.same_channel(&other_rx));
    assert_eq!(tx0.sender_count(), 2);
    assert_eq!(rx0.sender_count(), 2);
    assert_eq!(tx0.receiver_count(), 2);
    assert_eq!(rx0.receiver_count(), 2);

    drop(tx1);
    assert_eq!(tx0.sender_count(), 1);
    drop(rx1);
    assert_eq!(tx0.receiver_count(), 1);
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
fn receiver_reports_sender_disconnect_before_buffer_is_drained() {
    let (mut tx, mut rx) = channel(2);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    drop(tx);

    assert!(rx.is_disconnected());
    assert_eq!(rx.recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.recv(), Err(RecvError));
}

#[test]
fn blocking_iterators_stay_exhausted_after_disconnect() {
    let (mut tx, mut rx) = channel(1);
    tx.try_send(1).unwrap();
    drop(tx);

    let mut iter = rx.iter();
    assert_eq!(iter.next(), Some(1));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
}

#[test]
fn competing_receiver_may_observe_transient_empty() {
    let (mut tx, mut rx0) = channel(64);
    let mut rx1 = rx0.clone();
    for value in 0..64 {
        tx.try_send(value).unwrap();
    }

    assert_eq!(rx0.try_recv(), Ok(0));
    assert!(matches!(rx1.try_recv(), Ok(_) | Err(TryRecvError::Empty)));
}

#[test]
fn zero_sized_and_highly_aligned_values_round_trip() {
    #[derive(Debug, PartialEq, Eq)]
    struct ZeroSized;

    #[repr(align(256))]
    #[derive(Debug, PartialEq, Eq)]
    struct Aligned(usize);

    let (mut tx, mut rx) = channel(2);
    tx.try_send(ZeroSized).unwrap();
    tx.try_send(ZeroSized).unwrap();
    assert_eq!(rx.try_recv(), Ok(ZeroSized));
    assert_eq!(rx.try_recv(), Ok(ZeroSized));

    let (mut tx, mut rx) = channel(2);
    tx.try_send(Aligned(1)).unwrap();
    tx.try_send(Aligned(2)).unwrap();
    assert_eq!(rx.try_recv(), Ok(Aligned(1)));
    assert_eq!(rx.try_recv(), Ok(Aligned(2)));
}

#[test]
fn non_copy_values_survive_receiver_handoff() {
    let (mut tx, mut rx0) = channel(4);
    tx.try_send(String::from("first")).unwrap();
    tx.try_send(String::from("second")).unwrap();
    assert_eq!(rx0.try_recv().as_deref(), Ok("first"));

    let mut rx1 = rx0.clone();
    drop(rx0);
    assert_eq!(rx1.recv().as_deref(), Ok("second"));
}
