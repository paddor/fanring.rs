//! Switching receive methods in the middle of a lane burst keeps slot
//! release, rotation, and FIFO order exact.
//!
//! A lane with capacity 128 has a release batch of 64 and the receiver
//! rotates lanes every 64 values.

use fanring::mpsc::{Receiver, Sender, TryRecvError, TrySendError, channel};

/// Lane capacity whose release batch and burst limit are both 64.
const CAPACITY: u64 = 128;

/// A value tagged with its lane: 0 for busy, 1 for sparse.
type Tagged = (u8, u64);

/// One sender with a full lane.
fn full_lane() -> (Sender<u64>, Receiver<u64>) {
    let (mut tx, rx) = channel(CAPACITY as usize);
    for value in 0..CAPACITY {
        tx.try_send(value).unwrap();
    }
    (tx, rx)
}

/// A busy lane with `CAPACITY` values, queued ahead of a sparse lane holding
/// one value.
fn busy_and_sparse() -> (Sender<Tagged>, Sender<Tagged>, Receiver<Tagged>) {
    let (mut busy, rx) = channel(CAPACITY as usize);
    let mut sparse = busy.try_clone().unwrap();
    for sequence in 0..CAPACITY {
        busy.try_send((0, sequence)).unwrap();
    }
    sparse.try_send((1, 0)).unwrap();
    (busy, sparse, rx)
}

#[test]
fn single_receive_mid_burst_keeps_the_release_batch_boundary() {
    for blocking in [false, true] {
        let (mut tx, mut rx) = full_lane();
        let (lane, first) = rx.with_lane_ids().try_recv().unwrap();
        assert_eq!(first, 0);
        for value in 1..10 {
            assert_eq!(rx.try_recv(), Ok(value));
        }
        let middle = if blocking {
            rx.recv().unwrap()
        } else {
            rx.try_recv_from(&lane).unwrap()
        };
        assert_eq!(middle, 10);
        for value in 11..63 {
            assert_eq!(rx.try_recv(), Ok(value));
        }

        // 63 values consumed: one short of the release batch.
        assert_eq!(tx.try_send(CAPACITY), Err(TrySendError::Full(CAPACITY)));
        assert_eq!(rx.try_recv(), Ok(63));
        tx.try_send(CAPACITY).unwrap();
    }
}

#[test]
fn pause_mid_burst_releases_only_consumed_slots_and_keeps_order() {
    let (mut tx, mut rx) = full_lane();
    let (lane, first) = rx.with_lane_ids().try_recv().unwrap();
    assert_eq!(first, 0);
    for value in 1..5 {
        assert_eq!(rx.try_recv(), Ok(value));
    }

    rx.pause(&lane).unwrap();
    for value in CAPACITY..CAPACITY + 5 {
        tx.try_send(value).unwrap();
    }
    assert_eq!(tx.try_send(0), Err(TrySendError::Full(0)));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    rx.resume(&lane).unwrap();
    for value in 5..68 {
        assert_eq!(rx.try_recv(), Ok(value));
    }
    // 63 values consumed since the pause released the first five.
    assert_eq!(tx.try_send(0), Err(TrySendError::Full(0)));
    assert_eq!(rx.try_recv(), Ok(68));
    tx.try_send(CAPACITY + 5).unwrap();
    for value in 69..=CAPACITY + 5 {
        assert_eq!(rx.try_recv(), Ok(value));
    }
}

#[test]
fn bulk_receive_mid_burst_rotates_at_the_same_value() {
    let (_busy, _sparse, mut rx) = busy_and_sparse();
    for sequence in 0..10 {
        assert_eq!(rx.try_recv(), Ok((0, sequence)));
    }

    let mut output = Vec::new();
    assert_eq!(rx.try_recv_batch_into(&mut output, 100), Ok(100));

    // The busy lane finishes its 64-value burst, yields to the sparse lane,
    // then starts a new burst.
    let expected = (10..64)
        .map(|sequence| (0, sequence))
        .chain([(1, 0)])
        .chain((64..109).map(|sequence| (0, sequence)))
        .collect::<Vec<_>>();
    assert_eq!(output, expected);
}

#[test]
fn single_receives_after_bulk_receive_keep_rotation() {
    let (_busy, _sparse, mut rx) = busy_and_sparse();
    let mut output = Vec::new();
    assert_eq!(rx.try_recv_batch_into(&mut output, 10), Ok(10));
    assert_eq!(
        output,
        (0..10).map(|sequence| (0, sequence)).collect::<Vec<_>>()
    );

    for sequence in 10..64 {
        assert_eq!(rx.try_recv(), Ok((0, sequence)));
    }
    assert_eq!(rx.try_recv(), Ok((1, 0)));
    assert_eq!(rx.try_recv(), Ok((0, 64)));
}

#[test]
fn fair_and_single_receives_share_the_release_batch() {
    let (mut tx, mut rx) = full_lane();
    for value in 0..63 {
        let received = if value % 3 == 0 {
            rx.try_recv_fair()
        } else {
            rx.try_recv()
        };
        assert_eq!(received, Ok(value));
    }

    // 63 values consumed: one short of the release batch.
    assert_eq!(tx.try_send(CAPACITY), Err(TrySendError::Full(CAPACITY)));
    assert_eq!(rx.try_recv_fair(), Ok(63));
    tx.try_send(CAPACITY).unwrap();
}

#[test]
fn fair_receives_on_a_lone_lane_restart_its_burst() {
    for singles_first in [0, 10] {
        let (mut busy, mut rx) = channel(CAPACITY as usize);
        for sequence in 0..CAPACITY {
            busy.try_send((0, sequence)).unwrap();
        }
        for sequence in 0..singles_first {
            assert_eq!(rx.try_recv(), Ok((0, sequence)));
        }
        for sequence in singles_first..20 {
            assert_eq!(rx.try_recv_fair(), Ok((0, sequence)));
        }
        let mut sparse = busy.try_clone().unwrap();
        sparse.try_send((1, 0)).unwrap();

        // The last fair receive ended the burst, so the busy lane gets a full
        // burst before the new lane.
        for sequence in 20..84 {
            assert_eq!(rx.try_recv(), Ok((0, sequence)));
        }
        assert_eq!(rx.try_recv(), Ok((1, 0)));
        assert_eq!(rx.try_recv(), Ok((0, 84)));
    }
}
