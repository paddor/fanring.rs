//! Blocking bulk receives with `recv_batch_into`.

use fanring::mpsc::{RecvError, TryRecvError, TrySendError, channel_with_policy};
use fanring::teardown::{Coordinated, Deferred, Teardown};
use std::time::Duration;

#[test]
fn recv_batch_appends_bounded_values_and_releases_earlier_receives() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(rx.recv(), Ok(0));
        let mut output = Vec::with_capacity(3);
        output.push(-1);
        let allocation = output.as_ptr();
        let capacity = output.capacity();
        assert_eq!(rx.recv_batch_into(&mut output, 1), Ok(1));
        assert_eq!(output, [-1, 1]);
        assert_eq!(output.as_ptr(), allocation);
        assert_eq!(output.capacity(), capacity);
        tx.try_send(4).unwrap();
        tx.try_send(5).unwrap();
        assert_eq!(tx.try_send(6), Err(TrySendError::Full(6)));

        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 2), Ok(2));
        assert_eq!(output, [2, 3]);
        assert_eq!(output.as_ptr(), allocation);
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 3), Ok(2));
        assert_eq!(output, [4, 5]);
        assert_eq!(output.capacity(), capacity);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_waits_only_for_first_value() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut output = Vec::with_capacity(4);
            started_tx.send(()).unwrap();
            let result = rx.recv_batch_into(&mut output, 4);
            done_tx.send((result, output)).unwrap();
        });
        started_rx.recv().unwrap();
        tx.send(7).unwrap();
        // Keep the sender connected until the batch returns. Disconnect on
        // failure as well, so an implementation waiting for more can exit.
        let result = done_rx.recv_timeout(Duration::from_secs(5));
        drop(tx);
        receiver.join().unwrap();
        assert_eq!(result.unwrap(), (Ok(1), vec![7]));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_zero_limit_releases_without_receiving() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        let mut output = vec![9];
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        assert_eq!(rx.recv(), Ok(0));
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        assert_eq!(output, [9]);
        tx.try_send(4).unwrap();
        drop(tx);
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        // A large limit must not cause an up-front allocation for that limit.
        assert_eq!(rx.recv_batch_into(&mut output, usize::MAX), Ok(4));
        assert_eq!(output, [9, 1, 2, 3, 4]);
        assert_eq!(rx.recv_batch_into(&mut output, 1), Err(RecvError));
        assert_eq!(rx.recv_batch_into(&mut output, 0), Ok(0));
        assert_eq!(output, [9, 1, 2, 3, 4]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_disconnect_preserves_partial_batch_and_owned_values() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        tx.send(Box::new(1)).unwrap();
        tx.send(Box::new(2)).unwrap();
        drop(tx);
        let mut output = vec![Box::new(0)];
        assert_eq!(rx.recv_batch_into(&mut output, 4), Ok(2));
        assert_eq!(
            output.iter().map(|value| **value).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(rx.recv_batch_into(&mut output, 4), Err(RecvError));
        drop(rx);
        assert_eq!(
            output.iter().map(|value| **value).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_reclaims_exact_capacity_across_wraparound_and_release_boundaries() {
    fn check<P: Teardown>() {
        for requested_capacity in [1, 3, 64, 65] {
            let (mut tx, mut rx) = channel_with_policy::<_, P>(requested_capacity);
            let capacity = rx.capacity_per_sender();
            for value in 0..capacity {
                tx.try_send(value).unwrap();
            }
            let mut sent = capacity;
            let mut received = 0;
            let mut output = Vec::with_capacity(capacity);
            // Cross the 64-item release boundary and repeatedly reuse physical
            // slots, including capacities rounded upward by yring.
            for limit in [1, 3, 63, 64, 65, 129] {
                let count = capacity.min(limit);
                assert_eq!(rx.recv_batch_into(&mut output, limit), Ok(count));
                assert_eq!(output, (received..received + count).collect::<Vec<_>>());
                received += count;
                output.clear();

                for value in sent..sent + count {
                    tx.try_send(value).unwrap();
                }
                sent += count;
                assert_eq!(tx.try_send(sent), Err(TrySendError::Full(sent)));
            }
            drop(tx);
            assert_eq!(rx.recv_batch_into(&mut output, capacity + 1), Ok(capacity));
            assert_eq!(output, (received..sent).collect::<Vec<_>>());
            assert_eq!(rx.recv_batch_into(&mut output, 1), Err(RecvError));
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn recv_batch_retires_and_reuses_lane_without_releasing_new_unread_slots() {
    fn check<P: Teardown>() {
        let (root, mut rx) = channel_with_policy::<_, P>(4);
        let mut old = root.try_clone().unwrap();
        let slot = old.lane_id();
        for value in 0..4 {
            old.try_send(value).unwrap();
        }
        let mut output = Vec::with_capacity(4);
        assert_eq!(rx.recv_batch_into(&mut output, 2), Ok(2));
        assert_eq!(output, [0, 1]);
        drop(old);
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 4), Ok(2));
        assert_eq!(output, [2, 3]);
        // The empty poll retires this lane, leaving a hole in the receiver's
        // lane table. Releasing again must tolerate that hole.
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        rx.release_consumed();

        let mut new = root.try_clone().unwrap();
        assert_eq!(new.lane_id(), slot);
        for value in 4..8 {
            new.try_send(value).unwrap();
        }
        // The reused slot is registered but has not been imported or read yet.
        rx.release_consumed();
        assert_eq!(new.try_send(8), Err(TrySendError::Full(8)));
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 1), Ok(1));
        assert_eq!(output, [4]);
        new.try_send(8).unwrap();
        assert_eq!(new.try_send(9), Err(TrySendError::Full(9)));
        drop((root, new));
        output.clear();
        assert_eq!(rx.recv_batch_into(&mut output, 5), Ok(4));
        assert_eq!(output, [5, 6, 7, 8]);
        rx.release_consumed();
        assert_eq!(rx.recv_batch_into(&mut output, 1), Err(RecvError));
    }
    check::<Deferred>();
    check::<Coordinated>();
}
