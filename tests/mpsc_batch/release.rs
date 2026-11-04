//! Explicit slot release and its effect on sender capacity and value
//! ownership.

use crate::support::Counted;
use fanring::mpsc::{TryRecvError, TrySendError, channel_with_policy};
use fanring::teardown::{Coordinated, Deferred, Teardown};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn release_consumed_preserves_unread_values_and_is_idempotent() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        rx.release_consumed();
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));
        assert_eq!(rx.try_recv(), Ok(0));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));

        rx.release_consumed();
        rx.release_consumed();
        tx.try_send(4).unwrap();
        tx.try_send(5).unwrap();
        assert_eq!(tx.try_send(6), Err(TrySendError::Full(6)));
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), [2, 3, 4, 5]);
        rx.release_consumed();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn release_consumed_covers_all_lanes_without_resetting_rotation() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<_, P>(128);
        let mut tx1 = tx0.try_clone().unwrap();
        for value in 0..128 {
            tx0.try_send((0, value)).unwrap();
            tx1.try_send((1, value)).unwrap();
        }
        assert_eq!(rx.recv(), Ok((0, 0)));
        rx.release_consumed();
        tx0.try_send((0, 128)).unwrap();

        // Offset lane 0's release boundary from its rotation boundary so both
        // lanes hold partial release batches when lane 1 becomes active.
        for value in 1..64 {
            assert_eq!(rx.try_recv(), Ok((0, value)));
        }
        assert_eq!(rx.recv(), Ok((1, 0)));
        assert_eq!(tx0.try_send((0, 129)), Err(TrySendError::Full((0, 129))));
        assert_eq!(tx1.try_send((1, 128)), Err(TrySendError::Full((1, 128))));
        rx.release_consumed();
        for value in 129..192 {
            tx0.try_send((0, value)).unwrap();
        }
        tx1.try_send((1, 128)).unwrap();
        assert_eq!(tx0.try_send((0, 192)), Err(TrySendError::Full((0, 192))));
        assert_eq!(tx1.try_send((1, 129)), Err(TrySendError::Full((1, 129))));

        drop((tx0, tx1));
        let mut next = [64, 1];
        for (lane, value) in rx {
            assert_eq!(value, next[lane]);
            next[lane] += 1;
        }
        assert_eq!(next, [192, 129]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn released_slot_reuse_and_receiver_drop_preserve_output_ownership() {
    fn check<P: Teardown>() {
        for explicit_release in [false, true] {
            let drops = Arc::new((0..6).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            let value = |id| Counted {
                id,
                drops: drops.clone(),
            };
            let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
            for id in 0..4 {
                tx.try_send(value(id)).unwrap();
            }
            let mut output = Vec::with_capacity(2);
            if explicit_release {
                output.push(rx.recv().unwrap());
                output.push(rx.recv().unwrap());
                rx.release_consumed();
            } else {
                assert_eq!(rx.recv_batch_into(&mut output, 2), Ok(2));
            }
            tx.try_send(value(4)).unwrap();
            tx.try_send(value(5)).unwrap();
            assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 0));

            drop(rx);
            for count in &drops[2..] {
                assert_eq!(count.load(Ordering::Relaxed), usize::from(P::COORDINATED));
            }
            drop(tx);
            for count in &drops[2..] {
                assert_eq!(count.load(Ordering::Relaxed), 1);
            }
            assert_eq!(
                output.iter().map(|value| value.id).collect::<Vec<_>>(),
                [0, 1]
            );
            assert_eq!(drops[0].load(Ordering::Relaxed), 0);
            assert_eq!(drops[1].load(Ordering::Relaxed), 0);
            drop(output);
            assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 1));
        }
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn is_full_follows_lane_capacity_and_slot_release() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<_, P>(4);
        let mut other = tx.try_clone().unwrap();
        assert!(!tx.is_full());
        for value in 0..4 {
            tx.try_send(value).unwrap();
        }
        assert!(tx.is_full());
        // Lanes are independent.
        assert!(!other.is_full());
        other.try_send(10).unwrap();
        // Consumed slots become visible to the sender once released.
        assert_eq!(rx.try_recv(), Ok(0));
        rx.release_consumed();
        assert!(!tx.is_full());
        tx.try_send(4).unwrap();
        assert!(tx.is_full());
        let mut batch = Vec::new();
        assert_eq!(rx.try_recv_batch_into(&mut batch, 8), Ok(5));
        assert!(!tx.is_full());
    }
    check::<Deferred>();
    check::<Coordinated>();
}
