#![cfg(all(feature = "async", not(loom)))]

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use fanring::mpsc::{self, RecvError, TryRecvError, TryRegisterBoundedError};
use fanring::teardown::Coordinated;

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn capacity_wakes_only_its_producer_and_cancel_releases_registration() {
    let (mut first, mut rx) = mpsc::channel(1);
    let mut second = first.try_clone().unwrap();
    first.try_send(1).unwrap();
    second.try_send(2).unwrap();
    let first_wakes = Arc::new(Wakes::default());
    let second_wakes = Arc::new(Wakes::default());
    let first_waker = Waker::from(first_wakes.clone());
    let second_waker = Waker::from(second_wakes.clone());
    let mut send = pin!(first.send_async(3));
    let mut other = pin!(second.send_async(4));
    assert!(
        send.as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        other
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(first_wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(
        send.as_mut().poll(&mut Context::from_waker(&first_waker)),
        Poll::Ready(Ok(()))
    );
    let next = rx.try_recv().unwrap();
    if next == 3 {
        assert_eq!(rx.try_recv(), Ok(2));
    } else {
        assert_eq!(next, 2);
    }
    assert_eq!(second_wakes.0.load(Ordering::Relaxed), 1);
}

#[test]
fn canceled_send_and_receive_do_not_consume_capacity_or_keep_wakers() {
    let (mut tx, mut rx) = mpsc::channel(1);
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    {
        let mut receive = pin!(rx.recv_async());
        assert!(receive.as_mut().poll(&mut cx).is_pending());
    }
    tx.try_send(1).unwrap();
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    {
        let mut send = pin!(tx.send_async(2));
        assert!(send.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(tx.try_send(3), Ok(()));
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn registration_final_disconnect_and_receiver_drop_wake_waiters() {
    let (tx, mut rx) = mpsc::channel(1);
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(rx.poll_recv(&mut cx).is_pending());
    let mut added = tx.try_register().unwrap();
    added.try_send(7).unwrap();
    assert!(wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(rx.poll_recv(&mut cx), Poll::Ready(Ok(7)));
    drop(tx);
    drop(added);
    assert_eq!(rx.poll_recv(&mut cx), Poll::Ready(Err(RecvError)));

    let (mut tx, rx) = mpsc::channel(1);
    tx.try_send(1).unwrap();
    let mut send = pin!(tx.send_async(2));
    assert!(send.as_mut().poll(&mut cx).is_pending());
    let before = wakes.0.load(Ordering::Relaxed);
    drop(rx);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    assert_eq!(
        send.as_mut().poll(&mut cx),
        Poll::Ready(Err(mpsc::SendError(2)))
    );
}

#[test]
fn endpoint_drop_releases_manual_poll_wakers_while_other_endpoint_lives() {
    let (mut tx, rx) = mpsc::channel(1);
    tx.try_send(1).unwrap();
    let wakes = Arc::new(Wakes::default());
    let sender_wakes = Arc::downgrade(&wakes);
    {
        let waker = Waker::from(wakes);
        assert!(tx.poll_ready(&mut Context::from_waker(&waker)).is_pending());
    }
    assert!(sender_wakes.upgrade().is_some());
    drop(tx);
    assert!(sender_wakes.upgrade().is_none());
    drop(rx);

    let (tx, mut rx) = mpsc::channel::<u8>(1);
    let wakes = Arc::new(Wakes::default());
    let receiver_wakes = Arc::downgrade(&wakes);
    {
        let waker = Waker::from(wakes);
        assert!(rx.poll_recv(&mut Context::from_waker(&waker)).is_pending());
    }
    assert!(receiver_wakes.upgrade().is_some());
    drop(rx);
    assert!(receiver_wakes.upgrade().is_none());
    drop(tx);
}

#[test]
fn bounded_registration_counts_retired_rings_until_drained() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut second = tx.try_register_bounded(2).unwrap();
    second.try_send(9).unwrap();
    drop(second);
    assert_eq!(tx.registered_lanes(), 2);
    assert!(matches!(
        tx.try_register_bounded(2),
        Err(TryRegisterBoundedError::AtCapacity)
    ));
    assert_eq!(rx.try_recv(), Ok(9));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(tx.registered_lanes(), 1);
    assert!(tx.try_register_bounded(2).is_ok());
}

#[test]
fn async_bulk_releases_slots_and_preserves_partial_final_batch() {
    futures_lite::future::block_on(async {
        let (mut tx, mut rx) = mpsc::channel(4);
        for n in 0..4 {
            tx.send_async(n).await.unwrap();
        }
        let mut out = vec![99];
        assert_eq!(rx.recv_batch_into_async(&mut out, 2).await, Ok(2));
        assert_eq!(out, [99, 0, 1]);
        tx.send_async(4).await.unwrap();
        drop(tx);
        assert_eq!(rx.recv_batch_into_async(&mut out, 10).await, Ok(3));
        assert_eq!(out, [99, 0, 1, 2, 3, 4]);
        assert_eq!(rx.recv_batch_into_async(&mut out, 0).await, Ok(0));
        assert_eq!(rx.recv_async().await, Err(RecvError));
    });
}

#[test]
fn cross_thread_async_churn_preserves_fifo_and_reclaims_payloads() {
    let (tx, mut rx) = mpsc::channel_with_policy::<_, Coordinated>(2);
    let threads: Vec<_> = (0..4)
        .map(|sender| {
            let mut tx = tx.try_clone().unwrap();
            std::thread::spawn(move || {
                futures_lite::future::block_on(async {
                    for sequence in 0..1000 {
                        tx.send_async((sender, sequence)).await.unwrap();
                    }
                })
            })
        })
        .collect();
    drop(tx);
    let mut next = [0; 4];
    futures_lite::future::block_on(async {
        let mut batch = Vec::with_capacity(7);
        while rx.recv_batch_into_async(&mut batch, 7).await.is_ok() {
            for (sender, sequence) in batch.drain(..) {
                assert_eq!(sequence, next[sender]);
                next[sender] += 1;
            }
        }
    });
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(next, [1000; 4]);
}
