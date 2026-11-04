//! Unsignaled sends and the scans that receive them.

use fanring::mpsc::{TryRecvError, TrySendError, channel_with_policy};
use fanring::teardown::{Coordinated, Deferred, Teardown};

#[test]
fn scan_receives_unsignaled_values_in_per_sender_order() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<_, P>(8);
        let mut tx1 = tx0.try_clone().unwrap();
        for value in 0..4 {
            tx0.try_send_unsignaled((0, value)).unwrap();
            tx1.try_send_unsignaled((1, value)).unwrap();
        }
        let mut batch = Vec::with_capacity(8);
        assert_eq!(rx.try_recv_scan_into_while(&mut batch, 8, |_| true), Ok(8));
        for sender in 0..2 {
            let values: Vec<_> = batch
                .iter()
                .filter(|(from, _)| *from == sender)
                .map(|(_, value)| *value)
                .collect();
            assert_eq!(values, [0, 1, 2, 3]);
        }
        assert_eq!(
            rx.try_recv_scan_into_while(&mut batch, 8, |_| true),
            Err(TryRecvError::Empty)
        );
        drop((tx0, tx1));
        assert_eq!(
            rx.try_recv_scan_into_while(&mut batch, 8, |_| true),
            Err(TryRecvError::Disconnected)
        );
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn scan_visits_lanes_outside_the_current_rotation() {
    let (mut tx0, mut rx) = channel_with_policy::<usize, Deferred>(128);
    let mut tx1 = tx0.try_clone().unwrap();
    for value in 0..100 {
        tx0.try_send(value).unwrap();
    }
    let mut batch = Vec::with_capacity(128);
    // A limited receive leaves the first lane queued with values left.
    assert_eq!(rx.try_recv_batch_into(&mut batch, 10), Ok(10));
    tx1.try_send_unsignaled(1000).unwrap();
    assert_eq!(
        rx.try_recv_scan_into_while(&mut batch, 128, |_| true),
        Ok(91)
    );
    assert!(batch.contains(&1000));
    assert_eq!(
        batch.iter().filter(|&&value| value < 100).count(),
        100,
        "every signaled value arrives once"
    );
}

#[test]
fn scan_admission_keeps_rejected_value_queued() {
    let (mut tx, mut rx) = channel_with_policy::<usize, Deferred>(8);
    for value in [10, 20, 30] {
        tx.try_send_unsignaled(value).unwrap();
    }
    let mut batch = Vec::with_capacity(8);
    let mut budget = 25;
    let admitted = rx.try_recv_scan_into_while(&mut batch, 8, |value| {
        if *value > budget {
            return false;
        }
        budget -= *value;
        true
    });
    assert_eq!(admitted, Ok(1));
    assert_eq!(rx.try_recv_scan_into_while(&mut batch, 8, |_| false), Ok(0));
    assert_eq!(rx.try_recv_scan_into_while(&mut batch, 8, |_| true), Ok(2));
    assert_eq!(batch, [10, 20, 30]);
}

#[test]
fn unsignaled_send_reports_full_and_disconnected() {
    fn check<P: Teardown>() {
        let (mut tx, mut rx) = channel_with_policy::<usize, P>(2);
        tx.try_send_unsignaled(0).unwrap();
        tx.try_send_unsignaled(1).unwrap();
        assert_eq!(tx.try_send_unsignaled(2), Err(TrySendError::Full(2)));
        let mut batch = Vec::with_capacity(2);
        assert_eq!(rx.try_recv_scan_into_while(&mut batch, 2, |_| true), Ok(2));
        tx.try_send_unsignaled(2).unwrap();
        drop(rx);
        assert_eq!(
            tx.try_send_unsignaled(3),
            Err(TrySendError::Disconnected(3))
        );
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn scan_then_signaled_send_keeps_each_lane_queued_once() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<u32, P>(16);
        tx0.try_send_unsignaled(1).unwrap();
        tx0.try_send_unsignaled(2).unwrap();
        let mut out = Vec::new();
        // The scan queues lane 0 and leaves it queued with a value left.
        assert_eq!(rx.try_recv_scan_into_while(&mut out, 1, |_| true), Ok(1));
        // A signaled send on the still-queued lane indexes it again.
        for value in 10..14 {
            tx0.try_send(value).unwrap();
        }
        let mut tx1 = tx0.try_clone().expect("receiver alive");
        for value in 20..23 {
            tx1.try_send(value).unwrap();
        }
        let mut order = Vec::new();
        while let Ok(value) = rx.try_recv_fair() {
            order.push(value);
        }
        assert_eq!(order, [2, 20, 10, 21, 11, 22, 12, 13]);
    }
    check::<Deferred>();
    check::<Coordinated>();
}

#[test]
fn short_scan_without_rejection_observed_every_lane_empty() {
    fn check<P: Teardown>() {
        let (mut tx0, mut rx) = channel_with_policy::<u32, P>(16);
        let mut tx1 = tx0.try_clone().expect("receiver alive");
        tx0.try_send_unsignaled(1).unwrap();
        tx1.try_send(20).unwrap();
        tx1.try_send_unsignaled(21).unwrap();
        let mut out = Vec::new();
        assert_eq!(rx.try_recv_scan_into_while(&mut out, 8, |_| true), Ok(3));
        out.sort_unstable();
        assert_eq!(out, [1, 20, 21]);
        // Nothing is left behind in any lane, signaled or not.
        assert_eq!(
            rx.try_recv_scan_into_while(&mut out, 8, |_| true),
            Err(TryRecvError::Empty)
        );
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    }
    check::<Deferred>();
    check::<Coordinated>();
}
