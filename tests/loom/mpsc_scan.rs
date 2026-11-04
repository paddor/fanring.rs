//! MPSC unsignaled sends racing scans that announce sleep before parking.

use crate::support::model;
use fanring::mpsc::{TryRecvError, channel};
use loom::sync::Arc;
use loom::sync::atomic::Ordering;
use loom::thread;

/// The handoff documented on `Sender::try_send_unsignaled`: a sender that
/// sees the receiver awake skips its wake, so the receiver must find the
/// value when it scans after announcing sleep.
#[test]
fn mpsc_unsignaled_send_with_fenced_flag_is_not_stranded() {
    use loom::sync::atomic::{AtomicU8, fence};
    const AWAKE: u8 = 0;
    const SLEEPING: u8 = 1;
    model(|| {
        let (mut tx, mut rx) = channel::<usize>(2);
        let flag = Arc::new(AtomicU8::new(AWAKE));
        let sender = {
            let flag = flag.clone();
            thread::spawn(move || {
                tx.try_send_unsignaled(1).unwrap();
                fence(Ordering::SeqCst);
                let woke = flag.load(Ordering::Acquire) == SLEEPING;
                (tx, woke)
            })
        };

        let mut output = Vec::with_capacity(1);
        let _ = rx.try_recv_scan_into_while(&mut output, 1, |_| true);
        let mut parked = false;
        if output.is_empty() {
            flag.store(SLEEPING, Ordering::Release);
            fence(Ordering::SeqCst);
            parked = rx
                .try_recv_scan_into_while(&mut output, 1, |_| true)
                .is_err();
        }
        let (_tx, woke) = sender.join().unwrap();
        assert!(
            !parked || woke,
            "value published but receiver parked unwoken"
        );
        if !parked {
            assert_eq!(output, [1]);
        }
    });
}

/// Receiver side of the fenced unsignaled handoff: drain with scans until
/// short, then publish the sleeping flag, fence, and scan once more.
/// Returns whether the receiver would park.
fn scan_then_park(
    rx: &mut fanring::mpsc::Receiver<usize>,
    flag: &loom::sync::atomic::AtomicU8,
    output: &mut Vec<usize>,
    limit: usize,
) -> bool {
    use loom::sync::atomic::fence;
    while rx.try_recv_scan_into_while(output, limit, |_| true).is_ok() {}
    flag.store(1, Ordering::Release);
    fence(Ordering::SeqCst);
    let mut found = false;
    while rx.try_recv_scan_into_while(output, limit, |_| true).is_ok() {
        found = true;
    }
    !found
}

/// A sender that registers after the receiver's first scan still has its
/// unsignaled value found or wakes the receiver.
#[test]
fn mpsc_unsignaled_send_from_late_registered_sender_is_not_stranded() {
    use loom::sync::atomic::{AtomicU8, fence};
    model(|| {
        let (tx0, mut rx) = channel::<usize>(2);
        let flag = Arc::new(AtomicU8::new(0));
        let mut output = Vec::with_capacity(1);
        assert_eq!(
            rx.try_recv_scan_into_while(&mut output, 1, |_| true),
            Err(TryRecvError::Empty)
        );
        let sender = {
            let flag = flag.clone();
            thread::spawn(move || {
                let mut tx1 = tx0.try_clone().expect("receiver alive");
                tx1.try_send_unsignaled(1).unwrap();
                fence(Ordering::SeqCst);
                let woke = flag.load(Ordering::Acquire) == 1;
                (tx0, tx1, woke)
            })
        };
        let parked = scan_then_park(&mut rx, &flag, &mut output, 1);
        let (_tx0, _tx1, woke) = sender.join().unwrap();
        assert!(
            !(parked && !woke && output.is_empty()),
            "late lane's value stranded"
        );
    });
}

/// With two lanes and a limit below the queued total, repeated short scans
/// neither strand nor duplicate a value.
#[test]
fn mpsc_scan_below_queued_total_across_two_lanes_is_complete() {
    use loom::sync::atomic::{AtomicU8, fence};
    model(|| {
        let (mut tx0, mut rx) = channel::<usize>(2);
        let mut tx1 = tx0.try_clone().expect("receiver alive");
        tx0.try_send_unsignaled(1).unwrap();
        tx0.try_send_unsignaled(2).unwrap();
        let flag = Arc::new(AtomicU8::new(0));
        let sender = {
            let flag = flag.clone();
            thread::spawn(move || {
                tx1.try_send_unsignaled(3).unwrap();
                fence(Ordering::SeqCst);
                let woke = flag.load(Ordering::Acquire) == 1;
                (tx1, woke)
            })
        };
        let mut output = Vec::with_capacity(3);
        let parked = scan_then_park(&mut rx, &flag, &mut output, 1);
        let (_tx1, woke) = sender.join().unwrap();
        assert!(
            !(parked && !woke && output.len() < 3),
            "value stranded behind the limit"
        );
        while rx
            .try_recv_scan_into_while(&mut output, 1, |_| true)
            .is_ok()
        {}
        output.sort_unstable();
        assert_eq!(output, [1, 2, 3]);
        drop(tx0);
    });
}

/// Signaled and unsignaled sends on one lane racing scans keep the lane
/// queued once and lose nothing.
#[test]
fn mpsc_mixed_signaled_and_unsignaled_sends_on_one_lane_race_scans() {
    use loom::sync::atomic::{AtomicU8, fence};
    model(|| {
        let (mut tx, mut rx) = channel::<usize>(2);
        let flag = Arc::new(AtomicU8::new(0));
        let sender = {
            let flag = flag.clone();
            thread::spawn(move || {
                tx.try_send(1).unwrap();
                tx.try_send_unsignaled(2).unwrap();
                fence(Ordering::SeqCst);
                let woke = flag.load(Ordering::Acquire) == 1;
                (tx, woke)
            })
        };
        let mut output = Vec::with_capacity(2);
        let parked = scan_then_park(&mut rx, &flag, &mut output, 1);
        let (_tx, woke) = sender.join().unwrap();
        assert!(
            !(parked && !woke && output.len() < 2),
            "mixed-lane value stranded"
        );
        while rx
            .try_recv_scan_into_while(&mut output, 1, |_| true)
            .is_ok()
        {}
        assert_eq!(output, [1, 2], "per-lane FIFO, each value once");
        assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    });
}

/// A sender dropping right after an unsignaled send, racing a scan, keeps
/// its value and then reports disconnection.
#[test]
fn mpsc_sender_drop_racing_scan_preserves_value_then_disconnects() {
    model(|| {
        let (mut tx, mut rx) = channel::<usize>(2);
        let sender = thread::spawn(move || {
            tx.try_send_unsignaled(1).unwrap();
            drop(tx);
        });
        let mut output = Vec::with_capacity(1);
        let _ = rx.try_recv_scan_into_while(&mut output, 1, |_| true);
        sender.join().unwrap();
        loop {
            match rx.try_recv_scan_into_while(&mut output, 1, |_| true) {
                Ok(_) => {}
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => panic!("dropped sender's lane never drained"),
            }
        }
        assert_eq!(output, [1]);
    });
}
