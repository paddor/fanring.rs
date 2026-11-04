//! Deferred sends: publication on flush, full, and drop, and value
//! ownership after receiver drop.

use fanring::mpsc::{TryRecvError, TrySendError};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn deferred_sends_publish_on_flush_full_and_drop() {
    let (mut tx, mut rx) = fanring::mpsc::channel(2);
    tx.try_send_deferred(0).unwrap();
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Empty));
    tx.flush();
    assert_eq!(rx.try_recv_fair(), Ok(0));
    rx.release_consumed();
    tx.try_send_deferred(1).unwrap();
    tx.try_send_deferred(2).unwrap();
    assert_eq!(tx.try_send_deferred(3), Err(TrySendError::Full(3)));
    assert_eq!(rx.try_recv_fair(), Ok(1));
    assert_eq!(rx.try_recv_fair(), Ok(2));
    rx.release_consumed();
    tx.try_send_deferred(3).unwrap();
    drop(tx);
    assert_eq!(rx.try_recv_fair(), Ok(3));
    assert_eq!(rx.try_recv_fair(), Err(TryRecvError::Disconnected));
}

#[test]
fn deferred_values_remain_owned_across_receiver_drop() {
    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, rx) = fanring::mpsc::channel(2);
    assert!(tx.try_send_deferred(Tracked(drops.clone())).is_ok());
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let returned = tx.try_send_deferred(Tracked(drops.clone())).err().unwrap();
    assert!(matches!(returned, TrySendError::Disconnected(_)));
    drop(returned);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(tx);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}
