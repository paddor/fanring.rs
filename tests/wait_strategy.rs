use std::time::{Duration, Instant};

use fanring::{WaitStrategy, mpmc, mpsc};

const SPIN_FOR: Duration = Duration::from_micros(50);

#[test]
fn mpsc_wait_strategy_is_endpoint_local_and_inherited_by_senders() {
    let (mut tx, mut rx) = mpsc::channel::<u64>(1);
    assert_eq!(tx.wait_strategy(), WaitStrategy::Park);
    assert_eq!(rx.wait_strategy(), WaitStrategy::Park);

    tx.set_wait_strategy(WaitStrategy::SpinFor(SPIN_FOR));
    let tx2 = tx.try_clone().unwrap();
    assert_eq!(tx2.wait_strategy(), WaitStrategy::SpinFor(SPIN_FOR));
    assert_eq!(rx.wait_strategy(), WaitStrategy::Park);

    rx.set_wait_strategy(WaitStrategy::SpinFor(SPIN_FOR));
    assert_eq!(rx.wait_strategy(), WaitStrategy::SpinFor(SPIN_FOR));
}

#[test]
fn mpmc_wait_strategy_is_endpoint_local_and_inherited() {
    let (mut tx, mut rx) = mpmc::channel::<u64>(1);
    tx.set_wait_strategy(WaitStrategy::SpinFor(SPIN_FOR));
    rx.set_wait_strategy(WaitStrategy::SpinFor(SPIN_FOR));

    let tx2 = tx.try_clone().unwrap();
    let rx2 = rx.clone();
    assert_eq!(tx2.wait_strategy(), WaitStrategy::SpinFor(SPIN_FOR));
    assert_eq!(rx2.wait_strategy(), WaitStrategy::SpinFor(SPIN_FOR));
}

#[test]
fn mpsc_deadlines_cap_extended_spinning() {
    let (mut tx, mut rx) = mpsc::channel(1);
    tx.set_wait_strategy(WaitStrategy::SpinFor(Duration::from_secs(1)));
    rx.set_wait_strategy(WaitStrategy::SpinFor(Duration::from_secs(1)));

    tx.try_send(1).unwrap();
    assert!(matches!(
        tx.send_deadline(2, Instant::now()),
        Err(mpsc::SendTimeoutError::Timeout(2))
    ));
    assert_eq!(rx.recv().unwrap(), 1);
    assert_eq!(
        rx.recv_deadline(Instant::now()),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
}

#[test]
fn mpmc_deadlines_cap_extended_spinning() {
    let (mut tx, mut rx) = mpmc::channel(1);
    tx.set_wait_strategy(WaitStrategy::SpinFor(Duration::from_secs(1)));
    rx.set_wait_strategy(WaitStrategy::SpinFor(Duration::from_secs(1)));

    tx.try_send(1).unwrap();
    assert!(matches!(
        tx.send_deadline(2, Instant::now()),
        Err(mpmc::SendTimeoutError::Timeout(2))
    ));
    assert_eq!(rx.recv().unwrap(), 1);
    assert_eq!(
        rx.recv_deadline(Instant::now()),
        Err(mpmc::RecvTimeoutError::Timeout)
    );
}
