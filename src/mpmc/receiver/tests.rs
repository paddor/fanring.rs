use super::{LaneDrain, PREFETCH_LIMIT, RecvError};
use crate::mpmc::{LANES_PER_PAGE, channel_with_policy};
use crate::teardown::{Coordinated, Deferred, Teardown};

fn check_new_page_requeue<P: Teardown>() {
    let (tx, mut rx) = channel_with_policy::<_, P>(PREFETCH_LIMIT + 1);
    // Registration can happen after acquire_lane refreshes its snapshot
    // and before acquire_ready_lane claims a bit in an existing group.
    let ready = rx.ready.borrow().clone();
    assert_eq!(ready.pages.len(), 1);
    let mut senders = vec![tx];
    for _ in 0..LANES_PER_PAGE {
        senders.push(senders[0].try_clone().unwrap());
    }
    for value in 0..=PREFETCH_LIMIT {
        senders.last_mut().unwrap().try_send(value).unwrap();
    }

    let (lane, publication) = rx.acquire_ready_lane(&ready).unwrap();
    assert_eq!(lane.key.slot, LANES_PER_PAGE);
    // One value remains in the newly registered ring after the first
    // batch, forcing the normal drain path to requeue its lane token.
    let drained = rx.drain_lane(lane);
    drop(publication);
    let LaneDrain::Item { value, effects } = drained else {
        panic!("newly registered lane has a published batch");
    };
    rx.apply_effects(effects);
    assert_eq!(value, 0);
    for expected in 1..=PREFETCH_LIMIT {
        assert_eq!(rx.try_recv(), Ok(expected));
    }
    drop(senders);
    assert_eq!(rx.recv(), Err(RecvError));
}

fn check_new_page_work_claim<P: Teardown>() {
    let (tx, mut rx) = channel_with_policy::<_, P>(PREFETCH_LIMIT + 1);
    let ready = rx.ready.borrow().clone();
    let mut senders = vec![tx];
    for _ in 0..LANES_PER_PAGE {
        senders.push(senders[0].try_clone().unwrap());
    }
    for value in 0..=PREFETCH_LIMIT {
        senders.last_mut().unwrap().try_send(value).unwrap();
    }
    // A receiver with the new topology drains one batch and requeues
    // the remaining value before the stale receiver claims the work bit.
    let mut fresh = rx.clone();
    for expected in 0..PREFETCH_LIMIT {
        assert_eq!(fresh.try_recv(), Ok(expected));
    }
    let claimed = rx.acquire_work_lane(&ready).map(|(lane, publication)| {
        let drained = rx.drain_lane(lane);
        drop(publication);
        let LaneDrain::Item { value, effects } = drained else {
            panic!("requeued lane has a remaining value");
        };
        rx.apply_effects(effects);
        value
    });
    // Claiming a bit through an older topology must not lose access to
    // the lane. A deferred claim must leave it reachable on the next scan.
    assert_eq!(
        claimed.map_or_else(|| rx.try_recv(), Ok),
        Ok(PREFETCH_LIMIT)
    );
    drop(senders);
    assert_eq!(rx.recv(), Err(RecvError));
}

#[test]
fn requeue_new_page_after_acquiring_through_stale_topology() {
    let checks: [fn(); 2] = [
        check_new_page_requeue::<Deferred>,
        check_new_page_requeue::<Coordinated>,
    ];
    for check in checks {
        #[cfg(loom)]
        loom::model(check);
        #[cfg(not(loom))]
        check();
    }
}

#[test]
fn claim_new_page_work_through_stale_topology() {
    let checks: [fn(); 2] = [
        check_new_page_work_claim::<Deferred>,
        check_new_page_work_claim::<Coordinated>,
    ];
    for check in checks {
        #[cfg(loom)]
        loom::model(check);
        #[cfg(not(loom))]
        check();
    }
}
