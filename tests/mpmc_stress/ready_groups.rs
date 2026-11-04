//! Lane slot reuse, discovery, and starvation across the ready group
//! boundary. Each test registers at least 4096 lanes. `main.rs` skips this
//! module under Miri.

use std::sync::{Arc, Barrier};

use fanring::mpmc::{TryRecvError, channel};

#[test]
fn blocked_receiver_finds_sender_added_in_second_ready_group() {
    const FIRST_GROUP_LANES: usize = 64 * 64;

    let (root, mut rx) = channel(1);
    let mut existing = Vec::with_capacity(FIRST_GROUP_LANES);
    existing.push(root);
    for _ in 1..FIRST_GROUP_LANES {
        existing.push(existing[0].try_clone().unwrap());
    }
    assert_eq!(existing.last().unwrap().lane_id(), FIRST_GROUP_LANES - 1);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    let barrier = Arc::new(Barrier::new(2));
    let receiver_barrier = barrier.clone();
    let receiver = std::thread::spawn(move || {
        receiver_barrier.wait();
        let result = rx.recv();
        (rx, result)
    });
    barrier.wait();

    let mut newest = existing[0].try_clone().unwrap();
    assert_eq!(newest.lane_id(), FIRST_GROUP_LANES);
    newest.send(42).unwrap();

    let (rx, result) = receiver.join().unwrap();
    assert_eq!(result, Ok(42));
    drop(rx);
    drop(newest);
    drop(existing);
}

#[test]
fn busy_first_group_cannot_starve_sparse_second_group() {
    const SECOND_GROUP_SLOT: usize = 64 * 64;

    let (mut busy, mut rx) = channel(256);
    let mut other_senders = (0..SECOND_GROUP_SLOT)
        .map(|_| busy.try_clone().unwrap())
        .collect::<Vec<_>>();
    let sparse = other_senders.last_mut().unwrap();
    assert_eq!(sparse.lane_id(), SECOND_GROUP_SLOT);

    for sequence in 0..256 {
        busy.try_send((0, sequence)).unwrap();
    }
    assert_eq!(rx.try_recv(), Ok((0, 0)));
    sparse.try_send((1, 0)).unwrap();

    let sparse_position = (0..=128)
        .position(|_| rx.try_recv().unwrap().0 == 1)
        .expect("second ready group must be claimed");
    assert!(sparse_position <= 128);
}

#[test]
fn sender_slots_reuse_across_ready_group_boundary() {
    const DYNAMIC_SENDERS: usize = 64 * 64;
    const ROUNDS: usize = 2;

    let (root, mut rx) = channel(1);
    let mut expected_slots = None;

    for round in 0..ROUNDS {
        let mut senders = (0..DYNAMIC_SENDERS)
            .map(|_| root.try_clone().unwrap())
            .collect::<Vec<_>>();
        let mut slots = senders
            .iter()
            .map(fanring::mpmc::Sender::lane_id)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        assert_eq!(slots.first(), Some(&1));
        assert_eq!(slots.last(), Some(&DYNAMIC_SENDERS));
        if let Some(expected) = &expected_slots {
            assert_eq!(&slots, expected);
        } else {
            expected_slots = Some(slots);
        }

        for (sender, tx) in senders.iter_mut().enumerate() {
            tx.try_send(round * DYNAMIC_SENDERS + sender).unwrap();
        }
        drop(senders);

        let mut values = (0..DYNAMIC_SENDERS)
            .map(|_| rx.recv().unwrap())
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(
            values,
            (round * DYNAMIC_SENDERS..(round + 1) * DYNAMIC_SENDERS).collect::<Vec<_>>()
        );
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
}
