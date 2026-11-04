//! Lane slot reuse and starvation across ready page boundaries.

use fanring::mpmc::channel;

#[test]
fn sender_slots_reuse_across_ready_page_boundaries() {
    const DYNAMIC_SENDERS: usize = 129;
    const ROUNDS: usize = 4;

    let (root, mut rx) = channel(2);
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
        assert!(slots.last().copied().unwrap() >= 129);
        if let Some(expected) = &expected_slots {
            assert_eq!(&slots, expected);
        } else {
            expected_slots = Some(slots);
        }

        for (sender, tx) in senders.iter_mut().enumerate() {
            tx.send(round * DYNAMIC_SENDERS + sender).unwrap();
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
    }
}

#[test]
fn sparse_lane_on_later_ready_page_is_not_starved() {
    let (mut busy, mut rx) = channel(256);
    let mut senders = (0..128)
        .map(|_| busy.try_clone().unwrap())
        .collect::<Vec<_>>();
    let sparse = senders.last_mut().unwrap();

    for value in 0..256 {
        busy.try_send((0, value)).unwrap();
    }
    sparse.try_send((1, 0)).unwrap();

    let sparse_position = (0..=128)
        .position(|_| rx.try_recv().unwrap().0 == 1)
        .expect("later ready page must be activated");
    assert!(sparse_position <= 128);
}
