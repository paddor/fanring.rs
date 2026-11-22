use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{Consumer, Cursor, Producer, Ring, endpoints, spsc_growing};

/// Pop everything published. `prefetch` counts only new items, so pop
/// whatever the window holds before deciding the ring is drained.
fn drain(consumer: &mut Consumer<u32>) -> Vec<u32> {
    let mut out = Vec::new();
    loop {
        consumer.prefetch();
        let before = out.len();
        while let Some(value) = consumer.pop() {
            out.push(value);
        }
        consumer.release();
        if out.len() == before {
            return out;
        }
    }
}

fn push_all(producer: &mut Producer<u32>, values: std::ops::Range<u32>) {
    for value in values {
        producer.push(value).unwrap();
    }
    producer.flush();
}

#[test]
fn growing_ring_doubles_its_segment_up_to_capacity() {
    let (mut p, mut c) = spsc_growing::<u32>(2, 16);
    assert_eq!(p.capacity(), 16);
    assert_eq!(p.ring.segment_sizes(), [2]);

    push_all(&mut p, 0..16);
    assert_eq!(p.push(16), Err(16));
    assert!(p.is_full());
    assert_eq!(p.ring.segment_sizes(), [2, 4, 8, 16]);

    assert_eq!(drain(&mut c), (0..16).collect::<Vec<_>>());
    assert_eq!(c.ring.segment_sizes(), [16]);
}

#[test]
fn growing_ring_is_not_full_while_it_can_grow() {
    let (mut p, _c) = spsc_growing::<u32>(1, 4);
    p.push(0).unwrap();
    assert!(!p.is_full());
    assert_eq!(p.ring.segment_sizes(), [1]);
    push_all(&mut p, 1..4);
    assert!(p.is_full());
}

#[test]
fn growing_ring_keeps_fifo_across_segments() {
    let (mut p, mut c) = spsc_growing::<u32>(1, 64);
    let mut next = 0;
    let mut received = Vec::new();
    for burst in 1..=20 {
        push_all(&mut p, next..next + burst);
        next += burst;
        received.extend(drain(&mut c));
    }
    assert_eq!(received, (0..next).collect::<Vec<_>>());
}

#[test]
fn one_prefetch_covers_items_in_every_segment() {
    let (mut p, mut c) = spsc_growing::<u32>(2, 8);
    push_all(&mut p, 0..6);
    assert_eq!(c.prefetch(), 6);
    let mut out = Vec::new();
    assert_eq!(c.pop_into(&mut out, 3), 3);
    assert_eq!(c.pop_into(&mut out, 10), 3);
    assert_eq!(c.pop(), None);
    assert_eq!(out, [0, 1, 2, 3, 4, 5]);
    c.release();
    assert_eq!(c.ring.segment_sizes(), [4]);
}

#[test]
fn pop_into_while_crosses_segment_ends() {
    let (mut p, mut c) = spsc_growing::<u32>(2, 8);
    push_all(&mut p, 0..6);
    assert_eq!(c.prefetch(), 6);
    let mut out = Vec::new();
    assert_eq!(c.pop_into_while(&mut out, 1, |_| true), 1);
    assert_eq!(c.pop_into_while(&mut out, 10, |value| *value < 4), 3);
    assert_eq!(out, [0, 1, 2, 3]);
    assert_eq!(c.pop(), Some(4));
}

#[test]
fn pop_crosses_into_a_newer_segment() {
    let (mut p, mut c) = spsc_growing::<u32>(1, 4);
    push_all(&mut p, 0..3);
    assert_eq!(c.prefetch(), 3);
    assert_eq!(c.pop(), Some(0));
    assert!(!c.is_empty());
    assert_eq!(c.len(), 2);
    // The next pop crosses into the second segment without a prefetch.
    assert_eq!(c.pop(), Some(1));
    assert_eq!(c.prefetch_and_pop(), Some(2));
}

#[test]
fn growth_does_not_wait_for_the_consumer_to_reach_the_new_segment() {
    let (mut p, mut c) = spsc_growing::<u32>(2, 32);
    push_all(&mut p, 0..2);
    assert_eq!(c.prefetch(), 2);
    assert_eq!(c.pop(), Some(0));
    c.release();
    // One slot is free in the first segment, but the producer moved on.
    push_all(&mut p, 2..12);
    assert_eq!(p.ring.segment_sizes(), [2, 4, 8]);
    assert_eq!(c.pop(), Some(1));
    let mut rest = drain(&mut c);
    rest.insert(0, 1);
    assert_eq!(rest, (1..12).collect::<Vec<_>>());
}

#[test]
fn growing_ring_crosses_the_cursor_wrap() {
    let base: Cursor = usize::MAX - 2;
    let (mut p, mut c) = endpoints::<u32>(Ring::new(1, 8, base));
    push_all(&mut p, 0..8);
    assert_eq!(p.ring.segment_sizes(), [1, 2, 4, 8]);
    assert_eq!(drain(&mut c), (0..8).collect::<Vec<_>>());
    push_all(&mut p, 8..16);
    assert_eq!(drain(&mut c), (8..16).collect::<Vec<_>>());
    assert!(p.is_empty());
}

#[test]
fn dropping_a_grown_ring_drops_items_in_every_segment() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct Counted;
    impl Drop for Counted {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    DROPS.store(0, Ordering::Relaxed);
    let (mut p, mut c) = spsc_growing::<Counted>(1, 16);
    for _ in 0..10 {
        p.push(Counted).unwrap();
    }
    p.flush();
    c.prefetch();
    drop(c.pop());
    c.release();
    assert_eq!(DROPS.load(Ordering::Relaxed), 1);
    drop(p);
    drop(c);
    assert_eq!(DROPS.load(Ordering::Relaxed), 10);
}

#[test]
fn initial_size_is_clamped_to_capacity() {
    let (p, _c) = spsc_growing::<u32>(64, 8);
    assert_eq!(p.ring.segment_sizes(), [8]);
    let (p, _c) = spsc_growing::<u32>(0, 8);
    assert_eq!(p.ring.segment_sizes(), [1]);
}

#[test]
fn growing_ring_moves_items_across_threads() {
    let count: u32 = if cfg!(miri) { 2_048 } else { 200_000 };
    let (mut p, mut c) = spsc_growing::<u32>(1, 1 << 12);
    let producer = std::thread::spawn(move || {
        for value in 0..count {
            let mut value = value;
            while let Err(back) = p.push(value) {
                value = back;
                p.flush();
                std::thread::yield_now();
            }
            if value % 64 == 0 {
                p.flush();
            }
        }
        p.flush();
    });

    let mut expected = 0;
    while expected < count {
        if c.prefetch() == 0 {
            std::thread::yield_now();
            continue;
        }
        while let Some(value) = c.pop() {
            assert_eq!(value, expected);
            expected += 1;
        }
        c.release();
    }
    producer.join().unwrap();
}

#[test]
fn endpoints_stay_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Producer<u32>>();
    send_sync::<Consumer<u32>>();
    send_sync::<crate::ProducerOwner<u32>>();
    #[cfg(feature = "async")]
    {
        send_sync::<crate::AsyncProducer<u32>>();
        send_sync::<crate::AsyncConsumer<u32>>();
    }
}

#[test]
fn panicking_admission_moves_nothing_across_segments() {
    let (mut p, mut c) = spsc_growing::<u32>(1, 8);
    push_all(&mut p, 0..5);
    c.prefetch();
    let mut out = Vec::new();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        c.pop_into_while(&mut out, 10, |value| {
            assert!(*value < 3, "reject by panic");
            true
        })
    }));
    assert!(panicked.is_err());
    assert_eq!(out, [0u32; 0]);
    assert_eq!(c.pop_into_while(&mut out, 10, |value| *value < 3), 3);
    assert_eq!(drain(&mut c), [3, 4]);
}

/// Push `count` items made by `make` through a ring that grows from one slot,
/// draining in uneven batches, and check FIFO order with `same`.
fn round_trip<T: std::fmt::Debug>(
    count: usize,
    make: impl Fn(usize) -> T,
    same: impl Fn(&T, usize) -> bool,
) {
    let (mut p, mut c) = spsc_growing::<T>(1, 64);
    let mut next_in = 0;
    let mut next_out = 0;
    let mut out = Vec::new();
    while next_out < count {
        while next_in < count && p.push(make(next_in)).is_ok() {
            next_in += 1;
            if next_in % 7 == 0 {
                p.flush();
            }
        }
        p.flush();
        c.prefetch();
        c.pop_into(&mut out, 1 + next_out % 5);
        while let Some(value) = c.pop() {
            out.push(value);
            if out.len() % 3 == 0 {
                break;
            }
        }
        c.release();
        for value in out.drain(..) {
            assert!(same(&value, next_out), "{value:?} at {next_out}");
            next_out += 1;
        }
    }
}

#[test]
fn growing_ring_moves_items_of_every_size() {
    let count = if cfg!(miri) { 300 } else { 5_000 };
    round_trip(count, |_| (), |(), _| true);
    round_trip(count, |i| i as u8, |v, i| *v == i as u8);
    round_trip(
        count,
        |i| [i as u8; 64],
        |v, i| v.iter().all(|b| *b == i as u8),
    );
    round_trip(
        count,
        |i| [i as u64; 32],
        |v, i| v.iter().all(|b| *b == i as u64),
    );
    round_trip(count, |i| i.to_string(), |v, i| *v == i.to_string());
    round_trip(count, Box::new, |v, i| **v == i);
}
