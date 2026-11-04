use super::*;
use crate::compat::Ordering;

// CI runs Miri with many seeds. Keep cross-thread coverage without turning
// per-seed validation into a throughput test.
fn miri_sized(n: u64) -> u64 {
    if cfg!(miri) { 2_048 } else { n }
}

#[test]
fn push_pop_basic() {
    let (mut p, mut c) = spsc::<u32>(4);
    assert!(c.prefetch_and_pop().is_none());
    p.push(1).unwrap();
    p.push(2).unwrap();
    // Not visible yet.
    assert!(c.prefetch_and_pop().is_none());
    p.flush();
    assert_eq!(c.prefetch_and_pop(), Some(1));
    assert_eq!(c.prefetch_and_pop(), Some(2));
    assert!(c.prefetch_and_pop().is_none());
}

#[test]
fn push_and_flush() {
    let (mut p, mut c) = spsc::<u32>(4);
    p.push_and_flush(42).unwrap();
    assert_eq!(c.prefetch_and_pop(), Some(42));
}

#[test]
fn prefetch_and_pop_with_full_reports_full_transition() {
    let (mut p, mut c) = spsc::<u32>(2);
    assert_eq!(c.prefetch_and_pop_with_full(), None);
    p.push(10).unwrap();
    p.push(20).unwrap();
    assert!(p.is_full());
    p.flush();

    assert_eq!(c.prefetch_and_pop_with_full(), Some((10, true)));
    assert_eq!(c.prefetch_and_pop_with_full(), Some((20, false)));
    assert_eq!(c.prefetch_and_pop_with_full(), None);
}

#[test]
fn prefetch_and_pop_with_full_observes_waiter_behind_cached_tail() {
    let (mut p, mut c) = spsc::<u32>(2);
    p.push(10).unwrap();
    p.push(20).unwrap();
    p.flush();

    assert_eq!(c.prefetch(), 2);
    assert_eq!(c.prefetch_and_pop_with_full(), Some((10, true)));

    p.push(30).unwrap();
    p.flush();
    assert_eq!(p.push(40), Err(40));

    assert_eq!(c.prefetch_and_pop_with_full(), Some((20, true)));
    assert_eq!(c.prefetch_and_pop_with_full(), Some((30, false)));
}

#[test]
fn prefetch_and_pop_with_full_wakes_for_unflushed_full_ring() {
    let (mut p, mut c) = spsc::<u32>(2);
    p.push_and_flush(10).unwrap();
    p.push(20).unwrap();
    assert_eq!(p.push(30), Err(30));

    // Fullness includes the producer's unpublished slot. The cached
    // tail alone cannot tell that the producer needs a space wake.
    assert_eq!(c.prefetch_and_pop_with_full(), Some((10, true)));
    p.push_and_flush(30).unwrap();
    assert_eq!(c.prefetch_and_pop(), Some(20));
    assert_eq!(c.prefetch_and_pop(), Some(30));
}

#[cfg(all(loom, target_pointer_width = "64"))]
#[test]
fn prefetch_and_pop_with_full_handles_cursor_wrap() {
    let base = usize::MAX - 1;
    let (mut p, mut c) = loom_spsc_with_cursors::<u32>(2, base);
    assert_eq!(c.prefetch_and_pop_with_full(), None);
    p.push(10).unwrap();
    p.push(20).unwrap();
    assert!(p.is_full());
    p.flush();

    assert_eq!(c.prefetch_and_pop_with_full(), Some((10, true)));
    assert_eq!(c.prefetch_and_pop_with_full(), Some((20, false)));
    assert_eq!(c.prefetch_and_pop_with_full(), None);
}

#[test]
fn release_with_full_releases_batches_and_preserves_prefetched_items() {
    let (mut producer, mut consumer) = spsc(4);
    assert!(!consumer.release_with_full());
    for item in 1..=4 {
        producer.push(item).unwrap();
    }
    producer.flush();
    assert!(producer.is_full());
    assert_eq!(consumer.prefetch(), 4);
    assert_eq!(consumer.pop(), Some(1));
    assert_eq!(consumer.pop(), Some(2));
    assert!(producer.is_full(), "pops alone must not publish credits");
    assert!(consumer.release_with_full());
    assert!(!consumer.release_with_full(), "no new credits need no wake");
    producer.push(5).unwrap();
    producer.push(6).unwrap();
    producer.flush();
    assert!(producer.is_full());
    // The consumer still sees its old two-item window. Registration must
    // cover the producer's full ring beyond that cached boundary.
    assert_eq!(consumer.pop(), Some(3));
    assert_eq!(consumer.pop(), Some(4));
    assert!(consumer.release_with_full());
    assert_eq!(consumer.prefetch(), 2);
    assert_eq!(consumer.pop(), Some(5));
    assert_eq!(consumer.pop(), Some(6));
    assert!(!consumer.release_with_full());
}

#[test]
fn release_with_full_preserves_owned_values_after_slot_reuse() {
    let (mut producer, mut consumer) = spsc(2);
    assert!(!consumer.release_with_full());
    producer.push(Box::new(10)).unwrap();
    producer.push_and_flush(Box::new(20)).unwrap();
    assert!(producer.is_full());
    assert_eq!(consumer.prefetch(), 2);

    let first = consumer.pop().unwrap();
    assert!(consumer.release_with_full());
    producer.push_and_flush(Box::new(30)).unwrap();
    assert!(producer.push(Box::new(40)).is_err());
    let second = consumer.pop().unwrap();
    assert!(consumer.release_with_full());

    assert_eq!(consumer.prefetch(), 1);
    let third = consumer.pop().unwrap();
    assert!(!consumer.release_with_full());
    // Popped allocations and the unread cached slot survive ring reuse.
    assert_eq!((*first, *second, *third), (10, 20, 30));
}

#[test]
fn release_with_full_observes_unpublished_full_slots() {
    let (mut producer, mut consumer) = spsc(2);
    assert!(!consumer.release_with_full());
    producer.push_and_flush(10).unwrap();
    producer.push(20).unwrap();
    assert_eq!(producer.push(30), Err(30));
    assert_eq!(consumer.prefetch(), 1);
    assert_eq!(consumer.pop(), Some(10));
    assert!(consumer.release_with_full());
    producer.push_and_flush(30).unwrap();
    assert_eq!(consumer.prefetch(), 2);
    assert_eq!(consumer.pop(), Some(20));
    assert_eq!(consumer.pop(), Some(30));
    assert!(consumer.release_with_full());
}

#[test]
fn release_with_full_without_pops_keeps_full_registration() {
    let (mut producer, mut consumer) = spsc(1);
    assert!(!consumer.release_with_full());
    producer.push_and_flush(10).unwrap();
    assert!(producer.is_full());
    assert_eq!(consumer.prefetch(), 1);
    assert!(!consumer.release_with_full());
    assert_eq!(consumer.pop(), Some(10));
    assert!(consumer.release_with_full());
    producer.push_and_flush(20).unwrap();
    assert_eq!(consumer.prefetch_and_pop(), Some(20));
    assert!(!consumer.release_with_full());
}

#[test]
fn release_with_full_activation_is_conservative() {
    let (mut producer, mut consumer) = spsc(4);
    producer.push_and_flush(10).unwrap();
    assert_eq!(consumer.prefetch(), 1);
    assert_eq!(consumer.pop(), Some(10));
    assert!(consumer.release_with_full());
    assert!(!consumer.release_with_full());
}

#[test]
fn producer_observes_consumer_close() {
    let (p, mut c) = spsc::<u32>(4);
    assert!(!p.is_consumer_dropped());
    c.close();
    assert!(p.is_consumer_dropped());
}

#[test]
fn producer_observes_consumer_drop() {
    let (p, c) = spsc::<u32>(4);
    assert!(!p.is_consumer_dropped());
    drop(c);
    assert!(p.is_consumer_dropped());
}

#[test]
fn producer_owner_rejects_concurrent_producer_thread() {
    use std::sync::{Arc, Barrier};

    let (producer, _consumer) = spsc::<u32>(4);
    let owner = Arc::new(ProducerOwner::new(producer));
    let barrier = Arc::new(Barrier::new(2));
    let first = owner.clone();
    let second = owner.clone();
    let first_barrier = barrier.clone();
    let first = std::thread::spawn(move || {
        first_barrier.wait();
        first.push(1)
    });
    let second_barrier = barrier;
    let second = std::thread::spawn(move || {
        second_barrier.wait();
        second.push(2)
    });

    let first = first.join();
    let second = second.join();
    assert_ne!(first.is_ok(), second.is_ok());
}

#[cfg(not(loom))]
#[test]
fn producer_owner_tokens_never_wrap() {
    use crate::producer::allocate_thread_token;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Exercise the real allocator without exhausting other tests' tokens
    // or relaunching this binary outside a cross-compilation runner.
    let counter = AtomicUsize::new(usize::MAX - 1);
    assert_eq!(allocate_thread_token(&counter), usize::MAX - 1);
    for _ in 0..3 {
        assert!(std::panic::catch_unwind(|| allocate_thread_token(&counter)).is_err());
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
    }
}

#[test]
fn batch_prefetch() {
    let (mut p, mut c) = spsc::<u32>(8);
    for i in 0..5 {
        p.push(i).unwrap();
    }
    assert_eq!(c.prefetch(), 0); // not flushed yet
    p.flush();
    assert_eq!(c.prefetch(), 5);
    for i in 0..5 {
        assert_eq!(c.pop(), Some(i));
    }
    assert!(c.pop().is_none());
}

#[test]
fn pop_into_moves_prefetched_window_in_order_and_appends() {
    let (mut p, mut c) = spsc::<u32>(8);
    let mut out = vec![99];
    assert_eq!(c.pop_into(&mut out, 4), 0);
    for i in 0..5 {
        p.push(i).unwrap();
    }
    p.flush();
    assert_eq!(c.pop_into(&mut out, 4), 0); // not prefetched yet
    assert_eq!(c.prefetch(), 5);
    assert_eq!(c.pop_into(&mut out, 3), 3);
    assert_eq!(out, [99, 0, 1, 2]);
    assert_eq!(c.pop(), Some(3));
    assert_eq!(c.pop_into(&mut out, 8), 1);
    assert_eq!(out, [99, 0, 1, 2, 4]);
    assert_eq!(c.pop_into(&mut out, 8), 0);
    assert_eq!(c.pop(), None);
    c.release();
    assert!(p.is_empty());
}

#[test]
fn pop_into_crosses_ring_wraparound_and_reuses_slots() {
    let (mut p, mut c) = spsc::<u32>(4);
    for i in 0..3 {
        p.push(i).unwrap();
    }
    p.flush();
    assert_eq!(c.prefetch(), 3);
    let mut out = Vec::new();
    assert_eq!(c.pop_into(&mut out, 3), 3);
    c.release();
    for i in 3..7 {
        p.push(i).unwrap();
    }
    assert_eq!(p.push(7), Err(7));
    p.flush();
    assert_eq!(c.prefetch(), 4);
    assert_eq!(c.pop_into(&mut out, 2), 2);
    assert_eq!(c.pop_into(&mut out, 4), 2);
    assert_eq!(out, [0, 1, 2, 3, 4, 5, 6]);
    c.release();
    for i in 7..11 {
        p.push(i).unwrap();
    }
    p.flush();
    assert_eq!(c.prefetch(), 4);
    assert_eq!(c.pop_into(&mut out, usize::MAX), 4);
    assert_eq!(out, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn pop_into_moves_owned_values_exactly_once() {
    let (mut p, mut c) = spsc::<Box<usize>>(4);
    for i in 0..4 {
        p.push(Box::new(i)).unwrap();
    }
    p.flush();
    assert_eq!(c.prefetch(), 4);
    let mut out = Vec::with_capacity(1);
    assert_eq!(c.pop_into(&mut out, 4), 4);
    c.release();
    p.push(Box::new(4)).unwrap();
    p.flush();
    assert_eq!(
        out.iter().map(|value| **value).collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert_eq!(c.prefetch(), 1);
    assert_eq!(c.pop_into(&mut out, 1), 1);
    assert_eq!(*out[4], 4);
    drop(out);
    drop(c);
    drop(p);
}

#[test]
fn pop_into_handles_zero_sized_values() {
    let (mut p, mut c) = spsc::<()>(2);
    p.push(()).unwrap();
    p.push(()).unwrap();
    p.flush();
    assert_eq!(c.prefetch(), 2);
    let mut out = Vec::new();
    assert_eq!(c.pop_into(&mut out, 5), 2);
    assert_eq!(out.len(), 2);
    assert_eq!(c.pop_into(&mut out, 5), 0);
}

#[test]
fn pop_into_crosses_cursor_wrap_boundary() {
    let (mut p, mut c) = spsc::<u32>(4);
    let base: Cursor = usize::MAX - 1;
    p.cursor = base;
    p.cached_head = base;
    c.head = base;
    c.cached_tail = base;
    p.ring.head.0.store(base, Ordering::Relaxed);
    p.ring.tail.0.store(base, Ordering::Relaxed);

    for i in 1..=4 {
        p.push(i).unwrap();
    }
    p.flush();
    assert_eq!(c.prefetch(), 4);
    let mut out = Vec::new();
    assert_eq!(c.pop_into(&mut out, 4), 4);
    assert_eq!(out, [1, 2, 3, 4]);
    assert_eq!(c.head, base.wrapping_add(4));
    c.release();
    assert!(p.is_empty());
}

#[test]
fn pop_into_while_moves_accepted_prefix_and_keeps_rejected_item() {
    let (mut p, mut c) = spsc::<u32>(8);
    let mut out = vec![99];
    assert_eq!(c.pop_into_while(&mut out, 8, |_| true), 0);
    for i in 0..6 {
        p.push(i).unwrap();
    }
    p.flush();
    assert_eq!(c.pop_into_while(&mut out, 8, |_| true), 0); // not prefetched yet
    assert_eq!(c.prefetch(), 6);
    assert_eq!(c.pop_into_while(&mut out, 8, |value| *value < 3), 3);
    assert_eq!(out, [99, 0, 1, 2]);
    // The rejected item stays at the front of the window.
    assert_eq!(c.pop_into_while(&mut out, 8, |value| *value < 3), 0);
    assert_eq!(c.pop(), Some(3));
    assert_eq!(c.pop_into_while(&mut out, 1, |_| true), 1);
    assert_eq!(out, [99, 0, 1, 2, 4]);
    assert_eq!(c.pop_into_while(&mut out, 8, |_| true), 1);
    assert_eq!(out, [99, 0, 1, 2, 4, 5]);
    assert_eq!(c.pop_into_while(&mut out, 8, |_| true), 0);
    assert_eq!(c.pop(), None);
    c.release();
    assert!(p.is_empty());
}

#[test]
fn pop_into_while_crosses_wraparound_and_stops_calling_after_rejection() {
    let (mut p, mut c) = spsc::<u32>(4);
    for i in 0..3 {
        p.push(i).unwrap();
    }
    p.flush();
    assert_eq!(c.prefetch(), 3);
    let mut out = Vec::new();
    assert_eq!(c.pop_into(&mut out, 3), 3);
    c.release();
    for i in 3..7 {
        p.push(i).unwrap();
    }
    p.flush();
    assert_eq!(c.prefetch(), 4);
    let mut calls = 0;
    let moved = c.pop_into_while(&mut out, 4, |value| {
        calls += 1;
        *value != 5
    });
    assert_eq!(moved, 2);
    assert_eq!(calls, 3);
    assert_eq!(out, [0, 1, 2, 3, 4]);
    assert_eq!(c.pop(), Some(5));
    assert_eq!(c.pop_into_while(&mut out, 4, |_| true), 1);
    assert_eq!(out, [0, 1, 2, 3, 4, 6]);
    c.release();
    assert!(p.is_empty());
}

#[test]
fn pop_into_while_moves_owned_values_exactly_once() {
    let (mut p, mut c) = spsc::<Box<usize>>(4);
    for i in 0..4 {
        p.push(Box::new(i)).unwrap();
    }
    p.flush();
    assert_eq!(c.prefetch(), 4);
    let mut out = Vec::with_capacity(1);
    assert_eq!(c.pop_into_while(&mut out, 4, |value| **value < 2), 2);
    assert_eq!(c.pop_into_while(&mut out, 4, |_| true), 2);
    c.release();
    p.push(Box::new(4)).unwrap();
    p.flush();
    assert_eq!(
        out.iter().map(|value| **value).collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert_eq!(c.prefetch(), 1);
    assert_eq!(c.pop_into_while(&mut out, 1, |_| true), 1);
    assert_eq!(*out[4], 4);
    drop(out);
    drop(c);
    drop(p);
}

#[test]
fn pop_into_while_zero_limit_never_calls_admit() {
    let (mut p, mut c) = spsc::<u32>(2);
    p.push(1).unwrap();
    p.flush();
    assert_eq!(c.prefetch(), 1);
    let mut out = Vec::new();
    assert_eq!(c.pop_into_while(&mut out, 0, |_| panic!("admit called")), 0);
    assert_eq!(c.pop(), Some(1));
}

#[test]
fn release_after_prefetch_releases_only_popped_items() {
    let (mut p, mut c) = spsc::<u32>(2);

    p.push(10).unwrap();
    p.push(20).unwrap();
    p.flush();

    assert_eq!(c.prefetch(), 2);
    assert_eq!(c.pop(), Some(10));
    c.release();

    p.push(30).unwrap();
    assert_eq!(p.push(40), Err(40));

    assert_eq!(c.pop(), Some(20));
    c.release();

    p.push(40).unwrap();
    p.flush();

    assert_eq!(c.prefetch(), 2);
    assert_eq!(c.pop(), Some(30));
    assert_eq!(c.pop(), Some(40));
    assert_eq!(c.pop(), None);
    c.release();
}

#[test]
fn flush_and_check_reports_was_empty() {
    let (mut p, mut c) = spsc::<u32>(4);
    p.push(1).unwrap();
    let r = p.flush_and_check();
    assert_eq!(
        r,
        FlushResult::Flushed {
            count: 1,
            was_empty: true
        }
    );

    p.push(2).unwrap();
    let r = p.flush_and_check();
    // Consumer hasn't read yet, so was_empty depends on cached_head
    assert!(matches!(r, FlushResult::Flushed { count: 1, .. }));

    c.prefetch_and_pop();
    c.prefetch_and_pop();
    p.push(3).unwrap();
    // Force head cache refresh
    p.push(4).unwrap();
    let _ = p.push(5);
    let _ = p.push(6);
    // After consumer consumed, the producer might see stale cached_head
    let r = p.flush_and_check();
    assert!(matches!(r, FlushResult::Flushed { .. }));
}

#[test]
fn full_ring() {
    let (mut p, mut c) = spsc::<u32>(4);
    for i in 0..4 {
        p.push(i).unwrap();
    }
    assert!(p.push(99).is_err());
    p.flush();
    assert_eq!(c.prefetch_and_pop(), Some(0));
    p.push(99).unwrap();
    p.flush();
    for i in 1..=4 {
        let expected = if i < 4 { i } else { 99 };
        assert_eq!(c.prefetch_and_pop(), Some(expected));
    }
}

#[test]
fn wraps_around() {
    let (mut p, mut c) = spsc::<u32>(2);
    for round in 0..100 {
        p.push(round * 2).unwrap();
        p.push(round * 2 + 1).unwrap();
        p.flush();
        assert_eq!(c.prefetch_and_pop(), Some(round * 2));
        assert_eq!(c.prefetch_and_pop(), Some(round * 2 + 1));
    }
}

#[test]
fn capacity_rounds_up() {
    let (p, _c) = spsc::<u8>(3);
    assert_eq!(p.capacity(), 4);
    let (p, _c) = spsc::<u8>(5);
    assert_eq!(p.capacity(), 8);
    let (p, _c) = spsc::<u8>(1);
    assert_eq!(p.capacity(), 1);
}

#[test]
#[should_panic(expected = "capacity must fit in the next power of two")]
fn capacity_overflow_panics_with_message() {
    let _ = spsc::<u8>(usize::MAX);
}

#[test]
#[should_panic(expected = "capacity must be <= half the cursor range")]
fn capacity_half_range_panics_with_message() {
    let _ = spsc::<u8>((usize::MAX / 2) + 1);
}

#[test]
fn counters_wrap_without_panicking() {
    let (mut p, mut c) = spsc::<u32>(4);
    let base: Cursor = usize::MAX - 1;
    p.cursor = base;
    p.cached_head = base;
    c.head = base;
    c.cached_tail = base;
    p.ring.head.0.store(base, Ordering::Relaxed);
    p.ring.tail.0.store(base, Ordering::Relaxed);

    p.push(1).unwrap();
    p.push(2).unwrap();
    p.push(3).unwrap();
    assert_eq!(
        p.flush_and_check(),
        FlushResult::Flushed {
            count: 3,
            was_empty: true
        }
    );

    assert_eq!(c.prefetch(), 3);
    assert_eq!(c.pop(), Some(1));
    assert_eq!(c.pop(), Some(2));
    assert_eq!(c.pop(), Some(3));
    assert_eq!(c.pop(), None);
    c.release();
    assert!(p.is_empty());
}

#[test]
fn counters_cross_u32_boundary_without_aliasing() {
    let (mut p, mut c) = spsc::<u32>(4);
    let base: Cursor = u32::MAX as usize - 1;
    p.cursor = base;
    p.cached_head = base;
    c.head = base;
    c.cached_tail = base;
    p.ring.head.0.store(base, Ordering::Relaxed);
    p.ring.tail.0.store(base, Ordering::Relaxed);

    for i in 0..4 {
        p.push(i).unwrap();
    }
    assert_eq!(p.push(99), Err(99));
    assert_eq!(
        p.flush_and_check(),
        FlushResult::Flushed {
            count: 4,
            was_empty: true
        }
    );

    assert_eq!(c.prefetch(), 4);
    for i in 0..4 {
        assert_eq!(c.pop(), Some(i));
    }
    assert_eq!(c.pop(), None);
    c.release();
    assert!(p.is_empty());
}

#[test]
fn drop_remaining() {
    use std::sync::atomic::AtomicUsize;
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct Counted;
    impl Drop for Counted {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }
    DROPS.store(0, Ordering::Relaxed);
    let (mut p, c) = spsc::<Counted>(4);
    p.push(Counted).unwrap();
    p.push(Counted).unwrap();
    p.push(Counted).unwrap();
    p.flush();
    drop(p);
    drop(c);
    assert_eq!(DROPS.load(Ordering::Relaxed), 3);
}

#[test]
fn drop_remaining_handles_wrapped_counters() {
    use std::sync::atomic::AtomicUsize;
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct Counted;
    impl Drop for Counted {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    DROPS.store(0, Ordering::Relaxed);
    let (mut p, mut c) = spsc::<Counted>(4);
    let base: Cursor = usize::MAX - 1;
    p.cursor = base;
    p.cached_head = base;
    c.head = base;
    c.cached_tail = base;
    p.ring.head.0.store(base, Ordering::Relaxed);
    p.ring.tail.0.store(base, Ordering::Relaxed);

    p.push(Counted).unwrap();
    p.push(Counted).unwrap();
    p.push(Counted).unwrap();
    p.flush();
    drop(p);
    drop(c);
    assert_eq!(DROPS.load(Ordering::Relaxed), 3);
}

#[test]
fn drop_remaining_panicking_drop() {
    use std::sync::atomic::AtomicUsize;
    static DROPS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct PanicOnDrop(bool);
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
            if self.0 {
                self.0 = false;
                panic!("intentional panic in drop");
            }
        }
    }

    DROPS.store(0, Ordering::Relaxed);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (mut p, _c) = spsc::<PanicOnDrop>(4);
        p.push(PanicOnDrop(false)).unwrap();
        p.push(PanicOnDrop(true)).unwrap(); // this one panics
        p.push(PanicOnDrop(false)).unwrap();
        p.flush();
        // Ring::drop runs, calls drop_remaining. Item 1 panics.
        // Items 0 and 1 are dropped. Item 2 is leaked (not double-dropped).
    }));
    assert!(result.is_err());
    // Item 0 dropped normally, item 1's Drop panicked (but incremented
    // before panicking), item 2 leaked. Exactly 2 drops.
    assert_eq!(DROPS.load(Ordering::Relaxed), 2);
}

#[test]
fn cross_thread() {
    let (mut p, mut c) = spsc::<u64>(1024);
    let n = miri_sized(100_000);
    let sender = std::thread::spawn(move || {
        for i in 0..n {
            while p.push(i).is_err() {
                p.flush();
                std::thread::yield_now();
            }
            p.flush();
        }
    });
    let mut received = 0u64;
    while received < n {
        if c.prefetch() > 0 {
            while let Some(v) = c.pop() {
                assert_eq!(v, received);
                received += 1;
            }
            c.release();
        } else {
            std::thread::yield_now();
        }
    }
    sender.join().unwrap();
}

#[test]
fn flush_and_check_was_empty_after_consumer_drains() {
    let (mut p, mut c) = spsc::<u32>(4);
    for i in 0..5 {
        p.push_and_flush(i).unwrap();
        let val = c.prefetch_and_pop().unwrap();
        assert_eq!(val, i);
    }
    p.push(99).unwrap();
    let r = p.flush_and_check();
    assert_eq!(
        r,
        FlushResult::Flushed {
            count: 1,
            was_empty: true,
        }
    );
}
