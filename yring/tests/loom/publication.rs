//! Push, flush, prefetch, pop, and release across threads, slot reuse, and
//! cursor wrap.

use loom::thread;

#[test]
fn push_flush_prefetch_pop() {
    loom::model(|| {
        let (mut producer, mut consumer) = yring::spsc::<u32>(2);

        let handle = thread::spawn(move || {
            producer.push(1).unwrap();
            producer.push(2).unwrap();
            producer.flush();
        });

        loop {
            if consumer.prefetch() > 0 {
                let first = consumer.pop().unwrap();
                let second = consumer.pop().unwrap();
                assert_eq!(first, 1);
                assert_eq!(second, 2);
                consumer.release();
                break;
            }
            thread::yield_now();
        }

        handle.join().unwrap();
    });
}

#[test]
fn push_full_release_retry() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let h = thread::spawn(move || {
            c.prefetch();
            let v = c.pop().unwrap();
            c.release();
            v
        });

        while p.push(20).is_err() {
            p.flush();
            thread::yield_now();
        }
        p.flush();

        let v = h.join().unwrap();
        assert_eq!(v, 10);
    });
}

#[test]
fn release_after_prefetch_releases_only_popped_items() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);
        p.push(10).unwrap();
        p.push(20).unwrap();
        p.flush();

        let released_one = Arc::new(AtomicBool::new(false));
        let attempted_second_push = Arc::new(AtomicBool::new(false));
        let released_one_for_producer = released_one.clone();
        let attempted_second_push_for_producer = attempted_second_push.clone();

        let h = thread::spawn(move || {
            while !released_one_for_producer.load(Ordering::Acquire) {
                thread::yield_now();
            }

            while p.push(30).is_err() {
                thread::yield_now();
            }
            let mut value = match p.push(40) {
                Ok(()) => panic!("released prefetched-but-unpopped slot"),
                Err(value) => value,
            };
            attempted_second_push_for_producer.store(true, Ordering::Release);

            while let Err(returned) = p.push(value) {
                value = returned;
                thread::yield_now();
            }
            p.flush();
        });

        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.pop(), Some(10));
        c.release();
        released_one.store(true, Ordering::Release);

        while !attempted_second_push.load(Ordering::Acquire) {
            thread::yield_now();
        }

        assert_eq!(c.pop(), Some(20));
        c.release();

        loop {
            if c.prefetch() > 0 {
                assert_eq!(c.pop(), Some(30));
                assert_eq!(c.pop(), Some(40));
                assert_eq!(c.pop(), None);
                c.release();
                break;
            }
            thread::yield_now();
        }

        h.join().unwrap();
    });
}

#[test]
fn producer_drop_flushes() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(4);

        let h = thread::spawn(move || {
            p.push(42).unwrap();
            drop(p);
        });

        h.join().unwrap();

        assert_eq!(c.prefetch(), 1);
        assert_eq!(c.pop(), Some(42));
    });
}

#[test]
fn wraparound_correctness() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        let h = thread::spawn(move || {
            for round in 0..2u32 {
                p.push(round * 2).unwrap();
                p.push(round * 2 + 1).unwrap();
                p.flush();
                while p.is_full() {
                    thread::yield_now();
                }
            }
        });

        let mut received = Vec::new();
        while received.len() < 4 {
            if c.prefetch() > 0 {
                while let Some(v) = c.pop() {
                    received.push(v);
                }
                c.release();
            } else {
                thread::yield_now();
            }
        }

        h.join().unwrap();
        assert_eq!(received, [0, 1, 2, 3]);
    });
}

#[test]
fn popped_value_survives_slot_reuse() {
    use loom::sync::Arc;

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<Arc<usize>>(1);

        p.push(Arc::new(10)).unwrap();
        p.flush();

        let first = loop {
            if c.prefetch() > 0 {
                let value = c.pop().unwrap();
                c.release();
                break value;
            }
            thread::yield_now();
        };

        let h = thread::spawn(move || {
            while p.push(Arc::new(20)).is_err() {
                thread::yield_now();
            }
            p.flush();
        });

        h.join().unwrap();
        assert_eq!(*first, 10);

        assert_eq!(c.prefetch(), 1);
        let second = c.pop().unwrap();
        c.release();
        assert_eq!(*first, 10);
        assert_eq!(*second, 20);
    });
}

#[test]
fn pointer_width_cursor_wrap_boundary() {
    loom::model(|| {
        let base = usize::MAX - 1;
        let (mut p, mut c) = yring::loom_spsc_with_cursors::<u32>(2, base);

        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.flush();
        });

        loop {
            if c.prefetch() > 0 {
                assert_eq!(c.pop(), Some(10));
                assert_eq!(c.pop(), Some(20));
                assert_eq!(c.pop(), None);
                c.release();
                break;
            }
            thread::yield_now();
        }

        h.join().unwrap();
    });
}

#[test]
fn is_disconnected_after_producer_drop() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(4);

        let h = thread::spawn(move || {
            p.push(1).unwrap();
            drop(p);
        });

        h.join().unwrap();

        c.prefetch();
        c.pop();
        c.release();
        assert!(c.is_disconnected());
    });
}
