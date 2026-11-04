//! Bulk pops into a vector racing concurrent publication.

use loom::thread;

#[test]
fn pop_into_moves_concurrently_published_windows_in_order() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        let h = thread::spawn(move || {
            p.push(1).unwrap();
            p.flush();
            let mut value = 2;
            while value <= 3 {
                if p.push(value).is_ok() {
                    p.flush();
                    value += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        let mut out = Vec::new();
        while out.len() < 3 {
            if c.prefetch() > 0 {
                let before = out.len();
                let moved = c.pop_into(&mut out, 2);
                assert!((1..=2).contains(&moved));
                assert_eq!(out.len(), before + moved);
                assert_eq!(c.pop_into(&mut out, 2), 0);
                c.release();
            } else {
                thread::yield_now();
            }
        }
        assert_eq!(out, [1, 2, 3]);

        h.join().unwrap();
    });
}

#[test]
fn pop_into_values_survive_slot_reuse() {
    use loom::sync::Arc;

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<Arc<usize>>(2);

        p.push(Arc::new(10)).unwrap();
        p.push(Arc::new(20)).unwrap();
        p.flush();

        let mut out = Vec::new();
        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.pop_into(&mut out, 2), 2);
        c.release();

        let h = thread::spawn(move || {
            while p.push(Arc::new(30)).is_err() {
                thread::yield_now();
            }
            p.flush();
        });

        h.join().unwrap();
        assert_eq!(*out[0], 10);
        assert_eq!(*out[1], 20);

        assert_eq!(c.prefetch(), 1);
        assert_eq!(c.pop_into(&mut out, 2), 1);
        c.release();
        assert_eq!(
            out.iter().map(|value| **value).collect::<Vec<_>>(),
            [10, 20, 30]
        );
    });
}

#[test]
fn pop_into_crosses_pointer_width_cursor_wrap() {
    loom::model(|| {
        let base = usize::MAX - 1;
        let (mut p, mut c) = yring::loom_spsc_with_cursors::<u32>(2, base);

        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.flush();
        });

        let mut out = Vec::new();
        loop {
            if c.prefetch() > 0 {
                assert_eq!(c.pop_into(&mut out, 4), 2);
                assert_eq!(out, [10, 20]);
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
fn pop_into_while_moves_accepted_prefix_of_concurrent_windows() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        let h = thread::spawn(move || {
            p.push(1).unwrap();
            p.flush();
            let mut value = 2;
            while value <= 3 {
                if p.push(value).is_ok() {
                    p.flush();
                    value += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        let mut out = Vec::new();
        // Track the prefetched window: a rejected value stays in it without
        // adding to the next prefetch count.
        let mut window = 0usize;
        while out.len() < 3 {
            window += c.prefetch();
            if window == 0 {
                thread::yield_now();
                continue;
            }
            let before = out.len();
            // Reject 3 while it is at the front, then take it explicitly.
            let moved = c.pop_into_while(&mut out, 2, |value| *value != 3);
            assert!(moved <= 2);
            assert_eq!(out.len(), before + moved);
            window -= moved;
            if moved == 0 {
                assert_eq!(c.pop(), Some(3));
                out.push(3);
                window -= 1;
            }
            c.release();
        }
        assert_eq!(out, [1, 2, 3]);

        h.join().unwrap();
    });
}
