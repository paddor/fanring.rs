//! Empty and full transition reports that decide when an endpoint wakes
//! its peer.

use loom::thread;

#[test]
fn prefetch_and_pop_with_full_reports_full_transition() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        assert_eq!(c.prefetch_and_pop_with_full(), None);
        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            assert!(p.is_full());
            p.flush();
        });

        let first = loop {
            if let Some(item) = c.prefetch_and_pop_with_full() {
                break item;
            }
            thread::yield_now();
        };
        assert_eq!(first, (10, true));
        assert_eq!(c.prefetch_and_pop_with_full(), Some((20, false)));
        assert_eq!(c.prefetch_and_pop_with_full(), None);

        h.join().unwrap();
    });
}

#[test]
fn prefetch_and_pop_with_full_observes_refilled_full_ring() {
    use loom::sync::mpsc;

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);
        p.push(10).unwrap();
        p.push(20).unwrap();
        p.flush();

        let (released_tx, released_rx) = mpsc::channel();
        let (attempted_tx, attempted_rx) = mpsc::channel();

        let h = thread::spawn(move || {
            released_rx.recv().unwrap();
            p.push(30).unwrap();
            p.flush();

            let mut value = match p.push(40) {
                Ok(()) => panic!("released second slot before second pop"),
                Err(value) => value,
            };
            attempted_tx.send(()).unwrap();

            while let Err(returned) = p.push(value) {
                value = returned;
                thread::yield_now();
            }
            p.flush();
        });

        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.prefetch_and_pop_with_full(), Some((10, true)));
        released_tx.send(()).unwrap();
        attempted_rx.recv().unwrap();

        assert_eq!(c.prefetch_and_pop_with_full(), Some((20, true)));

        let mut tail = Vec::new();
        while tail.len() < 2 {
            if let Some((value, _was_full)) = c.prefetch_and_pop_with_full() {
                tail.push(value);
            } else {
                thread::yield_now();
            }
        }
        assert_eq!(tail, [30, 40]);

        h.join().unwrap();
    });
}

#[test]
fn prefetch_and_pop_with_full_handles_cursor_wrap() {
    loom::model(|| {
        let base = usize::MAX - 1;
        let (mut p, mut c) = yring::loom_spsc_with_cursors::<u32>(2, base);

        assert_eq!(c.prefetch_and_pop_with_full(), None);
        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            assert!(p.is_full());
            p.flush();
        });

        let first = loop {
            if let Some(item) = c.prefetch_and_pop_with_full() {
                break item;
            }
            thread::yield_now();
        };
        assert_eq!(first, (10, true));
        assert_eq!(c.prefetch_and_pop_with_full(), Some((20, false)));
        assert_eq!(c.prefetch_and_pop_with_full(), None);

        h.join().unwrap();
    });
}

#[test]
fn flush_and_check_reports_concurrent_empty_transition() {
    for (check_empty, armed) in [(false, false), (true, false), (false, true), (true, true)] {
        loom::model(move || {
            let (mut p, mut c) = yring::spsc::<u32>(2);
            if armed {
                p.flush_and_check();
                assert_eq!(c.prefetch(), 0);
            }
            p.push_and_flush(1).unwrap();
            let h = thread::spawn(move || {
                p.push(2).unwrap();
                p.flush_and_check()
            });

            assert_eq!(c.prefetch_and_pop(), Some(1));
            let empty = if check_empty {
                c.is_empty()
            } else {
                c.prefetch_and_pop().is_none()
            };
            let result = h.join().unwrap();
            if empty {
                // A caller that parks after observing empty depends on this
                // flag to receive a wake for the second item.
                assert!(
                    matches!(
                        result,
                        yring::FlushResult::Flushed {
                            was_empty: true,
                            ..
                        }
                    ),
                    "consumer parks, but flush suppresses its wake: {result:?}"
                );
            }
        });
    }
}

#[test]
fn prefetch_and_pop_with_full_reports_concurrent_full_transition() {
    for (check_full, armed) in [(false, false), (true, false), (false, true), (true, true)] {
        loom::model(move || {
            let (mut p, mut c) = yring::spsc::<u32>(2);
            if armed {
                assert_eq!(c.prefetch_and_pop_with_full(), None);
                p.push(0).unwrap();
                p.push_and_flush(0).unwrap();
                assert!(p.is_full());
                assert_eq!(c.prefetch_and_pop_with_full(), Some((0, true)));
                assert_eq!(c.prefetch_and_pop_with_full(), Some((0, false)));
            }
            p.push_and_flush(1).unwrap();
            let h = thread::spawn(move || {
                p.push_and_flush(2).unwrap();
                let blocked = if check_full {
                    p.is_full()
                } else {
                    p.push(3).is_err()
                };
                (p, blocked)
            });

            let (first, first_wake) = c.prefetch_and_pop_with_full().unwrap();
            assert_eq!(first, 1);
            // Keep the producer alive: shutdown must not rescue a lost space wake.
            let (_p, blocked) = h.join().unwrap();
            let (second, second_wake) = c.prefetch_and_pop_with_full().unwrap();
            assert_eq!(second, 2);
            if blocked {
                assert!(
                    first_wake || second_wake,
                    "producer saw full, but both pops suppress its space wake"
                );
            }
        });
    }
}

#[test]
fn release_with_full_does_not_miss_registration_beyond_cached_tail() {
    for (check_full, publish) in [(false, false), (false, true), (true, false), (true, true)] {
        for armed in [false, true] {
            for wrap in [false, true] {
                loom::model(move || {
                    let (mut producer, mut consumer) = if wrap {
                        yring::loom_spsc_with_cursors::<u32>(2, usize::MAX - 1)
                    } else {
                        yring::spsc::<u32>(2)
                    };
                    if armed {
                        assert!(!consumer.release_with_full());
                        producer.push(0).unwrap();
                        producer.push_and_flush(0).unwrap();
                        assert!(producer.is_full());
                        assert_eq!(consumer.prefetch(), 2);
                        assert_eq!(consumer.pop(), Some(0));
                        assert_eq!(consumer.pop(), Some(0));
                        assert!(consumer.release_with_full());
                    }
                    producer.push_and_flush(1).unwrap();
                    assert_eq!(consumer.prefetch(), 1);
                    let publisher = thread::spawn(move || {
                        producer.push(2).unwrap();
                        if publish {
                            producer.flush();
                        }
                        let blocked = if check_full {
                            producer.is_full()
                        } else {
                            producer.push(3).is_err()
                        };
                        (producer, blocked)
                    });
                    assert_eq!(consumer.pop(), Some(1));
                    let wake = consumer.release_with_full();
                    // The cached tail excludes item 2. This release is the
                    // only wake; endpoints remain alive across the assertion.
                    let (mut producer, blocked) = publisher.join().unwrap();
                    if blocked {
                        assert!(wake, "full producer lost its partial-release wake");
                    }
                    producer.flush();
                    let expected = if !check_full && !blocked { 2 } else { 1 };
                    assert_eq!(consumer.prefetch(), expected);
                    assert_eq!(consumer.pop(), Some(2));
                    if expected == 2 {
                        assert_eq!(consumer.pop(), Some(3));
                    }
                    let _ = consumer.release_with_full();
                    drop(producer);
                });
            }
        }
    }
}

#[test]
fn release_with_full_reuses_only_popped_slots_across_cursor_wrap() {
    loom::model(|| {
        let (mut producer, mut consumer) = yring::loom_spsc_with_cursors::<u32>(4, usize::MAX - 1);
        assert!(!consumer.release_with_full());
        for value in 0..4 {
            producer.push(value).unwrap();
        }
        producer.flush();
        assert!(producer.is_full());
        assert_eq!(consumer.prefetch(), 4);
        assert_eq!(consumer.pop(), Some(0));
        assert_eq!(consumer.pop(), Some(1));
        assert!(consumer.release_with_full());
        producer.push(4).unwrap();
        producer.push(5).unwrap();
        assert_eq!(producer.push(6), Err(6));
        producer.flush();
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
        assert!(consumer.release_with_full());
        assert_eq!(consumer.prefetch(), 2);
        assert_eq!(consumer.pop(), Some(4));
        assert_eq!(consumer.pop(), Some(5));
        assert!(!consumer.release_with_full());
    });
}
