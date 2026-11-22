//! Growing rings: segment links, windows that span segments, wakeup hints
//! across segments, and drop with items in several segments.

use loom::sync::Arc;
use loom::thread;

/// Run `f` under Loom with a preemption bound unless `LOOM_MAX_PREEMPTIONS`
/// asks for another one.
fn bounded(bound: usize, f: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    if std::env::var("LOOM_MAX_PREEMPTIONS").is_err() {
        builder.preemption_bound = Some(bound);
    }
    // Consumers spin with `yield_now` until items arrive.
    if std::env::var("LOOM_MAX_BRANCHES").is_err() {
        builder.max_branches = 100_000;
    }
    builder.check(f);
}

fn push_waiting(producer: &mut yring::Producer<u32>, value: u32) {
    let mut value = value;
    while let Err(back) = producer.push(value) {
        value = back;
        producer.flush();
        thread::yield_now();
    }
}

/// Pop until `count` items arrived. `prefetch` counts only new items, so pop
/// whatever the window holds before deciding to wait.
fn collect(consumer: &mut yring::Consumer<u32>, count: usize) -> Vec<u32> {
    let mut out = Vec::new();
    while out.len() < count {
        consumer.prefetch();
        let before = out.len();
        while let Some(value) = consumer.pop() {
            out.push(value);
        }
        consumer.release();
        if out.len() == before {
            thread::yield_now();
        }
    }
    out
}

#[test]
fn consumer_follows_each_link_the_producer_publishes() {
    for flush_each in [false, true] {
        bounded(3, move || {
            let (mut p, mut c) = yring::spsc_growing::<u32>(1, 4);
            let h = thread::spawn(move || {
                for value in 1..=4 {
                    p.push(value).unwrap();
                    if flush_each {
                        p.flush();
                    }
                }
                p.flush();
            });

            assert_eq!(collect(&mut c, 4), [1, 2, 3, 4]);
            h.join().unwrap();
        });
    }
}

#[test]
fn flush_reports_empty_transition_into_a_new_segment() {
    for (check_empty, armed) in [(false, false), (true, false), (false, true), (true, true)] {
        loom::model(move || {
            let (mut p, mut c) = yring::spsc_growing::<u32>(1, 2);
            if armed {
                p.flush_and_check();
                assert_eq!(c.prefetch(), 0);
            }
            p.push_and_flush(1).unwrap();
            let h = thread::spawn(move || {
                // Grows unless the consumer already freed the first slot.
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
fn release_wakes_a_producer_full_behind_an_older_segment() {
    for armed in [false, true] {
        loom::model(move || {
            let (mut p, mut c) = yring::spsc_growing::<u32>(1, 2);
            if armed {
                assert!(!c.release_with_full());
            }
            p.push(1).unwrap();
            // Fills the first segment, so this grows to the final one.
            p.push_and_flush(2).unwrap();
            // The window spans both segments.
            assert_eq!(c.prefetch(), 2);

            let h = thread::spawn(move || {
                let blocked = p.push(3).is_err();
                (p, blocked)
            });
            assert_eq!(c.pop(), Some(1));
            let wake = c.release_with_full();
            let (mut p, blocked) = h.join().unwrap();
            if blocked {
                assert!(wake, "producer full behind an older segment lost its wake");
            }

            p.flush();
            let expected: &[u32] = if blocked { &[2] } else { &[2, 3] };
            assert_eq!(collect(&mut c, expected.len()), expected);
        });
    }
}

#[test]
fn dropping_endpoints_drops_items_left_in_every_segment() {
    loom::model(|| {
        let tracker = Arc::new(());
        let (mut p, mut c) = yring::spsc_growing::<Arc<()>>(1, 4);
        let items = tracker.clone();
        let h = thread::spawn(move || {
            for _ in 0..3 {
                p.push(items.clone()).unwrap();
            }
            p.flush();
        });

        if c.prefetch() > 0 {
            drop(c.pop());
            c.release();
        }
        h.join().unwrap();
        drop(c);
        assert_eq!(Arc::strong_count(&tracker), 1);
    });
}

#[test]
fn growth_across_the_cursor_wrap_keeps_fifo() {
    bounded(3, || {
        let (mut p, mut c) = yring::loom_spsc_growing_with_cursors::<u32>(1, 4, usize::MAX - 1);
        let h = thread::spawn(move || {
            for value in 1..=4 {
                p.push(value).unwrap();
                p.flush();
            }
        });
        assert_eq!(collect(&mut c, 4), [1, 2, 3, 4]);
        h.join().unwrap();
    });
}

#[test]
fn slow_and_batched_consumers_see_every_item_once() {
    for batched in [false, true] {
        bounded(3, move || {
            let (mut p, mut c) = yring::spsc_growing::<u32>(1, 4);
            let h = thread::spawn(move || {
                for value in 1..=4 {
                    push_waiting(&mut p, value);
                }
                p.flush();
            });

            let mut out = Vec::new();
            while out.len() < 4 {
                c.prefetch();
                let got = if batched {
                    c.pop_into(&mut out, 8) > 0
                } else {
                    // One item per prefetch, releasing each.
                    let value = c.pop();
                    let got = value.is_some();
                    out.extend(value);
                    got
                };
                c.release();
                if !got {
                    thread::yield_now();
                }
            }
            assert_eq!(out, [1, 2, 3, 4]);
            h.join().unwrap();
        });
    }
}

#[test]
fn pop_into_while_keeps_a_rejected_item_across_segment_ends() {
    bounded(3, || {
        let (mut p, mut c) = yring::spsc_growing::<u32>(1, 4);
        let h = thread::spawn(move || {
            for value in 1..=3 {
                p.push(value).unwrap();
            }
            p.flush();
        });

        let mut out = Vec::new();
        let mut rejected = false;
        while out.len() < 3 {
            c.prefetch();
            let was_rejected = rejected;
            // Reject 2 once; it stays at the front for the next call.
            let moved = c.pop_into_while(&mut out, 8, |value| {
                if *value == 2 && !rejected {
                    rejected = true;
                    return false;
                }
                true
            });
            c.release();
            if moved == 0 && rejected == was_rejected {
                thread::yield_now();
            }
        }
        assert_eq!(out, [1, 2, 3]);
        h.join().unwrap();
    });
}

#[test]
fn final_segment_reuses_freed_slots_while_an_older_one_drains() {
    bounded(3, || {
        let (mut p, mut c) = yring::spsc_growing::<u32>(1, 2);
        p.push(1).unwrap();
        // The first segment is full, so this links the final one.
        p.push_and_flush(2).unwrap();

        let h = thread::spawn(move || {
            for value in 3..=5 {
                push_waiting(&mut p, value);
                p.flush();
            }
        });
        assert_eq!(collect(&mut c, 5), [1, 2, 3, 4, 5]);
        h.join().unwrap();
    });
}

#[test]
fn prefetch_and_pop_with_full_wakes_across_segments() {
    for armed in [false, true] {
        bounded(3, move || {
            let (mut p, mut c) = yring::spsc_growing::<u32>(1, 2);
            if armed {
                assert_eq!(c.prefetch_and_pop_with_full(), None);
            }
            p.push(1).unwrap();
            p.push_and_flush(2).unwrap();

            let h = thread::spawn(move || {
                let blocked = p.push(3).is_err();
                (p, blocked)
            });
            let (first, wake) = loop {
                if let Some(item) = c.prefetch_and_pop_with_full() {
                    break item;
                }
                thread::yield_now();
            };
            assert_eq!(first, 1);
            let (mut p, blocked) = h.join().unwrap();
            if blocked {
                assert!(wake, "full producer lost its wake across a segment end");
            }
            p.flush();
            let expected: &[u32] = if blocked { &[2] } else { &[2, 3] };
            assert_eq!(collect(&mut c, expected.len()), expected);
        });
    }
}

#[test]
fn consumer_drop_during_growth_drops_every_item() {
    bounded(3, || {
        let tracker = Arc::new(());
        let (mut p, mut c) = yring::spsc_growing::<Arc<()>>(1, 8);
        let items = tracker.clone();
        let h = thread::spawn(move || {
            for _ in 0..4 {
                if p.push(items.clone()).is_err() {
                    break;
                }
                p.flush();
            }
        });

        if c.prefetch() > 0 {
            drop(c.pop());
            c.release();
        }
        drop(c);
        h.join().unwrap();
        assert_eq!(Arc::strong_count(&tracker), 1);
    });
}

#[test]
fn consumer_drains_a_grown_ring_after_the_producer_drops() {
    bounded(3, || {
        let (mut p, mut c) = yring::spsc_growing::<u32>(1, 4);
        let h = thread::spawn(move || {
            for value in 1..=3 {
                p.push(value).unwrap();
            }
            drop(p);
        });

        let mut out = Vec::new();
        while !c.is_disconnected() {
            c.prefetch();
            let before = out.len();
            while let Some(value) = c.pop() {
                out.push(value);
            }
            c.release();
            if out.len() == before {
                thread::yield_now();
            }
        }
        assert_eq!(out, [1, 2, 3]);
        h.join().unwrap();
    });
}

#[test]
fn is_full_reports_room_while_growth_is_possible() {
    bounded(3, || {
        let (mut p, mut c) = yring::spsc_growing::<u32>(1, 2);
        p.push_and_flush(1).unwrap();
        let h = thread::spawn(move || {
            let full = p.is_full();
            let pushed = p.push(2).is_ok();
            (full, pushed, p)
        });
        let _ = c.prefetch_and_pop();
        let (full, pushed, _p) = h.join().unwrap();
        assert!(!full, "a ring that can grow reported full");
        assert!(pushed);
    });
}

#[test]
fn producer_owner_grows_a_shared_producer_handle() {
    bounded(3, || {
        let (p, mut c) = yring::spsc_growing::<u32>(1, 4);
        let owner = Arc::new(yring::ProducerOwner::new(p));
        let producer = owner.clone();
        let h = thread::spawn(move || {
            for value in 1..=4 {
                while producer.push_flush(value).is_err() {
                    thread::yield_now();
                }
            }
        });
        assert_eq!(collect(&mut c, 4), [1, 2, 3, 4]);
        h.join().unwrap();
        drop(owner);
    });
}

#[test]
fn batch_pops_cross_segments_and_the_cursor_wrap() {
    bounded(3, || {
        let (mut p, mut c) = yring::loom_spsc_growing_with_cursors::<u32>(1, 4, usize::MAX);
        let h = thread::spawn(move || {
            for value in 1..=4 {
                p.push(value).unwrap();
            }
            p.flush();
        });

        let mut out = Vec::new();
        while out.len() < 4 {
            c.prefetch();
            if c.pop_into(&mut out, 3) == 0 {
                thread::yield_now();
            }
            c.release();
        }
        assert_eq!(out, [1, 2, 3, 4]);
        h.join().unwrap();
    });
}

#[test]
fn space_waiter_wakes_after_a_retire() {
    for check_full in [false, true] {
        bounded(3, move || {
            let (mut p, mut c) = yring::spsc_growing::<u32>(1, 2);
            assert!(!c.release_with_full());
            p.push(1).unwrap();
            p.push_and_flush(2).unwrap();
            let h = thread::spawn(move || {
                let blocked = if check_full {
                    p.is_full()
                } else {
                    p.push(3).is_err()
                };
                (p, blocked)
            });
            // Popping 1 drains the first segment; the next prefetch retires it.
            assert_eq!(c.prefetch(), 2);
            assert_eq!(c.pop(), Some(1));
            assert_eq!(c.pop(), Some(2));
            let wake = c.release_with_full();
            let (mut p, blocked) = h.join().unwrap();
            if blocked {
                assert!(wake, "space waiter lost its wake after a retire");
            }
            p.flush();
            let expected: &[u32] = if !check_full && !blocked { &[3] } else { &[] };
            if !expected.is_empty() {
                assert_eq!(collect(&mut c, expected.len()), expected);
            }
        });
    }
}

#[test]
fn single_prefetch_sees_every_segment_the_producer_linked() {
    bounded(3, || {
        let (mut p, mut c) = yring::spsc_growing::<u32>(1, 8);
        let (done_tx, done_rx) = loom::sync::mpsc::channel();
        let h = thread::spawn(move || {
            for value in 1..=5 {
                p.push(value).unwrap();
            }
            p.flush();
            done_tx.send(()).unwrap();
            p
        });
        done_rx.recv().unwrap();
        // One prefetch covers the segments of 1, 2, and 4 slots.
        assert_eq!(c.prefetch(), 5);
        let mut out = Vec::new();
        while let Some(value) = c.pop() {
            out.push(value);
        }
        assert_eq!(out, [1, 2, 3, 4, 5]);
        drop(h.join().unwrap());
    });
}
