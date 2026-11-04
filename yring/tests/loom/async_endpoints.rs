//! Async producer and consumer wake registration races.

use loom::thread;

/// Counts the wakes delivered to a task.
struct CountWakes(loom::sync::atomic::AtomicUsize);

impl std::task::Wake for CountWakes {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, loom::sync::atomic::Ordering::Relaxed);
    }
}

/// Returns a waker that ignores every wake.
fn noop_waker() -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};

    fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: no vtable function reads the data pointer, so null is valid.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[cfg(feature = "async")]
#[test]
fn stream_drains_final_item_before_eof() {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use futures_core::Stream;

    loom::model(|| {
        let (mut p, mut c) = yring::async_spsc::<u32>(1);
        let h = thread::spawn(move || {
            p.push(42).unwrap();
            drop(p);
        });

        let mut cx = Context::from_waker(Waker::noop());
        // Race the empty check and EOF check against the producer's final
        // flush. Joining before this poll would hide the race.
        let result = Pin::new(&mut c).poll_next(&mut cx);
        h.join().unwrap();
        match result {
            Poll::Ready(None) => panic!("premature EOF with {} queued items", c.len()),
            Poll::Ready(Some(value)) => assert_eq!(value, 42),
            Poll::Pending => {
                assert_eq!(Pin::new(&mut c).poll_next(&mut cx), Poll::Ready(Some(42)));
            }
        }
    });
}

/// Verify `push_async` doesn't lose wakeups.
///
/// The critical race: consumer releases between the producer's
/// "ring is full" check and waker registration. The retry after
/// registration (step 4 in `PushFuture::poll`) must catch this.
#[test]
fn push_async_no_lost_wakeup() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    loom::model(|| {
        let (mut p, mut c) = yring::async_spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let pushed = Arc::new(AtomicBool::new(false));
        let pushed2 = pushed.clone();

        // Producer polls push_async on spawned thread.
        let h = thread::spawn(move || {
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            let mut fut = p.push_async(20);
            let mut pinned = Pin::new(&mut fut);

            loop {
                match pinned.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(())) => {
                        pushed2.store(true, Ordering::Relaxed);
                        break;
                    }
                    Poll::Ready(Err(_)) => panic!("push_async returned Err"),
                    Poll::Pending => thread::yield_now(),
                }
            }
        });

        // Consumer drains on main thread (stays alive until join).
        c.prefetch();
        let v = c.pop().unwrap();
        c.release();
        assert_eq!(v, 10);

        h.join().unwrap();
        assert!(pushed.load(Ordering::Relaxed));
    });
}

#[test]
fn push_async_pending_receives_space_wake() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    loom::model(|| {
        let (mut p, mut c) = yring::async_spsc::<u32>(1);
        p.push_and_flush(10).unwrap();
        let notifications = Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(notifications.clone());
        let released = Arc::new(AtomicBool::new(false));
        let released_for_producer = released.clone();
        let h = thread::spawn(move || {
            let mut cx = Context::from_waker(&waker);
            let result = {
                let mut future = p.push_async(20);
                let result = Pin::new(&mut future).poll(&mut cx);
                while !released_for_producer.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                result
            };
            (p, result)
        });

        assert_eq!(c.prefetch_and_pop(), Some(10));
        released.store(true, Ordering::Release);
        let (_p, result) = h.join().unwrap();
        // Never repoll an unwoken Pending future: that would hide a lost wake.
        match result {
            Poll::Pending => assert!(notifications.0.load(Ordering::Relaxed) > 0),
            Poll::Ready(result) => assert_eq!(result, Ok(())),
        }
    });
}

#[test]
fn poll_ready_pending_receives_space_wake() {
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    loom::model(|| {
        let (mut p, mut c) = yring::async_spsc::<u32>(1);
        p.push_and_flush(10).unwrap();
        let notifications = Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(notifications.clone());
        let released = Arc::new(AtomicBool::new(false));
        let released_for_producer = released.clone();
        let h = thread::spawn(move || {
            let mut cx = Context::from_waker(&waker);
            let result = {
                let result = p.poll_ready(&mut cx);
                while !released_for_producer.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                result
            };
            (p, result)
        });

        assert_eq!(c.prefetch_and_pop(), Some(10));
        released.store(true, Ordering::Release);
        let (mut producer, result) = h.join().unwrap();
        // Never repoll an unwoken Pending future: that would hide a lost wake.
        match result {
            Poll::Pending => assert!(notifications.0.load(Ordering::Relaxed) > 0),
            Poll::Ready(()) => assert!(!producer.is_full()),
        }
    });
}

#[test]
fn poll_ready_replaces_cancelled_waiter_without_losing_space_wake() {
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    loom::model(|| {
        let (mut producer, mut consumer) = yring::async_spsc::<u32>(1);
        producer.push_and_flush(10).unwrap();
        let old_notifications = Arc::new(CountWakes(AtomicUsize::new(0)));
        let old_waker = Waker::from(old_notifications.clone());
        assert_eq!(
            producer.poll_ready(&mut Context::from_waker(&old_waker)),
            Poll::Pending
        );
        // Cancel the first wait. The next wait supplies a different waker.
        let notifications = Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(notifications.clone());
        let released = Arc::new(AtomicBool::new(false));
        let released_for_producer = released.clone();
        let sender = thread::spawn(move || {
            let result = producer.poll_ready(&mut Context::from_waker(&waker));
            while !released_for_producer.load(Ordering::Acquire) {
                thread::yield_now();
            }
            (producer, result)
        });

        assert_eq!(consumer.prefetch_and_pop(), Some(10));
        released.store(true, Ordering::Release);
        let (mut producer, result) = sender.join().unwrap();
        // An old wake or endpoint drop must not rescue an unwoken new waiter.
        match result {
            Poll::Pending => assert!(notifications.0.load(Ordering::Relaxed) > 0),
            Poll::Ready(()) => assert!(!producer.is_full()),
        }
    });
}

#[test]
fn poll_ready_pending_receives_concurrent_shutdown_wake() {
    use std::sync::Arc;
    use std::task::{Context, Waker};

    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    loom::model(|| {
        let (mut producer, consumer) = yring::async_spsc::<u32>(1);
        producer.push_and_flush(10).unwrap();
        let notifications = Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(notifications.clone());
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_for_producer = dropped.clone();
        let sender = thread::spawn(move || {
            let result = producer.poll_ready(&mut Context::from_waker(&waker));
            while !dropped_for_producer.load(Ordering::Acquire) {
                thread::yield_now();
            }
            (producer, result)
        });

        drop(consumer);
        dropped.store(true, Ordering::Release);
        let (producer, result) = sender.join().unwrap();
        assert!(producer.is_consumer_dropped());
        if result.is_pending() {
            assert!(notifications.0.load(Ordering::Relaxed) > 0);
        }
    });
}

/// Verify `push_async` detects consumer drop.
#[test]
fn push_async_consumer_dropped() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    loom::model(|| {
        let (mut p, c) = yring::async_spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let h = thread::spawn(move || {
            drop(c);
        });

        h.join().unwrap();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = p.push_async(20);
        let pinned = Pin::new(&mut fut);

        match pinned.poll(&mut cx) {
            Poll::Ready(Err(20)) => {}
            other => panic!("expected Err(20), got {other:?}"),
        }
    });
}

/// Verify `push_async` detects consumer drop during waker registration.
#[test]
fn push_async_consumer_drop_during_registration() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use std::sync::{Arc, Mutex};

    struct DropConsumerOnClone {
        consumer: Mutex<Option<yring::AsyncConsumer<u32>>>,
    }

    fn clone(data: *const ()) -> RawWaker {
        let hook = unsafe { Arc::from_raw(data.cast::<DropConsumerOnClone>()) };
        hook.consumer.lock().unwrap().take();
        let cloned = hook.clone();
        std::mem::forget(hook);
        RawWaker::new(Arc::into_raw(cloned).cast(), &VTABLE)
    }

    fn drop_waker(data: *const ()) {
        unsafe { std::mem::drop(Arc::from_raw(data.cast::<DropConsumerOnClone>())) };
    }

    fn noop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, drop_waker);

    loom::model(|| {
        let (mut producer, consumer) = yring::async_spsc::<u32>(1);
        producer.push(1).unwrap();
        producer.flush();

        let hook = Arc::new(DropConsumerOnClone {
            consumer: Mutex::new(Some(consumer)),
        });
        let waker =
            unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(hook.clone()).cast(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        let mut future = producer.push_async(2);

        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(Err(2)));
    });
}

#[test]
fn poll_ready_consumer_drop_during_registration() {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use std::sync::{Arc, Mutex};

    struct DropConsumerOnClone {
        consumer: Mutex<Option<yring::AsyncConsumer<u32>>>,
    }

    fn clone(data: *const ()) -> RawWaker {
        let hook = unsafe { Arc::from_raw(data.cast::<DropConsumerOnClone>()) };
        hook.consumer.lock().unwrap().take();
        let cloned = hook.clone();
        std::mem::forget(hook);
        RawWaker::new(Arc::into_raw(cloned).cast(), &VTABLE)
    }

    fn drop_waker(data: *const ()) {
        unsafe { std::mem::drop(Arc::from_raw(data.cast::<DropConsumerOnClone>())) };
    }

    fn noop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, drop_waker, noop, drop_waker);

    loom::model(|| {
        let (mut producer, consumer) = yring::async_spsc::<u32>(1);
        producer.push(1).unwrap();
        producer.flush();

        let hook = Arc::new(DropConsumerOnClone {
            consumer: Mutex::new(Some(consumer)),
        });
        let waker =
            unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(hook.clone()).cast(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        assert_eq!(producer.poll_ready(&mut cx), Poll::Ready(()));
        assert!(producer.is_consumer_dropped());
    });
}
