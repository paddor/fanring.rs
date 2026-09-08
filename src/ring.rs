//! SPSC ownership handoff for receiver teardown.
//!
//! Closing a yring consumer alone retains unread values until its producer
//! drops. Deferred keeps that behavior. Coordinated gives cleanup ownership
//! to the receiver or to an overlapping send when it finishes.

use std::marker::PhantomData;

use crate::compat::{Arc, AtomicU8, Mutex, Ordering, lock};
use crate::teardown::Teardown;

const ACTIVE: u8 = 1;
const CLOSED: u8 = 2;

pub(crate) fn spsc<T, P: Teardown>(capacity: usize) -> (Producer<T, P>, Consumer<T, P>) {
    let (producer, consumer) = yring::spsc(capacity);
    let cleanup = P::COORDINATED.then(|| {
        Arc::new(Cleanup {
            state: AtomicU8::new(0),
            consumer: Mutex::new(None),
        })
    });
    (
        Producer {
            inner: producer,
            cleanup: cleanup.clone(),
            policy: PhantomData,
        },
        Consumer {
            inner: Some(consumer),
            cleanup,
            policy: PhantomData,
        },
    )
}

#[derive(Debug)]
struct Cleanup<T> {
    state: AtomicU8,
    consumer: Mutex<Option<yring::Consumer<T>>>,
}

impl<T> Cleanup<T> {
    #[inline]
    fn begin(&self) -> Option<SendGuard<'_, T>> {
        self.state
            .compare_exchange(0, ACTIVE, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| SendGuard(self))
    }

    fn close(&self, consumer: yring::Consumer<T>) {
        // Acquire the handoff slot before announcing CLOSED. The sender only
        // locks it after observing CLOSED, so teardown never waits for a
        // paused sender holding this mutex.
        let mut slot = lock(&self.consumer);
        *slot = Some(consumer);
        let previous = self.state.fetch_or(CLOSED, Ordering::AcqRel);
        let consumer = if previous & ACTIVE == 0 {
            slot.take()
        } else {
            None
        };
        drop(slot);
        if let Some(mut consumer) = consumer {
            discard(&mut consumer);
        }
    }
}

struct SendGuard<'a, T>(&'a Cleanup<T>);

impl<T> Drop for SendGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        if self.0.state.fetch_and(!ACTIVE, Ordering::AcqRel) & CLOSED != 0 {
            // Taking ownership before dropping payloads avoids running user
            // destructors under the handoff mutex.
            let consumer = lock(&self.0.consumer).take();
            if let Some(mut consumer) = consumer {
                discard(&mut consumer);
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Producer<T, P: Teardown> {
    inner: yring::Producer<T>,
    cleanup: Option<Arc<Cleanup<T>>>,
    policy: PhantomData<P>,
}

impl<T, P: Teardown> Producer<T, P> {
    #[inline]
    pub(crate) fn push_and_flush(&mut self, value: T) -> Result<(), T> {
        if !P::COORDINATED {
            self.inner.push(value)?;
            self.inner.flush();
            return Ok(());
        }
        let Some(_send) = self.cleanup.as_ref().expect("coordinated cleanup").begin() else {
            return Err(value);
        };
        self.inner.push(value)?;
        self.inner.flush();
        Ok(())
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub(crate) fn is_consumer_dropped(&self) -> bool {
        if P::COORDINATED {
            self.cleanup
                .as_ref()
                .expect("coordinated cleanup")
                .state
                .load(Ordering::Acquire)
                & CLOSED
                != 0
        } else {
            self.inner.is_consumer_dropped()
        }
    }

    #[inline]
    pub(crate) fn close(&mut self) {
        self.inner.close();
    }
}

pub(crate) struct Consumer<T, P: Teardown> {
    inner: Option<yring::Consumer<T>>,
    cleanup: Option<Arc<Cleanup<T>>>,
    policy: PhantomData<P>,
}

impl<T, P: Teardown> Consumer<T, P> {
    #[inline]
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.inner.as_mut().expect("consumer is open").pop()
    }

    #[inline]
    pub(crate) fn prefetch(&mut self) -> usize {
        self.inner.as_mut().expect("consumer is open").prefetch()
    }

    #[inline]
    pub(crate) fn release(&mut self) {
        self.inner.as_mut().expect("consumer is open").release();
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.inner.as_ref().expect("consumer is open").capacity()
    }

    #[inline]
    pub(crate) fn is_disconnected(&self) -> bool {
        self.inner
            .as_ref()
            .expect("consumer is open")
            .is_disconnected()
    }

    pub(crate) fn close(&mut self) {
        if let Some(mut consumer) = self.inner.take() {
            // Dispose of the current window even if a producer is paused.
            // The ownership handoff covers writes published after this drain.
            if P::COORDINATED {
                discard(&mut consumer);
                self.cleanup
                    .as_ref()
                    .expect("coordinated cleanup")
                    .close(consumer);
            } else {
                consumer.close();
            }
        }
    }
}

impl<T, P: Teardown> Drop for Consumer<T, P> {
    fn drop(&mut self) {
        self.close();
    }
}

fn discard<T>(consumer: &mut yring::Consumer<T>) {
    // prefetch() counts newly flushed values, not the unread part of an
    // existing window. Always pop the whole window, including a partial batch.
    consumer.prefetch();
    while let Some(value) = consumer.pop() {
        drop(value);
    }
    // Keep slot credit reserved until the final owner drops the consumer.
    // Releasing during the first drain could let an in-flight send reuse a
    // formerly full slot and succeed solely because the receiver is closing.
}

#[cfg(all(test, not(loom)))]
mod tests {
    use crate::teardown::Coordinated;
    fn spsc<T>(
        capacity: usize,
    ) -> (
        super::Producer<T, Coordinated>,
        super::Consumer<T, Coordinated>,
    ) {
        super::spsc(capacity)
    }
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn close_keeps_full_slot_reserved_until_active_send_finishes() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, rx) = spsc(1);
        tx.push_and_flush(Counted(drops.clone())).unwrap();
        let send = tx.cleanup.as_ref().unwrap().begin().unwrap();
        drop(rx);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        let value = tx.inner.push(Counted(drops.clone())).unwrap_err();
        drop(send);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(value);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        drop(tx);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn close_does_not_wait_for_paused_publication() {
        // Pause after acquiring publication ownership, on both sides of the
        // payload write and the flush. The producer stays alive for assertions.
        for phase in 0..3 {
            let first = Arc::new(AtomicUsize::new(0));
            let second = Arc::new(AtomicUsize::new(0));
            let (mut tx, rx) = spsc(2);
            tx.push_and_flush(Counted(first.clone())).unwrap();
            let value = Counted(second.clone());
            let (paused_tx, paused_rx) = std::sync::mpsc::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            let sender = std::thread::spawn(move || {
                let send = tx.cleanup.as_ref().unwrap().begin().unwrap();
                if phase == 0 {
                    paused_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }
                tx.inner.push(value).unwrap();
                if phase == 1 {
                    paused_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }
                tx.inner.flush();
                if phase == 2 {
                    paused_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }
                drop(send);
                tx
            });
            paused_rx.recv().unwrap();
            let (closed_tx, closed_rx) = std::sync::mpsc::channel();
            let receiver = std::thread::spawn(move || {
                drop(rx);
                closed_tx.send(()).unwrap();
            });
            let closed = closed_rx.recv_timeout(Duration::from_secs(5));
            let first_at_close = first.load(Ordering::Relaxed);
            let second_at_close = second.load(Ordering::Relaxed);
            // Resume even on timeout, so a failing test can join its threads.
            resume_tx.send(()).unwrap();
            receiver.join().unwrap();
            let tx = sender.join().unwrap();
            assert_eq!(closed, Ok(()), "phase {phase}");
            assert_eq!(first_at_close, 1, "phase {phase}");
            assert_eq!(second_at_close, usize::from(phase == 2), "phase {phase}");
            assert_eq!(first.load(Ordering::Relaxed), 1);
            assert_eq!(second.load(Ordering::Relaxed), 1);
            drop(tx);
            assert_eq!(first.load(Ordering::Relaxed), 1);
            assert_eq!(second.load(Ordering::Relaxed), 1);
        }
    }
}

#[cfg(all(test, loom, target_pointer_width = "64"))]
mod loom_tests {
    use crate::teardown::Coordinated;
    fn spsc<T>(
        capacity: usize,
    ) -> (
        super::Producer<T, Coordinated>,
        super::Consumer<T, Coordinated>,
    ) {
        super::spsc(capacity)
    }
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn close_racing_publication_drops_each_value_once() {
        loom::model(|| {
            let first = Arc::new(AtomicUsize::new(0));
            let second = Arc::new(AtomicUsize::new(0));
            let (mut tx, rx) = spsc(2);
            tx.push_and_flush(Counted(first.clone())).unwrap();
            let value = Counted(second.clone());
            let sender = loom::thread::spawn(move || {
                drop(tx.push_and_flush(value));
                tx
            });
            drop(rx);
            let tx = sender.join().unwrap();
            assert_eq!(first.load(Ordering::Relaxed), 1);
            assert_eq!(second.load(Ordering::Relaxed), 1);
            drop(tx);
            assert_eq!(first.load(Ordering::Relaxed), 1);
            assert_eq!(second.load(Ordering::Relaxed), 1);
        });
    }
}
