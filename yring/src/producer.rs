use std::cell::Cell;
use std::sync::OnceLock;

#[cfg(not(all(loom, target_pointer_width = "64")))]
use crate::compat::UnsafeCellExt;
use crate::compat::{Arc, Ordering, UnsafeCell};
use crate::{Cursor, FlushResult, Ring};

/// Sending half. Mutation requires exclusive access.
pub struct Producer<T> {
    pub(crate) ring: Arc<Ring<T>>,
    /// Private write position. No atomic; only the producer touches it.
    pub(crate) cursor: Cursor,
    /// Cached copy of the consumer's `head` to avoid Acquire loads.
    pub(crate) cached_head: Cursor,
}

impl<T> Producer<T> {
    /// Address of the underlying ring allocation. Two producers or
    /// consumers sharing the same ring return the same value.
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.close();
    }
}

// SAFETY: Producer mutations require &mut self and the underlying Ring is
// Send+Sync. Moving the producer to another thread is safe.
unsafe impl<T: Send> Send for Producer<T> {}

/// Single-thread owner handle for a producer shared through an `Arc`.
///
/// The caller must use this value from exactly one producer thread. This
/// preserves the SPSC producer cursor without a mutex. It is intended for
/// transports that already guarantee one socket thread, such as inproc. The
/// first producer call binds the owner to its current thread; later calls
/// from another thread panic.
pub struct ProducerOwner<T> {
    producer: UnsafeCell<Producer<T>>,
    owner: OnceLock<usize>,
}

// SAFETY: `with_producer` binds access to one thread before touching the
// UnsafeCell. Calls from other threads panic without accessing the cell.
unsafe impl<T: Send> Send for ProducerOwner<T> {}
unsafe impl<T: Send> Sync for ProducerOwner<T> {}

impl<T> ProducerOwner<T> {
    /// Wrap a producer for single-thread shared ownership.
    pub fn new(producer: Producer<T>) -> Self {
        Self {
            producer: UnsafeCell::new(producer),
            owner: OnceLock::new(),
        }
    }

    #[inline]
    fn with_producer<R>(&self, f: impl FnOnce(&mut Producer<T>) -> R) -> R {
        let current = current_thread_token();
        let owner = self.owner.get_or_init(|| current);
        assert_eq!(
            *owner, current,
            "ProducerOwner accessed from multiple producer threads"
        );
        // SAFETY: owner check above ensures one thread accesses producer.
        unsafe { self.producer.with_mut(|ptr| f(&mut *ptr)) }
    }

    /// Push one value without producer locking.
    #[inline]
    pub fn push(&self, value: T) -> Result<(), T> {
        self.with_producer(|producer| producer.push(value))
    }

    /// Push one value and publish pending values with one owner check.
    #[inline]
    pub fn push_flush(&self, value: T) -> Result<(), T> {
        self.with_producer(|producer| {
            let result = producer.push(value);
            if result.is_ok() {
                producer.flush();
            }
            result
        })
    }

    /// Publish pending values.
    #[inline]
    pub fn flush(&self) {
        self.with_producer(Producer::flush);
    }

    /// Test whether the ring is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.with_producer(Producer::is_full)
    }

    /// Test whether the consumer has gone away.
    #[inline]
    pub fn is_consumer_dropped(&self) -> bool {
        self.with_producer(|producer| producer.is_consumer_dropped())
    }
}

static NEXT_THREAD_TOKEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

thread_local! {
    static THREAD_TOKEN: Cell<usize> = const { Cell::new(0) };
}

#[inline]
fn current_thread_token() -> usize {
    THREAD_TOKEN.with(|token| {
        let current = token.get();
        if current != 0 {
            return current;
        }
        let current = allocate_thread_token(&NEXT_THREAD_TOKEN);
        token.set(current);
        current
    })
}

#[inline]
pub(crate) fn allocate_thread_token(counter: &std::sync::atomic::AtomicUsize) -> usize {
    let mut next = counter.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let following = next
            .checked_add(1)
            .expect("ProducerOwner thread tokens exhausted");
        match counter.compare_exchange_weak(
            next,
            following,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(current) => return current,
            Err(actual) => next = actual,
        }
    }
}

impl<T> std::fmt::Debug for ProducerOwner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerOwner").finish_non_exhaustive()
    }
}

impl<T> Producer<T> {
    /// Write a value without atomics while cached space remains.
    /// Returns `Err(val)` if full. When space hints are enabled, registers a
    /// waiter and retries before returning.
    /// The value is NOT visible to the consumer until [`flush`](Self::flush).
    #[inline]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        match self.ring.push(&mut self.cursor, &mut self.cached_head, val) {
            Ok(()) => Ok(()),
            Err(val) => self.push_after_full(val),
        }
    }

    #[cold]
    fn push_after_full(&mut self, val: T) -> Result<(), T> {
        if !self.ring.producer_waiting.0.register() {
            return Err(val);
        }
        self.ring.push(&mut self.cursor, &mut self.cached_head, val)
    }

    /// Make all pushed items visible to the consumer. One Release store.
    /// Does NOT load `head`; `push()` refreshes `cached_head` on demand
    /// when the ring appears full.
    #[inline]
    pub fn flush(&mut self) {
        self.ring.tail.0.store(self.cursor, Ordering::Release);
    }

    /// Flush and report whether the consumer needs a wake.
    ///
    /// In addition to the empty snapshot, this acknowledges registrations
    /// made by [`Consumer::prefetch`] or [`Consumer::is_empty`]. The consumer
    /// must use one of those checks before waiting, and the caller must
    /// signal whenever `was_empty` is true. Extra wakes are possible.
    /// Use [`flush`](Self::flush) when signaling through a separate protocol.
    ///
    /// [`Consumer::prefetch`]: crate::Consumer::prefetch
    /// [`Consumer::is_empty`]: crate::Consumer::is_empty
    #[inline]
    pub fn flush_and_check(&mut self) -> FlushResult {
        self.ring.flush_to(self.cursor, &mut self.cached_head)
    }

    /// Push + flush in one call (convenience for single-item sends).
    #[inline]
    pub fn push_and_flush(&mut self, val: T) -> Result<(), T> {
        self.push(val)?;
        self.flush();
        Ok(())
    }

    #[inline]
    /// Return whether the ring cannot accept another item.
    pub fn is_full(&mut self) -> bool {
        if !self.ring.is_full(self.cursor, &mut self.cached_head) {
            return false;
        }
        if !self.ring.producer_waiting.0.register() {
            return true;
        }
        self.ring.is_full(self.cursor, &mut self.cached_head)
    }

    #[inline]
    /// Return the ring capacity after power-of-two rounding.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    /// Return the number of flushed and pending items owned by the producer.
    pub fn len(&self) -> usize {
        self.ring.producer_len(self.cursor)
    }

    #[inline]
    /// Return whether the producer has no pending or flushed items.
    pub fn is_empty(&self) -> bool {
        self.ring.producer_is_empty(self.cursor)
    }

    /// The consumer half has been dropped or explicitly closed.
    ///
    /// `push` does not check this flag so the zero-atomic hot path stays
    /// unchanged. Callers that need disconnect detection can check this
    /// before retrying a full ring or before blocking for space.
    #[inline]
    pub fn is_consumer_dropped(&self) -> bool {
        self.ring.consumer_dropped.load(Ordering::Acquire)
    }

    /// Mark the producer side closed after publishing pending writes.
    ///
    /// This is normally called by `Drop`. Calling it explicitly is useful
    /// for wrappers that need to wake a consumer after marking the producer
    /// as gone. After calling this, the producer should not be used again.
    #[inline]
    pub fn close(&mut self) {
        self.flush();
        self.ring.producer_dropped.store(true, Ordering::Release);
    }
}

impl<T> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}
