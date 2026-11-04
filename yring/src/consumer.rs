use crate::compat::{Arc, Ordering};
use crate::{Cursor, Ring};

/// Receiving half. Mutation requires exclusive access.
pub struct Consumer<T> {
    pub(crate) ring: Arc<Ring<T>>,
    /// Private read position. Only the consumer touches it.
    pub(crate) head: Cursor,
    /// Cached copy of `tail`. Updated by `prefetch()`.
    pub(crate) cached_tail: Cursor,
}

impl<T> Consumer<T> {
    /// Address of the underlying ring allocation. Two producers or
    /// consumers sharing the same ring return the same value.
    pub fn ring_addr(&self) -> usize {
        Arc::as_ptr(&self.ring) as usize
    }
}

// SAFETY: Consumer mutations require &mut self and the underlying Ring is
// Send+Sync. Moving the consumer to another thread is safe.
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Consumer<T> {
    /// Pop one item. Zero atomics; reads from the prefetched window.
    /// Returns `None` when the prefetched window is exhausted. Call
    /// [`prefetch`](Self::prefetch) to load newly flushed items and
    /// [`release`](Self::release) to publish consumed slots back to the
    /// producer.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.ring.pop(&mut self.head, self.cached_tail)
    }

    /// Move up to `limit` prefetched items to the end of `output`.
    ///
    /// Zero atomics. Takes only from the window loaded by the last
    /// [`prefetch`](Self::prefetch), preserving FIFO order, and reserves
    /// output capacity for the moved items. Returns the number moved, which
    /// is zero when the window is exhausted. Call [`release`](Self::release)
    /// afterward to publish the consumed slots.
    #[inline]
    pub fn pop_into(&mut self, output: &mut Vec<T>, limit: usize) -> usize {
        let count = self.cached_tail.wrapping_sub(self.head).min(limit);
        if count == 0 {
            return 0;
        }
        // SAFETY: `count` is bounded by the prefetched window. The producer
        // published those slots with a Release store that `prefetch`
        // acquired, and the consumer owns them until `release` advances
        // `head` past them.
        unsafe { self.ring.pop_into(&mut self.head, count, output) };
        count
    }

    /// Move the accepted prefix of up to `limit` prefetched items to the end
    /// of `output`.
    ///
    /// `admit` sees each item in place, in FIFO order, and the move stops at
    /// the first item it rejects. That item stays at the front of the window
    /// for a later pop. Zero atomics; the accepted prefix moves with the same
    /// two contiguous copies as [`pop_into`](Self::pop_into). Returns the
    /// number moved, which is zero when the window is exhausted or the first
    /// item was rejected. Call [`release`](Self::release) afterward to
    /// publish the consumed slots.
    ///
    /// `admit` may see an item more than once: a rejected item is offered
    /// again by the next call, and if `admit` panics, nothing has moved and
    /// every item it saw is offered again.
    #[inline]
    pub fn pop_into_while(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
        mut admit: impl FnMut(&T) -> bool,
    ) -> usize {
        let window = self.cached_tail.wrapping_sub(self.head).min(limit);
        if window == 0 {
            return 0;
        }
        // SAFETY: `window` is bounded by the prefetched window, exactly as in
        // `pop_into`, and the consumer owns those slots until `release`.
        let count = unsafe { self.ring.accepted_prefix(self.head, window, &mut admit) };
        if count == 0 {
            return 0;
        }
        // SAFETY: `count <= window`, so the same bound holds.
        unsafe { self.ring.pop_into(&mut self.head, count, output) };
        count
    }

    /// Publish consumed position so the producer can reuse slots.
    /// One Release store. Call after draining a batch of pops.
    ///
    /// Only values already removed by [`pop`](Self::pop) are released.
    /// Prefetched-but-unpopped values stay reserved and remain readable.
    #[inline]
    pub fn release(&mut self) {
        self.ring.release(self.head);
    }

    /// Publish consumed slots and report whether the producer needs a wake.
    ///
    /// Like [`release`](Self::release), this releases only popped items. The
    /// caller chooses when to release a batch; this method sets no watermark.
    /// The hint includes observed fullness and registrations from
    /// [`Producer::push`] or [`Producer::is_full`], including producers ahead
    /// of the cached publication window. Signal whenever it returns true.
    /// Register external waiters before the final full check.
    ///
    /// Hints remain conservative until the producer acknowledges activation.
    /// No new credits returns false without clearing a producer registration.
    ///
    /// [`Producer::push`]: crate::Producer::push
    /// [`Producer::is_full`]: crate::Producer::is_full
    #[inline]
    #[must_use = "signal the producer whenever this hint is true"]
    pub fn release_with_full(&mut self) -> bool {
        self.ring.producer_waiting.0.enable();
        let published_head = self.ring.head.0.load(Ordering::Relaxed);
        if self.head == published_head {
            return false;
        }
        let was_full = self.cached_tail.wrapping_sub(published_head) >= self.capacity();
        self.ring.release(self.head);
        self.ring.producer_waiting.0.take(was_full)
    }

    /// Load all items flushed since the last prefetch.
    /// Returns the count of newly available items. When data hints are enabled,
    /// an empty window registers a waiter and rechecks before returning.
    #[inline]
    pub fn prefetch(&mut self) -> usize {
        let count = self.ring.prefetch(&mut self.cached_tail);
        if self.head != self.cached_tail {
            return count;
        }
        self.prefetch_after_empty()
    }

    #[cold]
    fn prefetch_after_empty(&mut self) -> usize {
        if !self.ring.consumer_waiting.0.register() {
            return 0;
        }
        self.ring.prefetch(&mut self.cached_tail)
    }

    /// Convenience: prefetch + pop + release. For callers that don't
    /// need batching.
    #[inline]
    pub fn prefetch_and_pop(&mut self) -> Option<T> {
        if self.head == self.cached_tail {
            self.prefetch();
        }
        let val = self.pop();
        if val.is_some() {
            self.release();
        }
        val
    }

    /// Prefetch + pop + release, returning whether the producer needs a wake.
    ///
    /// The hint includes both observed fullness and a producer registration
    /// from [`Producer::push`] or [`Producer::is_full`]. It may conservatively
    /// report true after a producer has already resumed. Signal whenever it
    /// is true; register external waiters before rechecking the full queue.
    ///
    /// [`Producer::push`]: crate::Producer::push
    /// [`Producer::is_full`]: crate::Producer::is_full
    #[inline]
    pub fn prefetch_and_pop_with_full(&mut self) -> Option<(T, bool)> {
        self.ring.producer_waiting.0.enable();
        if self.head == self.cached_tail {
            self.prefetch();
        }
        // A refilled ring can exceed this cached window. The producer's
        // full registration covers that case without a tail load per pop.
        let was_full = self.cached_tail.wrapping_sub(self.head) >= self.capacity();
        let val = self.pop()?;
        self.release();
        let wake = self.ring.producer_waiting.0.take(was_full);
        Some((val, wake))
    }

    #[inline]
    /// Return whether the consumer has no visible items.
    pub fn is_empty(&self) -> bool {
        if !self.ring.consumer_is_empty(self.head, self.cached_tail) {
            return false;
        }
        if !self.ring.consumer_waiting.0.register() {
            return true;
        }
        self.ring.consumer_is_empty(self.head, self.cached_tail)
    }

    /// The producer has been dropped and all flushed items have been
    /// consumed. No more data will ever arrive.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.ring.producer_dropped.load(Ordering::Acquire)
            && self.head == self.ring.tail.0.load(Ordering::Acquire)
    }

    #[inline]
    /// Return the ring capacity after power-of-two rounding.
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    /// Return the number of items visible to the consumer.
    pub fn len(&self) -> usize {
        self.ring.consumer_len(self.head)
    }

    /// Mark the consumer side closed and release consumed slots.
    ///
    /// This is normally called by `Drop`. Calling it explicitly is useful
    /// for wrappers that need to wake a producer after marking the consumer
    /// as gone. After calling this, the consumer should not be used again.
    #[inline]
    pub fn close(&mut self) {
        self.release();
        self.ring.consumer_dropped.store(true, Ordering::Release);
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T> std::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}
