use std::mem::MaybeUninit;

#[cfg(not(all(loom, target_pointer_width = "64")))]
use crate::compat::UnsafeCellExt;
use crate::compat::{AtomicBool, Ordering, UnsafeCell};
use crate::{AtomicCursor, Cursor, FlushResult, Padded};

// Enabled by the notifying endpoint on first use of a wakeup-hint helper.
// The peer acknowledges activation before hints may suppress notifications.
pub(crate) struct Waiter {
    pub(crate) enabled: AtomicBool,
    pub(crate) armed: AtomicBool,
    pub(crate) waiting: AtomicBool,
}

impl Waiter {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            armed: AtomicBool::new(false),
            waiting: AtomicBool::new(false),
        }
    }

    #[inline]
    pub(crate) fn enable(&self) {
        if !self.enabled.load(Ordering::Relaxed) {
            self.enabled.store(true, Ordering::Release);
        }
    }

    #[inline]
    pub(crate) fn register(&self) -> bool {
        if !self.enabled.load(Ordering::Acquire) {
            return false;
        }
        self.waiting.swap(true, Ordering::AcqRel);
        // A notifier may suppress wakes only after acquiring this acknowledgment.
        // Registration precedes it; the caller rechecks the queue afterward.
        self.armed.store(true, Ordering::Release);
        true
    }

    #[inline]
    pub(crate) fn take(&self, observed: bool) -> bool {
        if observed || !self.armed.load(Ordering::Acquire) {
            // Until the peer acknowledges activation, every publication wakes.
            self.waiting.store(false, Ordering::Release);
            true
        } else {
            self.waiting.swap(false, Ordering::AcqRel)
        }
    }
}

pub(crate) struct Ring<T> {
    pub(crate) buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    pub(crate) mask: usize,
    /// Consumer read position. Written by consumer, read by producer.
    pub(crate) head: Padded<AtomicCursor>,
    /// Last flushed position. Written by producer, read by consumer.
    pub(crate) tail: Padded<AtomicCursor>,
    /// Empty/full observers register before rechecking the opposite cursor.
    /// The notifying side exchanges this flag after publishing its cursor.
    pub(crate) consumer_waiting: Padded<Waiter>,
    pub(crate) producer_waiting: Padded<Waiter>,
    /// Set by `Producer::drop`. Lets the consumer detect that no more
    /// data will ever arrive.
    pub(crate) producer_dropped: AtomicBool,
    /// Set by `Consumer::drop` / `AsyncConsumer::drop`. Lets the producer
    /// detect that the consumer is gone.
    pub(crate) consumer_dropped: AtomicBool,
}

// SAFETY: Ring<T> is Send because all shared mutable state is accessed through
// atomics (head, tail) and UnsafeCell slots follow the SPSC protocol: the
// producer writes cursor..tail, the consumer reads head..tail, non-overlapping.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: Ring<T> is Sync for the same reason: concurrent access is mediated
// by atomics and the SPSC single-producer/single-consumer invariant.
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        let cap = capacity
            .checked_next_power_of_two()
            .expect("capacity must fit in the next power of two");
        assert!(
            cap <= usize::MAX / 2,
            "capacity must be <= half the cursor range"
        );
        let buf: Vec<UnsafeCell<MaybeUninit<T>>> = (0..cap)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        Self {
            buf: buf.into_boxed_slice(),
            mask: cap - 1,
            head: Padded(AtomicCursor::new(0)),
            tail: Padded(AtomicCursor::new(0)),
            consumer_waiting: Padded(Waiter::new()),
            producer_waiting: Padded(Waiter::new()),
            producer_dropped: AtomicBool::new(false),
            consumer_dropped: AtomicBool::new(false),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline]
    fn distance(from: Cursor, to: Cursor) -> Cursor {
        from.wrapping_sub(to)
    }

    #[inline]
    fn count(n: Cursor) -> usize {
        n
    }

    #[inline]
    fn index(&self, cursor: Cursor) -> usize {
        cursor & self.mask
    }

    #[inline]
    pub(crate) fn push(
        &self,
        cursor: &mut Cursor,
        cached_head: &mut Cursor,
        val: T,
    ) -> Result<(), T> {
        if Self::distance(*cursor, *cached_head) >= self.capacity() {
            *cached_head = self.head.0.load(Ordering::Acquire);
            if Self::distance(*cursor, *cached_head) >= self.capacity() {
                return Err(val);
            }
        }
        // SAFETY: capacity check above guarantees this slot is not
        // visible to the consumer (cursor < head + capacity).
        self.buf[self.index(*cursor)].with_mut(|ptr| unsafe {
            (*ptr).write(val);
        });
        *cursor = cursor.wrapping_add(1);
        Ok(())
    }

    #[inline]
    pub(crate) fn flush_to(&self, cursor: Cursor, cached_head: &mut Cursor) -> FlushResult {
        self.consumer_waiting.0.enable();
        let prev_tail = self.tail.0.load(Ordering::Relaxed);
        if cursor == prev_tail {
            return FlushResult::NothingToFlush;
        }
        let count = Self::distance(cursor, prev_tail);
        *cached_head = self.head.0.load(Ordering::Acquire);
        let was_empty = prev_tail == *cached_head;
        self.tail.0.store(cursor, Ordering::Release);
        // Pair with the consumer's register-then-recheck exchange. Either
        // this sees its registration, or its recheck sees our publication.
        let wake = self.consumer_waiting.0.take(was_empty);
        FlushResult::Flushed {
            count: Self::count(count),
            was_empty: wake,
        }
    }

    #[inline]
    pub(crate) fn is_full(&self, cursor: Cursor, cached_head: &mut Cursor) -> bool {
        if Self::distance(cursor, *cached_head) >= self.capacity() {
            *cached_head = self.head.0.load(Ordering::Acquire);
            Self::distance(cursor, *cached_head) >= self.capacity()
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn producer_len(&self, cursor: Cursor) -> usize {
        Self::count(cursor.wrapping_sub(self.head.0.load(Ordering::Acquire)))
    }

    #[inline]
    pub(crate) fn producer_is_empty(&self, cursor: Cursor) -> bool {
        cursor == self.head.0.load(Ordering::Acquire)
    }

    // Fold the Option into the caller's return value instead of copying a
    // payload-sized return buffer through this internal helper.
    #[expect(
        clippy::inline_always,
        reason = "profiling shows payload copies through out-of-line pop returns"
    )]
    #[inline(always)]
    pub(crate) fn pop(&self, head: &mut Cursor, cached_tail: Cursor) -> Option<T> {
        if *head == cached_tail {
            return None;
        }
        // SAFETY: head < cached_tail, so this slot was written by the
        // producer and made visible via flush (Release store).
        let val = self.buf[self.index(*head)].with_mut(|ptr| unsafe { (*ptr).assume_init_read() });
        *head = head.wrapping_add(1);
        Some(val)
    }

    /// Move `count` published items starting at `head` to the end of `output`.
    ///
    /// # Safety
    ///
    /// `count` must not exceed the consumer's prefetched window, so every slot
    /// in `head..head + count` holds a value the producer published before the
    /// consumer's Acquire load of `tail`. The consumer must own those slots
    /// exclusively until it releases them.
    #[inline]
    pub(crate) unsafe fn pop_into(&self, head: &mut Cursor, count: usize, output: &mut Vec<T>) {
        debug_assert!(count <= self.capacity());
        output.reserve(count);
        let start = self.index(*head);
        let first = count.min(self.capacity() - start);
        let len = output.len();
        // SAFETY: `reserve` provides `count` spare slots after `len`. The
        // first segment ends at the buffer end and the wrapped segment holds
        // at most `start` items, so both stay inside the buffer and do not
        // overlap. The caller guarantees the slots are initialized and owned.
        unsafe {
            let dst = output.as_mut_ptr().add(len);
            self.move_slots(start, first, dst);
            self.move_slots(0, count - first, dst.add(first));
            output.set_len(len + count);
        }
        *head = head.wrapping_add(count);
    }

    /// Count the longest prefix of `limit` published items starting at `head`
    /// that `admit` accepts, reading each item in place.
    ///
    /// # Safety
    ///
    /// Same contract as [`pop_into`](Self::pop_into): every slot in
    /// `head..head + limit` holds a published value that the consumer owns
    /// until it releases them.
    #[inline]
    pub(crate) unsafe fn accepted_prefix(
        &self,
        head: Cursor,
        limit: usize,
        admit: &mut impl FnMut(&T) -> bool,
    ) -> usize {
        let mut accepted = 0;
        while accepted < limit {
            let slot = &self.buf[self.index(head.wrapping_add(accepted))];
            // SAFETY: the caller guarantees the slot is initialized and owned
            // by the consumer, so a shared borrow of the value is valid for
            // the duration of the call.
            let accept = slot.with_mut(|ptr| unsafe { admit((*ptr).assume_init_ref()) });
            if !accept {
                break;
            }
            accepted += 1;
        }
        accepted
    }

    /// Move `len` initialized slots starting at buffer index `start` to `dst`.
    ///
    /// # Safety
    ///
    /// `start + len` must not exceed the capacity, the slots must hold
    /// initialized values owned by the caller, and `dst` must be valid for
    /// `len` writes.
    #[cfg(not(all(loom, target_pointer_width = "64")))]
    #[inline]
    unsafe fn move_slots(&self, start: usize, len: usize, dst: *mut T) {
        // SAFETY: `UnsafeCell` and `MaybeUninit` are both `repr(transparent)`,
        // so a slot pointer reads as a `T` pointer. The caller guarantees the
        // source range is inside the buffer and initialized, and `dst` has
        // room for `len` values. Moving out of the slots is a bitwise copy,
        // exactly as `pop` does one value at a time.
        unsafe {
            let src = UnsafeCell::raw_get(self.buf.as_ptr().add(start)).cast::<T>();
            std::ptr::copy_nonoverlapping(src, dst, len);
        }
    }

    /// Move `len` initialized slots starting at buffer index `start` to `dst`.
    ///
    /// # Safety
    ///
    /// Same contract as the non-loom implementation. Loom tracks each cell
    /// access separately, so this moves one slot at a time.
    #[cfg(all(loom, target_pointer_width = "64"))]
    unsafe fn move_slots(&self, start: usize, len: usize, dst: *mut T) {
        for (offset, slot) in self.buf[start..start + len].iter().enumerate() {
            // SAFETY: the caller guarantees the slot is initialized and owned
            // by the consumer, and that `dst` has room for `len` values.
            slot.with_mut(|ptr| unsafe { dst.add(offset).write((*ptr).assume_init_read()) });
        }
    }

    #[inline]
    pub(crate) fn release(&self, head: Cursor) {
        self.head.0.store(head, Ordering::Release);
    }

    #[inline]
    pub(crate) fn prefetch(&self, cached_tail: &mut Cursor) -> usize {
        let new_tail = self.tail.0.load(Ordering::Acquire);
        let count = new_tail.wrapping_sub(*cached_tail);
        *cached_tail = new_tail;
        Self::count(count)
    }

    #[inline]
    pub(crate) fn consumer_is_empty(&self, head: Cursor, cached_tail: Cursor) -> bool {
        head == cached_tail && self.tail.0.load(Ordering::Acquire) == head
    }

    #[inline]
    pub(crate) fn consumer_len(&self, head: Cursor) -> usize {
        Self::count(self.tail.0.load(Ordering::Acquire).wrapping_sub(head))
    }

    /// Drop all items between head and tail. Must only be called with
    /// exclusive access (i.e. in a `Drop` impl or when no concurrent
    /// readers/writers exist).
    ///
    /// Idempotent: it advances `head` to `tail` before draining, so a
    /// second call (e.g. `Ring::drop` running after an async wrapper
    /// already drained) sees an empty range and cannot double-drop.
    /// If a `T::drop` panics mid-loop, items past the panicking one are
    /// leaked rather than double-dropped.
    pub(crate) fn drop_remaining(&mut self) {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Relaxed);
        // Advance head before the loop so a panicking T::drop cannot
        // cause a second call to re-drop the same items.
        self.head.0.store(tail, Ordering::Relaxed);
        let mut i = head;
        while i != tail {
            // SAFETY: &mut self guarantees exclusive access. Slots
            // [head..tail] were written by the producer and flushed.
            self.buf[self.index(i)].with_mut(|ptr| unsafe {
                (*ptr).assume_init_drop();
            });
            i = i.wrapping_add(1);
        }
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        self.drop_remaining();
    }
}
