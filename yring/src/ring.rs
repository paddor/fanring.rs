use std::ptr::NonNull;

use crate::compat::{AtomicBool, Ordering};
use crate::segment::{Chain, ReadCursor, Refill, Segment, Window, WriteCursor};
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
    /// Bound on queued items: the producer stops at `head + capacity`.
    capacity: usize,
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
    /// Every live segment, from the consumer's on.
    chain: Chain<T>,
}

// SAFETY: Ring<T> is Send because all shared mutable state is accessed through
// atomics (head, tail, segment links) and UnsafeCell slots follow the SPSC
// protocol: the producer writes cursor..tail, the consumer reads head..tail,
// non-overlapping. A segment is freed only by its last user.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: Ring<T> is Sync for the same reason: concurrent access is mediated
// by atomics and the SPSC single-producer/single-consumer invariant.
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    /// A ring bounded at `capacity` items whose first segment holds
    /// `initial` of them, with both cursors at `start`. Both sizes round up
    /// to a power of two, and `initial` never exceeds `capacity`.
    pub(crate) fn new(initial: usize, capacity: usize, start: Cursor) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        let capacity = capacity
            .checked_next_power_of_two()
            .expect("capacity must fit in the next power of two");
        assert!(
            capacity <= usize::MAX / 2,
            "capacity must be <= half the cursor range"
        );
        let initial = initial.clamp(1, capacity).next_power_of_two();
        Self {
            capacity,
            head: Padded(AtomicCursor::new(start)),
            tail: Padded(AtomicCursor::new(start)),
            consumer_waiting: Padded(Waiter::new()),
            producer_waiting: Padded(Waiter::new()),
            producer_dropped: AtomicBool::new(false),
            consumer_dropped: AtomicBool::new(false),
            chain: Chain::new(Segment::allocate(initial, start)),
        }
    }

    /// Cursors for a new producer and consumer at the ring's start.
    pub(crate) fn cursors(&self) -> (WriteCursor<T>, ReadCursor<T>) {
        let seg = self.chain.oldest();
        // SAFETY: no endpoint exists yet, so the first segment is live.
        let segment = unsafe { seg.as_ref() };
        let start = segment.start;
        let mut writer = WriteCursor::new(seg, start);
        writer.limit = self.limit(segment, start, start);
        (writer, ReadCursor::new(seg, start))
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn distance(from: Cursor, to: Cursor) -> Cursor {
        from.wrapping_sub(to)
    }

    /// First cursor the producer cannot write while the consumer is at
    /// `head`: the ring bound or the end of its segment, whichever comes
    /// first.
    #[inline]
    fn limit(&self, segment: &Segment<T>, cursor: Cursor, head: Cursor) -> Cursor {
        let queued = Self::distance(cursor, head);
        let in_segment = queued.min(Self::distance(cursor, segment.start));
        let room = (self.capacity - queued).min(segment.capacity() - in_segment);
        cursor.wrapping_add(room)
    }

    #[inline]
    pub(crate) fn push(&self, writer: &mut WriteCursor<T>, val: T) -> Result<(), T> {
        if writer.cursor == writer.limit && !self.refill(writer, true) {
            return Err(val);
        }
        // SAFETY: the cursor is below `limit`, so the slot is free and not
        // visible to the consumer.
        unsafe { writer.write(val) };
        Ok(())
    }

    /// Refresh the producer at its limit. Returns whether a write fits now
    /// or after growth.
    #[inline]
    fn refill(&self, writer: &mut WriteCursor<T>, grow: bool) -> bool {
        // A full ring and a newer `head` freeing room in this segment are
        // both checked inline, as a fixed ring would. Only a full segment
        // in a ring that can still grow takes the out-of-line path.
        let head = self.head.0.load(Ordering::Acquire);
        let cursor = writer.cursor;
        if Self::distance(cursor, head) >= self.capacity {
            return false;
        }
        writer.limit = self.limit(writer.segment(), cursor, head);
        if cursor != writer.limit {
            return true;
        }
        let refill = self.make_room(writer.seg(), cursor, head, grow);
        writer.apply(refill)
    }

    /// Handle a producer whose segment is full for a `head` it just loaded:
    /// link a larger segment if `grow` is set.
    ///
    /// Values in and out: a `&mut` cursor passed to an out-of-line call
    /// would force the hot path to keep it in memory.
    #[cold]
    #[inline(never)]
    fn make_room(
        &self,
        seg: NonNull<Segment<T>>,
        cursor: Cursor,
        head: Cursor,
        grow: bool,
    ) -> Refill<T> {
        // SAFETY: the producer's segment stays live while the producer runs.
        let segment = unsafe { seg.as_ref() };
        let mut refill = Refill {
            seg,
            limit: cursor,
            fits: true,
        };
        let full = Self::distance(cursor, head) >= self.capacity;
        if full || segment.capacity() == self.capacity {
            refill.fits = false;
        } else if grow {
            let size = (segment.capacity() * 2).min(self.capacity);
            refill.seg = Self::grow(seg, size, cursor);
            // SAFETY: just allocated and owned by the chain.
            refill.limit = self.limit(unsafe { refill.seg.as_ref() }, cursor, head);
        }
        refill
    }

    /// Link a segment of `size` slots after the full `seg`, starting at
    /// `cursor`. Nothing is copied; the consumer drains the old one first.
    ///
    /// Takes a pointer, not a reference: once the link is visible, the
    /// consumer may drain and free `seg` before this function returns.
    fn grow(seg: NonNull<Segment<T>>, size: usize, cursor: Cursor) -> NonNull<Segment<T>> {
        let next = Segment::allocate(size, cursor);
        // SAFETY: the producer's segment is live until the link below is
        // visible. Only the `next` field is borrowed, so no reference to the
        // segment outlives the store.
        let link = unsafe { &(*seg.as_ptr()).next };
        // Release publishes the initialized segment. Every tail store that
        // covers its items comes later, so a consumer that acquires such a
        // tail also sees this link.
        link.store(next.as_ptr(), Ordering::Release);
        next
    }

    #[inline]
    pub(crate) fn flush_to(&self, writer: &mut WriteCursor<T>) -> FlushResult {
        self.consumer_waiting.0.enable();
        let cursor = writer.cursor;
        let prev_tail = self.tail.0.load(Ordering::Relaxed);
        if cursor == prev_tail {
            return FlushResult::NothingToFlush;
        }
        let count = Self::distance(cursor, prev_tail);
        let head = self.head.0.load(Ordering::Acquire);
        let was_empty = prev_tail == head;
        // Reuse the load to refresh the free space, so later pushes rarely
        // stop at a stale limit.
        writer.limit = self.limit(writer.segment(), cursor, head);
        self.tail.0.store(cursor, Ordering::Release);
        // Pair with the consumer's register-then-recheck exchange. Either
        // this sees its registration, or its recheck sees our publication.
        let wake = self.consumer_waiting.0.take(was_empty);
        FlushResult::Flushed {
            count,
            was_empty: wake,
        }
    }

    /// Whether a push would fail now. A ring that can still grow is not
    /// full; the next push grows it.
    #[inline]
    pub(crate) fn is_full(&self, writer: &mut WriteCursor<T>) -> bool {
        writer.cursor == writer.limit && !self.refill(writer, false)
    }

    /// Register a producer waiting for space. Out of line and value-only, so
    /// the producer's cursor stays in registers.
    #[cold]
    #[inline(never)]
    pub(crate) fn register_producer(&self) -> bool {
        self.producer_waiting.0.register()
    }

    /// Register a consumer waiting for data. See [`register_producer`].
    ///
    /// [`register_producer`]: Self::register_producer
    #[cold]
    #[inline(never)]
    pub(crate) fn register_consumer(&self) -> bool {
        self.consumer_waiting.0.register()
    }

    #[inline]
    pub(crate) fn producer_len(&self, cursor: Cursor) -> usize {
        cursor.wrapping_sub(self.head.0.load(Ordering::Acquire))
    }

    #[inline]
    pub(crate) fn producer_is_empty(&self, cursor: Cursor) -> bool {
        cursor == self.head.0.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn release(&self, head: Cursor) {
        self.head.0.store(head, Ordering::Release);
    }

    /// Load the items flushed since the last prefetch and return how many
    /// are new. The window may span segments; reads stop at each segment end
    /// until [`advance`](Self::advance) moves on.
    #[inline]
    pub(crate) fn prefetch(&self, reader: &mut ReadCursor<T>) -> usize {
        let tail = self.tail.0.load(Ordering::Acquire);
        let count = tail.wrapping_sub(reader.cached_tail);
        reader.cached_tail = tail;
        reader.limit = tail;
        // Only a segment below the ring capacity can have a successor, so
        // fixed and fully grown rings skip the header load. Acquire pairs
        // with `grow`. A tail past a link's start was published after the
        // link, so the tail load above guarantees this load sees it.
        if reader.segment_capacity() < self.capacity
            && !reader.segment().next.load(Ordering::Acquire).is_null()
        {
            reader.apply(self.window(reader.seg(), reader.head, tail));
        }
        count
    }

    /// Pop one item, moving to the next segment at a segment end.
    #[expect(
        clippy::inline_always,
        reason = "profiling shows payload copies through out-of-line pop returns"
    )]
    #[inline(always)]
    pub(crate) fn pop(&self, reader: &mut ReadCursor<T>) -> Option<T> {
        if reader.head == reader.limit {
            if reader.head == reader.cached_tail {
                return None;
            }
            self.next_segment(reader);
        }
        // SAFETY: head is below limit: `next_segment` moves to the segment
        // that stores `head` and sets a limit past it.
        Some(unsafe { reader.take() })
    }

    /// Move a consumer at a segment end inside its window into the next
    /// segment. Out of line so callers that inline [`pop`](Self::pop) stay
    /// small, and value-free so the item takes one path out of the slot.
    #[cold]
    #[inline(never)]
    fn next_segment(&self, reader: &mut ReadCursor<T>) {
        reader.apply(self.window(reader.seg(), reader.head, reader.cached_tail));
    }

    /// At the end of the consumer's segment, move into the next one.
    /// Returns false when the prefetched window is exhausted.
    #[inline]
    pub(crate) fn advance(&self, reader: &mut ReadCursor<T>) -> bool {
        if reader.head == reader.cached_tail {
            return false;
        }
        reader.apply(self.window(reader.seg(), reader.head, reader.cached_tail));
        true
    }

    /// The consumer's window part in its segment, for a window ending at
    /// `tail`. A drained segment with a newer one linked is freed, and the
    /// window continues in the newer one. Values only, as in
    /// [`make_room`](Self::make_room).
    #[cold]
    #[inline(never)]
    fn window(&self, mut seg: NonNull<Segment<T>>, head: Cursor, tail: Cursor) -> Window<T> {
        loop {
            // SAFETY: the consumer's segment and the segments linked after
            // it stay allocated until the consumer retires them here.
            let segment = unsafe { seg.as_ref() };
            let growing = segment.capacity() < self.capacity;
            let next = if growing {
                // Acquire pairs with `grow` and makes the linked segment
                // readable; see `prefetch` for why it is visible.
                segment.next.load(Ordering::Acquire)
            } else {
                std::ptr::null_mut()
            };
            let Some(next) = NonNull::new(next) else {
                return Window { seg, limit: tail };
            };

            // SAFETY: as above.
            let start = unsafe { next.as_ref() }.start;
            if head != start {
                // Items before `start` remain here; the producer may not
                // have flushed all of them yet.
                let limit = if Self::distance(start, head) < Self::distance(tail, head) {
                    start
                } else {
                    tail
                };
                return Window { seg, limit };
            }

            self.chain.set_oldest(next);
            // SAFETY: the producer moved to `next` before publishing anything
            // past this segment and never touches it again. The consumer has
            // popped every item stored here.
            unsafe { Segment::free(seg) };
            seg = next;
        }
    }

    #[inline]
    pub(crate) fn consumer_is_empty(&self, head: Cursor, cached_tail: Cursor) -> bool {
        head == cached_tail && self.tail.0.load(Ordering::Acquire) == head
    }

    #[inline]
    pub(crate) fn consumer_len(&self, head: Cursor) -> usize {
        self.tail.0.load(Ordering::Acquire).wrapping_sub(head)
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
        let mut segment = self.chain.oldest();
        let mut cursor = head;
        while cursor != tail {
            // SAFETY: &mut self guarantees exclusive access to the chain, and
            // every cursor in head..tail is stored in it, in order.
            unsafe {
                segment = Segment::holding(segment, cursor);
                segment.as_ref().drop_slot(cursor);
            }
            cursor = cursor.wrapping_add(1);
        }
    }
}

#[cfg(test)]
impl<T> Ring<T> {
    /// Sizes of the live segments, oldest first.
    pub(crate) fn segment_sizes(&self) -> Vec<usize> {
        let mut sizes = Vec::new();
        let mut segment = self.chain.oldest().as_ptr();
        while let Some(current) = NonNull::new(segment) {
            // SAFETY: tests call this while both endpoints are idle.
            let current = unsafe { current.as_ref() };
            sizes.push(current.capacity());
            segment = current.next.load(Ordering::Acquire);
        }
        sizes
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // The chain frees the segments afterwards, even if an item drop
        // panics here.
        self.drop_remaining();
    }
}
