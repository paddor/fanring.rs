//! Slot storage and the endpoint cursors that index it.
//!
//! A ring stores items in segments. A fixed ring has one. A growing ring
//! starts small and links a segment twice the size whenever the producer
//! fills the current one, up to the ring capacity. The producer only writes
//! its newest segment, and the consumer frees each older segment once it has
//! read it, so no item is ever copied between segments.

use std::mem::MaybeUninit;
use std::ptr::NonNull;

use crate::Cursor;
#[cfg(not(all(loom, target_pointer_width = "64")))]
use crate::compat::UnsafeCellExt;
use crate::compat::{AtomicPtr, Ordering, UnsafeCell};

pub(crate) struct Segment<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    /// First cursor stored here. Earlier cursors live in older segments.
    pub(crate) start: Cursor,
    /// The next, larger segment. The producer sets it once, before it
    /// publishes any item stored there.
    pub(crate) next: AtomicPtr<Segment<T>>,
}

impl<T> Segment<T> {
    /// Allocate a segment of `size` slots (a power of two) whose first
    /// cursor is `start`.
    pub(crate) fn allocate(size: usize, start: Cursor) -> NonNull<Self> {
        debug_assert!(size.is_power_of_two());
        let buf: Vec<UnsafeCell<MaybeUninit<T>>> = (0..size)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        let segment = Box::new(Self {
            buf: buf.into_boxed_slice(),
            mask: size - 1,
            start,
            next: AtomicPtr::new(std::ptr::null_mut()),
        });
        NonNull::from(Box::leak(segment))
    }

    /// Free a segment without dropping any slot.
    ///
    /// # Safety
    ///
    /// `segment` came from [`allocate`](Self::allocate), no endpoint uses it
    /// again, and every item stored in it has been moved out or dropped.
    pub(crate) unsafe fn free(segment: NonNull<Self>) {
        // SAFETY: the caller guarantees sole ownership of a leaked box.
        drop(unsafe { Box::from_raw(segment.as_ptr()) });
    }

    pub(crate) fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// End of a window ending at `tail` inside this segment: `tail`, or the
    /// start of the next segment when that comes first.
    pub(crate) fn end_within(&self, tail: Cursor) -> Cursor {
        // Acquire pairs with `grow`, as in `Ring::window`.
        match NonNull::new(self.next.load(Ordering::Acquire)) {
            // SAFETY: linked segments stay live while older ones are.
            Some(next) => {
                let start = unsafe { next.as_ref() }.start;
                if start.wrapping_sub(self.start) < tail.wrapping_sub(self.start) {
                    start
                } else {
                    tail
                }
            }
            None => tail,
        }
    }

    #[inline]
    fn slot(&self, cursor: Cursor) -> &UnsafeCell<MaybeUninit<T>> {
        &self.buf[cursor & self.mask]
    }

    /// The live segment after `segment` that stores `cursor`.
    ///
    /// # Safety
    ///
    /// The caller has exclusive access to the chain, and `cursor` is at or
    /// after the start of `segment` and stored in the chain.
    pub(crate) unsafe fn holding(mut segment: NonNull<Self>, cursor: Cursor) -> NonNull<Self> {
        loop {
            // SAFETY: the chain is exclusively owned and fully linked.
            let next = unsafe { segment.as_ref() }.next.load(Ordering::Relaxed);
            match NonNull::new(next) {
                // SAFETY: as above.
                Some(next) if unsafe { next.as_ref() }.start == cursor => segment = next,
                _ => return segment,
            }
        }
    }

    /// Drop the item at `cursor`.
    ///
    /// # Safety
    ///
    /// The slot holds an initialized item that the caller owns exclusively.
    pub(crate) unsafe fn drop_slot(&self, cursor: Cursor) {
        // SAFETY: guaranteed by the caller.
        self.slot(cursor)
            .with_mut(|ptr| unsafe { (*ptr).assume_init_drop() });
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
}

/// Owns every live segment, starting from the consumer's. Newer segments
/// hang off its `next` links. Dropping the chain frees them all, also when a
/// panicking item drop unwinds the ring's own `Drop`.
pub(crate) struct Chain<T> {
    /// Written by the consumer when it moves on; read by the ring's
    /// constructor and `Drop`.
    oldest: AtomicPtr<Segment<T>>,
}

impl<T> Chain<T> {
    pub(crate) fn new(first: NonNull<Segment<T>>) -> Self {
        Self {
            oldest: AtomicPtr::new(first.as_ptr()),
        }
    }

    /// The consumer's segment. Only valid before the endpoints exist or
    /// after both are gone.
    pub(crate) fn oldest(&self) -> NonNull<Segment<T>> {
        // Relaxed: no endpoint runs concurrently with the callers.
        NonNull::new(self.oldest.load(Ordering::Relaxed)).expect("chain has a segment")
    }

    /// Record that the consumer moved on to `segment`.
    pub(crate) fn set_oldest(&self, segment: NonNull<Segment<T>>) {
        // Relaxed: readers run after both endpoints are gone, and the
        // ring's reference count orders this store before them.
        self.oldest.store(segment.as_ptr(), Ordering::Relaxed);
    }
}

impl<T> Drop for Chain<T> {
    fn drop(&mut self) {
        let mut segment = self.oldest.load(Ordering::Relaxed);
        while let Some(current) = NonNull::new(segment) {
            // SAFETY: both endpoints are gone, so the chain is exclusively
            // owned. The ring dropped or leaked every item before this runs.
            unsafe {
                segment = current.as_ref().next.load(Ordering::Relaxed);
                Segment::free(current);
            }
        }
    }
}

/// Producer state after a refresh at its limit.
pub(crate) struct Refill<T> {
    pub(crate) seg: NonNull<Segment<T>>,
    pub(crate) limit: Cursor,
    /// A write fits now, or after growth when the refresh did not grow.
    pub(crate) fits: bool,
}

impl<T> Clone for Refill<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Refill<T> {}

/// Producer position. Only the producer touches it.
pub(crate) struct WriteCursor<T> {
    /// The newest segment, the only one the producer writes.
    seg: NonNull<Segment<T>>,
    /// Private write position.
    pub(crate) cursor: Cursor,
    /// First cursor that needs a refresh before a write: the ring is full
    /// or the segment is, as of the last `head` load.
    pub(crate) limit: Cursor,
}

impl<T> WriteCursor<T> {
    pub(crate) fn new(seg: NonNull<Segment<T>>, cursor: Cursor) -> Self {
        Self {
            seg,
            cursor,
            limit: cursor,
        }
    }

    #[inline]
    pub(crate) fn segment(&self) -> &Segment<T> {
        // SAFETY: the consumer frees only segments the producer has left,
        // and the ring frees the rest after both endpoints are gone.
        unsafe { self.seg.as_ref() }
    }

    pub(crate) fn seg(&self) -> NonNull<Segment<T>> {
        self.seg
    }

    /// Take a refreshed state. Returns whether a write fits.
    #[inline]
    pub(crate) fn apply(&mut self, refill: Refill<T>) -> bool {
        self.seg = refill.seg;
        self.limit = refill.limit;
        refill.fits
    }

    /// Write `val` at the cursor and advance it.
    ///
    /// # Safety
    ///
    /// `cursor` is below `limit`, so the slot is free and the consumer
    /// cannot see it.
    #[inline]
    pub(crate) unsafe fn write(&mut self, val: T) {
        // SAFETY: guaranteed by the caller.
        self.segment()
            .slot(self.cursor)
            .with_mut(|ptr| unsafe { (*ptr).write(val) });
        self.cursor = self.cursor.wrapping_add(1);
    }
}

/// Consumer segment state after a window check.
pub(crate) struct Window<T> {
    pub(crate) seg: NonNull<Segment<T>>,
    pub(crate) limit: Cursor,
    pub(crate) growing: bool,
}

impl<T> Clone for Window<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Window<T> {}

/// Consumer position. Only the consumer touches it.
pub(crate) struct ReadCursor<T> {
    /// The oldest live segment, the one the consumer reads.
    seg: NonNull<Segment<T>>,
    /// Private read position.
    pub(crate) head: Cursor,
    /// End of the prefetched window. It may reach into newer segments.
    pub(crate) cached_tail: Cursor,
    /// End of the window inside `seg`: `cached_tail`, or the start of the
    /// next segment when that comes first. Reads stop here.
    pub(crate) limit: Cursor,
    /// `seg` is smaller than the ring capacity, so a larger segment may
    /// follow it. Fixed rings and grown rings skip the link check.
    pub(crate) growing: bool,
}

impl<T> ReadCursor<T> {
    pub(crate) fn new(seg: NonNull<Segment<T>>, head: Cursor, growing: bool) -> Self {
        Self {
            seg,
            head,
            cached_tail: head,
            limit: head,
            growing,
        }
    }

    #[inline]
    pub(crate) fn segment(&self) -> &Segment<T> {
        // SAFETY: only the consumer frees its segment, after moving off it,
        // and the ring frees the rest after both endpoints are gone.
        unsafe { self.seg.as_ref() }
    }

    pub(crate) fn seg(&self) -> NonNull<Segment<T>> {
        self.seg
    }

    /// Take a checked window.
    #[inline]
    pub(crate) fn apply(&mut self, window: Window<T>) {
        self.seg = window.seg;
        self.limit = window.limit;
        self.growing = window.growing;
    }

    /// Prefetched items left in this segment.
    #[inline]
    pub(crate) fn in_segment(&self) -> usize {
        self.limit.wrapping_sub(self.head)
    }

    /// Read the item at `head` and advance.
    ///
    /// # Safety
    ///
    /// `head` is below `limit`, so the producer published this slot and the
    /// consumer owns it.
    #[expect(
        clippy::inline_always,
        reason = "profiling shows payload copies through out-of-line pop returns"
    )]
    #[inline(always)]
    pub(crate) unsafe fn take(&mut self) -> T {
        // SAFETY: guaranteed by the caller; the Acquire load of `tail` in
        // `prefetch` ordered the producer's write before this read.
        let val = self
            .segment()
            .slot(self.head)
            .with_mut(|ptr| unsafe { (*ptr).assume_init_read() });
        self.head = self.head.wrapping_add(1);
        val
    }

    /// Move `count` published items starting at `head` to the end of `output`.
    ///
    /// # Safety
    ///
    /// `count` must not exceed [`in_segment`](Self::in_segment), so every slot
    /// in `head..head + count` lies in this segment and holds a value the
    /// producer published before the consumer's Acquire load of `tail`. The
    /// consumer must own those slots exclusively until it releases them.
    #[inline]
    pub(crate) unsafe fn pop_into(&mut self, count: usize, output: &mut Vec<T>) {
        let segment = self.segment();
        let capacity = segment.capacity();
        debug_assert!(count <= capacity);
        output.reserve(count);
        let start = self.head & segment.mask;
        let first = count.min(capacity - start);
        let len = output.len();
        // SAFETY: `reserve` provides `count` spare slots after `len`. The
        // first segment ends at the buffer end and the wrapped segment holds
        // at most `start` items, so both stay inside the buffer and do not
        // overlap. The caller guarantees the slots are initialized and owned.
        unsafe {
            let dst = output.as_mut_ptr().add(len);
            segment.move_slots(start, first, dst);
            segment.move_slots(0, count - first, dst.add(first));
            output.set_len(len + count);
        }
        self.head = self.head.wrapping_add(count);
    }

    /// Count the longest prefix of `limit` published items starting at `head`
    /// that `admit` accepts, reading each item in place. The prefix may reach
    /// into newer segments; nothing is moved or freed.
    ///
    /// # Safety
    ///
    /// `limit` must not exceed the prefetched window (`cached_tail - head`),
    /// so every slot read holds a published value that the consumer owns
    /// until it releases it.
    #[inline]
    pub(crate) unsafe fn accepted_prefix(
        &self,
        limit: usize,
        admit: &mut impl FnMut(&T) -> bool,
    ) -> usize {
        let mut segment = self.segment();
        let mut end = self.limit;
        let mut cursor = self.head;
        let mut accepted = 0;
        while accepted < limit {
            if cursor == end {
                // Segment end inside the window. The window's Acquire load of
                // `tail` made the link and the next segment visible.
                let next = segment.next.load(Ordering::Acquire);
                // SAFETY: a cursor below `cached_tail` at a segment end
                // means a newer segment is linked and still live.
                let Some(next) = (unsafe { next.as_ref() }) else {
                    break;
                };
                segment = next;
                end = segment.end_within(self.cached_tail);
            }
            let slot = segment.slot(cursor);
            // SAFETY: the caller guarantees the slot is initialized and owned
            // by the consumer, so a shared borrow of the value is valid for
            // the duration of the call.
            let accept = slot.with_mut(|ptr| unsafe { admit((*ptr).assume_init_ref()) });
            if !accept {
                break;
            }
            accepted += 1;
            cursor = cursor.wrapping_add(1);
        }
        accepted
    }
}

#[cfg(test)]
mod tests;
