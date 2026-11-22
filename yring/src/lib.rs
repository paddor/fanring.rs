#![deny(unsafe_op_in_unsafe_fn)]
//! Bounded SPSC ring with ypipe-style batched flush/prefetch.
//!
//! Three pointers:
//! - `head`: consumer read position (`AtomicUsize`, consumer-owned)
//! - `cursor`: writer position (plain usize, producer-only, no atomic)
//! - `tail`: last flushed position (`AtomicUsize`, producer writes, consumer reads)
//!
//! `push` writes without atomics while cached space remains. `flush`
//! publishes writes with one Release store. `pop` reads without atomics;
//! `prefetch` acquires a batch, and `release` publishes consumed slots.
//! When wakeup hints are enabled, empty/full checks register waiters so
//! the optional wakeup hints cannot miss a concurrent state transition.
//!
//! [`spsc`] allocates every slot up front. [`spsc_growing`] starts small and
//! links a segment twice the size whenever the producer fills the current
//! one, up to the capacity. Items are never copied: the consumer drains each
//! old segment and frees it.

#[cfg(feature = "async")]
mod r#async;
#[cfg(feature = "async")]
pub use r#async::{AsyncConsumer, AsyncProducer, async_spsc};

#[cfg(not(target_has_atomic = "ptr"))]
compile_error!("yring requires target_has_atomic = \"ptr\"");

mod compat;
mod consumer;
mod producer;
mod ring;
mod segment;

use compat::{Arc, AtomicUsize};

pub use consumer::Consumer;
pub use producer::{Producer, ProducerOwner};
pub(crate) use ring::Ring;

type AtomicCursor = AtomicUsize;
type Cursor = usize;

#[repr(align(128))]
pub(crate) struct Padded<T>(pub(crate) T);

/// Result of a flush operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushResult {
    /// Items were flushed. The consumer may have been idle and needs waking.
    Flushed {
        /// Number of items made visible.
        count: usize,
        /// Whether the consumer needs a wake: the queue was observed empty
        /// or a waiter may exist. Conservative during protocol activation too.
        was_empty: bool,
    },
    /// Nothing to flush (cursor == tail already).
    NothingToFlush,
}

/// Create a bounded SPSC ring with the given capacity (rounded up to
/// next power of two).
///
/// # Panics
///
/// Panics if `capacity` is zero or above half the cursor range.
pub fn spsc<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    endpoints(Ring::new(capacity, capacity, 0))
}

/// Create a bounded SPSC ring that starts with `initial` slots and grows to
/// `capacity`. Both round up to a power of two, and `initial` is clamped to
/// `capacity`.
///
/// A push that finds the current segment full allocates one twice the size
/// and links it; nothing is copied. Memory follows the deepest backlog the
/// ring has held and does not shrink. The capacity bound, wakeup hints, and
/// every other method behave as in [`spsc`].
///
/// # Panics
///
/// Panics if `capacity` is zero or above half the cursor range.
pub fn spsc_growing<T>(initial: usize, capacity: usize) -> (Producer<T>, Consumer<T>) {
    endpoints(Ring::new(initial, capacity, 0))
}

fn endpoints<T>(ring: Ring<T>) -> (Producer<T>, Consumer<T>) {
    let (writer, reader) = ring.cursors();
    let ring = Arc::new(ring);
    (
        Producer {
            ring: ring.clone(),
            writer,
        },
        Consumer { ring, reader },
    )
}

#[cfg(all(loom, target_pointer_width = "64"))]
#[doc(hidden)]
pub fn loom_spsc_growing_with_cursors<T>(
    initial: usize,
    capacity: usize,
    cursor: usize,
) -> (Producer<T>, Consumer<T>) {
    endpoints(Ring::new(initial, capacity, cursor))
}

#[cfg(all(loom, target_pointer_width = "64"))]
#[doc(hidden)]
pub fn loom_spsc_with_cursors<T>(capacity: usize, cursor: usize) -> (Producer<T>, Consumer<T>) {
    endpoints(Ring::new(capacity, capacity, cursor))
}

#[cfg(test)]
mod tests;
