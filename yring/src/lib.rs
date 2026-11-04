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

#[cfg(all(loom, target_pointer_width = "64"))]
use compat::Ordering;
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
pub fn spsc<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::new(capacity));
    (
        Producer {
            ring: ring.clone(),
            cursor: 0,
            cached_head: 0,
        },
        Consumer {
            ring,
            head: 0,
            cached_tail: 0,
        },
    )
}

#[cfg(all(loom, target_pointer_width = "64"))]
#[doc(hidden)]
pub fn loom_spsc_with_cursors<T>(capacity: usize, cursor: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::new(capacity));
    ring.head.0.store(cursor, Ordering::Relaxed);
    ring.tail.0.store(cursor, Ordering::Relaxed);
    (
        Producer {
            ring: ring.clone(),
            cursor,
            cached_head: cursor,
        },
        Consumer {
            ring,
            head: cursor,
            cached_tail: cursor,
        },
    )
}

#[cfg(test)]
mod tests;
