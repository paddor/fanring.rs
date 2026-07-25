use std::fmt;

use crate::compat::{AtomicBool, AtomicU64, Ordering};

/// Bitmask of shards that may have data.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadyMask(u64);

impl ReadyMask {
    #[inline]
    pub(crate) const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub(crate) fn is_empty(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub(crate) fn contains(self, shard: usize) -> bool {
        self.0 & shard_bit(shard) != 0
    }

    #[inline]
    pub(crate) fn remove(&mut self, shard: usize) {
        self.0 &= !shard_bit(shard);
    }
}

impl fmt::Debug for ReadyMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadyMask")
            .field(&format_args!("{:#066b}", self.0))
            .finish()
    }
}

/// Shared ready-state protocol.
///
/// Senders set a padded per-shard flag and publish the shard bit only on an
/// empty-to-ready transition. The receiver drains the global bitmask, then uses
/// per-shard flags to close the race with concurrent sends.
pub(crate) struct ReadySet {
    global: AtomicU64,
    shards: Box<[ReadyFlag]>,
}

impl ReadySet {
    pub(crate) fn new(max_senders: usize) -> Self {
        Self {
            global: AtomicU64::new(0),
            shards: (0..max_senders).map(|_| ReadyFlag::new(false)).collect(),
        }
    }

    #[inline]
    pub(crate) fn mark_ready(&self, shard: usize) {
        let flag = &self.shards[shard];
        // Publish the flush even when the shard was already ready. The
        // receiver's matching swap synchronizes before its second prefetch.
        if !flag.swap(true, Ordering::AcqRel) {
            self.global.fetch_or(shard_bit(shard), Ordering::Release);
        }
    }

    #[inline]
    pub(crate) fn take_ready(&self) -> ReadyMask {
        ReadyMask(self.global.swap(0, Ordering::AcqRel))
    }

    #[inline]
    pub(crate) fn clear_shard(&self, shard: usize) {
        self.shards[shard].swap(false, Ordering::AcqRel);
    }

    #[inline]
    pub(crate) fn restore_shard(&self, shard: usize) {
        self.shards[shard].store(true, Ordering::Release);
    }
}

impl fmt::Debug for ReadySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadySet")
            .field("global", &ReadyMask(self.global.load(Ordering::Relaxed)))
            .finish_non_exhaustive()
    }
}

#[inline]
pub(crate) fn shard_bit(shard: usize) -> u64 {
    1u64 << shard
}

#[repr(align(128))]
struct ReadyFlag(AtomicBool);

impl ReadyFlag {
    #[inline]
    fn new(value: bool) -> Self {
        Self(AtomicBool::new(value))
    }

    #[inline]
    fn store(&self, value: bool, ordering: Ordering) {
        self.0.store(value, ordering);
    }

    #[inline]
    fn swap(&self, value: bool, ordering: Ordering) -> bool {
        self.0.swap(value, ordering)
    }
}
