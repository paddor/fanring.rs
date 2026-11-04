use crate::compat::Arc;
use crate::mpsc::{LaneKey, MIN_RELEASE_BATCH, PREFETCH_LIMIT};
use crate::ready::LaneSignal;
use crate::teardown::Teardown;

use super::Admit;

pub(in crate::mpsc) struct Lane<T, P: Teardown> {
    pub(super) key: LaneKey,
    pub(super) signal: Arc<LaneSignal>,
    pub(super) consumer: crate::ring::Consumer<T, P>,
    pub(super) cached_available: usize,
    pub(super) unreleased: usize,
    pub(super) release_batch: usize,
    pub(super) burst: usize,
    /// Whether `key` is in the receiver's `active` rotation.
    pub(super) queued: bool,
}

impl<T, P: Teardown> Lane<T, P> {
    pub(in crate::mpsc) fn new(
        key: LaneKey,
        signal: Arc<LaneSignal>,
        consumer: crate::ring::Consumer<T, P>,
    ) -> Self {
        // Scale credits to half a ring without shrinking the old batch size
        // for small rings. Large full lanes resume at their low watermark
        // while both sides still have work to do. This is independent of the
        // fairness burst: rotation does not force a capacity notification.
        // Empty-lane handling and explicit flushes release partial batches.
        let capacity = consumer.capacity();
        let release_batch = capacity.div_ceil(2).max(MIN_RELEASE_BATCH).min(capacity);
        Self {
            key,
            signal,
            consumer,
            cached_available: 0,
            unreleased: 0,
            release_batch,
            burst: 0,
            queued: false,
        }
    }

    #[inline]
    pub(super) fn release_pending(&mut self) -> bool {
        if self.unreleased == 0 {
            return false;
        }
        self.consumer.release();
        self.unreleased = 0;
        true
    }

    /// Prefetch once the cached window is used up. Returns how the lane
    /// settled when its ring is empty.
    #[inline(always)]
    pub(super) fn refill(&mut self) -> Option<EmptyLane> {
        if self.cached_available == 0 {
            self.cached_available = self.consumer.prefetch();
            if self.cached_available == 0 {
                return Some(self.settle_empty());
            }
        }
        None
    }

    /// Most values one bulk move may take before slot release or rotation.
    #[inline]
    pub(super) fn chunk_limit(&self) -> usize {
        self.cached_available
            .min(PREFETCH_LIMIT - self.burst)
            .min(self.release_batch - self.unreleased)
    }

    /// Move up to `take` prefetched values to `output`, stopping at the first
    /// value `admit` rejects. Releases slots once a release batch is complete.
    #[inline]
    pub(super) fn move_chunk<A: Admit<T>>(
        &mut self,
        output: &mut Vec<T>,
        take: usize,
        admit: &mut A,
    ) -> usize {
        let moved = if A::ALWAYS {
            let moved = self.consumer.pop_into(output, take);
            debug_assert_eq!(moved, take, "prefetched window shorter than cached");
            moved
        } else {
            self.consumer
                .pop_into_while(output, take, |value| admit.admit(value))
        };
        self.cached_available -= moved;
        self.unreleased += moved;
        self.burst += moved;
        if self.unreleased == self.release_batch && self.release_pending() {
            self.signal.notify_space();
        }
        moved
    }

    /// Handle a prefetch window that came back empty.
    ///
    /// Releases partial credits first so neither side parks on unpublished
    /// slots, marks the lane idle, then rechecks the ring. A lane whose
    /// recheck finds data stays queued: either this call claims it back, or
    /// a racing publication claimed it and its ready-page entry is ignored
    /// because the lane is still queued. So a lane leaves the rotation only
    /// after it was observed empty. The caller removes it from `active` on
    /// `Idle`.
    #[inline(always)]
    pub(super) fn settle_empty(&mut self) -> EmptyLane {
        let released = self.release_pending();
        self.signal.finish_drain();
        self.cached_available = self.consumer.prefetch();
        self.burst = 0;
        if self.cached_available != 0 {
            // Fails only when a racing publication already set the lane
            // pending, which keeps the queued-implies-pending invariant.
            self.signal.claim_after_empty();
            EmptyLane::Keep { released }
        } else {
            self.queued = false;
            EmptyLane::Idle {
                disconnected: self.consumer.is_disconnected(),
                released,
            }
        }
    }
}

pub(super) enum EmptyLane {
    Keep { released: bool },
    Idle { disconnected: bool, released: bool },
}

impl EmptyLane {
    /// Whether settling released consumed slots to the sender.
    #[inline]
    pub(super) const fn released(&self) -> bool {
        match self {
            Self::Keep { released } | Self::Idle { released, .. } => *released,
        }
    }
}

pub(super) enum LanePoll<T> {
    Item {
        value: T,
        rotate: bool,
        released: bool,
    },
    Keep {
        released: bool,
    },
    Idle {
        disconnected: bool,
        released: bool,
    },
    Stale,
}
