use crate::compat::Arc;
use crate::mpsc::{LaneKey, MIN_RELEASE_BATCH, PREFETCH_LIMIT};
use crate::ready::LaneSignal;
use crate::teardown::Teardown;

use super::Admit;

pub(in crate::mpsc) struct Lane<T, P: Teardown> {
    pub(super) key: LaneKey,
    pub(super) signal: Arc<LaneSignal>,
    pub(super) consumer: crate::ring::Consumer<T, P>,
    // The counters below change only through `Lane` methods, so every
    // reader sees them with the fast window closed.
    cached_available: usize,
    unreleased: usize,
    release_batch: usize,
    burst: usize,
    /// Values of an open fast window that single receives may still pop.
    /// The counters above already include them; `close_fast` refunds the
    /// rest.
    fast_left: usize,
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
            fast_left: 0,
            queued: false,
        }
    }

    /// Opens a fast window on this lane.
    ///
    /// The window holds up to `poll_budget` prefetched values and stops one
    /// short of the release batch and of the burst limit, so the value that
    /// completes either goes through [`poll`](Self::poll). Its values are
    /// charged to the lane counters up front, which lets
    /// [`pop_fast`](Self::pop_fast) skip per-value lane bookkeeping. The
    /// caller still charges the poll budget per value.
    pub(super) fn grant_fast(&mut self, poll_budget: usize) {
        self.close_fast();
        if self.cached_available == 0 {
            self.cached_available = self.consumer.prefetch();
        }
        let granted = self
            .cached_available
            .min(self.release_batch.saturating_sub(self.unreleased + 1))
            .min(PREFETCH_LIMIT.saturating_sub(self.burst + 1))
            .min(poll_budget);
        self.cached_available -= granted;
        self.unreleased += granted;
        self.burst += granted;
        self.fast_left = granted;
    }

    /// Pops the next value of the open fast window, if any.
    #[inline(always)]
    pub(super) fn pop_fast(&mut self) -> Option<T> {
        if self.fast_left == 0 {
            return None;
        }
        self.fast_left -= 1;
        Some(
            self.consumer
                .pop()
                .expect("a fast window holds prefetched values"),
        )
    }

    /// Refunds the unpopped part of the fast window to the counters.
    #[inline]
    pub(super) fn close_fast(&mut self) {
        if self.fast_left != 0 {
            self.cached_available += self.fast_left;
            self.unreleased -= self.fast_left;
            self.burst -= self.fast_left;
            self.fast_left = 0;
        }
    }

    /// Pops one value with full bookkeeping: slot release at the release
    /// batch and rotation at the burst limit, or every value when `FAIR`.
    #[inline(always)]
    pub(super) fn poll<const FAIR: bool>(&mut self) -> LanePoll<T> {
        if let Some(empty) = self.refill() {
            return match empty {
                EmptyLane::Keep { released } => LanePoll::Keep { released },
                EmptyLane::Idle {
                    disconnected,
                    released,
                } => LanePoll::Idle {
                    disconnected,
                    released,
                },
            };
        }

        let value = self
            .consumer
            .pop()
            .expect("cached_available guarantees prefetched data");
        self.cached_available -= 1;
        self.unreleased += 1;
        self.burst += 1;
        let released = self.unreleased == self.release_batch && self.release_pending();

        let rotate = FAIR || self.burst == PREFETCH_LIMIT;
        if rotate {
            self.burst = 0;
        }
        LanePoll::Item {
            value,
            rotate,
            released,
        }
    }

    /// Pops a prefetched value for a fair receive from the only active
    /// lane, through its fast window. Opens one if none is open.
    ///
    /// A fair receive ends the burst, so afterwards the burst counts only
    /// the window's unpopped values, which [`close_fast`](Self::close_fast)
    /// refunds. The value that completes the release batch goes through
    /// [`poll`](Self::poll).
    // Inline into the fair fast path: an out-of-line call returns a large
    // `T` through memory, which costs one more payload copy per value.
    #[inline(always)]
    pub(super) fn pop_fair(&mut self, poll_budget: usize) -> Option<T> {
        if self.fast_left == 0 {
            // No window is open, so there is nothing to refund. Ending the
            // burst first lets the window run to the burst limit.
            self.burst = 0;
            self.grant_fast(poll_budget);
        }
        let value = self.pop_fast()?;
        self.burst = self.fast_left;
        Some(value)
    }

    /// Pops a value after [`refill`](Self::refill) found one, leaving the
    /// burst alone. Returns it and whether its slot completed a release
    /// batch, which the caller signals.
    pub(super) fn pop_targeted(&mut self) -> (T, bool) {
        debug_assert_eq!(self.fast_left, 0, "refill closes the fast window");
        let value = self.consumer.pop().expect("prefetched value");
        self.cached_available -= 1;
        self.unreleased += 1;
        let released = self.unreleased == self.release_batch && self.release_pending();
        (value, released)
    }

    /// Ends a burst that reached the limit. Returns whether it did.
    #[inline]
    pub(super) fn end_full_burst(&mut self) -> bool {
        debug_assert_eq!(self.fast_left, 0, "refill closes the fast window");
        if self.burst != PREFETCH_LIMIT {
            return false;
        }
        self.burst = 0;
        true
    }

    /// Reloads an exhausted window. Returns whether the ring is still empty.
    pub(super) fn reload_is_empty(&mut self) -> bool {
        self.close_fast();
        if self.cached_available == 0 {
            self.cached_available = self.consumer.prefetch();
        }
        self.cached_available == 0
    }

    #[inline]
    pub(super) fn release_pending(&mut self) -> bool {
        self.close_fast();
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
        self.close_fast();
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
        debug_assert_eq!(self.fast_left, 0, "refill closes the fast window");
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
