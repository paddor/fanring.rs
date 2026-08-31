use std::fmt;

use crate::compat::{Arc, AtomicU8, AtomicU64, Ordering};
use crate::wait::{WaitCell, WaitRegistration};

const IDLE: u8 = 0;
const PENDING: u8 = 1;

pub(crate) const LANES_PER_PAGE: usize = u64::BITS as usize;

/// One page in the dynamic ready index.
pub(crate) struct ReadyPage {
    id: usize,
    bits: AtomicU64,
}

impl ReadyPage {
    pub(crate) fn new(id: usize) -> Self {
        Self {
            id,
            bits: AtomicU64::new(0),
        }
    }

    #[inline]
    pub(crate) const fn id(&self) -> usize {
        self.id
    }

    /// Mark a lane ready. Returns the page ID when the page needs publishing.
    #[inline]
    fn mark(&self, bit: u64) -> Option<usize> {
        (self.bits.fetch_or(bit, Ordering::Release) == 0).then_some(self.id)
    }

    #[inline]
    pub(crate) fn take(&self) -> u64 {
        self.bits.swap(0, Ordering::AcqRel)
    }
}

impl fmt::Debug for ReadyPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadyPage")
            .field("id", &self.id)
            .field("bits", &self.bits.load(Ordering::Relaxed))
            .finish()
    }
}

/// Per-lane coalescing protocol.
///
/// Every publication swaps the lane to `PENDING`. Only the first publication
/// after `IDLE` queues the lane. The receiver swaps back to `IDLE` before its
/// final ring recheck, preventing a publication from being lost at the empty
/// boundary.
pub(crate) struct LaneSignal {
    readiness: LaneReadiness,
    space_waiter: WaitCell,
}

#[repr(C, align(128))]
struct LaneReadiness {
    state: AtomicU8,
    page: Arc<ReadyPage>,
    bit: u64,
}

pub(crate) struct ReadyMark {
    pub(crate) page: Option<usize>,
    pub(crate) activated: bool,
}

impl LaneSignal {
    pub(crate) fn new(page: Arc<ReadyPage>, lane: usize) -> Self {
        Self {
            readiness: LaneReadiness {
                state: AtomicU8::new(IDLE),
                page,
                bit: 1u64 << (lane % LANES_PER_PAGE),
            },
            space_waiter: WaitCell::new(),
        }
    }

    #[inline]
    pub(crate) fn mark(&self) -> ReadyMark {
        if self.readiness.state.swap(PENDING, Ordering::AcqRel) == IDLE {
            ReadyMark {
                page: self.readiness.page.mark(self.readiness.bit),
                activated: true,
            }
        } else {
            ReadyMark {
                page: None,
                activated: false,
            }
        }
    }

    #[inline]
    pub(crate) fn finish_drain(&self) {
        self.readiness.state.swap(IDLE, Ordering::AcqRel);
    }

    #[inline]
    pub(crate) fn claim_after_empty(&self) -> bool {
        self.readiness
            .state
            .compare_exchange(IDLE, PENDING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    #[inline]
    pub(crate) fn is_pending(&self) -> bool {
        self.readiness.state.load(Ordering::Acquire) != IDLE
    }

    pub(crate) fn prepare_space_wait(&self) -> WaitRegistration<'_> {
        self.space_waiter.prepare()
    }

    #[inline]
    pub(crate) fn notify_space(&self) {
        self.space_waiter.notify();
    }
}

impl fmt::Debug for LaneSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LaneSignal")
            .field("state", &self.readiness.state.load(Ordering::Relaxed))
            .field("page", &self.readiness.page.id())
            .field("bit", &self.readiness.bit)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, target_arch = "x86_64", not(loom)))]
mod tests {
    use super::LaneSignal;

    #[test]
    fn lane_signal_stays_within_256_bytes() {
        assert!(size_of::<LaneSignal>() <= 256);
    }
}
