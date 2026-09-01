use std::fmt;

use crate::compat::{Arc, AtomicU8, AtomicU64, Ordering};
use crate::wait::{WaitCell, WaitRegistration};

const IDLE: u8 = 0;
const PENDING: u8 = 1;

pub(crate) const LANES_PER_PAGE: usize = u64::BITS as usize;
pub(crate) const PAGES_PER_GROUP: usize = u64::BITS as usize;

/// Top-level summary for 64 ready pages.
pub(crate) struct ReadyGroup {
    id: usize,
    bits: AtomicU64,
}

impl ReadyGroup {
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

    #[inline]
    pub(crate) fn mark(&self, bit: u64) {
        self.bits.fetch_or(bit, Ordering::Release);
    }

    #[inline]
    pub(crate) fn has_ready(&self) -> bool {
        self.bits.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub(crate) fn take_all(&self) -> u64 {
        self.bits.swap(0, Ordering::AcqRel)
    }

    #[inline]
    pub(crate) fn take_one_from(&self, start: usize) -> Option<usize> {
        debug_assert!(start < PAGES_PER_GROUP);
        let mut bits = self.bits.load(Ordering::Acquire);
        loop {
            if bits == 0 {
                return None;
            }
            let offset = bits.rotate_right(start as u32).trailing_zeros() as usize;
            let bit = (start + offset) % PAGES_PER_GROUP;
            let next = bits & !(1u64 << bit);
            match self
                .bits
                .compare_exchange_weak(bits, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(bit),
                Err(observed) => bits = observed,
            }
        }
    }
}

impl fmt::Debug for ReadyGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadyGroup")
            .field("id", &self.id)
            .field("bits", &self.bits.load(Ordering::Relaxed))
            .finish()
    }
}

/// One page in the dynamic ready index.
pub(crate) struct ReadyPage {
    id: usize,
    bits: AtomicU64,
    group: Arc<ReadyGroup>,
    group_bit: u64,
}

impl ReadyPage {
    pub(crate) fn new(id: usize, group: Arc<ReadyGroup>) -> Self {
        debug_assert_eq!(group.id(), id / PAGES_PER_GROUP);
        Self {
            id,
            bits: AtomicU64::new(0),
            group,
            group_bit: 1u64 << (id % PAGES_PER_GROUP),
        }
    }

    #[inline]
    pub(crate) const fn id(&self) -> usize {
        self.id
    }

    /// Mark a lane ready and publish its page when transitioning from empty.
    #[inline]
    fn mark(&self, bit: u64) {
        if self.bits.fetch_or(bit, Ordering::Release) == 0 {
            self.group.mark(self.group_bit);
        }
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
            .field("group", &self.group.id())
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
    pub(crate) fn mark(&self) -> bool {
        if self.readiness.state.swap(PENDING, Ordering::AcqRel) == IDLE {
            self.readiness.page.mark(self.readiness.bit);
            true
        } else {
            false
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
    use super::{LaneSignal, ReadyGroup, ReadyPage};
    use crate::compat::Arc;

    #[test]
    fn group_claim_rotates_from_cursor() {
        let group = ReadyGroup::new(0);
        group.mark((1 << 2) | (1 << 63));

        assert_eq!(group.take_one_from(3), Some(63));
        assert_eq!(group.take_one_from(3), Some(2));
        assert_eq!(group.take_one_from(0), None);
    }

    #[test]
    fn page_republishes_only_after_becoming_empty() {
        let group = Arc::new(ReadyGroup::new(0));
        let page = ReadyPage::new(0, group.clone());

        page.mark(1);
        assert_eq!(group.take_all(), 1);
        page.mark(2);
        assert_eq!(group.take_all(), 0);
        assert_eq!(page.take(), 3);

        page.mark(4);
        assert_eq!(group.take_all(), 1);
        assert_eq!(page.take(), 4);
    }

    #[test]
    fn lane_signal_stays_within_256_bytes() {
        assert!(size_of::<LaneSignal>() <= 256);
    }
}

#[cfg(all(test, loom, target_pointer_width = "64"))]
mod loom_tests {
    use super::{ReadyGroup, ReadyPage};
    use crate::compat::Arc;

    #[test]
    fn page_mark_racing_group_claim_remains_visible() {
        loom::model(|| {
            let group = Arc::new(ReadyGroup::new(0));
            let page = Arc::new(ReadyPage::new(0, group.clone()));
            let sender_page = page.clone();
            let sender = loom::thread::spawn(move || sender_page.mark(1));

            let mut lanes = 0;
            if group.take_all() != 0 {
                lanes |= page.take();
            }
            sender.join().unwrap();
            if group.take_all() != 0 {
                lanes |= page.take();
            }

            assert_eq!(lanes & 1, 1);
        });
    }
}
