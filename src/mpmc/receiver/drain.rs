use crate::compat::{Arc, Ordering};
use crate::mpmc::{PREFETCH_LIMIT, ReadyTopology};
use crate::publication::Publication;
use crate::ready::{LANES_PER_PAGE, LaneSignal, PAGES_PER_GROUP};
use crate::teardown::Teardown;

use super::{FinishLane, LaneToken, Receiver};

pub(super) enum LaneDrain<T> {
    Item { value: T, effects: Effects },
    Empty(Effects),
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Wake {
    #[default]
    None,
    One,
    All,
}

impl Wake {
    pub(super) const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::One, _) | (_, Self::One) => Self::One,
            (Self::None, Self::None) => Self::None,
        }
    }
}

#[derive(Default)]
pub(crate) struct Effects {
    pub(super) released: Option<Arc<LaneSignal>>,
    pub(super) wake: Wake,
    /// Bounded maintenance retired a lane or saw work move. More may
    /// remain, and no sender or publication has to wake a parked receiver.
    pub(super) progressed: bool,
}

impl Effects {
    pub(super) fn progressed() -> Self {
        Self::progressed_if(true)
    }

    pub(super) fn progressed_if(progressed: bool) -> Self {
        Self {
            progressed,
            ..Self::default()
        }
    }

    /// Whether there are notifications to send.
    pub(super) fn has_effect(&self) -> bool {
        self.released.is_some() || self.wake != Wake::None
    }

    /// Whether a blocking receive must retry instead of parking.
    pub(super) fn must_retry(&self) -> bool {
        self.has_effect() || self.progressed
    }
}

impl<T, P: Teardown> Receiver<T, P> {
    pub(super) fn acquire_lane(&self) -> Option<(LaneToken<T, P>, Publication<'_>)> {
        self.refresh_ready_topology();
        let ready = self.ready.borrow();
        let prefer_work = self.prefer_work.replace(!self.prefer_work.get());
        if !prefer_work && let Some(lane) = self.acquire_ready_lane(&ready) {
            return Some(lane);
        }

        let page_count = ready.pages.len();
        let direct_page = self.direct_page_cursor.get() % page_count;
        if !ready.pages[direct_page].is_empty() {
            let publication = self.shared.publications.begin();
            if let Some(lane) = ready.pages[direct_page].pop_direct() {
                self.direct_page_cursor.set((direct_page + 1) % page_count);
                return Some((lane, publication));
            }
            drop(publication);
        }

        if let Some(lane) = self.acquire_work_lane(&ready) {
            return Some(lane);
        }
        if prefer_work {
            return self.acquire_ready_lane(&ready);
        }
        None
    }

    pub(super) fn acquire_work_lane<'a>(
        &'a self,
        ready: &ReadyTopology<T, P>,
    ) -> Option<(LaneToken<T, P>, Publication<'a>)> {
        let group_count = ready.work_groups.len();
        let start = self.ready_group_cursor.get() % group_count;
        let page_start = self.ready_page_cursor.get();

        for offset in 0..group_count {
            let group_index = (start + offset) % group_count;
            let group = &ready.work_groups[group_index];
            if !group.has_ready() {
                continue;
            }
            let publication = self.shared.publications.begin();
            let Some(page_bit) = group.take_one_from(page_start) else {
                drop(publication);
                continue;
            };
            self.ready_group_cursor.set((group_index + 1) % group_count);
            self.ready_page_cursor.set((page_bit + 1) % PAGES_PER_GROUP);
            let page_id = group.id() * PAGES_PER_GROUP + page_bit;
            self.direct_page_cursor
                .set((page_id + 1) % ready.pages.len());
            // The shared group can expose work on a page registered after
            // this snapshot. Resolve that page through the published topology.
            let lane = if let Some(page) = ready.pages.get(page_id) {
                page.pop_after_claim()
            } else {
                self.shared.ready.load().pages[page_id].pop_after_claim()
            };
            if let Some(lane) = lane {
                return Some((lane, publication));
            }
            drop(publication);
        }
        None
    }

    pub(super) fn acquire_ready_lane<'a>(
        &'a self,
        ready: &ReadyTopology<T, P>,
    ) -> Option<(LaneToken<T, P>, Publication<'a>)> {
        let group_count = ready.ready_groups.len();
        let start = self.ready_group_cursor.get() % group_count;
        let page_start = self.ready_page_cursor.get();
        for offset in 0..group_count {
            let group_index = (start + offset) % group_count;
            let group = &ready.ready_groups[group_index];
            if !group.has_ready() {
                continue;
            }
            let publication = self.shared.publications.begin();
            let Some(page_bit) = group.take_one_from(page_start) else {
                drop(publication);
                continue;
            };
            self.ready_group_cursor.set((group_index + 1) % group_count);
            self.ready_page_cursor.set((page_bit + 1) % PAGES_PER_GROUP);
            let page_id = group.id() * PAGES_PER_GROUP + page_bit;
            self.direct_page_cursor
                .set((page_id + 1) % ready.pages.len());
            if let Some(lane) = self.shared.activate_page(page_id) {
                return Some((lane, publication));
            }
            drop(publication);
        }
        None
    }

    pub(super) fn drain_lane(&self, mut lane: LaneToken<T, P>) -> LaneDrain<T> {
        if lane.cached_available == 0 {
            lane.cached_available = lane.consumer.prefetch();
            if lane.cached_available == 0 {
                let released = lane.release_pending().then(|| lane.signal.clone());
                let (wake, retired) = self.finish_lane(lane);
                return LaneDrain::Empty(Effects {
                    released,
                    wake,
                    progressed: retired,
                });
            }
        }

        let batch = lane.cached_available.min(PREFETCH_LIMIT);
        let value = self.take_batch(&mut lane, batch);
        lane.cached_available -= batch;
        lane.unreleased += batch;
        // Release a full batch, or everything once this receiver has taken the
        // whole prefetched window. Withholding a partial batch from an idle
        // lane would shrink the sender's usable capacity below its lane size.
        let released = ((lane.unreleased >= lane.release_batch || lane.cached_available == 0)
            && lane.release_pending())
        .then(|| lane.signal.clone());

        let staged = if batch > 1 { Wake::All } else { Wake::None };
        let (wake, _) = self.settle_lane(lane);
        LaneDrain::Item {
            value,
            effects: Effects {
                released,
                wake: staged.merge(wake),
                progressed: false,
            },
        }
    }

    /// Pops `batch` prefetched values from `lane`. Returns the first and stages
    /// the rest for this receiver, or for stealing once it has clones.
    fn take_batch(&self, lane: &mut LaneToken<T, P>, batch: usize) -> T {
        let value = lane
            .consumer
            .pop()
            .expect("cached_available guarantees prefetched data");
        let rest = (1..batch).map(|_| {
            lane.consumer
                .pop()
                .expect("batch is bounded by cached availability")
        });
        // Only this receiver can create a second clone while the count is one.
        if self.shared.live_receivers.load(Ordering::Acquire) == 1 {
            self.private.borrow_mut().extend(rest);
        } else {
            self.local.push_batch(rest);
        }
        value
    }

    /// Requeues `lane` while it holds prefetched values, otherwise finishes it.
    /// Returns the receiver wake that calls for and whether the lane retired.
    fn settle_lane(&self, lane: LaneToken<T, P>) -> (Wake, bool) {
        if lane.cached_available != 0 {
            self.requeue_lane(lane);
            return (Wake::One, false);
        }
        self.finish_lane(lane)
    }

    /// Finishes an empty `lane`: requeues it if a send raced in, or parks or
    /// retires it. Returns the receiver wake and whether the lane retired.
    fn finish_lane(&self, lane: LaneToken<T, P>) -> (Wake, bool) {
        match self.shared.finish_empty_lane(lane) {
            FinishLane::Ready(lane) => {
                self.requeue_lane(lane);
                (Wake::One, false)
            }
            FinishLane::Parked => (Wake::None, false),
            FinishLane::Retired { wake_all } => {
                (if wake_all { Wake::All } else { Wake::None }, true)
            }
        }
    }

    pub(super) fn requeue_lane(&self, lane: LaneToken<T, P>) {
        let page_id = lane.key.slot / LANES_PER_PAGE;
        let ready = self.ready.borrow();
        if let Some(page) = ready.pages.get(page_id) {
            page.push(lane);
        } else {
            // A shared readiness group can expose a newly registered page
            // after our snapshot refresh. Registry activation happens after
            // that page's topology is published, so the shared snapshot has it.
            self.shared.ready.load().pages[page_id].push(lane);
        }
    }
}
