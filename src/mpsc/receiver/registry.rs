use std::mem;

use crate::compat::{Ordering, lock};
use crate::mpsc::LaneKey;
use crate::ready::{LANES_PER_PAGE, PAGES_PER_GROUP};
use crate::teardown::Teardown;

use super::{Lane, Receiver};

impl<T, P: Teardown> Receiver<T, P> {
    pub(super) fn collect_ready(&mut self, all: bool) {
        self.refresh_registry();
        let group_count = self.groups.len();
        // The cursor may equal `group_count`. Wrap it by comparison: this
        // runs on every readiness poll, and a modulo costs a division.
        let mut next_group = self.ready_group_cursor;
        for _ in 0..group_count {
            let group_index = if next_group < group_count {
                next_group
            } else {
                0
            };
            next_group = group_index + 1;
            // A load skips the swap for an empty group. Like the swap, it
            // sees every mark whose wake check missed a registered waiter.
            if !self.groups[group_index].has_ready() {
                continue;
            }
            let mut page_bits = self.groups[group_index].take_all();
            if page_bits == 0 {
                continue;
            }
            while page_bits != 0 {
                let page_bit = page_bits.trailing_zeros() as usize;
                page_bits &= page_bits - 1;
                let page_id = group_index * PAGES_PER_GROUP + page_bit;
                // A new page can publish after its group bit was claimed.
                self.refresh_registry();
                let Some(page) = self.pages.get(page_id) else {
                    continue;
                };
                let mut lane_bits = page.take();
                // A new lane can publish after its page bit was claimed.
                self.refresh_registry();
                while lane_bits != 0 {
                    let lane_bit = lane_bits.trailing_zeros() as usize;
                    lane_bits &= lane_bits - 1;
                    let slot = page_id * LANES_PER_PAGE + lane_bit;
                    let Some(lane) = self.lanes.get_mut(slot).and_then(Option::as_mut) else {
                        continue;
                    };
                    // A scan may already have queued a lane whose publication
                    // indexed it afterward. Queue each lane once.
                    if !self.paused[slot] && lane.signal.is_pending() && !lane.queued {
                        lane.queued = true;
                        self.active.push_back(lane.key);
                    }
                }
            }
            self.ready_group_cursor = next_group;
            if !all {
                break;
            }
        }
    }

    #[cold]
    pub(super) fn retire_empty_paused_lanes(&mut self) {
        // Paused lanes may have no ready-page entry left when their sender
        // drops. Empty paused lanes must not hide final channel disconnect;
        // unread paused values still require explicit drain.
        for slot in 0..self.lanes.len() {
            let retired = self.lanes[slot]
                .as_mut()
                .and_then(|lane| (self.paused[slot] && lane.reload_is_empty()).then_some(lane.key));
            if let Some(key) = retired {
                self.retire_lane(key);
            }
        }
    }

    pub(super) fn refresh_registry(&mut self) {
        let generation = self.shared.registry_generation.load(Ordering::Acquire);
        if generation == self.seen_registry_generation {
            return;
        }

        let (pending, groups, pages, seen) = {
            let mut registry = lock(&self.shared.registry);
            (
                mem::take(&mut registry.pending),
                registry.groups.clone(),
                registry.pages.clone(),
                self.shared.registry_generation.load(Ordering::Relaxed),
            )
        };
        self.groups = groups;
        self.pages = pages;
        for pending in pending {
            if self.lanes.len() <= pending.key.slot {
                self.lanes.resize_with(pending.key.slot + 1, || None);
                self.paused.resize(pending.key.slot + 1, false);
            }
            debug_assert!(self.lanes[pending.key.slot].is_none());
            self.paused[pending.key.slot] = false;
            self.lanes[pending.key.slot] =
                Some(Lane::new(pending.key, pending.signal, pending.consumer));
        }
        if self.active.capacity() < self.lanes.len() {
            self.active
                .reserve(self.lanes.len().saturating_sub(self.active.len()));
        }
        self.seen_registry_generation = seen;
    }

    pub(super) fn retire_lane(&mut self, key: LaneKey) {
        let Some(lane) = self.lanes.get_mut(key.slot).and_then(Option::take) else {
            return;
        };
        debug_assert_eq!(lane.key, key);
        self.paused[key.slot] = false;
        self.shared.registered_lanes.fetch_sub(1, Ordering::AcqRel);
        let mut registry = lock(&self.shared.registry);
        registry.retire_lane(key);
        drop(registry);
        drop(lane);
    }

    #[inline]
    pub(super) fn is_drained(&mut self) -> bool {
        if !self.is_disconnected() {
            return false;
        }
        if self.shared.registered_lanes.load(Ordering::Acquire) != 0 {
            self.retire_empty_paused_lanes();
        }
        self.shared.registered_lanes.load(Ordering::Acquire) == 0
    }
}
