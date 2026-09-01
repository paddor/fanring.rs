//! Dynamic MPMC channel built from one bounded SPSC ring per sender.
//!
//! Senders never contend on a shared queue tail. A receiver drains up to 64
//! values from a ready ring, returns one, and publishes the rest to its local
//! stealable FIFO. Other receivers can steal that work in batches. Ordering is
//! relaxed.

use std::fmt;

use arc_swap::ArcSwap;
use concurrent_queue::ConcurrentQueue;
use crossbeam_deque::{Injector, Steal, Stealer, Worker};

use crate::compat::{Arc, AtomicBool, AtomicUsize, Mutex, Ordering, lock};
use crate::config::validate_capacity;
use crate::publication::PublicationTracker;
use crate::ready::{LANES_PER_PAGE, LaneSignal, PAGES_PER_GROUP, ReadyGroup, ReadyPage};
use crate::wait::MultiWaitCell;

mod receiver;
mod sender;

use receiver::{FinishLane, Lane, LaneToken, close_lane};
pub use receiver::{IntoIter, Iter, Receiver, TryIter};
pub use sender::Sender;

pub use crate::error::{
    ChannelError, RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError,
    TryRegisterError, TrySendError,
};

/// Maximum per-sender capacity accepted by [`channel`] and [`try_channel`].
pub const MAX_CAPACITY_PER_SENDER: usize = 1usize << (usize::BITS - 2);

const PREFETCH_LIMIT: usize = 64;
const TRY_RECV_RETRIES: usize = 1;
#[cfg(not(loom))]
const PARK_SPINS: usize = 128;
#[cfg(loom)]
const PARK_SPINS: usize = 0;

/// Create an MPMC channel with one bounded ring per sender.
///
/// `capacity_per_sender` must be between 1 and [`MAX_CAPACITY_PER_SENDER`] and is
/// rounded up by `yring` to the next power of two.
///
/// # Panics
///
/// Panics when `capacity_per_sender` is zero or exceeds
/// [`MAX_CAPACITY_PER_SENDER`].
#[must_use]
pub fn channel<T>(capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    try_channel(capacity_per_sender).unwrap_or_else(|error| panic!("{error}"))
}

/// Try to create an MPMC channel with one bounded ring per sender.
///
/// # Errors
///
/// Returns [`ChannelError`] when `capacity_per_sender` is zero or exceeds
/// [`MAX_CAPACITY_PER_SENDER`].
pub fn try_channel<T>(
    capacity_per_sender: usize,
) -> Result<(Sender<T>, Receiver<T>), ChannelError> {
    validate_capacity(capacity_per_sender, MAX_CAPACITY_PER_SENDER)?;
    Ok(build_channel(capacity_per_sender))
}

fn build_channel<T>(capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    let (producer, consumer) = yring::spsc(capacity_per_sender);
    let group = Arc::new(ReadyGroup::new(0));
    let work_group = Arc::new(ReadyGroup::new(0));
    let page = Arc::new(ReadyPage::new(0, group.clone()));
    let lane_page = Arc::new(LanePage::new(page.clone(), work_group.clone()));
    let signal = Arc::new(LaneSignal::new(page.clone(), 0));
    let key = LaneKey {
        slot: 0,
        generation: 0,
    };
    let local = Worker::new_fifo();
    let stealer = local.stealer();
    let mut registry = Registry::new(group, work_group, lane_page, stealer.clone());
    registry.install_lane(LaneToken::new(Lane::new(key, signal.clone(), consumer)));
    let ready = std::sync::Arc::new(registry.clone_ready_topology());
    let shared = Arc::new(Shared {
        registry: Mutex::new(registry),
        stealers: ArcSwap::from_pointee(vec![(0, stealer)]),
        ready: ArcSwap::new(ready.clone()),
        ready_generation: AtomicUsize::new(0),
        injector: Injector::new(),
        publications: PublicationTracker::new(),
        registered_lanes: AtomicUsize::new(1),
        live_senders: AtomicUsize::new(1),
        live_receivers: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        data_waiters: MultiWaitCell::new(),
        capacity_per_sender,
    });

    (
        Sender {
            shared: shared.clone(),
            producer,
            key,
            signal,
        },
        Receiver {
            shared,
            id: 0,
            local,
            steal_cursor: 0,
            ready: std::cell::RefCell::new(ready),
            seen_ready_generation: std::cell::Cell::new(0),
            ready_group_cursor: std::cell::Cell::new(0),
            ready_page_cursor: std::cell::Cell::new(0),
            direct_page_cursor: std::cell::Cell::new(0),
            prefer_work: std::cell::Cell::new(false),
            capacity_per_sender: capacity_per_sender.next_power_of_two(),
        },
    )
}

struct Shared<T> {
    registry: Mutex<Registry<T>>,
    stealers: ArcSwap<Vec<(usize, Stealer<T>)>>,
    ready: ArcSwap<ReadyTopology<T>>,
    ready_generation: AtomicUsize,
    injector: Injector<T>,
    publications: PublicationTracker,
    registered_lanes: AtomicUsize,
    live_senders: AtomicUsize,
    live_receivers: AtomicUsize,
    receiver_alive: AtomicBool,
    data_waiters: MultiWaitCell,
    capacity_per_sender: usize,
}

impl<T> Shared<T> {
    #[allow(clippy::significant_drop_tightening)]
    fn register_sender(
        &self,
    ) -> Result<(LaneKey, Arc<LaneSignal>, yring::Producer<T>), TryRegisterError> {
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterError::Disconnected);
        }

        let mut registry = lock(&self.registry);
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterError::Disconnected);
        }

        let (key, page, topology_changed) = registry.allocate_lane();
        let signal = Arc::new(LaneSignal::new(page, key.slot));
        let (producer, consumer) = yring::spsc(self.capacity_per_sender);
        registry.install_lane(LaneToken::new(Lane::new(key, signal.clone(), consumer)));
        if topology_changed {
            self.ready
                .store(std::sync::Arc::new(registry.clone_ready_topology()));
            self.ready_generation.fetch_add(1, Ordering::Release);
        }
        // Receiver teardown consumes the registry and counters together.
        self.registered_lanes.fetch_add(1, Ordering::Release);
        self.live_senders.fetch_add(1, Ordering::AcqRel);
        Ok((key, signal, producer))
    }

    fn register_receiver(&self) -> (usize, Worker<T>) {
        let local = Worker::new_fifo();
        let stealer = local.stealer();
        let mut registry = lock(&self.registry);
        let id = registry.register_receiver(stealer);
        self.stealers
            .store(std::sync::Arc::new(registry.clone_stealers()));
        drop(registry);
        self.live_receivers.fetch_add(1, Ordering::AcqRel);
        self.publications.notify_change();
        (id, local)
    }

    fn unregister_receiver(&self, id: usize) {
        let mut registry = lock(&self.registry);
        registry.unregister_receiver(id);
        self.stealers
            .store(std::sync::Arc::new(registry.clone_stealers()));
        drop(registry);
        self.publications.notify_change();
    }

    fn ready_snapshot(&self) -> (std::sync::Arc<ReadyTopology<T>>, usize) {
        loop {
            let generation = self.ready_generation.load(Ordering::Acquire);
            let ready = self.ready.load_full();
            if generation == self.ready_generation.load(Ordering::Acquire) {
                return (ready, generation);
            }
        }
    }

    #[inline]
    fn mark_ready(&self, signal: &LaneSignal) -> bool {
        signal.mark()
    }

    fn activate_page(&self, page_id: usize) -> Option<LaneToken<T>> {
        lock(&self.registry).take_ready_lane(page_id)
    }

    fn finish_empty_lane(&self, mut lane: LaneToken<T>) -> FinishLane<T> {
        let mut registry = lock(&self.registry);
        lane.signal.finish_drain();
        lane.cached_available = lane.consumer.prefetch();

        if lane.cached_available != 0 {
            let _ = lane.signal.claim_after_empty();
            return FinishLane::Ready(lane);
        }

        if lane.consumer.is_disconnected() {
            registry.retire_lane(lane.key);
            let last = self.registered_lanes.fetch_sub(1, Ordering::AcqRel) == 1;
            drop(registry);
            drop(lane);
            FinishLane::Retired {
                wake_all: last && self.live_senders.load(Ordering::Acquire) == 0,
            }
        } else {
            registry.install_lane(lane);
            FinishLane::Parked
        }
    }

    fn close_all_receivers(&self) {
        self.receiver_alive.store(false, Ordering::Release);

        let idle = {
            let mut registry = lock(&self.registry);
            registry.take_all_lanes()
        };
        for lane in idle {
            close_lane(lane);
        }
        let ready = self.ready.load();
        for page in &ready.pages {
            while let Some(lane) = page.pop_direct() {
                close_lane(lane);
            }
        }
        loop {
            match self.injector.steal() {
                Steal::Success(value) => drop(value),
                Steal::Retry => {}
                Steal::Empty => break,
            }
        }
        self.data_waiters.notify_all();
    }
}

impl<T> fmt::Debug for Shared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field(
                "registered_lanes",
                &self.registered_lanes.load(Ordering::Relaxed),
            )
            .field("live_senders", &self.live_senders.load(Ordering::Relaxed))
            .field(
                "live_receivers",
                &self.live_receivers.load(Ordering::Relaxed),
            )
            .field(
                "receiver_alive",
                &self.receiver_alive.load(Ordering::Relaxed),
            )
            .field("capacity_per_sender", &self.capacity_per_sender)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneKey {
    slot: usize,
    generation: usize,
}

struct Slot<T> {
    generation: usize,
    lane: Option<LaneToken<T>>,
}

struct LanePage<T> {
    ready: Arc<ReadyPage>,
    work_group: Arc<ReadyGroup>,
    work_bit: u64,
    lanes: ConcurrentQueue<LaneToken<T>>,
}

impl<T> LanePage<T> {
    fn new(ready: Arc<ReadyPage>, work_group: Arc<ReadyGroup>) -> Self {
        let work_bit = 1u64 << (ready.id() % PAGES_PER_GROUP);
        Self {
            ready,
            work_group,
            work_bit,
            lanes: ConcurrentQueue::bounded(LANES_PER_PAGE),
        }
    }

    #[inline]
    fn push(&self, lane: LaneToken<T>) {
        self.lanes
            .push(lane)
            .unwrap_or_else(|_| unreachable!("one ready token exists per lane"));
        self.work_group.mark(self.work_bit);
    }

    /// Pop after claiming the page's summary bit.
    ///
    /// Re-publish before the pop so another receiver can claim a different
    /// queued lane without waiting for this receiver to finish its pop.
    #[inline]
    fn pop_after_claim(&self) -> Option<LaneToken<T>> {
        if !self.lanes.is_empty() {
            self.work_group.mark(self.work_bit);
        }
        self.lanes.pop().ok()
    }

    #[inline]
    fn pop_direct(&self) -> Option<LaneToken<T>> {
        self.lanes.pop().ok()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }
}

struct ReadyTopology<T> {
    ready_groups: Vec<Arc<ReadyGroup>>,
    work_groups: Vec<Arc<ReadyGroup>>,
    pages: Vec<Arc<LanePage<T>>>,
}

struct Registry<T> {
    slots: Vec<Slot<T>>,
    free: Vec<LaneKey>,
    groups: Vec<Arc<ReadyGroup>>,
    work_groups: Vec<Arc<ReadyGroup>>,
    pages: Vec<Arc<LanePage<T>>>,
    stealers: Vec<Option<Stealer<T>>>,
    free_receivers: Vec<usize>,
}

impl<T> Registry<T> {
    fn new(
        group: Arc<ReadyGroup>,
        work_group: Arc<ReadyGroup>,
        page: Arc<LanePage<T>>,
        stealer: Stealer<T>,
    ) -> Self {
        Self {
            slots: vec![Slot {
                generation: 0,
                lane: None,
            }],
            free: Vec::new(),
            groups: vec![group],
            work_groups: vec![work_group],
            pages: vec![page],
            stealers: vec![Some(stealer)],
            free_receivers: Vec::new(),
        }
    }

    fn register_receiver(&mut self, stealer: Stealer<T>) -> usize {
        if let Some(id) = self.free_receivers.pop() {
            debug_assert!(self.stealers[id].is_none());
            self.stealers[id] = Some(stealer);
            id
        } else {
            let id = self.stealers.len();
            self.stealers.push(Some(stealer));
            id
        }
    }

    fn unregister_receiver(&mut self, id: usize) {
        if self.stealers[id].take().is_some() {
            self.free_receivers.push(id);
        }
    }

    fn clone_stealers(&self) -> Vec<(usize, Stealer<T>)> {
        self.stealers
            .iter()
            .enumerate()
            .filter_map(|(id, stealer)| stealer.clone().map(|stealer| (id, stealer)))
            .collect()
    }

    fn allocate_lane(&mut self) -> (LaneKey, Arc<ReadyPage>, bool) {
        let key = self.free.pop().unwrap_or_else(|| {
            let key = LaneKey {
                slot: self.slots.len(),
                generation: 0,
            };
            self.slots.push(Slot {
                generation: 0,
                lane: None,
            });
            key
        });
        let page_id = key.slot / LANES_PER_PAGE;
        let old_page_count = self.pages.len();
        while self.pages.len() <= page_id {
            let id = self.pages.len();
            let group_id = id / PAGES_PER_GROUP;
            while self.groups.len() <= group_id {
                self.groups
                    .push(Arc::new(ReadyGroup::new(self.groups.len())));
                self.work_groups
                    .push(Arc::new(ReadyGroup::new(self.work_groups.len())));
            }
            let ready = Arc::new(ReadyPage::new(id, self.groups[group_id].clone()));
            self.pages.push(Arc::new(LanePage::new(
                ready,
                self.work_groups[group_id].clone(),
            )));
        }
        (
            key,
            self.pages[page_id].ready.clone(),
            self.pages.len() != old_page_count,
        )
    }

    fn install_lane(&mut self, lane: LaneToken<T>) {
        let slot = &mut self.slots[lane.key.slot];
        debug_assert_eq!(slot.generation, lane.key.generation);
        debug_assert!(slot.lane.is_none());
        slot.lane = Some(lane);
    }

    fn take_ready_lane(&mut self, page_id: usize) -> Option<LaneToken<T>> {
        let page = self.pages.get(page_id)?;
        let mut selected = None;
        let mut bits = page.ready.take();
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let slot_index = page_id * LANES_PER_PAGE + bit;
            let Some(slot) = self.slots.get_mut(slot_index) else {
                continue;
            };
            let Some(lane) = slot.lane.take() else {
                continue;
            };
            if lane.signal.is_pending() {
                if selected.is_none() {
                    selected = Some(lane);
                } else {
                    page.push(lane);
                }
            } else {
                slot.lane = Some(lane);
            }
        }
        selected
    }

    fn clone_ready_topology(&self) -> ReadyTopology<T> {
        ReadyTopology {
            ready_groups: self.groups.clone(),
            work_groups: self.work_groups.clone(),
            pages: self.pages.clone(),
        }
    }

    fn retire_lane(&mut self, key: LaneKey) {
        let slot = &mut self.slots[key.slot];
        debug_assert_eq!(slot.generation, key.generation);
        debug_assert!(slot.lane.is_none());
        slot.generation = key.generation.wrapping_add(1);
        self.free.push(LaneKey {
            slot: key.slot,
            generation: slot.generation,
        });
    }

    fn take_all_lanes(&mut self) -> Vec<LaneToken<T>> {
        self.slots
            .iter_mut()
            .filter_map(|slot| slot.lane.take())
            .collect()
    }
}
