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
use crate::ready::{LANES_PER_PAGE, LaneSignal, ReadyPage};
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
    let page = Arc::new(ReadyPage::new(0));
    let signal = Arc::new(LaneSignal::new(page.clone(), 0));
    let key = LaneKey {
        slot: 0,
        generation: 0,
    };
    let local = Worker::new_fifo();
    let stealer = local.stealer();
    let mut registry = Registry::new(page, stealer.clone());
    registry.install_lane(LaneToken::new(Lane::new(key, signal.clone(), consumer)));
    let shared = Arc::new(Shared {
        registry: Mutex::new(registry),
        stealers: ArcSwap::from_pointee(vec![(0, stealer)]),
        ready_pages: ConcurrentQueue::unbounded(),
        ready_lanes: ConcurrentQueue::unbounded(),
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
            capacity_per_sender: capacity_per_sender.next_power_of_two(),
        },
    )
}

struct Shared<T> {
    registry: Mutex<Registry<T>>,
    stealers: ArcSwap<Vec<(usize, Stealer<T>)>>,
    ready_pages: ConcurrentQueue<usize>,
    ready_lanes: ConcurrentQueue<LaneToken<T>>,
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

        let (key, page) = registry.allocate_lane();
        let signal = Arc::new(LaneSignal::new(page, key.slot));
        let (producer, consumer) = yring::spsc(self.capacity_per_sender);
        registry.install_lane(LaneToken::new(Lane::new(key, signal.clone(), consumer)));
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

    #[inline]
    fn mark_ready(&self, signal: &LaneSignal) -> bool {
        let mark = signal.mark();
        if let Some(page) = mark.page {
            self.ready_pages
                .push(page)
                .expect("ready-page queue is never closed");
        }
        mark.activated
    }

    fn activate_one_page(&self) -> bool {
        let Ok(page_id) = self.ready_pages.pop() else {
            return false;
        };
        lock(&self.registry).publish_ready_lanes(page_id, &self.ready_lanes);
        true
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
        while let Ok(lane) = self.ready_lanes.pop() {
            close_lane(lane);
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

struct Registry<T> {
    slots: Vec<Slot<T>>,
    free: Vec<LaneKey>,
    pages: Vec<Arc<ReadyPage>>,
    stealers: Vec<Option<Stealer<T>>>,
    free_receivers: Vec<usize>,
}

impl<T> Registry<T> {
    fn new(page: Arc<ReadyPage>, stealer: Stealer<T>) -> Self {
        Self {
            slots: vec![Slot {
                generation: 0,
                lane: None,
            }],
            free: Vec::new(),
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

    fn allocate_lane(&mut self) -> (LaneKey, Arc<ReadyPage>) {
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
        while self.pages.len() <= page_id {
            self.pages.push(Arc::new(ReadyPage::new(self.pages.len())));
        }
        (key, self.pages[page_id].clone())
    }

    fn install_lane(&mut self, lane: LaneToken<T>) {
        let slot = &mut self.slots[lane.key.slot];
        debug_assert_eq!(slot.generation, lane.key.generation);
        debug_assert!(slot.lane.is_none());
        slot.lane = Some(lane);
    }

    fn publish_ready_lanes(&mut self, page_id: usize, ready_lanes: &ConcurrentQueue<LaneToken<T>>) {
        let Some(page) = self.pages.get(page_id) else {
            return;
        };
        let mut bits = page.take();
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
                ready_lanes
                    .push(lane)
                    .unwrap_or_else(|_| unreachable!("ready-lane queue is never closed"));
            } else {
                slot.lane = Some(lane);
            }
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
