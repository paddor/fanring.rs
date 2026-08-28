//! Dynamic MPMC channel built from one bounded SPSC ring per sender.
//!
//! Senders never contend on a shared queue tail. A receiver drains up to 64
//! values from a ready ring, returns one, and publishes the rest to its local
//! stealable FIFO. Other receivers can steal that work in batches. Ordering is
//! relaxed.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use concurrent_queue::ConcurrentQueue;
use crossbeam_deque::{Injector, Steal, Stealer, Worker};

use crate::compat::{Arc, AtomicBool, AtomicUsize, Mutex, Ordering};
use crate::ready::{LANES_PER_PAGE, LaneSignal, ReadyPage};
use crate::wait::MultiWaitCell;

pub use crate::error::{
    ChannelError, RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError,
    TryRegisterError, TrySendError,
};

/// Maximum per-sender capacity accepted by [`channel`] and [`try_channel`].
pub const MAX_CAPACITY_PER_SENDER: usize = 1usize << (usize::BITS - 2);

const PREFETCH_LIMIT: usize = 64;
#[cfg(not(loom))]
const PARK_SPINS: usize = 128;
#[cfg(loom)]
const PARK_SPINS: usize = 0;

/// Create a bounded sharded MPMC channel.
///
/// `capacity_per_sender` must be in `1..=`[`MAX_CAPACITY_PER_SENDER`] and is
/// rounded up by `yring` to the next power of two.
pub fn channel<T>(capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    try_channel(capacity_per_sender).unwrap_or_else(|error| panic!("{error}"))
}

/// Try to create a bounded sharded MPMC channel.
pub fn try_channel<T>(
    capacity_per_sender: usize,
) -> Result<(Sender<T>, Receiver<T>), ChannelError> {
    validate_channel_config(capacity_per_sender)?;
    Ok(build_channel(capacity_per_sender))
}

fn validate_channel_config(capacity_per_sender: usize) -> Result<(), ChannelError> {
    if capacity_per_sender == 0 {
        return Err(ChannelError::ZeroCapacity);
    }
    if capacity_per_sender > MAX_CAPACITY_PER_SENDER {
        return Err(ChannelError::CapacityTooLarge {
            requested: capacity_per_sender,
            max: MAX_CAPACITY_PER_SENDER,
        });
    }
    Ok(())
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
        ready_pages: ConcurrentQueue::unbounded(),
        ready_lanes: ConcurrentQueue::unbounded(),
        injector: Injector::new(),
        stealer_generation: AtomicUsize::new(0),
        prefetched_items: AtomicUsize::new(0),
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
            stealers: vec![(0, stealer)],
            seen_stealer_generation: 0,
            steal_cursor: 0,
            capacity_per_sender: capacity_per_sender.next_power_of_two(),
        },
    )
}

struct Shared<T> {
    registry: Mutex<Registry<T>>,
    ready_pages: ConcurrentQueue<usize>,
    ready_lanes: ConcurrentQueue<LaneToken<T>>,
    injector: Injector<T>,
    stealer_generation: AtomicUsize,
    prefetched_items: AtomicUsize,
    registered_lanes: AtomicUsize,
    live_senders: AtomicUsize,
    live_receivers: AtomicUsize,
    receiver_alive: AtomicBool,
    data_waiters: MultiWaitCell,
    capacity_per_sender: usize,
}

impl<T> Shared<T> {
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
        self.registered_lanes.fetch_add(1, Ordering::Release);
        self.live_senders.fetch_add(1, Ordering::AcqRel);
        Ok((key, signal, producer))
    }

    fn register_receiver(&self) -> (usize, Worker<T>) {
        let local = Worker::new_fifo();
        let stealer = local.stealer();
        let id = lock(&self.registry).register_receiver(stealer);
        self.live_receivers.fetch_add(1, Ordering::AcqRel);
        self.stealer_generation.fetch_add(1, Ordering::Release);
        (id, local)
    }

    fn unregister_receiver(&self, id: usize) {
        lock(&self.registry).unregister_receiver(id);
        self.stealer_generation.fetch_add(1, Ordering::Release);
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
        let lanes = lock(&self.registry).take_ready_lanes(page_id);
        for lane in lanes {
            self.ready_lanes
                .push(lane)
                .unwrap_or_else(|_| unreachable!("ready-lane queue is never closed"));
        }
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
                Steal::Success(value) => {
                    self.prefetched_items.fetch_sub(1, Ordering::AcqRel);
                    drop(value);
                }
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

    fn take_ready_lanes(&mut self, page_id: usize) -> Vec<LaneToken<T>> {
        let Some(page) = self.pages.get(page_id) else {
            return Vec::new();
        };
        let mut bits = page.take();
        let mut lanes = Vec::with_capacity(bits.count_ones() as usize);
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
                lanes.push(lane);
            } else {
                slot.lane = Some(lane);
            }
        }
        lanes
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

/// Sending half.
///
/// Each sender owns one SPSC ring producer. Registering senders has no fixed
/// limit.
#[derive(Debug)]
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    producer: yring::Producer<T>,
    key: LaneKey,
    signal: Arc<LaneSignal>,
}

impl<T> Sender<T> {
    /// Try to register another sender.
    pub fn try_clone(&self) -> Option<Self> {
        self.try_register().ok()
    }

    /// Try to register another sender and report disconnection.
    pub fn try_register(&self) -> Result<Self, TryRegisterError> {
        let (key, signal, producer) = self.shared.register_sender()?;
        Ok(Self {
            shared: self.shared.clone(),
            producer,
            key,
            signal,
        })
    }

    /// Try to send one value.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        let (result, wake_receiver) = self.try_send_inner(value);
        if wake_receiver {
            self.shared.data_waiters.notify_one();
        }
        result
    }

    #[inline]
    fn try_send_inner(&mut self, value: T) -> (Result<(), TrySendError<T>>, bool) {
        if !self.shared.receiver_alive.load(Ordering::Acquire) {
            return (Err(TrySendError::Disconnected(value)), false);
        }

        match self.producer.push(value) {
            Ok(()) => {
                self.producer.flush();
                let wake_receiver = self.shared.mark_ready(&self.signal);
                (Ok(()), wake_receiver)
            }
            Err(value) => {
                if !self.shared.receiver_alive.load(Ordering::Acquire)
                    || self.producer.is_consumer_dropped()
                {
                    (Err(TrySendError::Disconnected(value)), false)
                } else {
                    (Err(TrySendError::Full(value)), false)
                }
            }
        }
    }

    /// Send one value, blocking while this sender's ring is full.
    #[inline]
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        match self.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Disconnected(value)) => Err(SendError(value)),
            Err(TrySendError::Full(value)) => self.send_slow(value),
        }
    }

    #[cold]
    #[inline(never)]
    fn send_slow(&mut self, mut value: T) -> Result<(), SendError<T>> {
        for _ in 0..PARK_SPINS {
            std::hint::spin_loop();
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(value)) => return Err(SendError(value)),
                Err(TrySendError::Full(returned)) => value = returned,
            }
        }

        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(value)) => return Err(SendError(value)),
                Err(TrySendError::Full(returned)) => value = returned,
            }

            let signal = self.signal.clone();
            let wait = signal.prepare_space_wait();
            let (result, wake_receiver) = self.try_send_inner(value);
            match result {
                Ok(()) => {
                    wait.cancel();
                    if wake_receiver {
                        self.shared.data_waiters.notify_one();
                    }
                    return Ok(());
                }
                Err(TrySendError::Disconnected(value)) => {
                    wait.cancel();
                    return Err(SendError(value));
                }
                Err(TrySendError::Full(returned)) => value = returned,
            }
            wait.wait();
        }
    }

    /// Send one value, blocking up to `timeout` while this sender's ring is
    /// full.
    pub fn send_timeout(&mut self, value: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return self
                .send(value)
                .map_err(|SendError(value)| SendTimeoutError::Disconnected(value));
        };
        self.send_deadline(value, deadline)
    }

    /// Send one value, blocking until `deadline` while this sender's ring is
    /// full.
    pub fn send_deadline(
        &mut self,
        mut value: T,
        deadline: Instant,
    ) -> Result<(), SendTimeoutError<T>> {
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(value)) => {
                    return Err(SendTimeoutError::Disconnected(value));
                }
                Err(TrySendError::Full(returned)) => value = returned,
            }

            let signal = self.signal.clone();
            let wait = signal.prepare_space_wait();
            let (result, wake_receiver) = self.try_send_inner(value);
            match result {
                Ok(()) => {
                    wait.cancel();
                    if wake_receiver {
                        self.shared.data_waiters.notify_one();
                    }
                    return Ok(());
                }
                Err(TrySendError::Disconnected(value)) => {
                    wait.cancel();
                    return Err(SendTimeoutError::Disconnected(value));
                }
                Err(TrySendError::Full(returned)) => value = returned,
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                wait.cancel();
                return Err(SendTimeoutError::Timeout(value));
            }
            if wait.wait_timeout(remaining) {
                match self.try_send(value) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Disconnected(value)) => {
                        return Err(SendTimeoutError::Disconnected(value));
                    }
                    Err(TrySendError::Full(value)) => {
                        return Err(SendTimeoutError::Timeout(value));
                    }
                }
            }
        }
    }

    /// Return this sender's lane slot.
    #[inline]
    pub fn shard(&self) -> usize {
        self.key.slot
    }

    /// Return this sender's capacity after `yring` rounding.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.producer.capacity()
    }

    /// Return whether every receiver has been dropped.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        !self.shared.receiver_alive.load(Ordering::Acquire)
    }

    /// Return a snapshot of the number of live senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.shared.live_senders.load(Ordering::Relaxed)
    }

    /// Return a snapshot of the number of live receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.shared.live_receivers.load(Ordering::Relaxed)
    }

    /// Return whether both senders belong to the same channel.
    #[inline]
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.producer.close();
        let _ = self.shared.mark_ready(&self.signal);
        self.shared.live_senders.fetch_sub(1, Ordering::AcqRel);
        self.shared.data_waiters.notify_one();
    }
}

/// Receiving half.
///
/// Clones compete for messages. Each receiver has a local work queue whose
/// buffered items remain stealable by other receivers.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    id: usize,
    local: Worker<T>,
    stealers: Vec<(usize, Stealer<T>)>,
    seen_stealer_generation: usize,
    steal_cursor: usize,
    capacity_per_sender: usize,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let (id, local) = self.shared.register_receiver();
        Self {
            shared: self.shared.clone(),
            id,
            local,
            stealers: Vec::new(),
            seen_stealer_generation: usize::MAX,
            steal_cursor: 0,
            capacity_per_sender: self.capacity_per_sender,
        }
    }
}

impl<T> Receiver<T> {
    /// Try to receive one value.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        loop {
            let (result, effects) = self.try_recv_inner();
            let retry = effects.has_effect() && matches!(result, Err(TryRecvError::Empty));
            self.apply_effects(effects);
            if !retry {
                return result;
            }
        }
    }

    #[inline]
    fn try_recv_inner(&mut self) -> (Result<T, TryRecvError>, Effects) {
        loop {
            let work_generation = match self.pop_work() {
                WorkPop::Item(value) => return (Ok(value), Effects::default()),
                WorkPop::Empty { generation } => generation,
            };

            let Some(lane) = self.acquire_lane() else {
                let disconnected = self.is_disconnected();
                // Receiver drop publishes local work before changing this
                // generation. Retry if the first work scan raced that handoff.
                if disconnected
                    && work_generation != self.shared.stealer_generation.load(Ordering::Acquire)
                {
                    continue;
                }
                return (
                    Err(if disconnected {
                        TryRecvError::Disconnected
                    } else {
                        TryRecvError::Empty
                    }),
                    Effects::default(),
                );
            };
            match self.drain_lane(lane) {
                LaneDrain::Item { value, effects } => return (Ok(value), effects),
                LaneDrain::Empty(effects) => {
                    if effects.has_effect() {
                        return (Err(TryRecvError::Empty), effects);
                    }
                }
            }
        }
    }

    /// Receive one value, blocking while the channel is empty.
    #[inline]
    pub fn recv(&mut self) -> Result<T, RecvError> {
        match self.try_recv() {
            Ok(value) => Ok(value),
            Err(TryRecvError::Disconnected) => Err(RecvError),
            Err(TryRecvError::Empty) => self.recv_slow(),
        }
    }

    #[cold]
    #[inline(never)]
    fn recv_slow(&mut self) -> Result<T, RecvError> {
        for _ in 0..PARK_SPINS {
            std::hint::spin_loop();
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {}
            }
        }

        loop {
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {}
            }

            let shared = self.shared.clone();
            let wait = shared.data_waiters.prepare();
            let (result, effects) = self.try_recv_inner();
            match result {
                Ok(value) => {
                    wait.cancel();
                    self.apply_effects(effects);
                    return Ok(value);
                }
                Err(TryRecvError::Disconnected) => {
                    wait.cancel();
                    return Err(RecvError);
                }
                Err(TryRecvError::Empty) if effects.has_effect() => {
                    wait.cancel();
                    self.apply_effects(effects);
                }
                Err(TryRecvError::Empty) => wait.wait(),
            }
        }
    }

    /// Receive one value, blocking for at most `timeout` while the channel is
    /// empty.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return self
                .recv()
                .map_err(|RecvError| RecvTimeoutError::Disconnected);
        };
        self.recv_deadline(deadline)
    }

    /// Receive one value, blocking until `deadline` while the channel is
    /// empty.
    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        loop {
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => {
                    return Err(RecvTimeoutError::Disconnected);
                }
                Err(TryRecvError::Empty) => {}
            }

            let shared = self.shared.clone();
            let wait = shared.data_waiters.prepare();
            let (result, effects) = self.try_recv_inner();
            match result {
                Ok(value) => {
                    wait.cancel();
                    self.apply_effects(effects);
                    return Ok(value);
                }
                Err(TryRecvError::Disconnected) => {
                    wait.cancel();
                    return Err(RecvTimeoutError::Disconnected);
                }
                Err(TryRecvError::Empty) if effects.has_effect() => {
                    wait.cancel();
                    self.apply_effects(effects);
                    continue;
                }
                Err(TryRecvError::Empty) => {}
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                wait.cancel();
                return Err(RecvTimeoutError::Timeout);
            }
            if wait.wait_timeout(remaining) {
                match self.try_recv() {
                    Ok(value) => return Ok(value),
                    Err(TryRecvError::Disconnected) => {
                        return Err(RecvTimeoutError::Disconnected);
                    }
                    Err(TryRecvError::Empty) => return Err(RecvTimeoutError::Timeout),
                }
            }
        }
    }

    fn pop_work(&mut self) -> WorkPop<T> {
        if let Some(value) = self.local.pop() {
            self.shared.prefetched_items.fetch_sub(1, Ordering::AcqRel);
            return WorkPop::Item(value);
        }

        let generation = self.shared.stealer_generation.load(Ordering::Acquire);
        loop {
            match self.shared.injector.steal_batch_and_pop(&self.local) {
                Steal::Success(value) => {
                    self.shared.prefetched_items.fetch_sub(1, Ordering::AcqRel);
                    return WorkPop::Item(value);
                }
                Steal::Retry => continue,
                Steal::Empty => break,
            }
        }

        self.refresh_stealers(generation);
        let len = self.stealers.len();
        for offset in 0..len {
            let index = (self.steal_cursor + offset) % len;
            let (id, stealer) = &self.stealers[index];
            if *id == self.id {
                continue;
            }
            loop {
                match stealer.steal_batch_and_pop(&self.local) {
                    Steal::Success(value) => {
                        self.steal_cursor = index.wrapping_add(1);
                        self.shared.prefetched_items.fetch_sub(1, Ordering::AcqRel);
                        return WorkPop::Item(value);
                    }
                    Steal::Retry => continue,
                    Steal::Empty => break,
                }
            }
        }
        WorkPop::Empty { generation }
    }

    fn refresh_stealers(&mut self, generation: usize) {
        if generation == self.seen_stealer_generation {
            return;
        }
        let registry = lock(&self.shared.registry);
        self.stealers = registry.clone_stealers();
        self.seen_stealer_generation = self.shared.stealer_generation.load(Ordering::Relaxed);
        if !self.stealers.is_empty() {
            self.steal_cursor %= self.stealers.len();
        }
    }

    fn acquire_lane(&self) -> Option<LaneToken<T>> {
        loop {
            self.shared.activate_one_page();
            if let Ok(lane) = self.shared.ready_lanes.pop() {
                return Some(lane);
            }
            if self.shared.ready_pages.is_empty() {
                return None;
            }
        }
    }

    fn drain_lane(&mut self, mut lane: LaneToken<T>) -> LaneDrain<T> {
        if lane.cached_available == 0 {
            lane.cached_available = lane.consumer.prefetch();
            if lane.cached_available == 0 {
                let released = lane.release_pending().then(|| lane.signal.clone());
                return match self.shared.finish_empty_lane(lane) {
                    FinishLane::Ready(lane) => {
                        self.requeue_lane(lane);
                        LaneDrain::Empty(Effects {
                            released,
                            wake: Wake::One,
                        })
                    }
                    FinishLane::Parked => LaneDrain::Empty(Effects {
                        released,
                        wake: Wake::None,
                    }),
                    FinishLane::Retired { wake_all } => LaneDrain::Empty(Effects {
                        released,
                        wake: if wake_all { Wake::All } else { Wake::None },
                    }),
                };
            }
        }

        let batch = lane.cached_available.min(PREFETCH_LIMIT);
        let value = lane
            .consumer
            .pop()
            .expect("cached_available guarantees prefetched data");
        if batch > 1 {
            self.shared
                .prefetched_items
                .fetch_add(batch - 1, Ordering::Release);
        }
        for _ in 1..batch {
            self.local.push(
                lane.consumer
                    .pop()
                    .expect("batch is bounded by cached availability"),
            );
        }
        lane.cached_available -= batch;
        lane.unreleased += batch;
        let released = (lane.unreleased >= lane.release_batch && lane.release_pending())
            .then(|| lane.signal.clone());

        let mut wake = if batch > 1 { Wake::All } else { Wake::None };
        if lane.cached_available != 0 {
            self.requeue_lane(lane);
            wake = wake.merge(Wake::One);
        } else {
            match self.shared.finish_empty_lane(lane) {
                FinishLane::Ready(lane) => {
                    self.requeue_lane(lane);
                    wake = wake.merge(Wake::One);
                }
                FinishLane::Parked => {}
                FinishLane::Retired { wake_all } => {
                    if wake_all {
                        wake = Wake::All;
                    }
                }
            }
        }

        LaneDrain::Item {
            value,
            effects: Effects { released, wake },
        }
    }

    fn requeue_lane(&self, lane: LaneToken<T>) {
        self.shared
            .ready_lanes
            .push(lane)
            .unwrap_or_else(|_| unreachable!("ready-lane queue is never closed"));
    }

    fn apply_effects(&self, effects: Effects) {
        if let Some(signal) = effects.released {
            signal.notify_space();
        }
        match effects.wake {
            Wake::None => {}
            Wake::One => self.shared.data_waiters.notify_one(),
            Wake::All => self.shared.data_waiters.notify_all(),
        }
    }

    /// Return whether all senders are gone and every lane is drained.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.shared.live_senders.load(Ordering::Acquire) == 0
            && self.shared.registered_lanes.load(Ordering::Acquire) == 0
            && self.shared.prefetched_items.load(Ordering::Acquire) == 0
    }

    /// Return per-sender capacity after `yring` rounding.
    #[inline]
    pub fn capacity_per_sender(&self) -> usize {
        self.capacity_per_sender
    }

    /// Return a snapshot of the number of live senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.shared.live_senders.load(Ordering::Relaxed)
    }

    /// Return a snapshot of the number of live receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.shared.live_receivers.load(Ordering::Relaxed)
    }

    /// Return whether both receivers belong to the same channel.
    #[inline]
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    /// Iterate until every sender disconnects and buffered values are drained.
    #[inline]
    pub fn iter(&mut self) -> Iter<'_, T> {
        Iter { receiver: self }
    }

    /// Iterate over values immediately available without blocking.
    #[inline]
    pub fn try_iter(&mut self) -> TryIter<'_, T> {
        TryIter { receiver: self }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut published = false;
        while let Some(value) = self.local.pop() {
            self.shared.injector.push(value);
            published = true;
        }
        self.shared.unregister_receiver(self.id);

        let previous = self.shared.live_receivers.fetch_sub(1, Ordering::AcqRel);
        if previous == 1 {
            self.shared.close_all_receivers();
        } else if published {
            self.shared.data_waiters.notify_all();
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("id", &self.id)
            .field("local_items", &self.local.len())
            .field(
                "registered_lanes",
                &self.shared.registered_lanes.load(Ordering::Relaxed),
            )
            .field(
                "prefetched_items",
                &self.shared.prefetched_items.load(Ordering::Relaxed),
            )
            .field(
                "live_receivers",
                &self.shared.live_receivers.load(Ordering::Relaxed),
            )
            .field("capacity_per_sender", &self.capacity_per_sender())
            .finish_non_exhaustive()
    }
}

/// Blocking iterator over a borrowed receiver.
#[derive(Debug)]
pub struct Iter<'a, T> {
    receiver: &'a mut Receiver<T>,
}

impl<T> Iterator for Iter<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl<T> std::iter::FusedIterator for Iter<'_, T> {}

/// Nonblocking iterator over a borrowed receiver.
#[derive(Debug)]
pub struct TryIter<'a, T> {
    receiver: &'a mut Receiver<T>,
}

impl<T> Iterator for TryIter<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.try_recv().ok()
    }
}

/// Blocking iterator that owns its receiver.
#[derive(Debug)]
pub struct IntoIter<T> {
    receiver: Receiver<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl<T> std::iter::FusedIterator for IntoIter<T> {}

impl<'a, T> IntoIterator for &'a mut Receiver<T> {
    type Item = T;
    type IntoIter = Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> IntoIterator for Receiver<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IntoIter { receiver: self }
    }
}

struct Lane<T> {
    key: LaneKey,
    signal: Arc<LaneSignal>,
    consumer: yring::Consumer<T>,
    cached_available: usize,
    unreleased: usize,
    release_batch: usize,
}

struct LaneToken<T>(Box<Lane<T>>);

impl<T> LaneToken<T> {
    fn new(lane: Lane<T>) -> Self {
        Self(Box::new(lane))
    }
}

impl<T> Deref for LaneToken<T> {
    type Target = Lane<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for LaneToken<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Lane<T> {
    fn new(key: LaneKey, signal: Arc<LaneSignal>, consumer: yring::Consumer<T>) -> Self {
        let release_batch = consumer.capacity().min(PREFETCH_LIMIT);
        Self {
            key,
            signal,
            consumer,
            cached_available: 0,
            unreleased: 0,
            release_batch,
        }
    }

    fn release_pending(&mut self) -> bool {
        if self.unreleased == 0 {
            return false;
        }
        self.consumer.release();
        self.unreleased = 0;
        true
    }
}

enum FinishLane<T> {
    Ready(LaneToken<T>),
    Parked,
    Retired { wake_all: bool },
}

enum LaneDrain<T> {
    Item { value: T, effects: Effects },
    Empty(Effects),
}

enum WorkPop<T> {
    Item(T),
    Empty { generation: usize },
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Wake {
    #[default]
    None,
    One,
    All,
}

impl Wake {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::One, _) | (_, Self::One) => Self::One,
            (Self::None, Self::None) => Self::None,
        }
    }
}

#[derive(Default)]
struct Effects {
    released: Option<Arc<LaneSignal>>,
    wake: Wake,
}

impl Effects {
    fn has_effect(&self) -> bool {
        self.released.is_some() || self.wake != Wake::None
    }
}

fn close_lane<T>(mut lane: LaneToken<T>) {
    lane.consumer.close();
    lane.signal.notify_space();
}

fn lock<T>(mutex: &Mutex<T>) -> crate::compat::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
