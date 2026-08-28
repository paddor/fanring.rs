//! Dynamic MPSC channel built from one bounded SPSC ring per sender.
//!
//! Sender registration is dynamic. Each sender owns its ring producer and the
//! receiver owns every ring consumer. Ordering is FIFO per sender and relaxed
//! across senders.

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::time::{Duration, Instant};

use concurrent_queue::ConcurrentQueue;

mod compat;
mod error;
mod ready;
mod wait;

use compat::{Arc, AtomicBool, AtomicUsize, Mutex, Ordering};
use ready::{LANES_PER_PAGE, LaneSignal, ReadyPage};
use wait::WaitCell;

pub use error::{
    ChannelError, RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError,
    TryRegisterError, TrySendError,
};

/// Maximum per-sender capacity accepted by [`channel`] and [`try_channel`].
///
/// `yring` rounds capacity up to a power of two and requires the rounded value
/// to fit in half the cursor range.
pub const MAX_CAPACITY_PER_SENDER: usize = 1usize << (usize::BITS - 2);

const PREFETCH_LIMIT: usize = 64;
const READY_POLL_INTERVAL: usize = 64;
#[cfg(not(loom))]
const PARK_SPINS: usize = 128;
#[cfg(loom)]
const PARK_SPINS: usize = 0;

/// Create a bounded sharded MPSC channel.
///
/// `capacity_per_sender` must be in `1..=`[`MAX_CAPACITY_PER_SENDER`] and is
/// rounded up by `yring` to the next power of two.
pub fn channel<T>(capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    try_channel(capacity_per_sender).unwrap_or_else(|error| panic!("{error}"))
}

/// Try to create a bounded sharded MPSC channel.
///
/// This is the fallible version of [`channel`]. It returns a [`ChannelError`]
/// instead of panicking when the configuration is invalid.
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
    let shared = Arc::new(Shared {
        registry: Mutex::new(Registry {
            pending: Vec::new(),
            free: Vec::new(),
            pages: vec![page.clone()],
            next_slot: 1,
        }),
        ready_pages: ConcurrentQueue::unbounded(),
        registry_generation: AtomicUsize::new(0),
        registered_lanes: AtomicUsize::new(1),
        live_senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        data_waiter: WaitCell::new(),
        capacity_per_sender,
    });

    (
        Sender {
            shared: shared.clone(),
            producer,
            key,
            signal: signal.clone(),
        },
        Receiver {
            shared,
            lanes: vec![Some(Lane::new(key, signal, consumer))],
            pages: vec![page],
            active: VecDeque::new(),
            seen_registry_generation: 0,
            items_until_ready_poll: READY_POLL_INTERVAL,
            capacity_per_sender: capacity_per_sender.next_power_of_two(),
        },
    )
}

struct Shared<T> {
    registry: Mutex<Registry<T>>,
    ready_pages: ConcurrentQueue<usize>,
    registry_generation: AtomicUsize,
    registered_lanes: AtomicUsize,
    live_senders: AtomicUsize,
    receiver_alive: AtomicBool,
    data_waiter: WaitCell,
    capacity_per_sender: usize,
}

impl<T> Shared<T> {
    fn register_sender(
        &self,
    ) -> Result<(LaneKey, Arc<LaneSignal>, yring::Producer<T>), TryRegisterError> {
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterError::Disconnected);
        }

        let mut registry = match self.registry.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterError::Disconnected);
        }

        let (key, page) = registry.allocate_lane();
        let signal = Arc::new(LaneSignal::new(page, key.slot));
        let (producer, consumer) = yring::spsc(self.capacity_per_sender);
        registry.pending.push(PendingLane {
            key,
            signal: signal.clone(),
            consumer,
        });
        self.registered_lanes.fetch_add(1, Ordering::Release);
        self.live_senders.fetch_add(1, Ordering::AcqRel);
        self.registry_generation.fetch_add(1, Ordering::Release);
        Ok((key, signal, producer))
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

struct PendingLane<T> {
    key: LaneKey,
    signal: Arc<LaneSignal>,
    consumer: yring::Consumer<T>,
}

struct Registry<T> {
    pending: Vec<PendingLane<T>>,
    free: Vec<LaneKey>,
    pages: Vec<Arc<ReadyPage>>,
    next_slot: usize,
}

impl<T> Registry<T> {
    fn allocate_lane(&mut self) -> (LaneKey, Arc<ReadyPage>) {
        let key = self.free.pop().unwrap_or_else(|| {
            let key = LaneKey {
                slot: self.next_slot,
                generation: 0,
            };
            self.next_slot += 1;
            key
        });
        let page_id = key.slot / LANES_PER_PAGE;
        while self.pages.len() <= page_id {
            self.pages.push(Arc::new(ReadyPage::new(self.pages.len())));
        }
        (key, self.pages[page_id].clone())
    }

    fn retire_lane(&mut self, key: LaneKey) {
        self.free.push(LaneKey {
            slot: key.slot,
            generation: key.generation.wrapping_add(1),
        });
    }
}

/// Sending half.
///
/// A `Sender` owns exactly one SPSC ring producer. It is `Send` when `T` is
/// `Send`, but it is not `Sync`. Sender registration has no configured limit.
#[derive(Debug)]
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    producer: yring::Producer<T>,
    key: LaneKey,
    signal: Arc<LaneSignal>,
}

impl<T> Sender<T> {
    /// Try to register another sender.
    ///
    /// Returns `None` when the receiver is gone.
    pub fn try_clone(&self) -> Option<Self> {
        self.try_register().ok()
    }

    /// Try to register another sender and report why registration failed.
    ///
    /// Prefer this over [`try_clone`](Self::try_clone) when the caller needs to
    /// distinguish a closed receiver from successful registration.
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
    ///
    /// A successful send is immediately visible to the receiver. Internally,
    /// the value is pushed into this sender's SPSC ring and flushed.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        let (result, wake_receiver) = self.try_send_inner(value);
        if wake_receiver {
            self.shared.data_waiter.notify();
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
                        self.shared.data_waiter.notify();
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

    /// Send one value, blocking for at most `timeout` while this sender's ring
    /// is full.
    pub fn send_timeout(
        &mut self,
        mut value: T,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<T>> {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return self
                .send(value)
                .map_err(|SendError(value)| SendTimeoutError::Disconnected(value));
        };

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
                        self.shared.data_waiter.notify();
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

    /// Return this sender's current lane slot.
    #[inline]
    pub fn shard(&self) -> usize {
        self.key.slot
    }

    /// Return this sender's per-shard capacity after `yring` rounding.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.producer.capacity()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.producer.close();
        let _ = self.shared.mark_ready(&self.signal);
        self.shared.live_senders.fetch_sub(1, Ordering::AcqRel);
        self.shared.data_waiter.notify();
    }
}

/// Receiving half.
///
/// The receiver owns every SPSC consumer and drains active lanes in bounded
/// round-robin bursts. It is `Send` when `T` is `Send`, but it is not `Sync`.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    lanes: Vec<Option<Lane<T>>>,
    pages: Vec<Arc<ReadyPage>>,
    active: VecDeque<LaneKey>,
    seen_registry_generation: usize,
    items_until_ready_poll: usize,
    capacity_per_sender: usize,
}

impl<T> Receiver<T> {
    /// Try to receive one value.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        loop {
            let (result, released) = self.try_recv_inner();
            let retry = released.is_some() && matches!(result, Err(TryRecvError::Empty));
            self.notify_released(released);
            if !retry {
                return result;
            }
        }
    }

    #[inline]
    fn try_recv_inner(&mut self) -> (Result<T, TryRecvError>, Option<LaneKey>) {
        loop {
            if self.active.is_empty() {
                self.collect_ready(true);
                if self.active.is_empty() {
                    return (
                        Err(if self.is_disconnected() {
                            TryRecvError::Disconnected
                        } else {
                            TryRecvError::Empty
                        }),
                        None,
                    );
                }
            } else if self.items_until_ready_poll == 0 {
                self.collect_ready(false);
                self.items_until_ready_poll = READY_POLL_INTERVAL;
            }

            let key = self.active.pop_front().expect("active lane present");
            match self.poll_lane(key) {
                LanePoll::Item {
                    value,
                    rotate,
                    released,
                } => {
                    if rotate {
                        self.active.push_back(key);
                    } else {
                        self.active.push_front(key);
                    }
                    self.items_until_ready_poll -= 1;
                    return (Ok(value), released.then_some(key));
                }
                LanePoll::Keep { released } => {
                    self.active.push_back(key);
                    if released {
                        return (Err(TryRecvError::Empty), Some(key));
                    }
                }
                LanePoll::Idle {
                    disconnected,
                    released,
                } => {
                    if disconnected {
                        self.retire_lane(key);
                    } else if released {
                        return (Err(TryRecvError::Empty), Some(key));
                    }
                }
                LanePoll::Stale => {}
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
            let wait = shared.data_waiter.prepare();
            let (result, released) = self.try_recv_inner();
            match result {
                Ok(value) => {
                    wait.cancel();
                    self.notify_released(released);
                    return Ok(value);
                }
                Err(TryRecvError::Disconnected) => {
                    wait.cancel();
                    return Err(RecvError);
                }
                Err(TryRecvError::Empty) => {
                    if released.is_some() {
                        wait.cancel();
                        self.notify_released(released);
                        continue;
                    }
                    wait.wait();
                }
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

        loop {
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => {
                    return Err(RecvTimeoutError::Disconnected);
                }
                Err(TryRecvError::Empty) => {}
            }

            let shared = self.shared.clone();
            let wait = shared.data_waiter.prepare();
            let (result, released) = self.try_recv_inner();
            match result {
                Ok(value) => {
                    wait.cancel();
                    self.notify_released(released);
                    return Ok(value);
                }
                Err(TryRecvError::Disconnected) => {
                    wait.cancel();
                    return Err(RecvTimeoutError::Disconnected);
                }
                Err(TryRecvError::Empty) => {
                    if released.is_some() {
                        wait.cancel();
                        self.notify_released(released);
                        continue;
                    }
                }
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

    #[inline]
    fn poll_lane(&mut self, key: LaneKey) -> LanePoll<T> {
        let Some(lane) = self.lanes.get_mut(key.slot).and_then(Option::as_mut) else {
            return LanePoll::Stale;
        };
        if lane.key != key {
            return LanePoll::Stale;
        }

        if lane.cached_available == 0 {
            lane.cached_available = lane.consumer.prefetch();
            if lane.cached_available == 0 {
                let released = lane.release_pending();
                lane.signal.finish_drain();
                lane.cached_available = lane.consumer.prefetch();
                lane.burst = 0;
                return if lane.cached_available != 0 && lane.signal.claim_after_empty() {
                    LanePoll::Keep { released }
                } else {
                    LanePoll::Idle {
                        disconnected: lane.consumer.is_disconnected(),
                        released,
                    }
                };
            }
        }

        let value = lane
            .consumer
            .pop()
            .expect("cached_available guarantees prefetched data");
        lane.cached_available -= 1;
        lane.unreleased += 1;
        lane.burst += 1;
        let released = lane.unreleased == lane.release_batch && lane.release_pending();

        let rotate = lane.burst == PREFETCH_LIMIT;
        if rotate {
            lane.burst = 0;
        }
        LanePoll::Item {
            value,
            rotate,
            released,
        }
    }

    #[inline]
    fn notify_released(&self, released: Option<LaneKey>) {
        let Some(key) = released else {
            return;
        };
        let Some(lane) = self.lanes.get(key.slot).and_then(Option::as_ref) else {
            return;
        };
        if lane.key == key {
            lane.signal.notify_space();
        }
    }

    fn collect_ready(&mut self, all: bool) {
        self.refresh_registry();
        while let Ok(page_id) = self.shared.ready_pages.pop() {
            if page_id >= self.pages.len() {
                self.refresh_registry();
            }
            let Some(page) = self.pages.get(page_id).cloned() else {
                continue;
            };
            let mut bits = page.take();
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let slot = page_id * LANES_PER_PAGE + bit;
                let Some(lane) = self.lanes.get(slot).and_then(Option::as_ref) else {
                    continue;
                };
                if lane.signal.is_pending() {
                    self.active.push_back(lane.key);
                }
            }
            if !all {
                break;
            }
        }
    }

    fn refresh_registry(&mut self) {
        let generation = self.shared.registry_generation.load(Ordering::Acquire);
        if generation == self.seen_registry_generation {
            return;
        }

        let (pending, pages, seen) = {
            let mut registry = match self.shared.registry.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            (
                mem::take(&mut registry.pending),
                registry.pages.clone(),
                self.shared.registry_generation.load(Ordering::Relaxed),
            )
        };
        self.pages = pages;
        for pending in pending {
            if self.lanes.len() <= pending.key.slot {
                self.lanes.resize_with(pending.key.slot + 1, || None);
            }
            debug_assert!(self.lanes[pending.key.slot].is_none());
            self.lanes[pending.key.slot] =
                Some(Lane::new(pending.key, pending.signal, pending.consumer));
        }
        self.seen_registry_generation = seen;
    }

    fn retire_lane(&mut self, key: LaneKey) {
        let Some(lane) = self.lanes.get_mut(key.slot).and_then(Option::take) else {
            return;
        };
        debug_assert_eq!(lane.key, key);
        self.shared.registered_lanes.fetch_sub(1, Ordering::AcqRel);
        let mut registry = match self.shared.registry.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        registry.retire_lane(key);
        drop(registry);
        drop(lane);
    }

    /// Return whether all senders are gone and every registered lane is drained.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.shared.live_senders.load(Ordering::Acquire) == 0
            && self.shared.registered_lanes.load(Ordering::Acquire) == 0
    }

    /// Return per-shard capacity after `yring` rounding.
    #[inline]
    pub fn capacity_per_sender(&self) -> usize {
        self.capacity_per_sender
    }
}

struct Lane<T> {
    key: LaneKey,
    signal: Arc<LaneSignal>,
    consumer: yring::Consumer<T>,
    cached_available: usize,
    unreleased: usize,
    release_batch: usize,
    burst: usize,
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
            burst: 0,
        }
    }

    #[inline]
    fn release_pending(&mut self) -> bool {
        if self.unreleased == 0 {
            return false;
        }
        self.consumer.release();
        self.unreleased = 0;
        true
    }
}

enum LanePoll<T> {
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

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_alive.store(false, Ordering::Release);
        for lane in self.lanes.iter_mut().flatten() {
            lane.consumer.close();
            lane.signal.notify_space();
        }
        let pending = {
            let mut registry = match self.shared.registry.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            mem::take(&mut registry.pending)
        };
        for mut lane in pending {
            lane.consumer.close();
            lane.signal.notify_space();
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("active_lanes", &self.active.len())
            .field(
                "registered_lanes",
                &self.shared.registered_lanes.load(Ordering::Relaxed),
            )
            .field("capacity_per_sender", &self.capacity_per_sender())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_send_recv() {
        let (mut tx, mut rx) = channel(4);

        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn blocking_send_waits_for_lane_capacity() {
        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();

        std::thread::scope(|scope| {
            let sender = scope.spawn(move || tx.send(2));
            assert_eq!(rx.recv(), Ok(1));
            assert_eq!(rx.recv(), Ok(2));
            assert_eq!(sender.join().unwrap(), Ok(()));
        });
    }

    #[test]
    fn blocking_recv_wakes_for_data_and_disconnect() {
        let (mut tx, mut rx) = channel(1);
        std::thread::scope(|scope| {
            scope.spawn(move || tx.send(7).unwrap());
            assert_eq!(rx.recv(), Ok(7));
        });

        assert_eq!(rx.recv(), Err(RecvError));
    }

    #[test]
    fn blocking_send_wakes_when_receiver_drops() {
        let (mut tx, rx) = channel(1);
        tx.try_send(1).unwrap();

        std::thread::scope(|scope| {
            let sender = scope.spawn(move || tx.send(2));
            drop(rx);
            assert_eq!(sender.join().unwrap(), Err(SendError(2)));
        });
    }

    #[test]
    fn blocking_timeouts_report_unsatisfied_operation() {
        let (mut tx, mut rx) = channel(1);
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(1)),
            Err(RecvTimeoutError::Timeout)
        );

        tx.try_send(1).unwrap();
        assert_eq!(
            tx.send_timeout(2, Duration::from_millis(1)),
            Err(SendTimeoutError::Timeout(2))
        );
        drop(rx);
        assert_eq!(
            tx.send_timeout(3, Duration::ZERO),
            Err(SendTimeoutError::Disconnected(3))
        );
    }

    #[test]
    fn blocking_timeouts_succeed_after_wakeup() {
        let (mut tx, mut rx) = channel(1);
        std::thread::scope(|scope| {
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(1));
                tx.send(1).unwrap();
            });
            assert_eq!(rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        });

        let (mut tx, mut rx) = channel(1);
        tx.try_send(1).unwrap();
        std::thread::scope(|scope| {
            let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(1));
                assert_eq!(rx.recv(), Ok(1));
                done_rx.recv().unwrap();
            });
            assert_eq!(tx.send_timeout(2, Duration::from_secs(1)), Ok(()));
            done_tx.send(()).unwrap();
        });
    }

    #[test]
    fn try_channel_validates_config() {
        assert_eq!(
            try_channel::<u8>(0).unwrap_err(),
            ChannelError::ZeroCapacity
        );
        assert_eq!(
            try_channel::<u8>(MAX_CAPACITY_PER_SENDER + 1).unwrap_err(),
            ChannelError::CapacityTooLarge {
                requested: MAX_CAPACITY_PER_SENDER + 1,
                max: MAX_CAPACITY_PER_SENDER,
            }
        );
    }

    #[test]
    fn capacity_reports_yring_rounding() {
        let (tx, rx) = try_channel::<u8>(3).unwrap();

        assert_eq!(tx.capacity(), 4);
        assert_eq!(rx.capacity_per_sender(), 4);
    }

    #[test]
    fn try_register_reports_failure_cause() {
        let (tx, rx) = channel::<u8>(1);
        assert!(tx.try_register().is_ok());
        drop(rx);
        assert_eq!(
            tx.try_register().unwrap_err(),
            TryRegisterError::Disconnected
        );
        assert!(tx.try_clone().is_none());
    }

    #[test]
    fn public_errors_implement_std_error() {
        fn assert_error<E: std::error::Error>() {}

        assert_error::<ChannelError>();
        assert_error::<TryRegisterError>();
        assert_error::<SendError<u8>>();
        assert_error::<TrySendError<u8>>();
        assert_error::<SendTimeoutError<u8>>();
        assert_error::<RecvError>();
        assert_error::<TryRecvError>();
        assert_error::<RecvTimeoutError>();

        assert_eq!(
            ChannelError::ZeroCapacity.to_string(),
            "capacity_per_sender must be > 0"
        );
        assert_eq!(TrySendError::Full(1).to_string(), "sender ring is full");
        assert_eq!(TryRecvError::Empty.to_string(), "channel is empty");
    }

    #[test]
    fn per_sender_fifo_relaxed_across_senders() {
        let (mut tx0, mut rx) = channel(8);
        let mut tx1 = tx0.try_clone().unwrap();

        for i in 0..4 {
            tx0.try_send((0, i)).unwrap();
            tx1.try_send((1, i)).unwrap();
        }

        let mut seen = [0, 0];
        for _ in 0..8 {
            let (sender, seq) = rx.try_recv().unwrap();
            assert_eq!(seq, seen[sender]);
            seen[sender] += 1;
        }
        assert_eq!(seen, [4, 4]);
    }

    #[test]
    fn sender_registration_has_no_fixed_limit() {
        let (tx0, mut rx) = channel::<usize>(4);
        let mut senders = Vec::new();
        for _ in 0..129 {
            senders.push(tx0.try_clone().expect("dynamic sender lane"));
        }

        for (i, tx) in senders.iter_mut().enumerate() {
            tx.try_send(i).unwrap();
        }

        let mut seen = vec![false; senders.len()];
        for _ in 0..senders.len() {
            seen[rx.try_recv().unwrap()] = true;
        }
        assert!(seen.into_iter().all(|value| value));
    }

    #[test]
    fn full_is_per_sender() {
        let (mut tx0, mut rx) = channel(2);
        let mut tx1 = tx0.try_clone().unwrap();

        tx0.try_send(1).unwrap();
        tx0.try_send(2).unwrap();
        assert_eq!(tx0.try_send(3), Err(TrySendError::Full(3)));

        tx1.try_send(10).unwrap();

        let mut values = [
            rx.try_recv().unwrap(),
            rx.try_recv().unwrap(),
            rx.try_recv().unwrap(),
        ];
        values.sort();
        assert_eq!(values, [1, 2, 10]);
    }

    #[test]
    fn drained_sender_lane_is_reused() {
        let (tx0, mut rx) = channel::<u8>(2);
        let old_slot = {
            let tx1 = tx0.try_clone().unwrap();
            tx1.shard()
        };

        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let mut tx2 = tx0.try_clone().unwrap();
        assert_eq!(tx2.shard(), old_slot);
        tx2.try_send(7).unwrap();
        assert_eq!(rx.try_recv(), Ok(7));
    }

    #[test]
    fn receive_releases_capacity_when_prefetched_batch_drains() {
        let (mut tx, mut rx) = channel(4);

        for i in 0..4 {
            tx.try_send(i).unwrap();
        }

        assert_eq!(rx.try_recv(), Ok(0));
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(3));
        tx.try_send(4).unwrap();
        assert_eq!(rx.try_recv(), Ok(4));
    }

    #[test]
    fn receive_releases_capacity_at_local_batch_limit() {
        let (mut tx, mut rx) = channel(128);

        for i in 0..128 {
            tx.try_send(i).unwrap();
        }

        for i in 0..63 {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert_eq!(tx.try_send(128), Err(TrySendError::Full(128)));

        assert_eq!(rx.try_recv(), Ok(63));
        tx.try_send(128).unwrap();

        for i in 64..128 {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert_eq!(rx.try_recv(), Ok(128));
    }

    #[test]
    fn empty_lane_releases_partial_credit_batch() {
        let (mut tx, mut rx) = channel(8);
        tx.try_send(0).unwrap();
        assert_eq!(rx.try_recv(), Ok(0));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        for value in 1..=8 {
            tx.try_send(value).unwrap();
        }
    }

    #[test]
    fn disconnects_after_draining() {
        let (mut tx, mut rx) = channel(4);
        tx.try_send(7).unwrap();
        drop(tx);

        assert_eq!(rx.try_recv(), Ok(7));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn send_fails_after_receiver_drop() {
        let (mut tx, rx) = channel(4);
        drop(rx);

        assert_eq!(tx.try_send(7), Err(TrySendError::Disconnected(7)));
    }

    #[test]
    fn cross_thread_many_senders() {
        let threads = 4;
        let per_thread = 1_000;
        let (tx0, mut rx) = channel(64);
        let mut senders = vec![tx0];
        for _ in 1..threads {
            let tx = senders[0].try_clone().unwrap();
            senders.push(tx);
        }

        std::thread::scope(|scope| {
            for (sender_id, mut tx) in senders.into_iter().enumerate() {
                scope.spawn(move || {
                    for seq in 0..per_thread {
                        let mut value = (sender_id, seq);
                        loop {
                            match tx.try_send(value) {
                                Ok(()) => break,
                                Err(TrySendError::Full(returned)) => {
                                    value = returned;
                                    std::thread::yield_now();
                                }
                                Err(TrySendError::Disconnected(_)) => panic!("receiver dropped"),
                            }
                        }
                    }
                });
            }

            let mut seen = vec![0usize; threads];
            let mut received = 0;
            while received < threads * per_thread {
                match rx.try_recv() {
                    Ok((sender_id, seq)) => {
                        assert_eq!(seq, seen[sender_id]);
                        seen[sender_id] += 1;
                        received += 1;
                    }
                    Err(TryRecvError::Empty) => std::thread::yield_now(),
                    Err(TryRecvError::Disconnected) => panic!("senders disconnected early"),
                }
            }
            assert_eq!(seen, vec![per_thread; threads]);
        });
    }
}
