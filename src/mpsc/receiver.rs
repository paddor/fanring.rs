use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::time::{Duration, Instant};

use crate::compat::{Arc, Ordering, lock};
use crate::ready::{LANES_PER_PAGE, LaneSignal, PAGES_PER_GROUP, ReadyGroup, ReadyPage};

use super::{
    LaneKey, PARK_SPINS, PREFETCH_LIMIT, READY_POLL_INTERVAL, RecvError, RecvTimeoutError, Shared,
    TryRecvError,
};

/// Receiving half.
///
/// The receiver owns every SPSC consumer and drains active lanes in bounded
/// round-robin bursts. It is `Send` when `T` is `Send`, but it is not `Sync`.
pub struct Receiver<T> {
    pub(super) shared: Arc<Shared<T>>,
    pub(super) lanes: Vec<Option<Lane<T>>>,
    pub(super) groups: Vec<Arc<ReadyGroup>>,
    pub(super) pages: Vec<Arc<ReadyPage>>,
    pub(super) active: VecDeque<LaneKey>,
    pub(super) ready_group_cursor: usize,
    pub(super) seen_registry_generation: usize,
    pub(super) items_until_ready_poll: usize,
    pub(super) capacity_per_sender: usize,
}

impl<T> Receiver<T> {
    /// Try to receive one value.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is currently available, or
    /// [`TryRecvError::Disconnected`] after all senders and buffered values are
    /// gone.
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
                        Err(if self.is_drained() {
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
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] after all senders and buffered values are gone.
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
    ///
    /// # Errors
    ///
    /// Returns [`RecvTimeoutError::Timeout`] when the timeout expires, or
    /// [`RecvTimeoutError::Disconnected`] after the channel disconnects.
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
    ///
    /// # Errors
    ///
    /// Returns [`RecvTimeoutError::Timeout`] at the deadline, or
    /// [`RecvTimeoutError::Disconnected`] after the channel disconnects.
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
        let group_count = self.groups.len();
        for offset in 0..group_count {
            let group_index = (self.ready_group_cursor + offset) % group_count;
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
                    let Some(lane) = self.lanes.get(slot).and_then(Option::as_ref) else {
                        continue;
                    };
                    if lane.signal.is_pending() {
                        self.active.push_back(lane.key);
                    }
                }
            }
            self.ready_group_cursor = (group_index + 1) % group_count;
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
            }
            debug_assert!(self.lanes[pending.key.slot].is_none());
            self.lanes[pending.key.slot] =
                Some(Lane::new(pending.key, pending.signal, pending.consumer));
        }
        if self.active.capacity() < self.lanes.len() {
            self.active
                .reserve(self.lanes.len().saturating_sub(self.active.len()));
        }
        self.seen_registry_generation = seen;
    }

    fn retire_lane(&mut self, key: LaneKey) {
        let Some(lane) = self.lanes.get_mut(key.slot).and_then(Option::take) else {
            return;
        };
        debug_assert_eq!(lane.key, key);
        self.shared.registered_lanes.fetch_sub(1, Ordering::AcqRel);
        let mut registry = lock(&self.shared.registry);
        registry.retire_lane(key);
        drop(registry);
        drop(lane);
    }

    /// Return whether all senders have been dropped.
    #[inline]
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.shared.live_senders.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn is_drained(&self) -> bool {
        self.is_disconnected() && self.shared.registered_lanes.load(Ordering::Acquire) == 0
    }

    /// Return per-sender capacity after `yring` rounding.
    #[inline]
    #[must_use]
    pub const fn capacity_per_sender(&self) -> usize {
        self.capacity_per_sender
    }

    /// Return a snapshot of the number of live senders.
    #[inline]
    #[must_use]
    pub fn sender_count(&self) -> usize {
        self.shared.live_senders.load(Ordering::Relaxed)
    }

    /// Return a snapshot of the number of live receivers.
    #[inline]
    #[must_use]
    pub fn receiver_count(&self) -> usize {
        usize::from(self.shared.receiver_alive.load(Ordering::Relaxed))
    }

    /// Return whether both receivers belong to the same channel.
    #[inline]
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    /// Iterate until every sender disconnects and buffered values are drained.
    #[inline]
    #[must_use]
    pub const fn iter(&mut self) -> Iter<'_, T> {
        Iter { receiver: self }
    }

    /// Iterate over values immediately available without blocking.
    #[inline]
    #[must_use]
    pub const fn try_iter(&mut self) -> TryIter<'_, T> {
        TryIter { receiver: self }
    }
}

pub(super) struct Lane<T> {
    key: LaneKey,
    signal: Arc<LaneSignal>,
    consumer: yring::Consumer<T>,
    cached_available: usize,
    unreleased: usize,
    release_batch: usize,
    burst: usize,
}

impl<T> Lane<T> {
    pub(super) fn new(key: LaneKey, signal: Arc<LaneSignal>, consumer: yring::Consumer<T>) -> Self {
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
            let mut registry = lock(&self.shared.registry);
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

#[allow(
    clippy::into_iter_without_iter,
    reason = "channel convention names the blocking iterator iter"
)]
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
