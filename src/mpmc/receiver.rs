use std::cell::{Cell, RefCell};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use crossbeam_deque::{Steal, Worker};

use crate::compat::{Arc, Ordering};
use crate::publication::Publication;
use crate::ready::{LaneSignal, PAGES_PER_GROUP};

use super::{
    PARK_SPINS, PREFETCH_LIMIT, ReadyTopology, RecvError, RecvTimeoutError, Shared,
    TRY_RECV_RETRIES, TryRecvError,
};

/// Receiving half.
///
/// Clones compete for messages. Each receiver has a local work queue whose
/// buffered items remain stealable by other receivers.
pub struct Receiver<T> {
    pub(super) shared: Arc<Shared<T>>,
    pub(super) id: usize,
    pub(super) local: Worker<T>,
    pub(super) steal_cursor: usize,
    pub(super) ready: RefCell<std::sync::Arc<ReadyTopology<T>>>,
    pub(super) seen_ready_generation: Cell<usize>,
    pub(super) ready_group_cursor: Cell<usize>,
    pub(super) ready_page_cursor: Cell<usize>,
    pub(super) direct_page_cursor: Cell<usize>,
    pub(super) prefer_work: Cell<bool>,
    pub(super) capacity_per_sender: usize,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let (id, local) = self.shared.register_receiver();
        let (ready, seen_ready_generation) = self.shared.ready_snapshot();
        Self {
            shared: self.shared.clone(),
            id,
            local,
            steal_cursor: 0,
            ready: RefCell::new(ready),
            seen_ready_generation: Cell::new(seen_ready_generation),
            ready_group_cursor: Cell::new(0),
            ready_page_cursor: Cell::new(0),
            direct_page_cursor: Cell::new(0),
            prefer_work: Cell::new(false),
            capacity_per_sender: self.capacity_per_sender,
        }
    }
}

impl<T> Receiver<T> {
    /// Try to receive one value.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is currently available, or
    /// while another receiver publishes a batch. Returns
    /// [`TryRecvError::Disconnected`] after all senders and buffered values are
    /// gone and no publication is in flight.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        for attempt in 0..=TRY_RECV_RETRIES {
            let (result, effects) = self.try_recv_inner();
            let retry = effects.has_effect() && matches!(result, Err(TryRecvError::Empty));
            self.apply_effects(effects);
            if !retry || attempt == TRY_RECV_RETRIES {
                return result;
            }
        }
        unreachable!()
    }

    #[inline]
    fn try_recv_inner(&mut self) -> (Result<T, TryRecvError>, Effects) {
        for attempt in 0..=TRY_RECV_RETRIES {
            let (work_generation, work_contended) = match self.pop_work() {
                WorkPop::Item(value) => return (Ok(value), Effects::default()),
                WorkPop::Empty {
                    generation,
                    contended,
                } => (generation, contended),
            };

            let Some((lane, publication)) = self.acquire_lane() else {
                if work_contended {
                    if attempt != TRY_RECV_RETRIES {
                        std::hint::spin_loop();
                        continue;
                    }
                    return (Err(TryRecvError::Empty), Effects::default());
                }
                match self.empty_scan_state(work_generation) {
                    EmptyScan::Changed if attempt != TRY_RECV_RETRIES => continue,
                    EmptyScan::Changed | EmptyScan::Publishing | EmptyScan::Open => {
                        return (Err(TryRecvError::Empty), Effects::default());
                    }
                    EmptyScan::Drained => {
                        return (Err(TryRecvError::Disconnected), Effects::default());
                    }
                }
            };
            let drained = self.drain_lane(lane);
            drop(publication);
            match drained {
                LaneDrain::Item { value, effects } => return (Ok(value), effects),
                LaneDrain::Empty(effects) => {
                    if effects.has_effect() {
                        return (Err(TryRecvError::Empty), effects);
                    }
                }
            }
        }
        (Err(TryRecvError::Empty), Effects::default())
    }

    #[inline]
    fn empty_scan_state(&self, generation: usize) -> EmptyScan {
        if self.shared.publications.changed_since(generation) {
            return EmptyScan::Changed;
        }
        if self.shared.publications.is_in_flight() {
            return EmptyScan::Publishing;
        }
        if !self.is_drained() {
            return EmptyScan::Open;
        }
        if self.shared.publications.is_in_flight() {
            return EmptyScan::Publishing;
        }
        if self.shared.publications.changed_since(generation) {
            return EmptyScan::Changed;
        }
        EmptyScan::Drained
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
            return WorkPop::Item(value);
        }

        let generation = self.shared.publications.snapshot();
        let mut contended = false;
        match self.shared.injector.steal_batch_and_pop(&self.local) {
            Steal::Success(value) => return WorkPop::Item(value),
            Steal::Retry => contended = true,
            Steal::Empty => {}
        }

        let stealers = self.shared.stealers.load();
        let len = stealers.len();
        for offset in 0..len {
            let index = (self.steal_cursor + offset) % len;
            let (id, stealer) = &stealers[index];
            if *id == self.id {
                continue;
            }
            match stealer.steal_batch_and_pop(&self.local) {
                Steal::Success(value) => {
                    self.steal_cursor = index.wrapping_add(1);
                    return WorkPop::Item(value);
                }
                Steal::Retry => contended = true,
                Steal::Empty => {}
            }
        }
        if len != 0 {
            self.steal_cursor = self.steal_cursor.wrapping_add(1) % len;
        }
        WorkPop::Empty {
            generation,
            contended,
        }
    }

    fn acquire_lane(&self) -> Option<(LaneToken<T>, Publication<'_>)> {
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

    fn acquire_work_lane<'a>(
        &'a self,
        ready: &ReadyTopology<T>,
    ) -> Option<(LaneToken<T>, Publication<'a>)> {
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
            if let Some(lane) = ready
                .pages
                .get(page_id)
                .and_then(|page| page.pop_after_claim())
            {
                return Some((lane, publication));
            }
            drop(publication);
        }
        None
    }

    fn acquire_ready_lane<'a>(
        &'a self,
        ready: &ReadyTopology<T>,
    ) -> Option<(LaneToken<T>, Publication<'a>)> {
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

    fn drain_lane(&self, mut lane: LaneToken<T>) -> LaneDrain<T> {
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
        let page_id = lane.key.slot / super::LANES_PER_PAGE;
        self.ready.borrow().pages[page_id].push(lane);
    }

    fn refresh_ready_topology(&self) {
        let generation = self.shared.ready_generation.load(Ordering::Acquire);
        if generation == self.seen_ready_generation.get() {
            return;
        }
        let (ready, generation) = self.shared.ready_snapshot();
        self.ready.replace(ready);
        self.seen_ready_generation.set(generation);
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
        self.shared.live_receivers.load(Ordering::Relaxed)
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

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let publication = self.shared.publications.begin();
        let mut published = false;
        while let Some(value) = self.local.pop() {
            self.shared.injector.push(value);
            published = true;
        }
        self.shared.unregister_receiver(self.id);
        drop(publication);

        let previous = self.shared.live_receivers.fetch_sub(1, Ordering::AcqRel);
        if previous == 1 {
            self.shared.close_all_receivers();
        } else if published || self.is_disconnected() {
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
                "publications_in_flight",
                &self.shared.publications.is_in_flight(),
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

pub(super) struct Lane<T> {
    pub(super) key: super::LaneKey,
    pub(super) signal: Arc<LaneSignal>,
    pub(super) consumer: yring::Consumer<T>,
    pub(super) cached_available: usize,
    pub(super) unreleased: usize,
    pub(super) release_batch: usize,
}

pub(super) struct LaneToken<T>(Box<Lane<T>>);

impl<T> LaneToken<T> {
    pub(super) fn new(lane: Lane<T>) -> Self {
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
    pub(super) fn new(
        key: super::LaneKey,
        signal: Arc<LaneSignal>,
        consumer: yring::Consumer<T>,
    ) -> Self {
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

pub(super) enum FinishLane<T> {
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
    Empty { generation: usize, contended: bool },
}

enum EmptyScan {
    Changed,
    Publishing,
    Open,
    Drained,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Wake {
    #[default]
    None,
    One,
    All,
}

impl Wake {
    const fn merge(self, other: Self) -> Self {
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

pub(super) fn close_lane<T>(mut lane: LaneToken<T>) {
    lane.consumer.close();
    lane.signal.notify_space();
}
