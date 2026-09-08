use crate::teardown::{Deferred, Teardown};

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use crate::compat::{Arc, Ordering};
use crate::publication::Publication;
use crate::ready::{LaneSignal, PAGES_PER_GROUP};

use super::{
    PARK_SPINS, PREFETCH_LIMIT, ReadyTopology, RecvError, RecvTimeoutError, Shared,
    TRY_RECV_RETRIES, TryRecvError,
};

/// Receiving half.
///
/// Clones compete for messages through bounded stealable work queues.
///
/// Dropping the last receiver disconnects senders. Unread payload destruction
/// follows `P`: [`Deferred`] may retain ring values until their sender drops;
/// [`Coordinated`](crate::teardown::Coordinated) reclaims them during teardown
/// or when an overlapping send resumes. Other MPMC receivers preserve access
/// to queued work until the last receiver drops.
pub struct Receiver<T, P: Teardown = Deferred> {
    pub(super) shared: Arc<Shared<T, P>>,
    pub(super) id: usize,
    pub(super) local: super::WorkQueue<T>,
    /// Unsynchronized staging used only while this is the sole receiver.
    pub(super) private: RefCell<VecDeque<T>>,
    pub(super) steal_cursor: usize,
    pub(super) ready: RefCell<std::sync::Arc<ReadyTopology<T, P>>>,
    pub(super) seen_ready_generation: Cell<usize>,
    pub(super) ready_group_cursor: Cell<usize>,
    pub(super) ready_page_cursor: Cell<usize>,
    pub(super) direct_page_cursor: Cell<usize>,
    pub(super) prefer_work: Cell<bool>,
    pub(super) capacity_per_sender: usize,
}

impl<T, P: Teardown> Clone for Receiver<T, P> {
    fn clone(&self) -> Self {
        // Make sole-receiver staging stealable before publishing another clone.
        let publication = self.shared.publications.begin();
        self.local.push_batch(self.private.borrow_mut().drain(..));
        let (id, local) = self.shared.register_receiver();
        drop(publication);
        let (ready, seen_ready_generation) = self.shared.ready_snapshot();
        Self {
            shared: self.shared.clone(),
            id,
            local,
            private: RefCell::new(VecDeque::with_capacity(PREFETCH_LIMIT)),
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

impl<T, P: Teardown> Receiver<T, P> {
    /// Try to receive one value.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is currently available, or
    /// while bounded lane maintenance or another receiver moves work. Returns
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
            let work_generation = match self.pop_work() {
                WorkPop::Item(value) => return (Ok(value), Effects::default()),
                WorkPop::Empty { generation } => generation,
            };
            let Some((lane, publication)) = self.acquire_lane() else {
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

    fn pop_work(&mut self) -> WorkPop<T> {
        // Clone publishes all private work before adding a receiver. After a
        // 2-to-1 transition, private is therefore empty and local/orphaned work
        // is observed before this receiver can stage another private batch.
        if let Some(value) = self.private.get_mut().pop_front() {
            return WorkPop::Item(value);
        }
        if let Some(value) = self.local.pop() {
            return WorkPop::Item(value);
        }

        let generation = self.shared.publications.snapshot();
        if let Some(value) = self.shared.orphaned_work.pop() {
            return WorkPop::Item(value);
        }
        if self.has_ready_lane() {
            return WorkPop::Empty { generation };
        }

        #[cfg(not(loom))]
        let work_queues = self.shared.work_queues.load();
        #[cfg(loom)]
        let work_queues = crate::compat::lock(&self.shared.work_queues);
        let len = work_queues.len();
        for offset in 0..len {
            let index = (self.steal_cursor + offset) % len;
            let (id, queue) = &work_queues[index];
            if *id == self.id || queue.len() == 0 {
                continue;
            }
            let publication = self.shared.publications.begin();
            let stolen = queue.steal_batch_into(&self.local);
            drop(publication);
            if let Some(value) = stolen {
                self.steal_cursor = index.wrapping_add(1);
                return WorkPop::Item(value);
            }
        }
        if len != 0 {
            self.steal_cursor = self.steal_cursor.wrapping_add(1) % len;
        }
        WorkPop::Empty { generation }
    }

    fn has_ready_lane(&self) -> bool {
        self.refresh_ready_topology();
        let ready = self.ready.borrow();
        ready.ready_groups.iter().any(|group| group.has_ready())
            || ready.work_groups.iter().any(|group| group.has_ready())
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

    fn acquire_lane(&self) -> Option<(LaneToken<T, P>, Publication<'_>)> {
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

    fn drain_lane(&self, mut lane: LaneToken<T, P>) -> LaneDrain<T> {
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
        // Only this receiver can create a second clone while the count is one.
        if self.shared.live_receivers.load(Ordering::Acquire) == 1 {
            self.private.borrow_mut().extend((1..batch).map(|_| {
                lane.consumer
                    .pop()
                    .expect("batch is bounded by cached availability")
            }));
        } else {
            self.local.push_batch((1..batch).map(|_| {
                lane.consumer
                    .pop()
                    .expect("batch is bounded by cached availability")
            }));
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

    fn requeue_lane(&self, lane: LaneToken<T, P>) {
        let page_id = lane.key.slot / super::LANES_PER_PAGE;
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
    pub const fn iter(&mut self) -> Iter<'_, T, P> {
        Iter { receiver: self }
    }

    /// Iterate over values immediately available without blocking.
    #[inline]
    #[must_use]
    pub const fn try_iter(&mut self) -> TryIter<'_, T, P> {
        TryIter { receiver: self }
    }
}

impl<T, P: Teardown> Drop for Receiver<T, P> {
    fn drop(&mut self) {
        let publication = self.shared.publications.begin();
        let private = self.private.get_mut();
        let mut published = !private.is_empty();
        self.shared.orphaned_work.push_batch(private.drain(..));
        published |= self.local.drain_into(&self.shared.orphaned_work);
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

impl<T, P: Teardown> fmt::Debug for Receiver<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("id", &self.id)
            .field(
                "local_items",
                &(self.private.borrow().len() + self.local.len()),
            )
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
pub struct Iter<'a, T, P: Teardown = Deferred> {
    receiver: &'a mut Receiver<T, P>,
}

impl<T, P: Teardown> Iterator for Iter<'_, T, P> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl<T, P: Teardown> std::iter::FusedIterator for Iter<'_, T, P> {}

/// Nonblocking iterator over a borrowed receiver.
#[derive(Debug)]
pub struct TryIter<'a, T, P: Teardown = Deferred> {
    receiver: &'a mut Receiver<T, P>,
}

impl<T, P: Teardown> Iterator for TryIter<'_, T, P> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.try_recv().ok()
    }
}

/// Blocking iterator that owns its receiver.
#[derive(Debug)]
pub struct IntoIter<T, P: Teardown = Deferred> {
    receiver: Receiver<T, P>,
}

impl<T, P: Teardown> Iterator for IntoIter<T, P> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

impl<T, P: Teardown> std::iter::FusedIterator for IntoIter<T, P> {}

#[allow(
    clippy::into_iter_without_iter,
    reason = "channel convention names the blocking iterator iter"
)]
impl<'a, T, P: Teardown> IntoIterator for &'a mut Receiver<T, P> {
    type Item = T;
    type IntoIter = Iter<'a, T, P>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T, P: Teardown> IntoIterator for Receiver<T, P> {
    type Item = T;
    type IntoIter = IntoIter<T, P>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IntoIter { receiver: self }
    }
}

pub(super) struct Lane<T, P: Teardown> {
    pub(super) key: super::LaneKey,
    pub(super) signal: Arc<LaneSignal>,
    pub(super) consumer: crate::ring::Consumer<T, P>,
    pub(super) cached_available: usize,
    pub(super) unreleased: usize,
    pub(super) release_batch: usize,
}

pub(super) struct LaneToken<T, P: Teardown>(Box<Lane<T, P>>);

impl<T, P: Teardown> LaneToken<T, P> {
    pub(super) fn new(lane: Lane<T, P>) -> Self {
        Self(Box::new(lane))
    }
}

impl<T, P: Teardown> Deref for LaneToken<T, P> {
    type Target = Lane<T, P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, P: Teardown> DerefMut for LaneToken<T, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T, P: Teardown> Lane<T, P> {
    pub(super) fn new(
        key: super::LaneKey,
        signal: Arc<LaneSignal>,
        consumer: crate::ring::Consumer<T, P>,
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

pub(super) enum FinishLane<T, P: Teardown> {
    Ready(LaneToken<T, P>),
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

pub(super) fn close_lane<T, P: Teardown>(mut lane: LaneToken<T, P>) {
    lane.consumer.close();
    lane.signal.notify_space();
}

#[cfg(test)]
mod tests {
    use super::{LaneDrain, PREFETCH_LIMIT, RecvError};
    use crate::mpmc::{LANES_PER_PAGE, channel_with_policy};
    use crate::teardown::{Coordinated, Deferred, Teardown};

    fn check_new_page_requeue<P: Teardown>() {
        let (tx, mut rx) = channel_with_policy::<_, P>(PREFETCH_LIMIT + 1);
        // Registration can happen after acquire_lane refreshes its snapshot
        // and before acquire_ready_lane claims a bit in an existing group.
        let ready = rx.ready.borrow().clone();
        assert_eq!(ready.pages.len(), 1);
        let mut senders = vec![tx];
        for _ in 0..LANES_PER_PAGE {
            senders.push(senders[0].try_clone().unwrap());
        }
        for value in 0..=PREFETCH_LIMIT {
            senders.last_mut().unwrap().try_send(value).unwrap();
        }

        let (lane, publication) = rx.acquire_ready_lane(&ready).unwrap();
        assert_eq!(lane.key.slot, LANES_PER_PAGE);
        // One value remains in the newly registered ring after the first
        // batch, forcing the normal drain path to requeue its lane token.
        let drained = rx.drain_lane(lane);
        drop(publication);
        let LaneDrain::Item { value, effects } = drained else {
            panic!("newly registered lane has a published batch");
        };
        rx.apply_effects(effects);
        assert_eq!(value, 0);
        for expected in 1..=PREFETCH_LIMIT {
            assert_eq!(rx.try_recv(), Ok(expected));
        }
        drop(senders);
        assert_eq!(rx.recv(), Err(RecvError));
    }

    #[test]
    fn requeue_new_page_after_acquiring_through_stale_topology() {
        let checks: [fn(); 2] = [
            check_new_page_requeue::<Deferred>,
            check_new_page_requeue::<Coordinated>,
        ];
        for check in checks {
            #[cfg(loom)]
            loom::model(check);
            #[cfg(not(loom))]
            check();
        }
    }
}
