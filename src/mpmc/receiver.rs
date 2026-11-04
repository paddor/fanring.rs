use crate::teardown::{Deferred, Teardown};

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

use crate::blocking::{self, Attempt, Blocking, Recheck};
use crate::compat::{Arc, Ordering};
use crate::config::{SpinWait, WaitStrategy};

use super::{
    PREFETCH_LIMIT, ReadyTopology, RecvError, RecvTimeoutError, Shared, TRY_RECV_RETRIES,
    TryRecvError,
};

mod drain;
mod iter;
mod lane;

use drain::{Effects, LaneDrain, Wake};
pub use iter::{IntoIter, Iter, TryIter};
pub(super) use lane::{FinishLane, Lane, LaneToken, close_lane};

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
    pub(super) wait_strategy: WaitStrategy,
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
            wait_strategy: self.wait_strategy,
        }
    }
}

impl<T, P: Teardown> Receiver<T, P> {
    /// Set the policy used by synchronous blocking receives before parking.
    ///
    /// Newly cloned receivers inherit this receiver's current policy.
    pub fn set_wait_strategy(&mut self, strategy: WaitStrategy) {
        self.wait_strategy = strategy;
    }

    /// Return this receiver's synchronous blocking wait policy.
    #[must_use]
    pub const fn wait_strategy(&self) -> WaitStrategy {
        self.wait_strategy
    }

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
        // Staged work needs no lane maintenance and has no effects.
        if let Some(value) = self.pop_staged() {
            return Ok(value);
        }
        self.try_recv_lanes()
    }

    fn try_recv_lanes(&mut self) -> Result<T, TryRecvError> {
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
        let mut progressed = false;
        for attempt in 0..=TRY_RECV_RETRIES {
            let work_generation = match self.pop_work() {
                WorkPop::Item(value) => return (Ok(value), Effects::default()),
                WorkPop::Empty { generation } => generation,
            };
            let Some((lane, publication)) = self.acquire_lane() else {
                match self.empty_scan_state(work_generation) {
                    EmptyScan::Changed if attempt != TRY_RECV_RETRIES => continue,
                    // Work moved during this scan, possibly by this receiver.
                    EmptyScan::Changed => return (Err(TryRecvError::Empty), Effects::progressed()),
                    EmptyScan::Publishing | EmptyScan::Open => {
                        return (Err(TryRecvError::Empty), Effects::progressed_if(progressed));
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
                    progressed |= effects.progressed;
                }
            }
        }
        (Err(TryRecvError::Empty), Effects::progressed_if(progressed))
    }

    /// Pops work this receiver already moved out of sender lanes.
    #[inline]
    fn pop_staged(&mut self) -> Option<T> {
        // Clone publishes all private work before adding a receiver. After a
        // 2-to-1 transition, private is therefore empty and local/orphaned work
        // is observed before this receiver can stage another private batch.
        if let Some(value) = self.private.get_mut().pop_front() {
            return Some(value);
        }
        self.local.pop()
    }

    fn pop_work(&mut self) -> WorkPop<T> {
        if let Some(value) = self.pop_staged() {
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
        let spin = SpinWait::blocking(self.wait_strategy);
        blocking::block(self, (), spin)
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
        let spin = SpinWait::deadline(self.wait_strategy, deadline);
        blocking::recv_timeout_result(blocking::block_until(self, (), spin, deadline))
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
}

impl<T, P: Teardown> Blocking<()> for Receiver<T, P> {
    type Output = Result<T, RecvError>;
    type Deferred = Effects;
    type Waiter = Arc<Shared<T, P>>;

    fn waiter(&self) -> Arc<Shared<T, P>> {
        self.shared.clone()
    }

    #[inline]
    fn attempt(&mut self, (): ()) -> Attempt<(), Result<T, RecvError>> {
        blocking::recv_attempt(self.try_recv(), ())
    }

    fn recheck(&mut self, (): ()) -> Recheck<(), Result<T, RecvError>, Effects> {
        let (result, effects) = self.try_recv_inner();
        match result {
            Ok(value) => Recheck::Done(Ok(value), effects),
            Err(TryRecvError::Disconnected) => Recheck::Done(Err(RecvError), Effects::default()),
            Err(TryRecvError::Empty) if effects.must_retry() => Recheck::Progressed((), effects),
            Err(TryRecvError::Empty) => Recheck::Pending(()),
        }
    }

    fn notify(&mut self, effects: Effects) {
        self.apply_effects(effects);
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
            .field("wait_strategy", &self.wait_strategy)
            .finish_non_exhaustive()
    }
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

#[cfg(test)]
mod tests;
