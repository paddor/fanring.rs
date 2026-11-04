use crate::teardown::{Deferred, Teardown};

use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::time::{Duration, Instant};

use crate::blocking::{self, Attempt, Blocking, Recheck};
use crate::compat::{Arc, Ordering, lock};
use crate::config::{SpinWait, WaitStrategy};
use crate::ready::{ReadyGroup, ReadyPage};

use super::{
    LaneKey, PREFETCH_LIMIT, READY_POLL_INTERVAL, RecvError, RecvTimeoutError, Shared, TryRecvError,
};

mod batch;
mod iter;
mod lane;
mod lane_control;
mod registry;

use batch::Admit;
#[cfg(feature = "async")]
pub(super) use batch::AdmitAll;
pub use iter::{IntoIter, Iter, TryIter};
pub(super) use lane::Lane;
use lane::{EmptyLane, LanePoll};
pub(super) use lane_control::ItemOnly;
pub use lane_control::LaneReceiver;
pub(crate) use lane_control::ReceiveOutput;
#[cfg(feature = "async")]
pub(super) use lane_control::WithLaneIds;

/// Receiving half.
///
/// The receiver owns every SPSC consumer and drains active lanes in bounded
/// round-robin bursts. It is `Send` when `T` is `Send`, but it is not `Sync`.
///
/// Single-value receives batch slot release, so receiving a value does not
/// necessarily make its slot immediately reusable by the sender. Call
/// [`release_consumed`](Self::release_consumed) or use
/// [`recv_batch_into`](Self::recv_batch_into) to publish freed slots before
/// processing received values.
///
/// Dropping the last receiver disconnects senders. Unread payload destruction
/// follows `P`: [`Deferred`] may retain ring values until their sender drops;
/// [`Coordinated`](crate::teardown::Coordinated) reclaims them during teardown
/// or when an overlapping send resumes.
pub struct Receiver<T, P: Teardown = Deferred> {
    pub(super) shared: Arc<Shared<T, P>>,
    pub(super) lanes: Vec<Option<Lane<T, P>>>,
    pub(super) groups: Vec<Arc<ReadyGroup>>,
    pub(super) pages: Vec<Arc<ReadyPage>>,
    /// Lanes in receive rotation. Each lane appears at most once; its
    /// `queued` flag mirrors membership.
    pub(super) active: VecDeque<LaneKey>,
    /// Receive control stays separate from the lane's cached hot-path data.
    /// Registration resets the slot before exposing a replacement lane.
    pub(super) paused: Vec<bool>,
    pub(super) ready_group_cursor: usize,
    pub(super) seen_registry_generation: usize,
    pub(super) items_until_ready_poll: usize,
    pub(super) capacity_per_sender: usize,
    pub(super) wait_strategy: WaitStrategy,
}

impl<T, P: Teardown> Receiver<T, P> {
    /// Set the policy used by synchronous blocking receives before parking.
    ///
    /// This does not affect asynchronous operations.
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
    /// Consumed slots are released in batches. Use
    /// [`release_consumed`](Self::release_consumed) to release them immediately.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is currently available, or
    /// [`TryRecvError::Disconnected`] after all senders and buffered values are
    /// gone.
    #[inline(always)]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.try_recv_output::<ItemOnly>()
    }

    #[inline]
    pub(super) fn try_recv_output<O: ReceiveOutput<T, P>>(
        &mut self,
    ) -> Result<O::Item, TryRecvError> {
        // Keep the front lane in place between release, rotation, and readiness
        // boundaries. Refresh short prefetch windows here as well: they do not
        // require lane scheduling or a wakeup.
        if self.items_until_ready_poll != 0
            && let Some(&key) = self.active.front()
            && let Some(lane) = self.lanes.get_mut(key.slot).and_then(Option::as_mut)
            && lane.key == key
        {
            if lane.cached_available == 0 {
                lane.cached_available = lane.consumer.prefetch();
            }
            if lane.cached_available != 0
                && lane.unreleased + 1 < lane.release_batch
                && lane.burst + 1 < PREFETCH_LIMIT
            {
                // Finish bookkeeping before loading T, so the payload does not
                // pass through the maintenance path's intermediate results.
                lane.cached_available -= 1;
                lane.unreleased += 1;
                lane.burst += 1;
                self.items_until_ready_poll -= 1;
                let value = lane
                    .consumer
                    .pop()
                    .expect("cached_available guarantees prefetched data");
                return Ok(O::item(self, key, value));
            }
        }
        loop {
            let (result, released) = self.try_recv_inner::<false, O>();
            let retry = released.is_some() && matches!(result, Err(TryRecvError::Empty));
            self.notify_released(released);
            if !retry {
                return result;
            }
        }
    }

    /// Try to receive one value, rotating to the next ready sender afterward.
    ///
    /// Unlike [`try_recv`](Self::try_recv), this never continues a per-sender
    /// burst. Newly ready senders are collected before each call and join the
    /// back of the active queue. FIFO within each sender is preserved, including
    /// when alternating this operation with ordinary batched scheduling.
    ///
    /// Slot release is still batched. Call [`release_consumed`](Self::release_consumed)
    /// before processing values if senders must immediately reuse their slots.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is ready, or
    /// [`TryRecvError::Disconnected`] after all senders and buffered values are gone.
    #[inline(always)]
    pub fn try_recv_fair(&mut self) -> Result<T, TryRecvError> {
        self.try_recv_fair_output::<ItemOnly>()
    }

    #[inline]
    pub(super) fn try_recv_fair_output<O: ReceiveOutput<T, P>>(
        &mut self,
    ) -> Result<O::Item, TryRecvError> {
        if self.shared.registry_generation.load(Ordering::Acquire) != self.seen_registry_generation
            || self.groups.iter().any(|group| group.has_ready())
        {
            self.collect_ready(true);
        }
        self.items_until_ready_poll = READY_POLL_INTERVAL;
        // A single active lane has nothing to rotate past. Keep its cached
        // window until slot release is due, after checking for new ready lanes.
        if self.active.len() == 1
            && let Some(&key) = self.active.front()
            && let Some(lane) = self.lanes.get_mut(key.slot).and_then(Option::as_mut)
            && lane.key == key
        {
            if lane.cached_available == 0 {
                lane.cached_available = lane.consumer.prefetch();
            }
            if lane.cached_available != 0 && lane.unreleased + 1 < lane.release_batch {
                lane.cached_available -= 1;
                lane.unreleased += 1;
                lane.burst = 0;
                self.items_until_ready_poll -= 1;
                let value = lane
                    .consumer
                    .pop()
                    .expect("cached_available guarantees prefetched data");
                return Ok(O::item(self, key, value));
            }
        }
        loop {
            let (result, released) = self.try_recv_inner::<true, O>();
            let retry = released.is_some() && matches!(result, Err(TryRecvError::Empty));
            self.notify_released(released);
            if !retry {
                return result;
            }
        }
    }

    // Fold the intermediate result into each caller's return buffer.
    #[inline(always)]
    fn try_recv_inner<const FAIR: bool, O: ReceiveOutput<T, P>>(
        &mut self,
    ) -> (Result<O::Item, TryRecvError>, Option<LaneKey>) {
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
            match self.poll_lane::<FAIR>(key) {
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
                    return (Ok(O::item(self, key, value)), released.then_some(key));
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
    /// Consumed slots are released in batches. Use
    /// [`release_consumed`](Self::release_consumed) to release them immediately.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] after all senders and buffered values are gone.
    #[inline(always)]
    pub fn recv(&mut self) -> Result<T, RecvError> {
        self.recv_output::<ItemOnly>()
    }

    #[inline]
    pub(super) fn recv_output<O: ReceiveOutput<T, P>>(&mut self) -> Result<O::Item, RecvError> {
        // Convert directly to the blocking result type. Going through try_recv
        // adds another payload-sized return on this path.
        loop {
            let (result, released) = self.try_recv_inner::<false, O>();
            let retry = released.is_some() && matches!(result, Err(TryRecvError::Empty));
            self.notify_released(released);
            match result {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) if retry => {}
                Err(TryRecvError::Empty) => return self.recv_slow::<O>(),
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn recv_slow<O: ReceiveOutput<T, P>>(&mut self) -> Result<O::Item, RecvError> {
        let spin = SpinWait::blocking(self.wait_strategy);
        blocking::block(self, PhantomData::<O>, spin)
    }

    /// Receive one value, blocking for at most `timeout` while the channel is
    /// empty.
    ///
    /// # Errors
    ///
    /// Returns [`RecvTimeoutError::Timeout`] when the timeout expires, or
    /// [`RecvTimeoutError::Disconnected`] after the channel disconnects.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.recv_timeout_output::<ItemOnly>(timeout)
    }

    fn recv_timeout_output<O: ReceiveOutput<T, P>>(
        &mut self,
        timeout: Duration,
    ) -> Result<O::Item, RecvTimeoutError> {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return self
                .recv_output::<O>()
                .map_err(|RecvError| RecvTimeoutError::Disconnected);
        };
        self.recv_deadline_output::<O>(deadline)
    }

    /// Receive one value, blocking until `deadline` while the channel is
    /// empty.
    ///
    /// # Errors
    ///
    /// Returns [`RecvTimeoutError::Timeout`] at the deadline, or
    /// [`RecvTimeoutError::Disconnected`] after the channel disconnects.
    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        self.recv_deadline_output::<ItemOnly>(deadline)
    }

    fn recv_deadline_output<O: ReceiveOutput<T, P>>(
        &mut self,
        deadline: Instant,
    ) -> Result<O::Item, RecvTimeoutError> {
        let spin = SpinWait::deadline(self.wait_strategy, deadline);
        blocking::recv_timeout_result(blocking::block_until(
            self,
            PhantomData::<O>,
            spin,
            deadline,
        ))
    }

    // Keep LanePoll in the caller. Extra yring branches can otherwise exceed
    // the compiler's inline budget and add a call/return to every blocking recv.
    #[inline(always)]
    fn poll_lane<const FAIR: bool>(&mut self, key: LaneKey) -> LanePoll<T> {
        let Some(lane) = self.lanes.get_mut(key.slot).and_then(Option::as_mut) else {
            return LanePoll::Stale;
        };
        if lane.key != key {
            return LanePoll::Stale;
        }

        if let Some(empty) = lane.refill() {
            return match empty {
                EmptyLane::Keep { released } => LanePoll::Keep { released },
                EmptyLane::Idle {
                    disconnected,
                    released,
                } => LanePoll::Idle {
                    disconnected,
                    released,
                },
            };
        }

        let value = lane
            .consumer
            .pop()
            .expect("cached_available guarantees prefetched data");
        lane.cached_available -= 1;
        lane.unreleased += 1;
        lane.burst += 1;
        let released = lane.unreleased == lane.release_batch && lane.release_pending();

        let rotate = FAIR || lane.burst == PREFETCH_LIMIT;
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

    /// Return whether all senders have been dropped.
    #[inline]
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.shared.live_senders.load(Ordering::Acquire) == 0
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
}

/// Blocking receives. `PhantomData<O>` selects the output view.
impl<T, P: Teardown, O: ReceiveOutput<T, P>> Blocking<PhantomData<O>> for Receiver<T, P> {
    type Output = Result<O::Item, RecvError>;
    type Deferred = Option<LaneKey>;
    type Waiter = Arc<Shared<T, P>>;

    fn waiter(&self) -> Arc<Shared<T, P>> {
        self.shared.clone()
    }

    #[inline]
    fn attempt(&mut self, view: PhantomData<O>) -> Attempt<PhantomData<O>, Self::Output> {
        blocking::recv_attempt(self.try_recv_output::<O>(), view)
    }

    fn recheck(
        &mut self,
        view: PhantomData<O>,
    ) -> Recheck<PhantomData<O>, Self::Output, Option<LaneKey>> {
        let (result, released) = self.try_recv_inner::<false, O>();
        match result {
            Ok(value) => Recheck::Done(Ok(value), released),
            Err(TryRecvError::Disconnected) => Recheck::Done(Err(RecvError), None),
            Err(TryRecvError::Empty) if released.is_some() => Recheck::Progressed(view, released),
            Err(TryRecvError::Empty) => Recheck::Pending(view),
        }
    }

    fn notify(&mut self, released: Option<LaneKey>) {
        self.notify_released(released);
    }
}

impl<T, P: Teardown> Drop for Receiver<T, P> {
    fn drop(&mut self) {
        #[cfg(feature = "async")]
        self.cancel_recv_wait();
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

impl<T, P: Teardown> fmt::Debug for Receiver<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("active_lanes", &self.active.len())
            .field(
                "registered_lanes",
                &self.shared.registered_lanes.load(Ordering::Relaxed),
            )
            .field("capacity_per_sender", &self.capacity_per_sender())
            .field("wait_strategy", &self.wait_strategy)
            .finish_non_exhaustive()
    }
}
