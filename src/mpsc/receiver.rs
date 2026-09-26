use crate::teardown::{Deferred, Teardown};

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::time::{Duration, Instant};

use crate::compat::{Arc, Ordering, lock};
use crate::config::{SpinWait, WaitStrategy};
use crate::ready::{LANES_PER_PAGE, LaneSignal, PAGES_PER_GROUP, ReadyGroup, ReadyPage};

use super::{
    LaneKey, MIN_RELEASE_BATCH, PARK_SPINS, PREFETCH_LIMIT, READY_POLL_INTERVAL, RecvError,
    RecvTimeoutError, Shared, TryRecvError,
};

/// Per-value admission for the bulk drain.
///
/// [`AdmitAll`] compiles to a plain window move. A predicate stops the drain
/// at the first value it rejects.
pub(super) trait Admit<T> {
    const ALWAYS: bool;
    fn admit(&mut self, value: &T) -> bool;
}

pub(super) struct AdmitAll;

impl<T> Admit<T> for AdmitAll {
    const ALWAYS: bool = true;

    #[inline]
    fn admit(&mut self, _value: &T) -> bool {
        true
    }
}

impl<T, F: FnMut(&T) -> bool> Admit<T> for F {
    const ALWAYS: bool = false;

    #[inline]
    fn admit(&mut self, value: &T) -> bool {
        self(value)
    }
}

/// Outcome of one bulk drain.
pub(super) struct Drained {
    /// Values appended to the output.
    pub(super) received: usize,
    /// The admission predicate rejected a published value, so the drain
    /// stopped with data still available.
    pub(super) rejected: bool,
}

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
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
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
                return Ok(lane
                    .consumer
                    .pop()
                    .expect("cached_available guarantees prefetched data"));
            }
        }
        loop {
            let (result, released) = self.try_recv_inner::<false>();
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
    #[inline]
    pub fn try_recv_fair(&mut self) -> Result<T, TryRecvError> {
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
                return Ok(lane
                    .consumer
                    .pop()
                    .expect("cached_available guarantees prefetched data"));
            }
        }
        loop {
            let (result, released) = self.try_recv_inner::<true>();
            let retry = released.is_some() && matches!(result, Err(TryRecvError::Empty));
            self.notify_released(released);
            if !retry {
                return result;
            }
        }
    }

    // Fold the intermediate result into each caller's return buffer.
    #[inline(always)]
    fn try_recv_inner<const FAIR: bool>(&mut self) -> (Result<T, TryRecvError>, Option<LaneKey>) {
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
    /// Consumed slots are released in batches. Use
    /// [`release_consumed`](Self::release_consumed) to release them immediately.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] after all senders and buffered values are gone.
    #[inline]
    pub fn recv(&mut self) -> Result<T, RecvError> {
        // Convert directly to the blocking result type. Going through try_recv
        // adds another payload-sized return on this path.
        loop {
            let (result, released) = self.try_recv_inner::<false>();
            let retry = released.is_some() && matches!(result, Err(TryRecvError::Empty));
            self.notify_released(released);
            match result {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) if retry => {}
                Err(TryRecvError::Empty) => return self.recv_slow(),
            }
        }
    }

    /// Publish all consumed slots and notify senders waiting for capacity.
    ///
    /// This releases slots consumed by earlier receives across all sender
    /// lanes, including partial batches. It does not receive more values or
    /// release unread slots. Calling it again without receiving more values
    /// has no effect.
    ///
    /// Call this before returning application permits or issuing completions
    /// that allow producers to send more work.
    ///
    /// # Example
    ///
    /// ```
    /// use fanring::mpsc::{channel, TrySendError};
    ///
    /// let (mut tx, mut rx) = channel(4);
    /// for value in 0..4 {
    ///     tx.try_send(value).unwrap();
    /// }
    /// assert_eq!(rx.recv(), Ok(0));
    /// assert_eq!(rx.recv(), Ok(1));
    /// assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));
    /// rx.release_consumed();
    /// assert_eq!(tx.try_send(4), Ok(()));
    /// assert_eq!(tx.try_send(5), Ok(()));
    /// ```
    pub fn release_consumed(&mut self) {
        for lane in self.lanes.iter_mut().flatten() {
            if lane.release_pending() {
                lane.signal.notify_space();
            }
        }
    }

    /// Append up to `limit` values to `output`, returning the number appended.
    ///
    /// Waits only for the first value. Values already published are moved in
    /// bulk: each step takes a whole prefetched window from the front sender
    /// lane, bounded by the same per-lane burst, slot-release, and
    /// readiness-poll limits as single-value receives, so lane rotation and
    /// FIFO order within each lane match repeated [`try_recv`](Self::try_recv)
    /// calls. Existing output values are preserved.
    ///
    /// Before returning, publishes all consumed slots and notifies senders
    /// waiting for capacity, including slots consumed by earlier receive calls.
    /// A zero limit receives nothing, releases consumed slots, and returns
    /// `Ok(0)` even if the channel is disconnected.
    ///
    /// The output vector grows as needed. Reserve space for `limit` additional
    /// values before calling to avoid reallocating it. Receiver topology may
    /// still allocate when new senders are registered.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] only when `limit` is nonzero and all senders and
    /// buffered values are gone before receiving the first value. A partial
    /// batch returns `Ok(count)` even if the channel disconnects.
    ///
    /// # Example
    ///
    /// ```
    /// use fanring::mpsc::channel;
    ///
    /// let (mut tx, mut rx) = channel(4);
    /// for value in 0..4 {
    ///     tx.try_send(value).unwrap();
    /// }
    /// let mut batch = Vec::with_capacity(2);
    /// assert_eq!(rx.recv_batch_into(&mut batch, 2), Ok(2));
    /// assert_eq!(batch, [0, 1]);
    /// assert_eq!(tx.try_send(4), Ok(()));
    /// assert_eq!(tx.try_send(5), Ok(()));
    /// ```
    pub fn recv_batch_into(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
    ) -> Result<usize, RecvError> {
        let result = if limit == 0 {
            Ok(0)
        } else {
            match self.drain_into(output, limit, &mut AdmitAll).received {
                0 => self.recv().map(|first| {
                    output.push(first);
                    1 + self.drain_into(output, limit - 1, &mut AdmitAll).received
                }),
                received => Ok(received),
            }
        };
        self.release_consumed();
        result
    }

    /// Append up to `limit` immediately available values to `output`.
    ///
    /// This is the nonblocking form of [`recv_batch_into`](Self::recv_batch_into)
    /// and moves values with the same bulk steps, lane rotation, and per-lane
    /// FIFO order. Before returning, it publishes all consumed slots and
    /// notifies senders waiting for capacity, including slots consumed by
    /// earlier receive calls. A zero limit receives nothing, releases consumed
    /// slots, and returns `Ok(0)` even if the channel is disconnected.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when `limit` is nonzero and no value is
    /// currently available, or [`TryRecvError::Disconnected`] when `limit` is
    /// nonzero and all senders and buffered values are gone. A partial batch
    /// returns `Ok(count)` even if the channel disconnects.
    ///
    /// # Example
    ///
    /// ```
    /// use fanring::mpsc::{TryRecvError, channel};
    ///
    /// let (mut tx, mut rx) = channel(4);
    /// let mut batch = Vec::with_capacity(4);
    /// assert_eq!(rx.try_recv_batch_into(&mut batch, 4), Err(TryRecvError::Empty));
    /// for value in 0..3 {
    ///     tx.try_send(value).unwrap();
    /// }
    /// assert_eq!(rx.try_recv_batch_into(&mut batch, 2), Ok(2));
    /// assert_eq!(rx.try_recv_batch_into(&mut batch, 2), Ok(1));
    /// assert_eq!(batch, [0, 1, 2]);
    /// drop(tx);
    /// assert_eq!(
    ///     rx.try_recv_batch_into(&mut batch, 1),
    ///     Err(TryRecvError::Disconnected)
    /// );
    /// ```
    pub fn try_recv_batch_into(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
    ) -> Result<usize, TryRecvError> {
        let received = self.drain_into(output, limit, &mut AdmitAll).received;
        self.release_consumed();
        if received == 0 && limit != 0 {
            return Err(if self.is_drained() {
                TryRecvError::Disconnected
            } else {
                TryRecvError::Empty
            });
        }
        Ok(received)
    }

    /// Append immediately available values to `output` while `admit` accepts
    /// them.
    ///
    /// Works like [`try_recv_batch_into`](Self::try_recv_batch_into) with one
    /// difference: `admit` sees each value in place before it moves, in the
    /// order the values are appended, and the drain stops at the first value
    /// it rejects. That value stays at the front of its sender lane for a
    /// later receive, and the lane keeps its turn. This bounds a batch by a
    /// caller-defined measure, such as a byte budget, while each accepted
    /// window still moves in bulk. Before returning, publishes all consumed
    /// slots and notifies senders waiting for capacity.
    ///
    /// `admit` may see the same value more than once: a rejected value is
    /// offered again on the next call, and if `admit` panics, the values it
    /// saw in that window stay queued and are offered again. Keep side effects
    /// in `admit` limited to accounting for the values it accepts.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] or [`TryRecvError::Disconnected`] under
    /// the same conditions as `try_recv_batch_into`, only when no value was
    /// available. When a value is available but `admit` rejects it before
    /// anything was appended, returns `Ok(0)`.
    ///
    /// # Example
    ///
    /// ```
    /// use fanring::mpsc::{TryRecvError, channel};
    ///
    /// let (mut tx, mut rx) = channel(8);
    /// for value in [10usize, 20, 30, 40] {
    ///     tx.try_send(value).unwrap();
    /// }
    /// let mut batch = Vec::with_capacity(4);
    /// let mut budget = 50usize;
    /// let admitted = rx.try_recv_batch_into_while(&mut batch, 4, |value| {
    ///     if *value > budget {
    ///         return false;
    ///     }
    ///     budget -= *value;
    ///     true
    /// });
    /// assert_eq!(admitted, Ok(2));
    /// assert_eq!(batch, [10, 20]);
    /// assert_eq!(rx.try_recv_batch_into_while(&mut batch, 4, |_| false), Ok(0));
    /// assert_eq!(rx.try_recv_batch_into(&mut batch, 4), Ok(2));
    /// assert_eq!(batch, [10, 20, 30, 40]);
    /// assert_eq!(
    ///     rx.try_recv_batch_into_while(&mut batch, 1, |_| true),
    ///     Err(TryRecvError::Empty)
    /// );
    /// ```
    pub fn try_recv_batch_into_while(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
        mut admit: impl FnMut(&T) -> bool,
    ) -> Result<usize, TryRecvError> {
        let drained = self.drain_into(output, limit, &mut admit);
        self.release_consumed();
        if drained.received == 0 && limit != 0 && !drained.rejected {
            return Err(if self.is_drained() {
                TryRecvError::Disconnected
            } else {
                TryRecvError::Empty
            });
        }
        Ok(drained.received)
    }

    /// Scan every registered lane and append available values to `output`
    /// while `admit` accepts them.
    ///
    /// Works like [`try_recv_batch_into_while`](Self::try_recv_batch_into_while),
    /// but it does not rely on lane readiness. Every registered lane joins the
    /// rotation first, so values sent with
    /// [`Sender::try_send_unsignaled`](super::Sender::try_send_unsignaled) are
    /// found as well. A lane leaves the rotation only after this call observes
    /// it empty, also when a signaled send on that lane races the scan. So
    /// when fewer than `limit` values are appended and `admit` rejected none,
    /// every lane registered when the call started was observed empty during
    /// this call. Lanes the rotation already held keep their order, and each
    /// lane is queued at most once. The cost is proportional to the number of
    /// registered lanes.
    ///
    /// # Errors
    ///
    /// Same as `try_recv_batch_into_while`.
    ///
    /// # Example
    ///
    /// ```
    /// use fanring::mpsc::{TryRecvError, channel};
    ///
    /// let (mut tx0, mut rx) = channel(8);
    /// let mut tx1 = tx0.try_clone().expect("receiver alive");
    /// tx0.try_send_unsignaled(1).unwrap();
    /// tx1.try_send_unsignaled(2).unwrap();
    /// let mut batch = Vec::with_capacity(8);
    /// assert_eq!(rx.try_recv_scan_into_while(&mut batch, 8, |_| true), Ok(2));
    /// batch.sort_unstable();
    /// assert_eq!(batch, [1, 2]);
    /// assert_eq!(
    ///     rx.try_recv_scan_into_while(&mut batch, 8, |_| true),
    ///     Err(TryRecvError::Empty)
    /// );
    /// ```
    pub fn try_recv_scan_into_while(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
        mut admit: impl FnMut(&T) -> bool,
    ) -> Result<usize, TryRecvError> {
        self.activate_all_lanes();
        self.try_recv_batch_into_while(output, limit, &mut admit)
    }

    /// Put every registered lane in the rotation, keeping the current order
    /// of lanes that are already queued.
    fn activate_all_lanes(&mut self) {
        self.refresh_registry();
        for lane in self.lanes.iter_mut().flatten() {
            if !lane.queued {
                lane.queued = true;
                self.active.push_back(lane.key);
            }
        }
    }

    /// Move up to `limit` published values to `output` without blocking.
    ///
    /// Each step moves one contiguous chunk from the front lane's prefetched
    /// window and then applies the same bookkeeping that single receives
    /// perform one value at a time: slot release at the release batch, lane
    /// rotation at the burst limit, and a readiness poll at the poll interval.
    /// With an admission predicate, a chunk ends at the first rejected value
    /// and the drain stops there without rotating the lane. Space wakeups are
    /// sent inline; no wait registration is held here. Zero received values
    /// with no rejection means no value was published; the caller decides
    /// between empty and disconnected.
    pub(super) fn drain_into<A: Admit<T>>(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
        admit: &mut A,
    ) -> Drained {
        let mut received = 0;
        let mut rejected = false;
        while received < limit {
            if self.active.is_empty() {
                self.collect_ready(true);
                if self.active.is_empty() {
                    break;
                }
            } else if self.items_until_ready_poll == 0 {
                self.collect_ready(false);
                self.items_until_ready_poll = READY_POLL_INTERVAL;
            }

            let key = *self.active.front().expect("active lane present");
            let Some(lane) = self
                .lanes
                .get_mut(key.slot)
                .and_then(Option::as_mut)
                .filter(|lane| lane.key == key)
            else {
                self.active.pop_front();
                continue;
            };

            if lane.cached_available == 0 {
                lane.cached_available = lane.consumer.prefetch();
                if lane.cached_available == 0 {
                    match lane.settle_empty() {
                        EmptyLane::Keep { released } => {
                            if released {
                                lane.signal.notify_space();
                            }
                            self.active.rotate_left(1);
                        }
                        EmptyLane::Idle {
                            disconnected,
                            released,
                        } => {
                            if released {
                                lane.signal.notify_space();
                            }
                            self.active.pop_front();
                            if disconnected {
                                self.retire_lane(key);
                            }
                        }
                    }
                    continue;
                }
            }

            let take = (limit - received)
                .min(lane.cached_available)
                .min(PREFETCH_LIMIT - lane.burst)
                .min(lane.release_batch - lane.unreleased)
                .min(self.items_until_ready_poll);
            let moved = if A::ALWAYS {
                let moved = lane.consumer.pop_into(output, take);
                debug_assert_eq!(moved, take, "prefetched window shorter than cached");
                moved
            } else {
                lane.consumer
                    .pop_into_while(output, take, |value| admit.admit(value))
            };
            lane.cached_available -= moved;
            lane.unreleased += moved;
            lane.burst += moved;
            self.items_until_ready_poll -= moved;
            received += moved;
            if lane.unreleased == lane.release_batch && lane.release_pending() {
                lane.signal.notify_space();
            }
            if lane.burst == PREFETCH_LIMIT {
                lane.burst = 0;
                self.active.rotate_left(1);
            }
            if moved < take {
                rejected = true;
                break;
            }
        }
        Drained { received, rejected }
    }

    #[cold]
    #[inline(never)]
    fn recv_slow(&mut self) -> Result<T, RecvError> {
        let mut spin = SpinWait::blocking(self.wait_strategy, PARK_SPINS);
        while spin.step() {
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
            let (result, released) = self.try_recv_inner::<false>();
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
        let mut spin = SpinWait::deadline(self.wait_strategy, deadline);
        while spin.step() {
            std::hint::spin_loop();
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => {
                    return Err(RecvTimeoutError::Disconnected);
                }
                Err(TryRecvError::Empty) => {}
            }
        }

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
            let (result, released) = self.try_recv_inner::<false>();
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

        if lane.cached_available == 0 {
            lane.cached_available = lane.consumer.prefetch();
            if lane.cached_available == 0 {
                return match lane.settle_empty() {
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

    fn collect_ready(&mut self, all: bool) {
        self.refresh_registry();
        let group_count = self.groups.len();
        let start = self.ready_group_cursor;
        for offset in 0..group_count {
            let group_index = (start + offset) % group_count;
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
                    if lane.signal.is_pending() && !lane.queued {
                        lane.queued = true;
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

pub(super) struct Lane<T, P: Teardown> {
    key: LaneKey,
    signal: Arc<LaneSignal>,
    consumer: crate::ring::Consumer<T, P>,
    cached_available: usize,
    unreleased: usize,
    release_batch: usize,
    burst: usize,
    /// Whether `key` is in the receiver's `active` rotation.
    queued: bool,
}

impl<T, P: Teardown> Lane<T, P> {
    pub(super) fn new(
        key: LaneKey,
        signal: Arc<LaneSignal>,
        consumer: crate::ring::Consumer<T, P>,
    ) -> Self {
        // Scale credits to half a ring without shrinking the old batch size
        // for small rings. Large full lanes resume at their low watermark
        // while both sides still have work to do. This is independent of the
        // fairness burst: rotation does not force a capacity notification.
        // Empty-lane handling and explicit flushes release partial batches.
        let capacity = consumer.capacity();
        let release_batch = capacity.div_ceil(2).max(MIN_RELEASE_BATCH).min(capacity);
        Self {
            key,
            signal,
            consumer,
            cached_available: 0,
            unreleased: 0,
            release_batch,
            burst: 0,
            queued: false,
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

    /// Handle a prefetch window that came back empty.
    ///
    /// Releases partial credits first so neither side parks on unpublished
    /// slots, marks the lane idle, then rechecks the ring. A lane whose
    /// recheck finds data stays queued: either this call claims it back, or
    /// a racing publication claimed it and its ready-page entry is ignored
    /// because the lane is still queued. So a lane leaves the rotation only
    /// after it was observed empty. The caller removes it from `active` on
    /// `Idle`.
    #[inline(always)]
    fn settle_empty(&mut self) -> EmptyLane {
        let released = self.release_pending();
        self.signal.finish_drain();
        self.cached_available = self.consumer.prefetch();
        self.burst = 0;
        if self.cached_available != 0 {
            // Fails only when a racing publication already set the lane
            // pending, which keeps the queued-implies-pending invariant.
            self.signal.claim_after_empty();
            EmptyLane::Keep { released }
        } else {
            self.queued = false;
            EmptyLane::Idle {
                disconnected: self.consumer.is_disconnected(),
                released,
            }
        }
    }
}

enum EmptyLane {
    Keep { released: bool },
    Idle { disconnected: bool, released: bool },
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
