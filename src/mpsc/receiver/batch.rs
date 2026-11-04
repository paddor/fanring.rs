use crate::mpsc::{LaneKey, READY_POLL_INTERVAL, RecvError, TryRecvError};
use crate::teardown::Teardown;

use super::{EmptyLane, Receiver};

/// Per-value admission for the bulk drain.
///
/// [`AdmitAll`] compiles to a plain window move. A predicate stops the drain
/// at the first value it rejects.
pub(in crate::mpsc) trait Admit<T> {
    const ALWAYS: bool;
    fn admit(&mut self, value: &T) -> bool;
}

pub(in crate::mpsc) struct AdmitAll;

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
pub(in crate::mpsc) struct Drained {
    /// Values appended to the output.
    pub(in crate::mpsc) received: usize,
    /// The admission predicate rejected a published value, so the drain
    /// stopped with data still available.
    pub(in crate::mpsc) rejected: bool,
}

impl<T, P: Teardown> Receiver<T, P> {
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

    /// Scan every unpaused registered lane and append available values to `output`
    /// while `admit` accepts them.
    ///
    /// Works like [`try_recv_batch_into_while`](Self::try_recv_batch_into_while),
    /// but it does not rely on lane readiness. Every unpaused registered lane
    /// joins the rotation first, so values sent with
    /// [`Sender::try_send_unsignaled`](crate::mpsc::Sender::try_send_unsignaled) are
    /// found as well. A lane leaves the rotation only after this call observes
    /// it empty, also when a signaled send on that lane races the scan. So
    /// when fewer than `limit` values are appended and `admit` rejected none,
    /// every unpaused lane registered when the call started was observed empty
    /// during this call. Lanes the rotation already held keep their order, and each
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
        self.poll_all_lanes();
        self.try_recv_batch_into_while(output, limit, &mut admit)
    }

    /// Check every unpaused registered lane on subsequent receives, including
    /// values sent with [`Sender::try_send_unsignaled`](crate::mpsc::Sender::try_send_unsignaled).
    ///
    /// Keeps the order of already active lanes and adds each other lane once.
    /// Does not consume values, resume paused lanes, or wake a blocked receiver.
    /// Call before a scalar or tagged receive when providing an external
    /// wake protocol. That protocol still needs the fences documented on the
    /// unsignaled send method before allowing the consumer to sleep.
    pub fn poll_all_lanes(&mut self) {
        self.refresh_registry();
        for (slot, lane) in self.lanes.iter_mut().enumerate() {
            let Some(lane) = lane else { continue };
            if !self.paused[slot] && !lane.queued {
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
    pub(in crate::mpsc) fn drain_into<A: Admit<T>>(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
        admit: &mut A,
    ) -> Drained {
        let mut received = 0;
        while received < limit && self.refresh_active() {
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
            if let Some(empty) = lane.refill() {
                if empty.released() {
                    lane.signal.notify_space();
                }
                self.settle_empty_front(key, empty);
                continue;
            }

            let take = (limit - received)
                .min(self.items_until_ready_poll)
                .min(lane.chunk_limit());
            let moved = lane.move_chunk(output, take, admit);
            self.items_until_ready_poll -= moved;
            received += moved;
            if lane.end_full_burst() {
                self.active.rotate_left(1);
            }
            if moved < take {
                return Drained {
                    received,
                    rejected: true,
                };
            }
        }
        Drained {
            received,
            rejected: false,
        }
    }

    /// Whether a lane is queued for draining. Collects ready lanes when none
    /// is queued, and when the readiness poll is due.
    fn refresh_active(&mut self) -> bool {
        if self.active.is_empty() {
            self.collect_ready(true);
            return !self.active.is_empty();
        }
        if self.items_until_ready_poll == 0 {
            self.collect_ready(false);
            self.items_until_ready_poll = READY_POLL_INTERVAL;
        }
        true
    }

    /// Rotates past the front lane after its ring came back empty, or removes
    /// it and retires it once its sender is gone.
    fn settle_empty_front(&mut self, key: LaneKey, empty: EmptyLane) {
        match empty {
            EmptyLane::Keep { .. } => self.active.rotate_left(1),
            EmptyLane::Idle { disconnected, .. } => {
                self.active.pop_front();
                if disconnected {
                    self.retire_lane(key);
                }
            }
        }
    }
}
