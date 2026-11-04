use std::fmt;
use std::time::{Duration, Instant};

use crate::mpsc::{LaneId, LaneKey, RecvError, RecvTimeoutError, TryRecvError};
use crate::teardown::{Deferred, Teardown};

use super::{EmptyLane, Lane, Receiver};

/// Borrowed single-value receive view that includes each value's source lane.
///
/// Created by [`Receiver::with_lane_ids`]. Scheduling, FIFO, slot release, and
/// disconnect behavior match the corresponding [`Receiver`] methods. Each
/// successful receive creates a [`Copy`] [`LaneId`]; plain receives do not.
pub struct LaneReceiver<'a, T, P: Teardown = Deferred> {
    pub(in crate::mpsc) receiver: &'a mut Receiver<T, P>,
}

impl<T, P: Teardown> fmt::Debug for LaneReceiver<'_, T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LaneReceiver")
            .field("receiver", &self.receiver)
            .finish()
    }
}

impl<T, P: Teardown> LaneReceiver<'_, T, P> {
    /// Receive a value and its source lane with bounded per-sender bursts.
    /// See [`Receiver::try_recv`] for slot release and errors.
    #[inline]
    pub fn try_recv(&mut self) -> Result<(LaneId, T), TryRecvError> {
        self.receiver.try_recv_output::<WithLaneIds>()
    }

    /// Receive a value and its source lane, rotating after every value.
    /// See [`Receiver::try_recv_fair`] for scheduling and errors.
    #[inline]
    pub fn try_recv_fair(&mut self) -> Result<(LaneId, T), TryRecvError> {
        self.receiver.try_recv_fair_output::<WithLaneIds>()
    }

    /// Wait for a value and return it with its source lane.
    /// See [`Receiver::recv`] for slot release and errors.
    #[inline]
    pub fn recv(&mut self) -> Result<(LaneId, T), RecvError> {
        self.receiver.recv_output::<WithLaneIds>()
    }

    /// Wait up to `timeout` for a value and return it with its source lane.
    /// See [`Receiver::recv_timeout`] for errors.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<(LaneId, T), RecvTimeoutError> {
        self.receiver.recv_timeout_output::<WithLaneIds>(timeout)
    }

    /// Wait until `deadline` for a value and return it with its source lane.
    /// See [`Receiver::recv_deadline`] for errors.
    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<(LaneId, T), RecvTimeoutError> {
        self.receiver.recv_deadline_output::<WithLaneIds>(deadline)
    }
}

// Specialize the output construction without adding a runtime mode branch.
// Both modes use the same receive and waiter protocols.
pub(crate) trait ReceiveOutput<T, P: Teardown> {
    type Item;
    fn item(receiver: &Receiver<T, P>, key: LaneKey, value: T) -> Self::Item;
}

pub(in crate::mpsc) struct ItemOnly;
pub(in crate::mpsc) struct WithLaneIds;

impl<T, P: Teardown> ReceiveOutput<T, P> for ItemOnly {
    type Item = T;

    #[inline(always)]
    fn item(_receiver: &Receiver<T, P>, _key: LaneKey, value: T) -> T {
        value
    }
}

impl<T, P: Teardown> ReceiveOutput<T, P> for WithLaneIds {
    type Item = (LaneId, T);

    #[inline(always)]
    fn item(receiver: &Receiver<T, P>, key: LaneKey, value: T) -> (LaneId, T) {
        (
            LaneId {
                key,
                channel_id: receiver.shared.channel_id,
            },
            value,
        )
    }
}

impl<T, P: Teardown> Receiver<T, P> {
    /// Include the source [`LaneId`] in single-value receive results.
    ///
    /// The view borrows this receiver and uses its existing scheduling and
    /// waiter state. It adds no runtime output-mode switch. Plain receives
    /// create no lane IDs; tagged receives copy the channel identity and lane
    /// key. Return to this receiver to pause, resume, or drain that lane.
    ///
    /// ```
    /// use fanring::mpsc::channel;
    /// let (mut tx, mut rx) = channel(4);
    /// tx.try_send(7).unwrap();
    /// let (lane, value) = rx.with_lane_ids().try_recv_fair().unwrap();
    /// assert_eq!(value, 7);
    /// rx.pause(&lane).unwrap();
    /// assert_eq!(rx.try_recv_from(&lane), Err(fanring::mpsc::TryRecvError::Empty));
    /// rx.resume(&lane).unwrap();
    /// ```
    #[inline]
    pub fn with_lane_ids(&mut self) -> LaneReceiver<'_, T, P> {
        LaneReceiver { receiver: self }
    }

    /// Exclude a lane from all ordinary receives, including bulk and scan
    /// receives. Targeted receives remain available. Publishes slots already
    /// consumed from the lane, so its sender can fill the whole ring. Does not
    /// release unread slots. Repeated pauses are harmless.
    ///
    /// Returns `RecvError` for a retired lane or an ID from another channel.
    pub fn pause(&mut self, id: &LaneId) -> Result<(), RecvError> {
        let lane = self.lane_mut(id).ok_or(RecvError)?;
        lane.queued = false;
        // Ordinary receives skip a paused lane, so nothing else would
        // publish its partial release batch.
        if lane.release_pending() {
            lane.signal.notify_space();
        }
        if self.paused[id.key.slot] {
            return Ok(());
        }
        self.paused[id.key.slot] = true;
        self.active.retain(|key| *key != id.key);
        Ok(())
    }

    /// Restore a paused lane to ordinary receive rotation. Checks the ring on
    /// the next receive even if publications occurred while it was paused.
    /// Repeated resumes are harmless.
    ///
    /// Returns `RecvError` for a retired lane or an ID from another channel.
    pub fn resume(&mut self, id: &LaneId) -> Result<(), RecvError> {
        self.lane_mut(id).ok_or(RecvError)?;
        if !self.paused[id.key.slot] {
            return Ok(());
        }
        self.paused[id.key.slot] = false;
        let lane = self.lanes[id.key.slot].as_mut().expect("validated lane");
        lane.signal.mark();
        lane.queued = true;
        self.active.push_back(id.key);
        self.shared.data_waiter.notify();
        Ok(())
    }

    /// Receive only from this lane, including while it is paused. Never
    /// resumes it or consumes another lane's value. Finds published values
    /// even when sent without a readiness signal. Slot release is batched as
    /// for ordinary receives.
    ///
    /// Returns `Empty` when this lane is empty and its sender is alive, or
    /// `Disconnected` for an empty disconnected lane, a retired lane, or an
    /// ID from another channel. Buffered values precede disconnect.
    pub fn try_recv_from(&mut self, id: &LaneId) -> Result<T, TryRecvError> {
        let lane = self.lane_mut(id).ok_or(TryRecvError::Disconnected)?;
        if let Some(empty) = lane.refill() {
            if empty.released() {
                lane.signal.notify_space();
            }
            if let EmptyLane::Idle { disconnected, .. } = empty {
                self.active.retain(|key| *key != id.key);
                if disconnected {
                    self.retire_lane(id.key);
                    return Err(TryRecvError::Disconnected);
                }
                return Err(TryRecvError::Empty);
            }
        }
        let lane = self.lanes[id.key.slot].as_mut().expect("validated lane");
        let value = lane.consumer.pop().expect("prefetched value");
        lane.cached_available -= 1;
        lane.unreleased += 1;
        // A targeted receive does not change the ordinary lane rotation.
        if lane.unreleased == lane.release_batch && lane.release_pending() {
            lane.signal.notify_space();
        }
        if !self.paused[id.key.slot] && !lane.queued {
            lane.signal.mark();
            lane.queued = true;
            self.active.push_back(id.key);
        }
        Ok(value)
    }

    /// Disconnect and retire one lane, including a paused lane with unread
    /// values. Future sends fail and registration can reuse the slot.
    /// Payload destruction follows the channel's teardown policy.
    ///
    /// Returns `RecvError` for a retired lane or an ID from another channel.
    pub fn close_lane(&mut self, id: &LaneId) -> Result<(), RecvError> {
        let lane = self.lane_mut(id).ok_or(RecvError)?;
        lane.consumer.close();
        lane.signal.notify_space();
        self.active.retain(|key| *key != id.key);
        self.retire_lane(id.key);
        Ok(())
    }

    fn lane_mut(&mut self, id: &LaneId) -> Option<&mut Lane<T, P>> {
        if self.shared.channel_id != id.channel_id {
            return None;
        }
        self.refresh_registry();
        self.lanes
            .get_mut(id.key.slot)?
            .as_mut()
            .filter(|lane| lane.key == id.key)
    }
}
