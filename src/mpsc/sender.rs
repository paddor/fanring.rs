use crate::teardown::{Deferred, Teardown};

use std::time::{Duration, Instant};

use crate::compat::{Arc, Ordering};
use crate::config::{SpinWait, WaitStrategy};
use crate::ready::LaneSignal;

use super::{
    LaneId, LaneKey, PARK_SPINS, SendError, SendTimeoutError, Shared, TryRegisterBoundedError,
    TryRegisterError, TrySendError,
};

/// Sending half.
///
/// A `Sender` owns exactly one SPSC ring producer. It is `Send` when `T` is
/// `Send`, but it is not `Sync`. Sender registration has no configured limit.
#[derive(Debug)]
pub struct Sender<T, P: Teardown = Deferred> {
    pub(super) shared: Arc<Shared<T, P>>,
    pub(super) producer: crate::ring::Producer<T, P>,
    pub(super) key: LaneKey,
    pub(super) signal: Arc<LaneSignal>,
    pub(super) wait_strategy: WaitStrategy,
}

impl<T, P: Teardown> Sender<T, P> {
    /// Return this sender's lane identity for targeted receive and drainage
    /// control. Registered senders, including clones, have distinct identities.
    #[must_use]
    pub fn lane(&self) -> LaneId {
        LaneId {
            key: self.key,
            channel_id: self.shared.channel_id,
        }
    }

    /// Set the policy used by synchronous blocking sends before parking.
    ///
    /// Newly registered senders inherit this sender's current policy. This
    /// does not affect asynchronous operations.
    pub fn set_wait_strategy(&mut self, strategy: WaitStrategy) {
        self.wait_strategy = strategy;
    }

    /// Return this sender's synchronous blocking wait policy.
    #[must_use]
    pub const fn wait_strategy(&self) -> WaitStrategy {
        self.wait_strategy
    }

    /// Try to register another sender.
    ///
    /// Returns `None` when the receiver is gone.
    #[must_use]
    pub fn try_clone(&self) -> Option<Self> {
        self.try_register().ok()
    }

    /// Try to register another sender and report why registration failed.
    ///
    /// Prefer this over [`try_clone`](Self::try_clone) when the caller needs to
    /// distinguish a closed receiver from successful registration.
    ///
    /// # Errors
    ///
    /// Returns [`TryRegisterError::Disconnected`] when the receiver is gone.
    pub fn try_register(&self) -> Result<Self, TryRegisterError> {
        self.try_register_bounded(usize::MAX)
            .map_err(|error| match error {
                TryRegisterBoundedError::Disconnected => TryRegisterError::Disconnected,
                TryRegisterBoundedError::AtCapacity => unreachable!("unbounded registration"),
            })
    }

    /// Register another sender with an independent ring capacity.
    ///
    /// Capacity is rounded up to a power of two. Ordinary registrations still
    /// use the channel's original capacity, including clones of this sender.
    /// Wait strategy and teardown policy follow this sender as usual.
    ///
    /// # Errors
    ///
    /// Returns [`TryRegisterError::Disconnected`] when the receiver is gone.
    ///
    /// # Panics
    ///
    /// Panics when capacity is zero or exceeds [`super::MAX_CAPACITY_PER_SENDER`].
    pub fn try_register_with_capacity(&self, capacity: usize) -> Result<Self, TryRegisterError> {
        crate::config::validate_capacity(capacity, super::MAX_CAPACITY_PER_SENDER)
            .unwrap_or_else(|error| panic!("{error}"));
        self.register_with_capacity(usize::MAX, capacity)
            .map_err(|error| match error {
                TryRegisterBoundedError::Disconnected => TryRegisterError::Disconnected,
                TryRegisterBoundedError::AtCapacity => unreachable!("unbounded registration"),
            })
    }

    /// Register only if fewer than `max_lanes` receive lanes are registered.
    ///
    /// Includes this sender and dropped senders whose lanes the receiver has
    /// not yet observed empty, whether or not values remain. The receiver
    /// must retire an old ring before its registration can be reused; it
    /// does so on a receive that finds the dropped sender's lane empty.
    /// Explicitly closed lanes stop counting immediately; `Deferred` teardown
    /// can still retain their unread storage until those senders drop.
    /// Returns [`TryRegisterBoundedError::AtCapacity`] when the limit is reached.
    pub fn try_register_bounded(&self, max_lanes: usize) -> Result<Self, TryRegisterBoundedError> {
        self.register_with_capacity(max_lanes, self.shared.capacity_per_sender)
    }

    fn register_with_capacity(
        &self,
        max_lanes: usize,
        capacity: usize,
    ) -> Result<Self, TryRegisterBoundedError> {
        let (key, signal, producer) = self.shared.register_sender(max_lanes, capacity)?;
        Ok(Self {
            shared: self.shared.clone(),
            producer,
            key,
            signal,
            wait_strategy: self.wait_strategy,
        })
    }

    /// Number of receive lanes, including dropped senders awaiting drain.
    pub fn registered_lanes(&self) -> usize {
        self.shared.registered_lanes.load(Ordering::Acquire)
    }

    /// Try to send one value.
    ///
    /// A successful send is immediately visible to the receiver. Internally,
    /// the value is pushed into this sender's SPSC ring and flushed.
    /// Consumed slots remain occupied until the receiver releases them. Use
    /// [`Receiver::release_consumed`](super::Receiver::release_consumed) or
    /// [`Receiver::recv_batch_into`](super::Receiver::recv_batch_into) when
    /// capacity must be reusable before processing received values.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] when this sender's lane is full, or
    /// [`TrySendError::Disconnected`] when the receiver is gone or this lane
    /// was closed.
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
        // The ring also closes when only this lane is retired. Receiver drop
        // closes every ring, including registrations still pending adoption.
        if self.producer.is_consumer_dropped() {
            return (Err(TrySendError::Disconnected(value)), false);
        }

        match self.producer.push_and_flush(value) {
            Ok(()) => {
                let wake_receiver = self.shared.mark_ready(&self.signal);
                (Ok(()), wake_receiver)
            }
            Err(value) => (Err(self.push_error(value)), false),
        }
    }

    #[inline]
    fn push_error(&self, value: T) -> TrySendError<T> {
        if !self.shared.receiver_alive.load(Ordering::Acquire)
            || self.producer.is_consumer_dropped()
        {
            TrySendError::Disconnected(value)
        } else {
            TrySendError::Full(value)
        }
    }

    /// Try to send one value without marking this lane ready.
    ///
    /// The value is pushed and published like [`try_send`](Self::try_send),
    /// but the receiver is not told which lane has data and is not woken.
    /// This skips the atomic read-modify-write that `try_send` performs on
    /// every send (with [`Deferred`](crate::teardown::Deferred) teardown;
    /// `Coordinated` teardown still does one to track the send).
    ///
    /// [`Receiver::try_recv_scan_into_while`](super::Receiver::try_recv_scan_into_while)
    /// finds values sent this way because it visits every registered lane.
    /// [`Receiver::poll_all_lanes`](super::Receiver::poll_all_lanes) also makes
    /// them discoverable through ordinary and tagged receives. Otherwise those
    /// receives need a later signaled send or [`flush`](Self::flush) on the same
    /// lane. Signaled and unsignaled sends may be mixed on one lane.
    ///
    /// The caller provides the wakeup. Between this send and reading its own
    /// wake flag, the caller needs a sequentially consistent fence, and the
    /// receiver needs one between clearing that flag and scanning. Without
    /// both fences, a send can read a stale flag while the receiver reads a
    /// stale ring and parks.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] when this sender's lane is full, or
    /// [`TrySendError::Disconnected`] when the receiver is gone or this lane
    /// was closed.
    #[inline]
    pub fn try_send_unsignaled(&mut self, value: T) -> Result<(), TrySendError<T>> {
        if self.producer.is_consumer_dropped() {
            return Err(TrySendError::Disconnected(value));
        }
        self.producer
            .push_and_flush(value)
            .map_err(|value| self.push_error(value))
    }

    /// Send one value, blocking while this sender's ring is full.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] with the unsent value when the receiver disconnects
    /// or this lane was closed.
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
        let mut spin = SpinWait::blocking(self.wait_strategy, PARK_SPINS);
        while spin.step() {
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
    ///
    /// # Errors
    ///
    /// Returns [`SendTimeoutError::Timeout`] with the unsent value when the
    /// timeout expires, or [`SendTimeoutError::Disconnected`] when the receiver
    /// disconnects.
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
    ///
    /// # Errors
    ///
    /// Returns [`SendTimeoutError::Timeout`] with the unsent value at the
    /// deadline, or [`SendTimeoutError::Disconnected`] when the receiver
    /// disconnects.
    pub fn send_deadline(
        &mut self,
        mut value: T,
        deadline: Instant,
    ) -> Result<(), SendTimeoutError<T>> {
        let mut spin = SpinWait::deadline(self.wait_strategy, deadline);
        while spin.step() {
            std::hint::spin_loop();
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(value)) => {
                    return Err(SendTimeoutError::Disconnected(value));
                }
                Err(TrySendError::Full(returned)) => value = returned,
            }
        }

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

    /// Return this sender's current numeric lane slot.
    ///
    /// Slots can be reused. Use [`lane`](Self::lane) for a generation-safe ID.
    #[inline]
    #[must_use]
    pub const fn lane_id(&self) -> usize {
        self.key.slot
    }

    /// Return this sender's lane capacity after `yring` rounding.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.producer.capacity()
    }

    /// Return whether this sender's lane cannot accept another value now.
    ///
    /// Only the receiver frees lane slots, so a full lane stays full until
    /// the receiver consumes and releases values. Producers that drop on a
    /// full lane can use this to skip building a value they would drop. A
    /// `false` result does not reserve a slot.
    #[inline]
    #[must_use]
    pub fn is_full(&mut self) -> bool {
        self.producer.is_full()
    }

    /// Return whether the receiver has been dropped or this lane was closed.
    #[inline]
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        !self.shared.receiver_alive.load(Ordering::Acquire) || self.producer.is_consumer_dropped()
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

    /// Return whether both senders belong to the same channel.
    #[inline]
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl<T> Sender<T, Deferred> {
    /// Enqueue without publishing, allowing one publication for a batch.
    ///
    /// Call [`flush`](Self::flush) before waiting for space or data. An ordinary
    /// successful send or dropping this sender also publishes pending values.
    /// Only the deferred teardown policy supports unpublished values.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] when this sender's lane is full or
    /// [`TrySendError::Disconnected`] when the receiver is gone or this lane
    /// was closed. A full result publishes earlier values so the receiver can
    /// free capacity.
    #[inline]
    pub fn try_send_deferred(&mut self, value: T) -> Result<(), TrySendError<T>> {
        if self.producer.is_consumer_dropped() {
            return Err(TrySendError::Disconnected(value));
        }
        self.producer.push_deferred(value).map_err(|value| {
            self.flush();
            if self.is_disconnected() {
                TrySendError::Disconnected(value)
            } else {
                TrySendError::Full(value)
            }
        })
    }

    /// Publish earlier deferred sends and wake the receiver if needed.
    #[inline]
    pub fn flush(&mut self) {
        self.producer.flush();
        if self.shared.mark_ready(&self.signal) {
            self.shared.data_waiter.notify();
        }
    }
}

impl<T, P: Teardown> Drop for Sender<T, P> {
    fn drop(&mut self) {
        #[cfg(feature = "async")]
        self.cancel_send_wait();
        self.producer.close();
        let _ = self.shared.mark_ready(&self.signal);
        self.shared.live_senders.fetch_sub(1, Ordering::AcqRel);
        self.shared.data_waiter.notify();
    }
}
