use std::time::{Duration, Instant};

use crate::compat::{Arc, Ordering};
use crate::ready::LaneSignal;

use super::{
    LaneKey, PARK_SPINS, SendError, SendTimeoutError, Shared, TryRegisterError, TrySendError,
};

/// Sending half.
///
/// Each sender owns one SPSC ring producer. Registering senders has no fixed
/// limit.
#[derive(Debug)]
pub struct Sender<T> {
    pub(super) shared: Arc<Shared<T>>,
    pub(super) producer: yring::Producer<T>,
    pub(super) key: LaneKey,
    pub(super) signal: Arc<LaneSignal>,
}

impl<T> Sender<T> {
    /// Try to register another sender.
    #[must_use]
    pub fn try_clone(&self) -> Option<Self> {
        self.try_register().ok()
    }

    /// Try to register another sender and report disconnection.
    ///
    /// # Errors
    ///
    /// Returns [`TryRegisterError::Disconnected`] when every receiver is gone.
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
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] when this sender's lane is full, or
    /// [`TrySendError::Disconnected`] when every receiver is gone.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        let (result, wake_receiver) = self.try_send_inner(value);
        if wake_receiver {
            self.shared.data_waiters.notify_one();
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
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] with the unsent value when every receiver
    /// disconnects.
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
                        self.shared.data_waiters.notify_one();
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

    /// Send one value, blocking up to `timeout` while this sender's ring is
    /// full.
    ///
    /// # Errors
    ///
    /// Returns [`SendTimeoutError::Timeout`] with the unsent value when the
    /// timeout expires, or [`SendTimeoutError::Disconnected`] when every
    /// receiver disconnects.
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
    /// deadline, or [`SendTimeoutError::Disconnected`] when every receiver
    /// disconnects.
    pub fn send_deadline(
        &mut self,
        mut value: T,
        deadline: Instant,
    ) -> Result<(), SendTimeoutError<T>> {
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
                        self.shared.data_waiters.notify_one();
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

    /// Return this sender's lane slot.
    #[inline]
    #[must_use]
    pub const fn lane_id(&self) -> usize {
        self.key.slot
    }

    /// Return this sender's capacity after `yring` rounding.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.producer.capacity()
    }

    /// Return whether every receiver has been dropped.
    #[inline]
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        !self.shared.receiver_alive.load(Ordering::Acquire)
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

    /// Return whether both senders belong to the same channel.
    #[inline]
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.producer.close();
        let _ = self.shared.mark_ready(&self.signal);
        if self.shared.live_senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.data_waiters.notify_all();
        } else {
            self.shared.data_waiters.notify_one();
        }
    }
}
