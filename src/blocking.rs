//! Spin-then-park retry loop shared by blocking sends and receives.

use std::ops::Deref;
use std::time::Instant;

use crate::config::SpinWait;
use crate::error::{RecvError, RecvTimeoutError, SendTimeoutError, TryRecvError, TrySendError};
use crate::wait::{Park, WaitSource};

/// Result of one nonblocking attempt.
pub(crate) enum Attempt<V, R> {
    /// The operation finished.
    Done(R),
    /// Not ready yet. Carries the input to the next attempt.
    Pending(V),
}

/// Result of an attempt made while registered as a waiter.
pub(crate) enum Recheck<V, R, D> {
    /// The operation finished. Send `D` after unregistering.
    Done(R, D),
    /// Not ready, but the attempt freed capacity or moved work. Send `D`
    /// after unregistering, then retry without parking.
    Progressed(V, D),
    /// Not ready. Park before retrying.
    Pending(V),
}

/// An endpoint operation that can block on input `V`.
pub(crate) trait Blocking<V> {
    type Output;
    /// Notifications held back until the waiter unregisters. Sending them
    /// while registered makes the woken peer signal this idle waiter.
    type Deferred;
    /// Shared owner of the wait cell, so registrations do not borrow the
    /// endpoint.
    type Waiter: Deref<Target: WaitSource>;

    fn waiter(&self) -> Self::Waiter;
    /// Try once and send all notifications.
    fn attempt(&mut self, input: V) -> Attempt<V, Self::Output>;
    /// Try once while registered.
    fn recheck(&mut self, input: V) -> Recheck<V, Self::Output, Self::Deferred>;
    fn notify(&mut self, deferred: Self::Deferred);
}

/// Spin per `spin`, then park until `endpoint`'s operation finishes.
pub(crate) fn block<V, E: Blocking<V>>(endpoint: &mut E, input: V, spin: SpinWait) -> E::Output {
    match run(endpoint, input, spin, None) {
        Ok(output) => output,
        Err(_) => unreachable!("waits without a deadline never time out"),
    }
}

/// Spin per `spin`, then park until the operation finishes. Returns the
/// pending input once `deadline` passes.
pub(crate) fn block_until<V, E: Blocking<V>>(
    endpoint: &mut E,
    input: V,
    spin: SpinWait,
    deadline: Instant,
) -> Result<E::Output, V> {
    run(endpoint, input, spin, Some(deadline))
}

/// Map a nonblocking send. Disconnect finishes with the unsent value.
#[inline]
pub(crate) fn send_attempt<T>(result: Result<(), TrySendError<T>>) -> Attempt<T, Result<(), T>> {
    match result {
        Ok(()) => Attempt::Done(Ok(())),
        Err(TrySendError::Disconnected(value)) => Attempt::Done(Err(value)),
        Err(TrySendError::Full(value)) => Attempt::Pending(value),
    }
}

/// Map a registered send that defers waking the receiver.
pub(crate) fn send_recheck<T>(
    result: Result<(), TrySendError<T>>,
    wake_receiver: bool,
) -> Recheck<T, Result<(), T>, bool> {
    match result {
        Ok(()) => Recheck::Done(Ok(()), wake_receiver),
        Err(TrySendError::Disconnected(value)) => Recheck::Done(Err(value), false),
        Err(TrySendError::Full(value)) => Recheck::Pending(value),
    }
}

/// Map a nonblocking receive, keeping `input` for the next attempt.
#[inline]
pub(crate) fn recv_attempt<V, I>(
    result: Result<V, TryRecvError>,
    input: I,
) -> Attempt<I, Result<V, RecvError>> {
    match result {
        Ok(value) => Attempt::Done(Ok(value)),
        Err(TryRecvError::Disconnected) => Attempt::Done(Err(RecvError)),
        Err(TryRecvError::Empty) => Attempt::Pending(input),
    }
}

/// Map a timed send that returns the unsent value on disconnect.
pub(crate) fn send_timeout_result<T>(
    result: Result<Result<(), T>, T>,
) -> Result<(), SendTimeoutError<T>> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(value)) => Err(SendTimeoutError::Disconnected(value)),
        Err(value) => Err(SendTimeoutError::Timeout(value)),
    }
}

/// Map a timed receive.
pub(crate) fn recv_timeout_result<V, I>(
    result: Result<Result<V, RecvError>, I>,
) -> Result<V, RecvTimeoutError> {
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(RecvError)) => Err(RecvTimeoutError::Disconnected),
        Err(_) => Err(RecvTimeoutError::Timeout),
    }
}

/// Retry until the operation finishes. Spins per `spin`, then parks between
/// attempts.
///
/// Each park registers before one more attempt, so a notification that
/// races that attempt ends the sleep. Returns the pending input once
/// `deadline` passes.
fn run<V, E: Blocking<V>>(
    endpoint: &mut E,
    mut input: V,
    mut spin: SpinWait,
    deadline: Option<Instant>,
) -> Result<E::Output, V> {
    while spin.step() {
        std::hint::spin_loop();
        input = match endpoint.attempt(input) {
            Attempt::Done(output) => return Ok(output),
            Attempt::Pending(input) => input,
        };
    }

    let waiter = endpoint.waiter();
    loop {
        input = match endpoint.attempt(input) {
            Attempt::Done(output) => return Ok(output),
            Attempt::Pending(input) => input,
        };
        let registration = waiter.register();
        let progressed = match endpoint.recheck(input) {
            Recheck::Done(output, deferred) => {
                drop(registration);
                endpoint.notify(deferred);
                return Ok(output);
            }
            Recheck::Progressed(next, deferred) => {
                input = next;
                Some(deferred)
            }
            Recheck::Pending(next) => {
                input = next;
                None
            }
        };
        if let Some(deferred) = progressed {
            drop(registration);
            endpoint.notify(deferred);
            continue;
        }

        let Some(deadline) = deadline else {
            registration.wait();
            continue;
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(input);
        }
        if registration.wait_timeout(remaining) {
            return match endpoint.attempt(input) {
                Attempt::Done(output) => Ok(output),
                Attempt::Pending(input) => Err(input),
            };
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
