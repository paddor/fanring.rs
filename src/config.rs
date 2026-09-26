use std::time::{Duration, Instant};

use crate::error::ChannelError;

const CLOCK_CHECK_INTERVAL: usize = 16;

/// Policy used by synchronous blocking operations before they park.
///
/// This policy belongs to one endpoint. Newly registered senders and cloned
/// MPMC receivers inherit the source endpoint's current policy.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Use the standard short retry phase, then park the thread.
    ///
    /// This is the default and favors low idle CPU usage. Untimed blocking
    /// operations currently execute up to 128 [`std::hint::spin_loop`] retries
    /// before parking. Timeout and deadline operations park immediately after
    /// their lost-wakeup recheck.
    #[default]
    Park,
    /// Actively retry for at most this duration, then park the thread.
    ///
    /// Active retries use [`std::hint::spin_loop`] and never yield to the OS
    /// scheduler. This is intended for threads pinned to distinct CPUs. It can
    /// consume one CPU while waiting. Timeout and deadline operations cap the
    /// spin phase at their operation deadline.
    SpinFor(Duration),
}

#[derive(Debug)]
pub(crate) struct SpinWait {
    mode: SpinMode,
}

#[derive(Debug)]
enum SpinMode {
    Retries(usize),
    Timed {
        started: Instant,
        duration: Duration,
        operation_deadline: Option<Instant>,
        until_clock_check: usize,
    },
}

impl SpinWait {
    pub(crate) fn blocking(strategy: WaitStrategy, park_spins: usize) -> Self {
        Self::new(strategy, park_spins, None)
    }

    pub(crate) fn deadline(strategy: WaitStrategy, deadline: Instant) -> Self {
        Self::new(strategy, 0, Some(deadline))
    }

    fn new(strategy: WaitStrategy, park_spins: usize, operation_deadline: Option<Instant>) -> Self {
        let mode = match strategy {
            WaitStrategy::Park => SpinMode::Retries(park_spins),
            WaitStrategy::SpinFor(duration) => SpinMode::Timed {
                started: Instant::now(),
                duration,
                operation_deadline,
                until_clock_check: 0,
            },
        };
        Self { mode }
    }

    #[inline]
    pub(crate) fn step(&mut self) -> bool {
        match &mut self.mode {
            SpinMode::Retries(remaining) => {
                let spin = *remaining != 0;
                *remaining = remaining.saturating_sub(1);
                spin
            }
            SpinMode::Timed {
                started,
                duration,
                operation_deadline,
                until_clock_check,
            } => {
                if let Some(deadline) = *operation_deadline {
                    let now = Instant::now();
                    return now.duration_since(*started) < *duration && now < deadline;
                }
                if *until_clock_check != 0 {
                    *until_clock_check -= 1;
                    return true;
                }

                let now = Instant::now();
                if now.duration_since(*started) >= *duration {
                    return false;
                }
                *until_clock_check = CLOCK_CHECK_INTERVAL - 1;
                true
            }
        }
    }
}

pub(crate) const fn validate_capacity(capacity: usize, max: usize) -> Result<(), ChannelError> {
    if capacity == 0 {
        return Err(ChannelError::ZeroCapacity);
    }
    if capacity > max {
        return Err(ChannelError::CapacityTooLarge {
            requested: capacity,
            max,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SpinWait, WaitStrategy};
    use std::time::{Duration, Instant};

    #[test]
    fn park_uses_requested_retry_count() {
        let mut wait = SpinWait::blocking(WaitStrategy::Park, 3);
        assert!(wait.step());
        assert!(wait.step());
        assert!(wait.step());
        assert!(!wait.step());
    }

    #[test]
    fn zero_duration_does_not_spin() {
        let mut wait = SpinWait::blocking(WaitStrategy::SpinFor(Duration::ZERO), 128);
        assert!(!wait.step());
    }

    #[test]
    fn operation_deadline_caps_spin_duration() {
        let mut wait = SpinWait::deadline(
            WaitStrategy::SpinFor(Duration::from_secs(1)),
            Instant::now(),
        );
        assert!(!wait.step());
    }
}
