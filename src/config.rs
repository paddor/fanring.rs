use std::time::{Duration, Instant};

use crate::error::ChannelError;

const CLOCK_CHECK_INTERVAL: usize = 16;

/// How long [`WaitStrategy::Park`] spins before an untimed operation parks.
/// A time budget keeps the window independent of per-attempt cost. It is
/// about one park and wake round trip, which bounds wasted spinning by the
/// cost of parking.
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) const PARK_SPIN: Duration = Duration::from_micros(10);
#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) const PARK_SPIN: Duration = Duration::ZERO;

/// Policy used by synchronous blocking operations before they park.
///
/// This policy belongs to one endpoint. Newly registered senders and cloned
/// MPMC receivers inherit the source endpoint's current policy.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Use the standard short retry phase, then park the thread.
    ///
    /// This is the default and favors low idle CPU usage. Untimed blocking
    /// operations currently retry with [`std::hint::spin_loop`] for about ten
    /// microseconds before parking. Timeout and deadline operations park
    /// immediately after their lost-wakeup recheck.
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
    /// No spin phase.
    Done,
    Timed {
        started: Instant,
        duration: Duration,
        operation_deadline: Option<Instant>,
        until_clock_check: usize,
    },
}

impl SpinWait {
    /// Spin phase of an untimed operation.
    pub(crate) fn blocking(strategy: WaitStrategy) -> Self {
        Self::new(strategy, PARK_SPIN, None)
    }

    /// Spin phase of a timed operation. Park mode does not spin.
    pub(crate) fn deadline(strategy: WaitStrategy, deadline: Instant) -> Self {
        Self::new(strategy, Duration::ZERO, Some(deadline))
    }

    fn new(
        strategy: WaitStrategy,
        park_spin: Duration,
        operation_deadline: Option<Instant>,
    ) -> Self {
        let duration = match strategy {
            WaitStrategy::Park => park_spin,
            WaitStrategy::SpinFor(duration) => duration,
        };
        if duration.is_zero() {
            return Self {
                mode: SpinMode::Done,
            };
        }
        Self {
            mode: SpinMode::Timed {
                started: Instant::now(),
                duration,
                operation_deadline,
                until_clock_check: 0,
            },
        }
    }

    #[inline]
    pub(crate) fn step(&mut self) -> bool {
        match &mut self.mode {
            SpinMode::Done => false,
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

    #[cfg(not(loom))]
    #[test]
    fn park_spins_for_the_park_budget() {
        let started = Instant::now();
        let mut wait = SpinWait::blocking(WaitStrategy::Park);
        let mut steps = 0;
        while wait.step() {
            steps += 1;
        }
        assert!(steps > 0);
        assert!(started.elapsed() >= super::PARK_SPIN);
    }

    #[test]
    fn timed_operations_do_not_spin_in_park_mode() {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut wait = SpinWait::deadline(WaitStrategy::Park, deadline);
        assert!(!wait.step());
    }

    #[test]
    fn zero_duration_does_not_spin() {
        let mut wait = SpinWait::blocking(WaitStrategy::SpinFor(Duration::ZERO));
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
