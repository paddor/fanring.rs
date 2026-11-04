//! Round counts and timeouts shared by several stress modules.

use std::time::Duration;

#[cfg(miri)]
pub(crate) const RACE_ROUNDS: usize = 2;
#[cfg(not(miri))]
pub(crate) const RACE_ROUNDS: usize = 100;

#[cfg(miri)]
pub(crate) const DISCONNECT_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(not(miri))]
pub(crate) const DISCONNECT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
