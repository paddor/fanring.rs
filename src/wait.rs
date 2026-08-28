use std::fmt;
use std::time::Duration;

use crate::compat::{AtomicUsize, Condvar, Mutex, MutexGuard, Ordering};

const WAITING: usize = 1;
const NOTIFY_INCREMENT: usize = 2;

/// Single-waiter condition cell.
///
/// The low state bit records a registered waiter. Higher bits form a
/// notification generation. Clearing a registration preserves notifications
/// that raced with wakeup handling.
///
/// The registration owns the mutex across the caller's condition recheck.
/// A notifier that observes `waiting` cannot signal until `Condvar::wait` has
/// atomically released that mutex.
pub(crate) struct WaitCell {
    state: PaddedWaitState,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl WaitCell {
    pub(crate) fn new() -> Self {
        Self {
            state: PaddedWaitState(AtomicUsize::new(0)),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    pub(crate) fn prepare(&self) -> WaitRegistration<'_> {
        let guard = match self.mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let previous = self.state.0.fetch_or(WAITING, Ordering::AcqRel);
        debug_assert_eq!(previous & WAITING, 0);
        WaitRegistration {
            cell: self,
            guard: Some(guard),
            snapshot: previous | WAITING,
        }
    }

    /// Notify the waiter, if one has registered.
    #[inline]
    pub(crate) fn notify(&self) {
        let previous = self.state.0.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
        if previous & WAITING == 0 {
            return;
        }

        let _guard = match self.mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.state.0.load(Ordering::Relaxed) & WAITING != 0 {
            self.condvar.notify_one();
        }
    }
}

impl fmt::Debug for WaitCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitCell")
            .field("state", &self.state.0.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) struct WaitRegistration<'a> {
    cell: &'a WaitCell,
    guard: Option<MutexGuard<'a, ()>>,
    snapshot: usize,
}

impl WaitRegistration<'_> {
    pub(crate) fn wait(mut self) {
        if self.cell.state.0.load(Ordering::Acquire) != self.snapshot {
            self.clear();
            return;
        }
        let guard = self.guard.take().expect("wait registration is active");
        let guard = match self.cell.condvar.wait(guard) {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.guard = Some(guard);
        self.clear();
    }

    /// Wait for at most `timeout`. Returns whether the wait timed out.
    pub(crate) fn wait_timeout(mut self, timeout: Duration) -> bool {
        if self.cell.state.0.load(Ordering::Acquire) != self.snapshot {
            self.clear();
            return false;
        }
        let guard = self.guard.take().expect("wait registration is active");
        let (guard, result) = match self.cell.condvar.wait_timeout(guard, timeout) {
            Ok(result) => result,
            Err(poisoned) => poisoned.into_inner(),
        };
        let timed_out = result.timed_out();
        self.guard = Some(guard);
        self.clear();
        timed_out
    }

    pub(crate) fn cancel(mut self) {
        self.clear();
    }

    fn clear(&mut self) {
        self.cell.state.0.fetch_and(!WAITING, Ordering::AcqRel);
        self.guard.take();
    }
}

#[repr(align(128))]
struct PaddedWaitState(AtomicUsize);

impl Drop for WaitRegistration<'_> {
    fn drop(&mut self) {
        if self.guard.is_some() {
            self.clear();
        }
    }
}
