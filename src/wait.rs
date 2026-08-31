use std::fmt;
use std::time::Duration;

use crate::compat::{
    AtomicUsize, Condvar, Mutex, MutexGuard, Ordering, lock, wait_on, wait_on_timeout,
};

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
    state: AtomicUsize,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl WaitCell {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    pub(crate) fn prepare(&self) -> WaitRegistration<'_> {
        let guard = lock(&self.mutex);
        let previous = self.state.fetch_or(WAITING, Ordering::AcqRel);
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
        let previous = self.state.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
        if previous & WAITING == 0 {
            return;
        }

        let _guard = lock(&self.mutex);
        if self.state.load(Ordering::Relaxed) & WAITING != 0 {
            self.condvar.notify_one();
        }
    }
}

impl fmt::Debug for WaitCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitCell")
            .field("state", &self.state.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) struct WaitRegistration<'a> {
    cell: &'a WaitCell,
    guard: Option<MutexGuard<'a, ()>>,
    snapshot: usize,
}

impl WaitRegistration<'_> {
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the condition mutex must stay held until the waiter bit is cleared"
    )]
    pub(crate) fn wait(mut self) {
        if self.cell.state.load(Ordering::Acquire) != self.snapshot {
            self.clear();
            return;
        }
        let guard = self.guard.take().expect("wait registration is active");
        let guard = wait_on(&self.cell.condvar, guard);
        self.guard = Some(guard);
        self.clear();
    }

    /// Wait for at most `timeout`. Returns whether the wait timed out.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the condition mutex must stay held until the waiter bit is cleared"
    )]
    pub(crate) fn wait_timeout(mut self, timeout: Duration) -> bool {
        if self.cell.state.load(Ordering::Acquire) != self.snapshot {
            self.clear();
            return false;
        }
        let guard = self.guard.take().expect("wait registration is active");
        let (guard, timed_out) = wait_on_timeout(&self.cell.condvar, guard, timeout);
        self.guard = Some(guard);
        self.clear();
        timed_out
    }

    pub(crate) fn cancel(mut self) {
        self.clear();
    }

    fn clear(&mut self) {
        self.cell.state.fetch_and(!WAITING, Ordering::AcqRel);
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

/// Multi-waiter condition cell.
///
/// Waiters snapshot a notification generation while holding the condition
/// mutex. Notifications advance the generation before taking that mutex, so a
/// waiter either observes the change or is asleep when the condition variable
/// is signaled. The low atomic state bit records whether any waiter exists;
/// the exact count lives under the mutex already required by the condition
/// variable.
pub(crate) struct MultiWaitCell {
    state: PaddedWaitState,
    mutex: Mutex<usize>,
    condvar: Condvar,
}

impl MultiWaitCell {
    pub(crate) fn new() -> Self {
        Self {
            state: PaddedWaitState(AtomicUsize::new(0)),
            mutex: Mutex::new(0),
            condvar: Condvar::new(),
        }
    }

    pub(crate) fn prepare(&self) -> MultiWaitRegistration<'_> {
        let mut guard = lock(&self.mutex);
        *guard = guard.checked_add(1).expect("waiter count overflow");
        let previous = self.state.0.fetch_or(WAITING, Ordering::AcqRel);
        MultiWaitRegistration {
            cell: self,
            guard: Some(guard),
            snapshot: previous | WAITING,
        }
    }

    #[inline]
    pub(crate) fn notify_one(&self) {
        let previous = self.state.0.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
        if previous & WAITING == 0 {
            return;
        }

        let guard = lock(&self.mutex);
        if *guard != 0 {
            self.condvar.notify_one();
        }
    }

    pub(crate) fn notify_all(&self) {
        let previous = self.state.0.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
        if previous & WAITING == 0 {
            return;
        }

        let guard = lock(&self.mutex);
        if *guard != 0 {
            self.condvar.notify_all();
        }
    }
}

impl fmt::Debug for MultiWaitCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiWaitCell")
            .field("state", &self.state.0.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) struct MultiWaitRegistration<'a> {
    cell: &'a MultiWaitCell,
    guard: Option<MutexGuard<'a, usize>>,
    snapshot: usize,
}

impl MultiWaitRegistration<'_> {
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the condition mutex protects the waiter count during cleanup"
    )]
    pub(crate) fn wait(mut self) {
        if self.cell.state.0.load(Ordering::Acquire) != self.snapshot {
            self.clear();
            return;
        }
        let guard = self.guard.take().expect("wait registration is active");
        let guard = wait_on(&self.cell.condvar, guard);
        self.guard = Some(guard);
        self.clear();
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the condition mutex protects the waiter count during cleanup"
    )]
    pub(crate) fn wait_timeout(mut self, timeout: Duration) -> bool {
        if self.cell.state.0.load(Ordering::Acquire) != self.snapshot {
            self.clear();
            return false;
        }
        let guard = self.guard.take().expect("wait registration is active");
        let (guard, timed_out) = wait_on_timeout(&self.cell.condvar, guard, timeout);
        self.guard = Some(guard);
        self.clear();
        timed_out
    }

    pub(crate) fn cancel(mut self) {
        self.clear();
    }

    fn clear(&mut self) {
        let guard = self.guard.as_mut().expect("wait registration is active");
        **guard = (**guard).checked_sub(1).expect("waiter count underflow");
        if **guard == 0 {
            self.cell.state.0.fetch_and(!WAITING, Ordering::AcqRel);
        }
        self.guard.take();
    }
}

impl Drop for MultiWaitRegistration<'_> {
    fn drop(&mut self) {
        if self.guard.is_some() {
            self.clear();
        }
    }
}

#[cfg(all(test, loom, target_pointer_width = "64"))]
mod loom_tests {
    use super::{MultiWaitCell, WaitCell};
    use super::{NOTIFY_INCREMENT, WAITING};
    use crate::compat::{Arc, AtomicBool, Ordering};

    #[test]
    fn wait_notify_cannot_be_lost() {
        loom::model(|| {
            let cell = Arc::new(WaitCell::new());
            let ready = Arc::new(AtomicBool::new(false));
            let waiter_cell = cell.clone();
            let waiter_ready = ready.clone();

            let waiter = loom::thread::spawn(move || {
                let registration = waiter_cell.prepare();
                if waiter_ready.load(Ordering::Acquire) {
                    registration.cancel();
                } else {
                    registration.wait();
                }
                assert!(waiter_ready.load(Ordering::Acquire));
            });

            ready.store(true, Ordering::Release);
            cell.notify();
            waiter.join().unwrap();
        });
    }

    #[test]
    fn wait_two_notifications_do_not_restore_snapshot() {
        loom::model(|| {
            let cell = WaitCell::new();
            let registration = cell.prepare();
            cell.state.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
            cell.state.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
            assert_ne!(cell.state.load(Ordering::Acquire), WAITING);
            registration.cancel();
        });
    }

    #[test]
    fn multi_wait_notify_one_cannot_be_lost() {
        loom::model(|| {
            let cell = Arc::new(MultiWaitCell::new());
            let ready = Arc::new(AtomicBool::new(false));
            let waiter_cell = cell.clone();
            let waiter_ready = ready.clone();

            let waiter = loom::thread::spawn(move || {
                let registration = waiter_cell.prepare();
                if waiter_ready.load(Ordering::Acquire) {
                    registration.cancel();
                } else {
                    registration.wait();
                }
                assert!(waiter_ready.load(Ordering::Acquire));
            });

            ready.store(true, Ordering::Release);
            cell.notify_one();
            waiter.join().unwrap();
        });
    }

    #[test]
    fn multi_wait_two_notifications_do_not_restore_snapshot() {
        loom::model(|| {
            let cell = MultiWaitCell::new();
            let registration = cell.prepare();
            cell.state.0.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
            cell.state.0.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
            assert_ne!(cell.state.0.load(Ordering::Acquire), WAITING);
            registration.cancel();
        });
    }

    #[test]
    fn multi_wait_notify_all_releases_every_waiter() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.max_branches = 10_000;
        builder.check(|| {
            let cell = Arc::new(MultiWaitCell::new());
            let ready = Arc::new(AtomicBool::new(false));
            let mut waiters = Vec::new();

            for _ in 0..2 {
                let waiter_cell = cell.clone();
                let waiter_ready = ready.clone();
                waiters.push(loom::thread::spawn(move || {
                    let registration = waiter_cell.prepare();
                    if waiter_ready.load(Ordering::Acquire) {
                        registration.cancel();
                    } else {
                        registration.wait();
                    }
                    assert!(waiter_ready.load(Ordering::Acquire));
                }));
            }

            ready.store(true, Ordering::Release);
            cell.notify_all();
            for waiter in waiters {
                waiter.join().unwrap();
            }
        });
    }
}
