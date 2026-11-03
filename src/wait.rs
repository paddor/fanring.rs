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
/// A registration snapshots the generation. The caller rechecks its
/// condition without holding the condition mutex; `wait` compares the
/// snapshot under that mutex. A notifier advances the generation before it
/// takes the mutex, so it either changes the snapshot or finds the waiter
/// asleep in `Condvar::wait`. Notifiers never wait for a recheck.
pub(crate) struct WaitCell {
    state: AtomicUsize,
    mutex: Mutex<()>,
    condvar: Condvar,
    #[cfg(feature = "async")]
    waker: atomic_waker::AtomicWaker,
}

impl WaitCell {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
            #[cfg(feature = "async")]
            waker: atomic_waker::AtomicWaker::new(),
        }
    }

    pub(crate) fn prepare(&self) -> WaitRegistration<'_> {
        // AcqRel: a notify that missed WAITING precedes this RMW, so the
        // caller's recheck sees what that notifier published.
        let previous = self.state.fetch_or(WAITING, Ordering::AcqRel);
        debug_assert_eq!(previous & WAITING, 0);
        WaitRegistration {
            cell: self,
            snapshot: previous | WAITING,
        }
    }

    /// Notify the waiter, if one has registered.
    #[inline]
    pub(crate) fn notify(&self) {
        let previous = self.state.fetch_add(NOTIFY_INCREMENT, Ordering::AcqRel);
        #[cfg(feature = "async")]
        self.waker.wake();
        if previous & WAITING == 0 {
            return;
        }

        let _guard = lock(&self.mutex);
        if self.state.load(Ordering::Relaxed) & WAITING != 0 {
            self.condvar.notify_one();
        }
    }

    #[cfg(feature = "async")]
    pub(crate) fn register_async(&self, waker: &std::task::Waker) {
        self.waker.register(waker);
        // Pair with notify even when its wake preceded registration. The
        // caller rechecks the queue after acquiring that publication.
        self.state.fetch_add(0, Ordering::AcqRel);
    }

    #[cfg(feature = "async")]
    pub(crate) fn cancel_async(&self) {
        self.waker.take();
    }
}

impl fmt::Debug for WaitCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitCell")
            .field("state", &self.state.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Registered single waiter. Dropping it unregisters the waiter.
pub(crate) struct WaitRegistration<'a> {
    cell: &'a WaitCell,
    snapshot: usize,
}

impl WaitRegistration<'_> {
    pub(crate) fn wait(self) {
        let guard = lock(&self.cell.mutex);
        if self.is_current() {
            drop(wait_on(&self.cell.condvar, guard));
        }
    }

    /// Wait for at most `timeout`. Returns whether the wait timed out.
    pub(crate) fn wait_timeout(self, timeout: Duration) -> bool {
        let guard = lock(&self.cell.mutex);
        if !self.is_current() {
            return false;
        }
        let (_guard, timed_out) = wait_on_timeout(&self.cell.condvar, guard, timeout);
        timed_out
    }

    pub(crate) fn cancel(self) {
        drop(self);
    }

    /// Whether no notification arrived since `prepare`. Callers hold the
    /// condition mutex, so a later notifier signals only after they sleep.
    fn is_current(&self) -> bool {
        self.cell.state.load(Ordering::Acquire) == self.snapshot
    }
}

impl Drop for WaitRegistration<'_> {
    fn drop(&mut self) {
        // Keep the generation bits; the next snapshot starts from them.
        self.cell.state.fetch_and(!WAITING, Ordering::AcqRel);
    }
}

#[repr(align(128))]
struct PaddedWaitState(AtomicUsize);

/// Multi-waiter condition cell.
///
/// Waiters snapshot a notification generation and recheck their condition
/// without holding the condition mutex; `wait` compares the snapshot under
/// that mutex. Notifications advance the generation before taking the mutex,
/// so a waiter either observes the change or is asleep when the condition
/// variable is signaled. The low atomic state bit records whether any waiter
/// exists; the exact count lives under the mutex already required by the
/// condition variable. That bit changes only under the mutex and stays set
/// while a registration counts, so only notifications change a snapshot.
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
        let mut waiters = lock(&self.mutex);
        *waiters = waiters.checked_add(1).expect("waiter count overflow");
        // AcqRel: a notify that missed WAITING precedes this RMW, so the
        // caller's recheck sees what that notifier published.
        let previous = self.state.0.fetch_or(WAITING, Ordering::AcqRel);
        MultiWaitRegistration {
            cell: self,
            snapshot: previous | WAITING,
        }
    }

    fn remove_waiter(&self, waiters: &mut usize) {
        *waiters = waiters.checked_sub(1).expect("waiter count underflow");
        if *waiters == 0 {
            self.state.0.fetch_and(!WAITING, Ordering::AcqRel);
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

/// Registered waiter of a [`MultiWaitCell`]. Dropping it unregisters the
/// waiter.
pub(crate) struct MultiWaitRegistration<'a> {
    cell: &'a MultiWaitCell,
    snapshot: usize,
}

impl MultiWaitRegistration<'_> {
    pub(crate) fn wait(self) {
        let cell = self.cell;
        let mut waiters = lock(&cell.mutex);
        if self.is_current() {
            waiters = wait_on(&cell.condvar, waiters);
        }
        self.finish(waiters);
    }

    /// Wait for at most `timeout`. Returns whether the wait timed out.
    pub(crate) fn wait_timeout(self, timeout: Duration) -> bool {
        let cell = self.cell;
        let waiters = lock(&cell.mutex);
        if !self.is_current() {
            self.finish(waiters);
            return false;
        }
        let (waiters, timed_out) = wait_on_timeout(&cell.condvar, waiters, timeout);
        self.finish(waiters);
        timed_out
    }

    pub(crate) fn cancel(self) {
        drop(self);
    }

    /// Whether no notification arrived since `prepare`. Callers hold the
    /// condition mutex, so a later notifier signals only after they sleep.
    fn is_current(&self) -> bool {
        self.cell.state.0.load(Ordering::Acquire) == self.snapshot
    }

    /// Unregister with the condition mutex already held.
    fn finish(self, mut waiters: MutexGuard<'_, usize>) {
        self.cell.remove_waiter(&mut waiters);
        // Already unregistered; skip the `Drop` that would lock again.
        std::mem::forget(self);
    }
}

impl Drop for MultiWaitRegistration<'_> {
    fn drop(&mut self) {
        let mut waiters = lock(&self.cell.mutex);
        self.cell.remove_waiter(&mut waiters);
    }
}

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom, target_pointer_width = "64"))]
mod loom_tests {
    use super::{MultiWaitCell, WaitCell};
    use super::{NOTIFY_INCREMENT, WAITING};
    use crate::compat::{Arc, AtomicBool, Ordering};
    use std::time::Duration;

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
    fn timeout_racing_notify_clears_single_waiter_registration() {
        loom::model(|| {
            let cell = Arc::new(WaitCell::new());
            let registration = cell.prepare();
            let notifier_cell = cell.clone();
            let notifier = loom::thread::spawn(move || notifier_cell.notify());

            registration.wait_timeout(Duration::ZERO);
            notifier.join().unwrap();
            assert_eq!(cell.state.load(Ordering::Acquire) & WAITING, 0);
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
    fn timeout_racing_notify_clears_multi_waiter_registration() {
        loom::model(|| {
            let cell = Arc::new(MultiWaitCell::new());
            let registration = cell.prepare();
            let notifier_cell = cell.clone();
            let notifier = loom::thread::spawn(move || notifier_cell.notify_one());

            registration.wait_timeout(Duration::ZERO);
            notifier.join().unwrap();
            assert_eq!(cell.state.0.load(Ordering::Acquire) & WAITING, 0);
            assert_eq!(*crate::compat::lock(&cell.mutex), 0);
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
