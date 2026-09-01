//! Compatibility shim: swap std sync types for loom types in loom builds.

use std::time::Duration;

#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) use loom::sync::Arc;
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) use std::sync::Arc;

#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) use loom::sync::Mutex;
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) use std::sync::Mutex;

#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) use loom::sync::{Condvar, MutexGuard};
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) use std::sync::{Condvar, MutexGuard};

#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

#[inline]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn wait_on<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    match condvar.wait(guard) {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn wait_on_timeout<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    timeout: Duration,
) -> (MutexGuard<'a, T>, bool) {
    match condvar.wait_timeout(guard, timeout) {
        Ok((guard, result)) => (guard, result.timed_out()),
        Err(poisoned) => {
            let (guard, result) = poisoned.into_inner();
            (guard, result.timed_out())
        }
    }
}
