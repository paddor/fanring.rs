//! Compatibility shim: swap std types for loom types in supported loom builds.

#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) use std::cell::UnsafeCell;

#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) use loom::sync::Arc;
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) use std::sync::Arc;

#[cfg(all(loom, target_pointer_width = "64"))]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(not(all(loom, target_pointer_width = "64")))]
pub(crate) trait UnsafeCellExt<T> {
    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R;
}

#[cfg(not(all(loom, target_pointer_width = "64")))]
impl<T> UnsafeCellExt<T> for std::cell::UnsafeCell<T> {
    #[inline]
    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.get())
    }
}

#[cfg(all(feature = "async", loom, target_pointer_width = "64"))]
pub(crate) use async_waker::AtomicWaker;

#[cfg(all(feature = "async", loom, target_pointer_width = "64"))]
mod async_waker {
    use loom::sync::Mutex;
    use std::task::Waker;

    // Model atomic-waker's contract: register acquires earlier wake releases,
    // and a later wake notifies the registered task. Its std atomics are
    // invisible to Loom, which otherwise reports false shutdown lost wakes.
    #[derive(Debug)]
    pub(crate) struct AtomicWaker(Mutex<Option<Waker>>);

    impl AtomicWaker {
        pub(crate) fn new() -> Self {
            Self(Mutex::new(None))
        }

        pub(crate) fn register(&self, waker: &Waker) {
            // Waker callbacks can reenter wake; invoke them outside the lock.
            let cloned = waker.clone();
            let previous = self.0.lock().unwrap().replace(cloned);
            drop(previous);
        }

        pub(crate) fn wake(&self) {
            let waker = self.0.lock().unwrap().take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
