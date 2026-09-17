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
