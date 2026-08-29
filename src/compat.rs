//! Compatibility shim: swap std sync types for loom types in loom builds.

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
