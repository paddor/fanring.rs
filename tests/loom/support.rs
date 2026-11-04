//! Shared Loom model bounds and drop-counting values.

use loom::sync::Arc;
use loom::sync::atomic::Ordering;

/// Checks `check` with the end-to-end bounds unless a `LOOM_*` variable
/// overrides them.
pub(crate) fn model(check: impl Fn() + Sync + Send + 'static) {
    let mut builder = loom::model::Builder::new();
    if std::env::var_os("LOOM_MAX_BRANCHES").is_none() {
        builder.max_branches = 10_000;
    }
    if std::env::var_os("LOOM_MAX_PERMUTATIONS").is_none() {
        builder.max_permutations = Some(10_000);
    }
    if std::env::var_os("LOOM_MAX_PREEMPTIONS").is_none() {
        builder.preemption_bound = Some(2);
    }
    builder.check(check);
}

/// Counts its own drops.
#[derive(Debug)]
pub(crate) struct DropCount(pub(crate) Arc<loom::sync::atomic::AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
