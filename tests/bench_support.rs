//! Unit tests of the shared benchmark harness. The benchmark targets use
//! `harness = false`, so their own builds drop `#[test]` functions.

#[expect(
    dead_code,
    unused_imports,
    reason = "this target only runs the harness unit tests"
)]
#[path = "../benches/support/mod.rs"]
mod support;
