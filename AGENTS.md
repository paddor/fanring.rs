# AGENTS.md

Two-crate workspace: the root `fanring` package (typed MPSC and MPMC
channels) and `yring/` (the bounded SPSC ring it builds on).
[`DESIGN.md`](DESIGN.md) covers the design; [`DEVELOPMENT.md`](DEVELOPMENT.md)
covers checks, Loom, Miri, benchmarks, and releases.

## Lints

Clippy must pass with `-D warnings` under both CI configurations:

```sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --all-targets -- -D warnings
```

Fix warnings instead of silencing them. For a demonstrated false positive,
use the narrowest `#[expect(lint, reason = "...")]`. Use `#[allow]` only
when the lint fires in some feature or `cfg` combinations (`async`, `loom`)
but not others.

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**. `fanring` also denies `missing_docs`
and forbids `unsafe_code`. `yring` warns on clippy `pedantic`; `fanring`
warns on `too_many_lines`.

`clippy.toml` caps functions at 60 lines and 5 parameters. Rust 2024
edition, MSRV **1.93**. ASCII-only source.

## Code quality

API design:

- Keep public APIs small and hard to misuse. Default to private or
  `pub(crate)`. Do not leak implementation types (rings, registries, wait
  cells, lane tokens) or private errors in public signatures.
- Take borrowed inputs (`&str`, `&[T]`, `&T`) when ownership is not
  needed. Take `impl Into<...>` where the callee keeps the data.
- Prefer standard traits (`From`/`TryFrom`, `Default`, `Display`,
  `Iterator`, `IntoIterator`). Add an inherent `new`, `get` or `to_*` only
  when no trait fits.
- Document every public item, with `# Errors` and `# Panics` where they
  apply. Make functions `const` where possible.

Code shape:

- One concern per module. Split a Rust file past about 800 lines (unit
  tests excluded) into submodules along its concerns. This covers
  `tests/`, `benches/`, and the chart tool too. Unit tests go in a sibling
  `tests.rs`.
- Functions stay under 60 lines and 5 parameters (`clippy.toml`). When
  arguments travel together, pass a named struct. A flat `match` with one
  arm per variant may `#[expect(clippy::too_many_lines)]`.
- Group struct fields that change together into a named sub-struct. A
  struct with more than about 12 fields usually hides one.
- Logic repeated across `mpsc` and `mpmc`, blocking and async, or teardown
  policies lives in one shared helper, not in copies.
- Off the hot path, plain readable code beats clever code.

Safety and errors:

- `fanring` forbids `unsafe`; slot safety lives in `yring`. Keep each
  `unsafe` block small, with a `// SAFETY:` comment. Unsafe changes need
  Miri and Loom coverage.
- Public error types implement `Display` and `std::error::Error`. Public
  error enums are `#[non_exhaustive]`. Send errors return the unsent value.
- No `unwrap`, `expect` or panic in library code for recoverable failures.
  Endpoint drop at any point never panics the other side. Check capacities
  and limits before allocating.

Tests:

- Every new or changed function is exercised by a test. Unit tests sit next
  to private logic; public behavior goes in `tests/`. Name tests by
  behavior.
- Add a regression test before fixing a bug.
- Wait on events with a timeout, not on sleeps.
- New synchronization protocols get a Loom model.

## Concurrency

- No lost wakes: make a state change visible before signaling, and check
  the condition after registering the wait.
- Never hold a lock guard across `.await` or a blocking wait. Keep lock
  holds short. Comment the order where two locks nest.
- Every waiter, waker, lane slot and registry entry has an owner that
  removes it on every exit path, including timeout, cancellation and drop.
  Prefer drop guards to manual cleanup.
- Futures are cancel-safe: dropping a pending future loses no value and
  leaves no registration behind.
- Use the weakest correct atomic ordering, and comment why.
- Bound every queue and buffer, by count or bytes.

## Performance

Profile before optimizing. Change measured hotspots. Back performance
claims with before and after numbers from release builds.

- Send and receive paths do not allocate or copy per message.
- Walk a collection once. Fuse filter, count and collect passes; use the
  entry API instead of `contains` then `insert`.
- Reuse buffers: `clear` keeps capacity. Use `Box<[T]>` or `[T; N]` for
  storage that never grows.
- Budget every drain loop so one busy lane cannot starve the others.
- Signal readiness on the idle-to-pending transition, not per push.
