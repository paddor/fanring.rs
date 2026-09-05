# Changelog

All notable changes to this project are documented here.

## [Unreleased]

## [0.3.1] - 2026-09-05

### Changed

- Reduce MPSC receive bookkeeping and intermediate payload moves between lane
  maintenance boundaries.
- Require yring 0.3.15 to eliminate intermediate ring-pop payload copies.

## [0.3.0] - 2026-09-03

### Breaking

- Remove the `charts` Cargo feature and published `fanring-chart` binary. The
  repository tool remains available through `cargo run --example fanring-chart`;
  the channel API is unchanged.

### Changed

- Exclude chart-generator sources from the published package, reducing its
  compressed size from 53 KiB to 37 KiB.

## [0.2.2] - 2026-09-03

### Changed

- Limit published packages to crate sources and user-facing documentation.

## [0.2.1] - 2026-09-02

### Changed

- Replaced the MPMC receiver's crossbeam work-stealing deque with internal
  bounded work queues, batched transfers, and private sole-receiver staging.
- Kept benchmark charts on complete nonblocking runs and refreshed comparison
  data with explicit hardware topology metadata.

### Validation

- Expanded Loom coverage for receiver clone, drop, publication, topology, and
  work-stealing races.
- Rechecked MPMC ownership and concurrency with Miri, Tree Borrows, ThreadSanitizer,
  randomized stress tests, and deeper bounded Loom exploration.

## [0.2.0] - 2026-09-01

### Breaking

- Move the original channel API under `fanring::mpsc`.
- Replace `channel(max_senders, capacity_per_sender)` with
  `mpsc::channel(capacity_per_sender)`. Sender registration is now dynamic and
  no longer has a configured limit.
- Split nonblocking errors into `TrySendError` and `TryRecvError`; `SendError`
  and `RecvError` now describe blocking disconnection.

### Added

- Add `fanring::mpmc`, with cloneable competing receivers and stealable
  receive-side batches.
- Add blocking, deadline, and timeout send/receive operations with lost-wakeup
  protection and short adaptive spins before parking.
- Reuse drained sender slots and expose endpoint counts, channel identity,
  capacity, disconnection state, and receiver iterators.
- Add reproducible MPSC, MPMC, throughput, and wake-latency benchmarks with
  machine-readable results and tracked charts.

### Changed

- Replace the fixed 64-sender readiness mask with dynamically growing
  hierarchical bitmaps and bounded per-page queues.
- Keep steady-state readiness indexing allocation-free while the sender and
  receiver topology is unchanged.
- Document per-sender high-water marks, MPMC staging, transient empty receives,
  lock boundaries, allocation boundaries, and fairness behavior.

### Fixed

- Prevent dynamic MPSC registration from losing readiness when it races a
  receiver claiming a ready group or page.
- Prevent MPMC receivers from reporting disconnection while prefetched or
  concurrently published work remains reachable.
- Harden sender/receiver parking, disconnect wakeups, receiver handoff, lane
  reuse, and ready-page requeue behavior.

### Validation

- Add bounded Loom models for readiness, publication, registration, parking,
  disconnect, topology growth, lane reuse, and MPMC handoff races.
- Add Miri ownership/layout coverage and randomized native stress models for
  dynamic endpoints, timeout races, topology boundaries, starvation, and exact
  delivery.

## [0.1.0] - 2026-07-29

- Initial bounded, nonblocking MPSC implementation with a fixed sender limit.

[Unreleased]: https://github.com/paddor/fanring.rs/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/paddor/fanring.rs/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/paddor/fanring.rs/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/paddor/fanring.rs/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/paddor/fanring.rs/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/paddor/fanring.rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/paddor/fanring.rs/tree/v0.1.0
