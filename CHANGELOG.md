# Changelog

All notable changes to this project are documented here.

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

[0.2.0]: https://github.com/paddor/fanring.rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/paddor/fanring.rs/tree/v0.1.0
