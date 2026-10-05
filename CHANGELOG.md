# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added

- MPSC `Receiver::poll_all_lanes` discovers unsignaled publications for scalar
  and tagged receives while preserving paused lanes and receive rotation.
- MPSC `Sender::try_register_with_capacity` registers a lane with independent
  ring capacity. Ordinary registrations retain the channel's original capacity.
- MPSC `Sender::lane` returns an opaque, Copy, generation-safe `LaneId`.
  Channel identities never wrap; exhausted lane generations are not reused.
- MPSC `Receiver::with_lane_ids` borrows a receive view returning `(LaneId, T)`
  from ordinary, fair, blocking, timed, and asynchronous single-value receives.
  Plain receives share the drain implementation without constructing lane IDs.
- MPSC receivers can pause/resume ordinary drainage, receive from one lane,
  and close a lane independently. Paused lanes retain FIFO and backpressure;
  targeted receives remain available and stale IDs cannot select reused slots.

### Fixed

- Closing a deferred MPSC lane rejects subsequent sends even when the producer
  still has cached space. Signaled, unsignaled, and deferred sends check the
  lane's own disconnect state.

## [0.3.8] - 2026-10-04

### Changed

- Require yring 0.3.19 with caller-controlled consumer batch release hints
  and asynchronous producer capacity polling.

## [0.3.7] - 2026-09-26

### Added

- Add endpoint-local `WaitStrategy::SpinFor` for bounded active spinning before
  synchronous send and receive operations park.
- MPSC `Receiver::try_recv_batch_into` appends a bounded batch without
  blocking, releases consumed slots before returning, and reports `Empty` or
  `Disconnected` when nothing was appended.
- MPSC `Sender::is_full` reports whether the sender's lane can take another
  value, so lossy producers can skip building a value they would drop.
- MPSC `Receiver::try_recv_batch_into_while` takes an admission predicate that
  sees each value in place and stops the batch at the first rejected value,
  which stays queued. Accepted windows still move in bulk. The predicate may
  see a value again on a later call. Requires yring 0.3.18.
- MPSC `Sender::try_send_unsignaled` publishes a value without marking its
  lane ready or waking the receiver, which skips the per-send atomic
  read-modify-write under `Deferred` teardown. The caller provides the wakeup
  and the fences it needs. Signaled and unsignaled sends may share a lane.
- MPSC `Receiver::try_recv_scan_into_while` visits every registered lane
  instead of relying on readiness, so it finds unsignaled values. Each lane
  stays queued at most once. When it returns fewer values than requested
  without a rejection, every lane registered when the call started was
  observed empty during the call.

### Changed

- MPSC single-value receive credits scale to half-ring batches, independent of
  the 64-item fairness burst. Rings above 128 slots resume full producers at
  the half-full low watermark; smaller rings retain their existing batch size.
  Empty-lane handling and explicit, bulk, and async receive flushes still
  release partial credits; capacity-one channels and teardown still wake
  immediately when progress is possible.
- MPSC `recv_batch_into` and `recv_batch_into_async` move whole prefetched
  windows out of sender rings with `yring::Consumer::pop_into` instead of
  popping one value at a time. Lane rotation, readiness polling, slot release,
  and per-sender FIFO order match repeated `try_recv` calls. Requires yring
  0.3.18.

### Fixed

- MPMC receivers return consumed ring slots once they take a lane's whole
  prefetched window, not only after `min(64, capacity)` values. A lane whose
  sender keeps few values outstanding no longer reports `Full` below its
  capacity.

## [0.3.6] - 2026-09-19

### Added

- `Receiver::try_recv_fair` rotates after each received value so ready sender
  lanes take turns, while preserving FIFO order within each sender.
- `Sender<Deferred>::try_send_deferred` and `flush` queue multiple values
  before one publication and receiver wakeup.

### Fixed

- Advance ready-group traversal after scanning sender readiness.

## [0.3.5] - 2026-09-15

### Added

- Optional runtime-independent `async` MPSC send, receive, and bounded batch
  receive, with per-producer capacity wakeups and cancellation-safe waits.
- `try_register_bounded`, `registered_lanes`, and `TryRegisterBoundedError`
  bound live and retired producer rings without changing `TryRegisterError`.

## [0.3.4] - 2026-09-10

### Added

- MPSC `Receiver::release_consumed` publishes consumed slots across sender
  lanes and notifies blocked senders, including after partial receive batches.
- MPSC `Receiver::recv_batch_into` appends a bounded batch to a caller-owned
  vector, waits only for the first value, and releases consumed slots before
  returning.

## [0.3.3] - 2026-09-08

### Added

- Per-channel `Deferred` and `Coordinated` teardown policies, selected with
  `channel_with_policy` or `try_channel_with_policy`. `Deferred` is the default
  and preserves the send path without cleanup coordination.
- `Coordinated` teardown destroys unread payloads while sender handles remain
  alive. Overlapping sends reclaim late publications when they resume, without
  making receiver teardown wait for a paused producer.

## [0.3.2] - 2026-09-08

### Changed

- Require yring 0.3.16 with corrected data/space wakeup handshakes.
- Keep MPSC lane polling inlined when underlying ring operations grow, avoiding
  extra receive calls and intermediate payload moves.

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

[Unreleased]: https://github.com/paddor/fanring.rs/compare/fanring-v0.3.6...HEAD
[0.3.6]: https://github.com/paddor/fanring.rs/compare/fanring-v0.3.5...fanring-v0.3.6
[0.3.5]: https://github.com/paddor/fanring.rs/compare/v0.3.4...fanring-v0.3.5
[0.3.4]: https://github.com/paddor/fanring.rs/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/paddor/fanring.rs/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/paddor/fanring.rs/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/paddor/fanring.rs/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/paddor/fanring.rs/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/paddor/fanring.rs/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/paddor/fanring.rs/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/paddor/fanring.rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/paddor/fanring.rs/tree/v0.1.0
