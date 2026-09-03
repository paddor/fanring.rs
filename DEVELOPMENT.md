# Development

## Checks

```sh
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
RUSTFLAGS="--cfg loom" cargo test --lib --test loom -- --test-threads=1
cargo +nightly miri test --all-features -- --test-threads=1
MIRIFLAGS="-Zmiri-tree-borrows" \
  cargo +nightly miri test --all-features -- --test-threads=1
```

The test suite covers per-sender FIFO, per-sender backpressure, disconnect and
timeout races, drop cleanup, dynamic lane registration and reuse, ready
page/group boundaries, starvation bounds, MPMC batch stealing and receiver
churn, randomized endpoint state machines, unusual value layouts, and the
64-bit ready mask. Small private wait-cell, readiness, and publication-tracker
Loom models are exhaustive. End-to-end channel models use a preemption bound
of two and at most 10,000 permutations.

Loom reduces pages and groups to two entries and the MPMC work-queue capacity
and release batch to two, so small models cross topology and requeue
boundaries. `LOOM_MAX_BRANCHES`,
`LOOM_MAX_PERMUTATIONS`, and `LOOM_MAX_PREEMPTIONS` override the end-to-end
defaults for deeper local runs.

`fanring` forbids direct `unsafe` code. Slot safety is delegated to
`yring`, which has its own Miri/Loom coverage. MPMC lane-token queues use
`concurrent-queue`'s Loom backend, while receiver work queues use the
compatibility mutex and are directly modeled. `ArcSwap` topology publication
is covered by normal stress tests and sanitizer runs.

## Release

`release-plz` runs on every push to `main`
(`.github/workflows/release-plz.yml`). It opens or updates a release PR. After
that PR merges, it creates an annotated `v<version>` tag, publishes to
crates.io, and creates a GitHub release. Configuration lives in
`release-plz.toml`; changelogs remain hand-curated.

Publishing uses crates.io trusted publishing through GitHub Actions OIDC.
Configure the trusted publisher with:

```text
GitHub owner: paddor
GitHub repository: fanring.rs
Workflow filename: release-plz.yml
Environment name: (none)
```

Review the release-plz PR, verify its semver bump, and move the relevant
`Unreleased` changelog entries into a dated version section. Before merging the
release PR, run:

```sh
cargo +1.93.0 test --all-features --locked
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
cargo package --locked
cargo publish --dry-run --locked
```

Merging the release PR is the explicit publish step. CI then tags and publishes
the version through trusted publishing.

## Benchmarks

```sh
cargo bench --bench comparison
FANRING_BENCH_MODE=blocking cargo bench --bench comparison
cargo bench --bench mpmc
FANRING_BENCH_MODE=blocking cargo bench --bench mpmc
cargo bench --bench wake_latency
```

The benchmark compares `fanring` against:

- `crossbeam-channel`
- `crossfire`
- `flume`
- `kanal`
- `concurrent-queue`
- `thingbuf`

Defaults:

- warmup: 250 ms per implementation and configuration
- measurement: 5 samples of 1 second each
- producers: 1, 2, 4, 8
- MPMC consumers: 1, 2, 4, 8
- nominal capacity: 8192 items
- payloads: `u64`, `[u8; 64]`, `[u8; 256]`
- affinity: pin benchmark threads to physical cores first, then SMT siblings

Default runs let queue occupancy vary naturally (`uncontrolled`). Set
`FANRING_BENCH_PROFILE=saturated` with nonblocking mode to cycle occupancy from
full toward half capacity. Producers report actual full events. Consumers drain
half of nominal capacity, then wait until every producer has observed a full
queue again. This adds no shared atomic operation per item. Blocking mode does
not support this profile because blocking sends do not expose full events.

Implementations rotate order between samples. Shutdown and queue draining are
included in elapsed time. Every run asserts that sent and received counts
match. Every sample is appended to
`~/.cache/fanring/<implementation>/throughput-{mpsc,mpmc}.jsonl`.

JSONL rows record `nominal_capacity`, `capacity_model`, `affinity`,
`throughput_profile`, `low_watermark`, and `high_watermark`. Fanring uses a per-ring HWM. MPMC
receiver staging is additional. Competing channels use one shared bound in
these benchmarks. The chart reader accepts legacy `total_capacity` rows but
does not combine uncontrolled and saturated throughput runs.

Comparison benches accept `FANRING_BENCH_MODE`, `FANRING_BENCH_SECS`,
`FANRING_BENCH_PRODUCERS`, `FANRING_BENCH_CAPACITY`,
`FANRING_BENCH_PAYLOADS`, `FANRING_BENCH_IMPLS`, `FANRING_BENCH_SAMPLES`,
`FANRING_BENCH_WARMUP_SECS`, `FANRING_BENCH_PROFILE`, and
`FANRING_BENCH_AFFINITY` (`auto` or `off`). MPMC also accepts
`FANRING_BENCH_CONSUMERS`. `auto` is the default and records the exact logical
CPU order in each row. Use `taskset` to restrict the available CPUs.

Wake latency accepts `FANRING_WAKE_ROUNDS`, `FANRING_WAKE_WARMUP`,
`FANRING_WAKE_SETTLE_NS`, and `FANRING_WAKE_SETTLE_MODE` (`sleep` or `spin`). It
measures both a blocked receiver woken by a send and a blocked sender woken by
a receive on capacity-one channels. Results are appended to
`~/.cache/fanring/<implementation>/latency-{mpsc,mpmc}.jsonl`.

`FANRING_BENCH_CACHE_DIR` overrides the `~/.cache/fanring` result root for every
benchmark and the chart generator. Result files are append-only.

Short smoke run:

```sh
FANRING_BENCH_SECS=0.1 \
FANRING_BENCH_SAMPLES=1 \
FANRING_BENCH_WARMUP_SECS=0 \
cargo bench --bench comparison
```

Focused run:

```sh
FANRING_BENCH_PAYLOADS=bytes64 \
FANRING_BENCH_PRODUCERS=8 \
FANRING_BENCH_IMPLS=fanring,crossbeam-channel \
cargo bench --bench comparison
```

Saturated occupancy run:

```sh
FANRING_BENCH_PROFILE=saturated cargo bench --bench comparison
```

## Charts

Generate an SVG from the latest benchmark run:

```sh
cargo run --example fanring-chart
```

Default output: `doc/charts/throughput-mpsc.svg`.

The chart tool reads every implementation's append-only result file. It merges
the latest compatible complete per-implementation runs, aggregates samples by
median, and shows relative median absolute deviation below each throughput
value. An explicit run ID selects one complete run and may select blocking data.

Generate the MPMC detail chart:

```sh
cargo run --example fanring-chart -- --mpmc
```

Generate the two-topology summary chart from the latest complete nonblocking
MPSC and MPMC runs:

```sh
cargo run --example fanring-chart -- --summary
```

Default output: `doc/charts/throughput-summary.svg`.

Generate blocking wake-latency charts from the latest complete run:

```sh
cargo run --example fanring-chart -- --latency mpsc
cargo run --example fanring-chart -- --latency mpmc
```

Default outputs: `doc/charts/latency-mpsc.svg` and
`doc/charts/latency-mpmc.svg`.

Chart subtitles use the ignored `.chart_hw` file in the repository root:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=6 physical cores / 12 threads, performance governor, turbo off
```

`FANRING_HW_LABEL` overrides the complete label. `FANRING_HW_PREFIX` and
`FANRING_HW_POSTFIX` override the corresponding file values.

Use a different result root:

```sh
cargo run --example fanring-chart -- \
  --results-dir /path/to/fanring-results \
  --output /path/to/throughput-mpsc.svg

cargo run --example fanring-chart -- \
  --summary \
  --results-dir /path/to/fanring-results \
  --output /path/to/throughput-summary.svg

cargo run --example fanring-chart -- \
  --latency mpsc \
  --results-dir /path/to/fanring-results \
  --run RUN_ID \
  --output /path/to/latency-mpsc.svg
```
