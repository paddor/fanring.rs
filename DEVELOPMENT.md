# Development

## Checks

```sh
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
RUSTFLAGS="--cfg loom" cargo test --lib --test loom -- --test-threads=1
```

The test suite covers per-sender FIFO, per-sender backpressure, disconnect
behavior, drop cleanup, sparse lane use, MPMC receiver churn, and the 64-bit
ready mask. Small private wait-cell Loom models are exhaustive. End-to-end
channel models use a preemption bound of two and at most 10,000 permutations.

`fanring` forbids direct `unsafe` code. Slot safety is delegated to
`yring`, which has its own Miri/Loom coverage. Crossbeam deque and `ArcSwap`
internals are not Loom-instrumented here; normal stress tests cover their
integration with fanring.

## Benchmarks

```sh
cargo bench --bench comparison
cargo bench --bench mpmc
```

The benchmark compares `fanring` against:

- `crossbeam-channel`
- `flume`
- `kanal`
- `concurrent-queue`
- `thingbuf`

Defaults:

- warmup: 250 ms per implementation and configuration
- measurement: 5 samples of 1 second each
- producers: 1, 2, 4, 8
- MPMC consumers: 1, 2, 4, 8
- capacity: 8192 total items
- payloads: `u64`, `[u8; 64]`, `[u8; 256]`

Worker threads synchronize on a start barrier. Implementations rotate order
between samples. Shutdown and queue draining are included in elapsed time, and
each run asserts that sent and received counts match. Output includes every
sample and is appended to `target/fanring-bench/results.jsonl` or
`target/fanring-bench/mpmc.jsonl`.

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
FANRING_BENCH_OUT=target/fanring-bench/focused.jsonl \
cargo bench --bench comparison
```

## Charts

Generate an SVG from the latest benchmark run:

```sh
cargo run --features charts --bin fanring-chart
```

Default output: `doc/charts/mpsc.svg`.

The chart tool selects the latest complete run, aggregates samples by median,
and shows relative median absolute deviation below each throughput value.

Custom paths:

```sh
cargo run --features charts --bin fanring-chart -- \
  --input target/fanring-bench/results.jsonl \
  --output doc/charts/mpsc.svg

cargo run --features charts --bin fanring-chart -- \
  --input target/fanring-bench/mpmc.jsonl \
  --output doc/charts/mpmc.svg
```
