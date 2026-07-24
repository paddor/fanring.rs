# Development

## Checks

```sh
cargo test --all-targets --features charts
RUSTFLAGS="--cfg loom" cargo test --test loom
cargo clippy --all-targets --features charts -- -D warnings
```

The test suite covers per-sender FIFO, per-sender backpressure, disconnect
behavior, drop cleanup, sparse shard use, and the 64-bit ready mask.
The Loom test models the ready-bit race and receiver drop handoff.

`fanring` forbids direct `unsafe` code. Slot safety is delegated to
`yring`, which has its own Miri/Loom coverage.

## Benchmarks

```sh
cargo bench --bench comparison
```

The benchmark compares `fanring` against:

- `crossbeam-channel`
- `flume`
- `kanal`
- `concurrent-queue`
- `thingbuf`

Defaults:

- duration: 2 seconds per configuration
- producers: 1, 2, 4, 8
- capacity: 1024 per producer for `fanring`, same total capacity for
  shared-queue baselines
- payloads: `u64`, `[u8; 64]`, `[u8; 256]`

Output is appended to `target/fanring-bench/results.jsonl`.

Short smoke run:

```sh
FANRING_BENCH_SECS=0.1 cargo bench --bench comparison
```

Focused run:

```sh
FANRING_BENCH_PAYLOADS=bytes64 \
FANRING_BENCH_PRODUCERS=8 \
FANRING_BENCH_IMPLS=fanring,crossbeam-channel \
cargo bench --bench comparison
```

## Charts

Generate an SVG from the latest benchmark run:

```sh
cargo run --features charts --bin fanring-chart
```

Default output: `doc/charts/mpsc.svg`.

Custom paths:

```sh
cargo run --features charts --bin fanring-chart -- \
  --input target/fanring-bench/results.jsonl \
  --output doc/charts/mpsc.svg
```
