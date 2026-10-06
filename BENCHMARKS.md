# Benchmarks

Measurements collected on 2026-10-06 from library sources at
`7861646`, including the
MPSC lane-control additions. The charted values are medians of repeated samples.
Hardware and affinity are shown in each chart.

## Throughput

| Setting | MPSC and MPMC comparisons |
| --- | --- |
| Measurement | Five one-second samples per case |
| Warmup | 250 ms per implementation and configuration |
| Nominal capacity | 8192 items |
| Producers | 1, 2, 4, 8 |
| MPMC consumers | 1, 2, 4, 8 |
| Payloads | `u64`, 64 bytes, 256 bytes |
| Queue occupancy | Natural; nonblocking operations |
| Affinity | Workers pinned to CPUs 0-5; configurations with more workers share CPUs |

Implementation order rotates between samples. Every sample drains accepted
values and checks sent/received counts. Fanring capacity is per sender ring;
MPMC staging adds storage outside those bounds. The comparison channels use a
shared queue bound. Details are in [DEVELOPMENT.md](DEVELOPMENT.md#benchmarks).

### Summary

![Common MPSC and MPMC cases](doc/charts/throughput-summary.svg)

### MPSC

![MPSC throughput](doc/charts/throughput-mpsc.svg)

### MPMC

![MPMC throughput](doc/charts/throughput-mpmc.svg)

## Blocking wake latency

Capacity-one channels measure a blocked receiver woken by send and a blocked
sender woken by receive. Each direction has 10,000 rounds after 200 warmup
rounds, with a 25 microsecond active settling interval. Communicating threads
use distinct physical cores. The `fanring spin` series uses
`WaitStrategy::SpinFor` with a 50 microsecond budget.

![MPSC wake latency](doc/charts/latency-mpsc.svg)

![MPMC wake latency](doc/charts/latency-mpmc.svg)

## Teardown policy throughput

These charts select the `u64` rows for `Deferred` and `Coordinated` from the
same throughput sweeps above. Each cell shows median throughput and relative
median absolute deviation. They measure steady traffic, including ordinary
draining.

![MPSC teardown policy throughput](doc/charts/teardown-mpsc.svg)

![MPMC teardown policy throughput](doc/charts/teardown-mpmc.svg)

[Teardown ownership](DESIGN.md#teardown-policies) describes the contracts.
[Reproduction commands](DEVELOPMENT.md#teardown-policy-comparisons) also run an
owned-payload probe against the dependency versions in `Cargo.lock`.

## SPSC measurements

The yring comparison uses five two-second samples per case after 250 ms warmup,
capacity 1024, and publication batches of 1 or 64. Producer and consumer threads
are pinned to CPUs 0 and 1. Every sample checks the complete sent/received count.
Implementation order rotates between samples. The chart includes all four
payload sizes and plots medians from cached JSONL rows.

![SPSC throughput comparison](yring/doc/spsc_comparison.svg)

The harness appends raw rows to `~/.cache/yring/comparison.jsonl`. The generator
selects a complete run with matching configuration and distinct CPUs.
[Development commands](DEVELOPMENT.md#spsc-comparison) reproduce the benchmark
and chart; `--results` selects a different result file.

## Run records

This refresh's raw rows and source hashes are under
`~/.cache/fanring/chart-refresh-20261006-0109/`. `provenance.json` records the
library sources, benchmark harness, and configuration. The SPSC data is in
`spsc-comparison.jsonl`; throughput and latency data retain the usual
per-implementation JSONL layout.
