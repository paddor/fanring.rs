# fanring

Typed MPSC and MPMC channels with one bounded SPSC ring per producer.
Each sender writes to its own ring; the receive side batches work across lanes.
Senders can register dynamically.

Requires Rust 1.93 or newer. The workspace also contains
[`yring`](yring/README.md), published independently.

## Quick start

```rust
use fanring::mpsc;

let (mut tx, mut rx) = mpsc::channel(256);
let mut other = tx.try_clone().expect("receiver alive");

tx.send("first lane").unwrap();
other.send("second lane").unwrap();
drop(tx);
drop(other);

let mut messages = Vec::with_capacity(32);
while rx.recv_batch_into(&mut messages, 32).is_ok() {
    for message in messages.drain(..) {
        println!("{message}");
    }
}
```

Use `mpmc::channel` when multiple receivers need to share work.
Enable the optional `async` feature for runtime-independent MPSC futures.

## Channel contract

| | MPSC | MPMC |
| --- | --- | --- |
| Receivers | One | Cloneable |
| Ordering | FIFO within each sender lane | Relaxed |
| Capacity | Per sender, rounded up to a power of two | Per sender, plus receiver staging |
| Receive scheduling | Bounded bursts; optional one-value rotation | Lane claims and batch stealing |
| Per-lane pause/resume | Yes; optional source IDs on receive | No |
| Async | Optional `async` feature | Synchronous |

Both channels support nonblocking, blocking, timeout, and deadline operations.
Blocking endpoints briefly retry before parking and have configurable wait
strategies. Sender registration and topology maintenance use locks; MPMC also
synchronizes receiver staging.

Dropping the last sender lets receivers drain buffered values. Dropping the
last receiver disconnects senders. The default `Deferred` teardown can retain
unread ring values until their sender drops. Choose `Coordinated` when payload
cleanup must be independent of idle sender lifetimes.

These channels suit per-producer backpressure and batched processing. They do
not provide global FIFO, strict round robin by default, or one exact capacity
shared across all producers.

## Benchmarks

![Common MPSC and MPMC benchmark cases](doc/charts/throughput-summary.svg)

[Benchmark results](BENCHMARKS.md) include detailed throughput, wake latency,
and teardown-policy charts. [DEVELOPMENT.md](DEVELOPMENT.md#benchmarks)
describes the measurement harness and reproduction commands.

## Documentation

- [Getting started](GETTING_STARTED.md): registration, batching, lane control,
  async operations, and teardown choices.
- [Design](DESIGN.md): ownership, publication, scheduling, and teardown.
- [API reference](https://docs.rs/fanring): published API.
- [Development](DEVELOPMENT.md): checks, benchmarks, and releases.
- [Changelog](CHANGELOG.md): release history.

## License

[ISC](LICENSE)
