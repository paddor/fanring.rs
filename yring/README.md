# yring

Bounded SPSC ring buffer with ypipe-style batched flush/prefetch.

Requires Rust 1.93 or newer.

## Batched publication

The producer writes into its private window, then `flush()` publishes the
batch. The consumer `prefetch()`es published values, pops from its cached
window, and `release()`s consumed slots back to the producer.

| Operation | Effect |
| --- | --- |
| `push()` | Writes a value; cached space needs no atomic operation |
| `flush()` | Publishes pending writes with release ordering |
| `prefetch()` | Acquires a window of published values |
| `pop()` | Reads from that window without atomic operations |
| `release()` | Publishes consumed capacity with release ordering |

Each endpoint caches its own position and the opposite endpoint's published
boundary. Synchronization happens at batch boundaries. The ring is bounded,
with capacity rounded up to a power of two.

## Usage

```rust
let (mut producer, mut consumer) = yring::spsc(1024);

// Producer: push with zero atomics, flush once per batch
for i in 0..100 {
    producer.push(i).unwrap();
}
producer.flush(); // one Release store makes all 100 items visible

// Consumer: prefetch with one Acquire load, pop with zero atomics
consumer.prefetch(); // one Acquire load
while let Some(val) = consumer.pop() {
    // process val
}
consumer.release(); // one Release store frees slots for producer
```

`pop_into()` moves the prefetched window into a `Vec` in one step instead
of popping items one at a time:

```rust
let (mut producer, mut consumer) = yring::spsc(1024);
for i in 0..100 {
    producer.push(i).unwrap();
}
producer.flush();

let mut batch = Vec::with_capacity(64);
consumer.prefetch();
assert_eq!(consumer.pop_into(&mut batch, 64), 64);
consumer.release();
assert_eq!(batch.len(), 64);
```

`pop_into_while()` moves only the prefix that an admission predicate accepts.
The predicate sees each item in place, and the first rejected item stays at
the front of the window:

```rust
let (mut producer, mut consumer) = yring::spsc(8);
for i in 0..6u32 {
    producer.push(i).unwrap();
}
producer.flush();

let mut batch = Vec::new();
consumer.prefetch();
assert_eq!(consumer.pop_into_while(&mut batch, 8, |i| *i < 3), 3);
assert_eq!(batch, [0, 1, 2]);
assert_eq!(consumer.pop(), Some(3));
consumer.release();
```

## Sharing the producer handle

`Producer` needs `&mut self` for writes. To write through a handle stored in
an `Arc`, wrap it in `ProducerOwner`:

```rust
use std::sync::Arc;

let (producer, mut consumer) = yring::spsc(1024);
let producer = Arc::new(yring::ProducerOwner::new(producer));

let producer_thread = producer.clone();
std::thread::spawn(move || {
    producer_thread.push(42).unwrap();
    producer_thread.flush();
})
.join()
.unwrap();

consumer.prefetch();
assert_eq!(consumer.pop(), Some(42));
consumer.release();
```

`ProducerOwner` preserves the producer's non-atomic cursor while allowing the
handle itself to be shared. The first producer call binds it to the current
thread. Later calls from another thread panic. Multiple owners are fine, but
all producer calls must come from the same thread. For producer access from
multiple threads, use a channel with a multi-producer API instead.
Thread tokens never wrap: allocation panics if the token range is exhausted.

## Backpressure

`push()` returns `Err(val)` when the ring is full. `pop()` returns
`None` when the prefetched window is exhausted. Neither side blocks or
spins internally.

The `async` feature (opt-in) adds `AsyncProducer`/`AsyncConsumer` with
waker integration: the producer wakes the consumer on flush, the
consumer wakes the producer on release.

`AsyncConsumer`'s `Stream` implementation releases each item before returning
it. Use `prefetch()`/`pop()`/`release()` directly for batched release.
`push_async()` buffers without flushing; flush pending items before awaiting
space in a full ring. `AsyncProducer::poll_ready()` registers a capacity waker
without taking a value; ready also covers consumer shutdown. Check
`is_consumer_dropped()` before retrying admission.

## Wakeup hints

These helpers provide conservative wake hints, including registered waiters.
Signal whenever a hint is true; it is not an exact empty/full snapshot.

| Helper | Hint |
| --- | --- |
| `flush_and_check()` | `FlushResult::Flushed { was_empty, .. }`: consumer may need a wake |
| `prefetch_and_pop_with_full()` | Returned boolean: producer may need a wake |
| `release_with_full()` | Producer may need a wake after capacity publication |

Using the helpers enables opposite-endpoint waiter registration. Hints stay
conservatively true until that endpoint acknowledges activation. Ordinary
flush/release avoid this registration protocol when helpers are unused.

Once enabled, an empty `prefetch()` or `is_empty()` check registers a data
waiter and rechecks publication. A full `push()` or `is_full()` check registers
a space waiter and retries. Register an external waker before the final queue
check, or use a stateful notification primitive. Exhausting the cached `pop()`
window alone does not register a waiter.

`release_with_full()` publishes only consumed slots. The application chooses
its batch boundaries; the ring sets no release watermark. With no newly
consumed slots, it returns false and preserves the producer registration.
Ordinary `flush()` and `release()` each use a release store when a separate
protocol handles signaling.

## Correctness checks

The unsafe ring core is checked with Miri:

```sh
cargo +nightly miri test -p yring
```

The Loom suite models SPSC cursor ordering, wraparound, producer drop,
`push_async` wakeups, and upper-layer readiness patterns:

```sh
RUSTFLAGS="--cfg loom" cargo test -p yring --features async --test loom
```

Loom builds model `AtomicWaker`'s documented registration/wake ordering with
a Loom mutex. Production builds use `atomic-waker` directly. The models cover
ring ordering and wake delivery while assuming that dependency's contract.

## Benchmarks

From the workspace root:

```sh
cargo bench -p yring --bench yring_comparison
cargo bench -p yring --bench yring_throughput
python3 yring/scripts/gen_chart.py
```

The comparison harness covers cross-thread traffic with several payload sizes
and batch sizes, using repeated samples and pinned threads. The throughput
harness varies ring capacity and publication batch size.

![SPSC throughput comparison](doc/spsc_comparison.svg)

[Benchmark notes](../BENCHMARKS.md#spsc-measurements) describe the measurements.

[API reference](https://docs.rs/yring) and
[changelog](CHANGELOG.md) provide further detail.

## License

[ISC](LICENSE)
