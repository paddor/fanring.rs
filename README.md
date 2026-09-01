# fanring

Fast typed MPSC and MPMC channels built from one SPSC `yring` per producer.

Sender registration is dynamic. Each sender writes to its own ring, so
producers do not contend on a shared queue tail. Choose `mpsc` for one consumer
or `mpmc` for cloneable competing consumers.

Requires Rust 1.93 or newer.

## Performance

#### MPSC

![MPSC benchmark chart](https://raw.githubusercontent.com/paddor/fanring.rs/main/doc/charts/mpsc.svg)

#### MPMC

![MPMC benchmark chart](https://raw.githubusercontent.com/paddor/fanring.rs/main/doc/charts/mpmc.svg)

## MPSC

```rust
use fanring::mpsc::channel;

let (mut tx0, mut rx) = channel(8);
let mut tx1 = tx0.try_clone().expect("receiver alive");

tx0.send("from sender 0").unwrap();
tx1.send("from sender 1").unwrap();

for _ in 0..2 {
    println!("{}", rx.recv().unwrap());
}

assert_eq!(tx0.send("still open"), Ok(()));
drop(rx);
assert_eq!(tx0.send("closed").unwrap_err().into_inner(), "closed");
```

`mpsc` keeps receive-side batching entirely inside `recv`/`try_recv`. It
preserves FIFO within each sender lane and relaxes order across senders.

## MPMC

```rust
use fanring::mpmc::channel;

let (mut tx, mut rx0) = channel(256);
let mut rx1 = rx0.clone();

tx.send("work 0").unwrap();
tx.send("work 1").unwrap();

let a = rx0.recv().unwrap();
let b = rx1.recv().unwrap();
assert_ne!(a, b);
```

`mpmc` drains ready sender rings in batches of at most 64. One item is returned
and the rest enter the receiver's local FIFO, where other receivers can steal
them in batches. Receiver drop republishes its buffered work. Ordering is
relaxed. Moving values into that second-stage queue costs more for large inline
types; box large payloads when move bandwidth dominates.

`mpmc::try_recv` may return a transient `Empty` while bounded lane maintenance
or another receiver moves work. `Disconnected` is final: all senders are gone,
sender rings are drained, no staged work remains, and no work publication is in
flight.

## Contract

- Per-producer-HWM MPSC or MPMC with blocking, timeout, and non-blocking
  operations.
- No configured producer limit.
- One bounded SPSC ring per live sender.
- Capacity is one HWM per sender and is rounded up to a power of two.
- MPMC receiver staging is outside sender-ring HWM. Total resident items can
  temporarily exceed the sum of sender-ring capacities.
- `try_send` returns `Full` when that sender's ring is full.
- MPSC `try_recv` returns `Empty` when no active ring has visible data.
- MPMC `try_recv` can also return `Empty` while bounded lane maintenance or a
  competing receiver moves internal work.
- Receiver `is_disconnected` becomes true as soon as all senders are dropped;
  buffered values remain readable afterward.
- `send` and `recv` park only after the corresponding try operation fails.
- Blocking operations spin briefly before parking.
- `send_timeout` and `recv_timeout` bound that parked wait.
- MPSC is FIFO per sender; MPMC ordering is relaxed.
- Active sender lanes are served in batches of at most 64 items.
- Dropped sender slots are reused after their rings drain.
- Receive-side batching is internal to `recv`/`try_recv`.
- Steady successful sends and prefetched value pops take no explicit mutex.
  Registration, ready-page activation, lane idle transitions, receiver
  maintenance, and parking may lock.
- Ready indexing uses fixed-capacity page queues and hierarchical bitmaps. It
  does not allocate while the producer topology is unchanged. Registration,
  receiver creation or teardown, and registry growth may allocate.
- Try operations are lock-free on their common paths, but not wait-free.
- `unsafe` is forbidden in this crate. Ring storage is delegated to `yring`.

## Good Fit

- Many long-lived or frequently changing producers feeding one or more
  consumers.
- MPSC callers need only per-producer FIFO; MPMC callers accept relaxed order.
- Per-producer HWM matches the desired backpressure model.
- Callers can use built-in parking or own the backoff policy around `try_*`.

## Bad Fit

- Need global FIFO or strict one-item round robin.
- Need one exact capacity shared across all producers.
- Need an exact total MPMC bound that includes receiver staging.
- Need async wakeups.

More detail: [DESIGN.md](DESIGN.md)

Release history: [CHANGELOG.md](CHANGELOG.md)

## Benchmarks

```sh
cargo bench --bench comparison
FANRING_BENCH_MODE=blocking cargo bench --bench comparison
cargo bench --bench mpmc
FANRING_BENCH_MODE=blocking cargo bench --bench mpmc
cargo bench --bench wake_latency
```

Comparison benches accept `FANRING_BENCH_SECS`, `FANRING_BENCH_PRODUCERS`,
`FANRING_BENCH_CAPACITY`, `FANRING_BENCH_PAYLOADS`,
`FANRING_BENCH_IMPLS`, `FANRING_BENCH_SAMPLES`,
`FANRING_BENCH_WARMUP_SECS`, and `FANRING_BENCH_OUT`. MPMC also accepts
`FANRING_BENCH_CONSUMERS`. Defaults are five one-second samples after a 250 ms
warmup. Implementation order rotates between samples. Output includes every
sample; summaries and charts use the median and relative median absolute
deviation.

Wake latency accepts `FANRING_WAKE_ROUNDS`, `FANRING_WAKE_WARMUP`,
`FANRING_WAKE_SETTLE_NS`, `FANRING_WAKE_SETTLE_MODE` (`sleep` or `spin`), and
`FANRING_WAKE_OUT`. All benchmarks append machine-readable JSONL.
