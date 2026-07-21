# Design

`fanring` is a bounded, sharded MPSC channel built from SPSC rings. Each
registered sender owns one `yring::Producer`; the receiver owns all matching
`yring::Consumer`s.

## Fundamentals

Hot send path:

1. Push into this sender's private ring.
2. Flush the ring tail so the receiver can see the value.
3. Mark the shard ready.

The shared ready state has two layers:

- one padded per-shard `AtomicBool`
- one global `AtomicU64` ready bitmask

Senders only set the global bit on an empty-to-ready transition. While a shard is
already marked ready, sending only needs a cheap load of the per-shard flag. This
keeps producer fan-in away from a shared queue tail.

The receiver swaps the global ready bitmask into a local cache, then polls ready
shards round-robin. It resumes from `next_shard` on each receive, so continuously
ready shards do not suffer fixed-priority starvation. This is best-effort
fairness, not strict fairness: empty shards are skipped, a hot shard can still
produce more total messages than cold shards, and ready-bit races can change the
exact interleaving.

For each ready shard, the receiver pops from the already-prefetched window first.
When that window is empty, it calls `prefetch()`, tracks the visible item count
locally, then serves future `try_recv` calls from that window. `release()` runs
after 64 popped items or when the prefetched window drains, so producer capacity
is returned in batches rather than one item at a time.

The receiver clears a shard's ready flag only after a failed prefetch. After
clearing, it prefetches once more before dropping the bit; this closes the race
with a concurrent sender.

Ordering is FIFO per sender. Ordering across senders is intentionally relaxed.

## Strengths

- Lock-free hot path for sending and receiving.
- No producer-vs-producer contention on a shared queue tail.
- Bounded memory and explicit sender count.
- FIFO per sender.
- Cheap receive-side scheduling via ready bitmask and round-robin cursor.
- Registration uses a mutex, but only when cloning/registering senders.

## Limitations

- MPSC only. No MPMC receiver set yet.
- Max 64 senders because the ready set is a `u64`.
- Dropped sender slots are not reused.
- Capacity is per sender, so total capacity can fragment under uneven load.
- Received slots are released back to the producer after a receive batch or when
  an internal prefetch window drains, not necessarily after every `try_recv`.
- Global FIFO is not provided.
- Fairness is best-effort round-robin, not strict or weighted.
- No blocking or async API yet.
- `fanring` itself contains no `unsafe`; it relies on `yring` for the ring
  implementation.

## Performance

Benchmark chart: [doc/charts/mpsc.svg](doc/charts/mpsc.svg)

Chart axes:

- X-axis: producer threads (`1`, `2`, `4`, `8`)
- Y-axis: million messages/sec, higher is better

Latest 2s comparison run on this VM (`capacity_per_sender = 1024`):

| Payload | Producers | fanring | next best baseline |
|---|---:|---:|---:|
| `u64` | 1 | 184M/s | 30M/s (`crossbeam-channel`) |
| `u64` | 8 | 116M/s | 18M/s (`crossbeam-channel`) |
| 64 B | 1 | 100M/s | 36M/s (`crossbeam-channel`) |
| 64 B | 8 | 84M/s | 10M/s (`crossbeam-channel`) |
| 256 B | 1 | 40M/s | 30M/s (`concurrent-queue`) |
| 256 B | 8 | 37M/s | 9M/s (`crossbeam-channel`) |

These numbers favor fanring when producers are registered up front and send at
high rate. They do not measure blocking wakeups, async integration, or workloads
where one producer needs to borrow unused capacity from another producer's shard.
