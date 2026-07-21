# fanring

Fast bounded MPSC channel built from one SPSC ring per producer.

Each sender writes to its own ring. One receiver polls ready rings with a
bitmask and round-robin cursor. This avoids producer-vs-producer contention on a
shared queue tail.

## Contract

- Bounded, non-blocking MPSC.
- `try_send` returns immediately with `Full` instead of blocking.
- `try_recv` returns immediately with `Empty` instead of blocking or awaiting.
- FIFO per sender.
- Relaxed ordering across senders.
- Best-effort round-robin across ready senders.
- Fixed sender limit, currently `<= 64`.
- Capacity is per sender: `max_senders * capacity_per_sender`.
- No `unsafe` in this crate; ring storage is delegated to `yring`.

## Good Fit

- Long-lived producers registered up front.
- One receiver doing high-rate fan-in.
- Per-producer FIFO is enough.
- Bounded per-producer queues are acceptable.
- Caller wants to own backoff policy: spin, yield, sleep, park, or async wrapper.

## Bad Fit

- Need multiple receivers or load-balanced consumers.
- Need global FIFO across all producers.
- Need strict fairness across producers.
- Need built-in blocking or async wakeups.
- Producer set churns heavily.
- One producer may burst much harder than others and should borrow idle capacity
  from other producers.
- Need each received item to return capacity immediately. Capacity is released
  after a receive batch or when an internal prefetch window drains.
- Sender slots must be reused after senders are dropped.

## Tradeoff

The speed comes from a narrower contract:

- no shared producer tail
- no global FIFO
- no strict fairness
- no dynamic capacity sharing
- no immediate per-message capacity release
- no built-in parking/waking

Benchmark chart: [doc/charts/mpsc.svg](doc/charts/mpsc.svg)

More detail: [DESIGN.md](DESIGN.md)
