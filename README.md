# fanring

Fast typed MPSC and MPMC channels built from one SPSC `yring` per producer.

Sender registration is dynamic. Each sender writes to its own ring, so
producers do not contend on a shared queue tail. MPSC drains those rings
directly; MPMC stages prefetched batches in stealable receiver-local queues.

Requires Rust 1.93 or newer.

| | Nonblocking | Blocking | Timeout | Deadline |
| --- | --- | --- | --- | --- |
| Send | `try_send` | `send` | `send_timeout` | `send_deadline` |
| Receive | `try_recv` | `recv` | `recv_timeout` | `recv_deadline` |

## Performance

Median throughput and blocking wake latency on the system named in each
chart, measured with the `performance` governor and CPU turbo disabled.
Detailed throughput heatmaps cover thread counts and payload sizes.

![Common MPSC and MPMC benchmark cases](https://raw.githubusercontent.com/paddor/fanring.rs/main/doc/charts/throughput-summary.svg)

#### Detailed MPSC

![MPSC benchmark chart](https://raw.githubusercontent.com/paddor/fanring.rs/main/doc/charts/throughput-mpsc.svg)

#### Detailed MPMC

![MPMC benchmark chart](https://raw.githubusercontent.com/paddor/fanring.rs/main/doc/charts/throughput-mpmc.svg)

#### MPSC wake latency

![MPSC wake-latency chart](https://raw.githubusercontent.com/paddor/fanring.rs/main/doc/charts/latency-mpsc.svg)

#### MPMC wake latency

![MPMC wake-latency chart](https://raw.githubusercontent.com/paddor/fanring.rs/main/doc/charts/latency-mpmc.svg)

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

`mpmc` drains up to 64 values from a ready sender ring. With one receiver, the
remaining values stay in a private deque. Cloning publishes that deque before
the new receiver becomes visible. With multiple receivers, remaining values
enter bounded synchronized work queues, and competitors steal up to eight at a
time. Receiver drop republishes its buffered work. Ordering is relaxed. Moving
values into that second-stage queue costs more for large inline types; box large
payloads when move bandwidth dominates.

`mpmc::try_recv` may return a transient `Empty` while bounded lane maintenance
or another receiver moves work. `Disconnected` is final: all senders are gone,
sender rings and staged queues are drained, and no work publication is in
flight.

## Contract

- One bounded ring per dynamic sender; capacity is a per-sender HWM rounded up
  to a power of two.
- MPSC preserves FIFO within each sender lane. MPMC ordering is relaxed and its
  receiver staging can temporarily exceed sender-ring capacity.
- Nonblocking, blocking, timeout, and deadline operations are available.
  Blocking operations spin briefly before parking.
- Disconnection never discards buffered values. MPMC `Empty` may be transient
  while receivers move internal work; `Disconnected` is final.
- Sender hot paths and MPSC batching avoid a shared queue lock. MPMC work
  distribution and topology maintenance use mutexes; blocking operations may
  park.

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

## Further reading

* More detail: [DESIGN.md](DESIGN.md)
* Development and benchmark reproduction: [DEVELOPMENT.md](DEVELOPMENT.md)
* Release history: [CHANGELOG.md](CHANGELOG.md)

## License

[ISC](LICENSE)
