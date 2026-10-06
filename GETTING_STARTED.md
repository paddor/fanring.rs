# Getting started

## Choose a channel

`mpsc` has one receiver and preserves FIFO within each sender lane. `mpmc`
allows receiver clones and distributes work with relaxed ordering.

```rust
use fanring::mpmc;

let (mut tx, mut rx0) = mpmc::channel(256);
let mut rx1 = rx0.clone();
tx.send("work 0").unwrap();
tx.send("work 1").unwrap();
assert_ne!(rx0.recv().unwrap(), rx1.recv().unwrap());
```

Each sender owns a separate bounded ring. Cloning a sender registers another
lane and adds capacity. This is a per-producer bound; MPMC also holds staged
values outside the rings. Box large inline payloads when moving them dominates
processing cost.

## Register senders

`try_clone()` registers another sender while receivers are alive. Use
`try_register()` for an error result. For MPSC, `try_register_with_capacity()`
selects a different capacity for the new lane; ordinary clones and registrations
still use the channel's original capacity. Capacities round up to a power of two.

MPSC's `try_register_bounded(max_lanes)` limits registered receive lanes,
including dropped senders awaiting drain. `registered_lanes()` counts those
lanes. Explicitly closed lanes stop counting immediately; `Deferred` teardown
can still retain their unread storage until those senders drop. Ordinary
registration has no configured limit.

## Send and receive

| Operation | Nonblocking | Blocking | Timeout | Deadline |
| --- | --- | --- | --- | --- |
| Send | `try_send` | `send` | `send_timeout` | `send_deadline` |
| Receive | `try_recv` | `recv` | `recv_timeout` | `recv_deadline` |

A full ring applies backpressure to its sender. Send errors return the unsent
value. Dropping the last sender preserves buffered values for receivers to
drain before disconnect. Successful send means acceptance into the channel,
not acknowledgement of application processing.

`Sender::is_full()` checks admission before constructing a value the
application would discard on a full lane.

MPSC serves bounded bursts from ready lanes. `try_recv_fair()` rotates after
one value and collects newly ready lanes on every call. Both preserve FIFO
within each lane. MPMC can report transient `Empty` while another receiver
moves work; `Disconnected` is final.

## Receive MPSC batches

Reserve output storage once. `recv_batch_into()` appends at most the requested
limit, waits only for the first value, and releases consumed slots before
returning. A partial batch succeeds even after disconnect.

```rust
use fanring::mpsc;

let (mut tx, mut rx) = mpsc::channel(8);
for value in 0..6 {
    tx.send(value).unwrap();
}
drop(tx);

let mut batch = Vec::with_capacity(4);
while let Ok(count) = rx.recv_batch_into(&mut batch, 4) {
    assert_eq!(count, batch.len());
    for value in batch.drain(..) {
        // Process value; its ring slot is already reusable.
        println!("{value}");
    }
}
```

`try_recv_batch_into()` is nonblocking. A zero limit receives nothing and
returns `Ok(0)` after releasing consumed slots. Bulk scheduling follows the
same lane order as ordinary single-value receives.

`try_recv_batch_into_while()` adds an admission predicate, for example to
bound both item count and bytes. It inspects each value before moving it and
stops at the first rejection, leaving that value queued. Rejecting the first
value returns `Ok(0)`; `Empty` and `Disconnected` mean no value was available.

```rust
use fanring::mpsc;

let (mut tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
tx.send(vec![0; 32]).unwrap();
tx.send(vec![0; 64]).unwrap();

let mut batch = Vec::with_capacity(8);
let mut bytes_left = 48;
let count = rx.try_recv_batch_into_while(&mut batch, 8, |message| {
    if message.len() > bytes_left {
        return false;
    }
    bytes_left -= message.len();
    true
}).unwrap();
assert_eq!(count, 1);
assert_eq!(rx.try_recv().unwrap().len(), 64);
```

### Publish consumed capacity

Single-value MPSC receives batch slot release. Receiving a value may leave
its slot unavailable to the sender until a later receive reaches the release
boundary or observes the lane empty. Larger rings release around their
half-ring low watermark; small rings release in batches capped at capacity.

Call `rx.release_consumed()` before waiting outside fanring or returning
application permits that allow more sends. It publishes consumed slots across
lanes and wakes waiting senders, preserving unread values. Bulk and async
receives release consumed slots before returning.

## Control individual MPSC lanes

`tx.lane()` identifies one registration in one channel. `rx.with_lane_ids()`
provides a borrowed receive view returning `(LaneId, T)`. Its single-value
nonblocking, fair, blocking, and timed methods share the receiver's scheduling
and slot-release behavior. IDs are `Copy` and retain no allocation.

```rust
use fanring::mpsc;

let (mut tx, mut rx) = mpsc::channel(8);
let mut other = tx.try_clone().unwrap();
tx.send("held").unwrap();
let (lane, held) = rx.with_lane_ids().try_recv_fair().unwrap();

rx.pause(&lane).unwrap();
tx.send("queued while paused").unwrap();
other.send("another lane").unwrap();
assert_eq!(rx.try_recv_fair().unwrap(), "another lane");

// The application owns held; fanring stores only the pause state.
assert_eq!(held, "held");
rx.resume(&lane).unwrap();
assert_eq!(rx.try_recv().unwrap(), "queued while paused");
rx.release_consumed();
```

- `pause(&lane)` skips the lane in ordinary, fair, bulk, scan, and iterator
  receives. Its sender can fill the ring and then encounters backpressure.
- `try_recv_from(&lane)` drains that lane even while paused, without resuming it.
- `resume(&lane)` restores receive eligibility. It frees no unread slots;
  subsequent receives release capacity normally.
- `close_lane(&lane)` disconnects and retires it. Unread destruction follows
  the teardown policy.

Old and foreign IDs are rejected. Numeric `lane_id()` is a reusable slot,
so use `LaneId` for identities that must survive registration changes.

Pause and resume require the single receiver. Blocking ordinary receive waits
when all remaining lanes are paused; arrange resumption before entering it.
Applications sharing the receiver can supply their own lifecycle and wake
signals. Targeted receives are nonblocking.

## Async MPSC

Enable `async` in the dependency:

```toml
[dependencies]
fanring = { version = "0.3", features = ["async"] }
```

`send_async()`, `recv_async()`, and `recv_batch_into_async()` work with any
executor. Successful sends publish immediately. Bulk receive waits only for
the first value. All async receive forms release consumed slots before return,
including the tagged view's `recv_async()` and `poll_recv()`.

Canceling an incomplete send drops its unsent value without reserving capacity.
Canceling an incomplete receive consumes nothing. Both remove retained wakers.
Manual `poll_ready()` and `poll_recv()` users must call `cancel_send_wait()` or
`cancel_recv_wait()` when abandoning a pending registration. MPMC is synchronous.

## Choose a teardown policy

`channel()` and `try_channel()` use `Deferred`: unread ring values may stay
alive until their sender drops. `Coordinated` reclaims them independently of
idle sender lifetimes and adds send coordination. Use it for queued reply
senders, permits, or other resources requiring that cleanup.

```rust
use fanring::{mpsc, teardown::Coordinated};

let (mut tx, rx) = mpsc::channel_with_policy::<_, Coordinated>(4);
let (reply, response) = std::sync::mpsc::channel::<()>();
tx.try_send(reply).unwrap();
drop(rx);
assert_eq!(response.try_recv(), Err(std::sync::mpsc::TryRecvError::Disconnected));
drop(tx);
```

`mpmc::channel_with_policy()` selects the same policies.
`try_channel_with_policy()` validates capacity without panicking. The policy
is part of the endpoint types and is inherited by all handles.

An overlapping send can finish cleanup when it resumes; receiver teardown
does not wait for that producer. Payload destructors can run on either endpoint
thread. See [teardown ownership](DESIGN.md#teardown-policies).

## Configure blocking waits

The default `WaitStrategy::Park` briefly retries before parking. Endpoints can
instead spin for a bounded duration before using the same parking protocol:

```rust
use std::time::Duration;
use fanring::{WaitStrategy, mpsc};

let (_tx, mut rx) = mpsc::channel::<u64>(256);
rx.set_wait_strategy(WaitStrategy::SpinFor(Duration::from_micros(50)));
```

This policy is endpoint-local and copied by newly registered senders and MPMC
receiver clones. `SpinFor` uses active spinning until its budget or the
operation deadline expires. Pin communicating threads to distinct CPUs when
measuring wake latency and choose a duration within the application's CPU
budget. Async operations ignore this setting.

## Own publication and wakeups

MPSC's `try_send_deferred()` buffers values until `flush()` or a later signaled
send. It is available with `Deferred` teardown only. A full result flushes
previous values; callers must flush before waiting for capacity.

`try_send_unsignaled()` publishes immediately without marking the lane ready
or waking the receiver. Use `poll_all_lanes()` before ordinary or tagged
receives, or `try_recv_scan_into_while()` for a bulk scan. Both include every
unpaused registered lane. Without these, ordinary receives need a later
signaled send or `flush()` on that lane to discover unsignaled publications.

The caller owns the wake protocol: place a sequentially consistent fence
between sending and reading the external wake flag, and another between
clearing that flag and scanning on the receiver. Without both fences, a
receiver can park with data queued. See the
[`try_send_unsignaled` API contract](https://docs.rs/fanring/latest/fanring/mpsc/struct.Sender.html#method.try_send_unsignaled)
before integrating external signaling.
