# Design

`fanring` assigns one SPSC `yring` to each registered sender. Each sender owns
its producer. The receive side owns the consumers. Registration and retirement
use a mutex. Common try operations use no locks unless they cross a lane idle
boundary or must wake an already-registered blocking peer.

## Dynamic Registry

The MPSC registry contains:

- pending consumers awaiting receiver adoption
- reusable lane slots with generation counters
- ready pages covering 64 lane slots each
- ready groups summarizing 64 pages each

Registration allocates a yring, assigns a slot, and publishes its consumer to
the pending list. The receiver imports pending consumers when the registry
generation changes. A disconnected empty lane returns its slot to the free
list. The number of producers is limited only by available memory.

The MPMC registry stores idle consumers directly in lane slots. Each page owns
a fixed 64-entry lock-free lane-token queue, allocated when that page is added.
Separate bitmap groups summarize newly ready registry lanes and requeued lane
tokens. The immutable page/group topology is published with `ArcSwap` only
when registration crosses a 64-lane boundary; each receiver caches it until a
generation change.

The registry also tracks one bounded work queue per live receiver. Receiver
creation and drop rebuild an immutable queue snapshot under the registry mutex
and publish it with `ArcSwap`. Receivers load that snapshot without locking
only after their own queue, the shared orphan queue, and visible sender lanes
miss. Persistent work-queue storage is linear in the number of receivers.

## Send Path

1. Check that the receiver is alive.
2. Push into the sender-owned yring.
3. Flush the producer.
4. Mark the lane ready.

## Lane Readiness

Each lane has a cache-line-aligned readiness block containing its two-state
signal, ready page, and page bit:

- `IDLE`: receiver is not tracking the lane
- `PENDING`: lane is queued or active

After flushing the yring, every publication atomically swaps the signal to
`PENDING`. Only an `IDLE`-to-`PENDING` transition sets the lane bit in its ready
page. A page transition from empty to nonempty sets its bit in a ready group.
Later publications coalesce into the existing pending lane. One group lookup
covers 4,096 sender lanes without allocating or touching a shared queue tail.

At visible empty, the receiver swaps the signal to `IDLE` and then prefetches
the yring again. A producer that published before the swap synchronizes through
that swap, so the recheck sees its tail. A producer that publishes after the
swap queues the lane. If the recheck finds data while the signal remains idle,
the receiver claims the lane locally. This closes the empty-boundary race
without scanning every registered lane.

## MPSC Receive Path

The receiver claims ready-group bits, swaps each selected page's lane bits to
zero, and appends those lanes to a local active deque. It serves up to 64 items
from one lane before rotating it to the back. Newly ready groups are polled at
the same interval, so an always-busy lane cannot hide new producers.

After claiming a page-summary bit, the receiver refreshes the registry before
looking up that page. After claiming lane bits, it refreshes again before
looking up those lanes. A registration that races either snapshot is therefore
imported before its claimed readiness bit is interpreted.

`yring::prefetch` caches all flushed items with one Acquire load. Pops are
non-atomic. Consumed capacity is released after `min(64, lane capacity)` items
or when the lane reaches visible empty. A full release batch can span several
prefetch windows. Empty-lane release prevents a producer and receiver from
parking while partial credits remain unpublished.

## MPMC Receive Path

Each receiver owns a bounded lock-free work queue. A receive first checks its
local queue and the shared orphan queue. If a sender lane is visibly ready, it
claims that lane before stealing so multiple producers naturally spread across
receivers. Otherwise it rotates through the other receiver queues and steals a
batch.

After claiming a lane, a receiver prefetches and removes at most 64 values. The
first value satisfies the current receive. Remaining values move to the local
work queue and become immediately stealable. A steal returns one value and
moves at most seven more into the thief's local queue, bounding transfer work
while amortizing victim discovery.

Receivers normally pop directly from a remembered page queue, allowing
multiple receivers to claim different lanes concurrently. Bitmap summaries
find work on other pages without a linear scan. Raw registry readiness and
requeued work alternate priority, and both page and group cursors rotate, so a
busy page cannot hide sparse lanes. Clearing a bitmap bit and moving its token
remain inside the publication tracker. Page queues are bounded by the 64 lane
tokens that can belong to them, so steady requeue traffic performs no heap
allocation.

Local queues are bounded by the largest 64-value prefetch batch and allocate
once when a receiver is created. Receiver drop moves any remaining local values
to the shared unbounded orphan queue and wakes competitors. This is one
topology for all receiver counts; there is no single-consumer specialization.

A publication tracker protects transfers that can make work temporarily
invisible: lane acquisition and drain, work-queue stealing, lane requeue or
retirement, and receiver handoff or removal. Publishers increment a generation
before decrementing the in-flight count. An empty receiver scan can return
`Disconnected` only when its generation is unchanged, no publication is in
flight, all senders are gone, and all sender lanes are retired. A changed
generation produces a bounded retry; exhaustion reports transient `Empty`,
never premature `Disconnected`.

MPMC ordering is relaxed. Batches from one sender can execute concurrently, and
values from different sender rings may overtake each other.

## Blocking Waits

Each sender lane has one producer space wait cell. MPSC has a single-waiter
receiver data cell. MPMC packs a waiter-present bit and monotonic notification
generation into one atomic state; its exact waiter count lives under the
condition-variable mutex. With no registered waiter, notification is one
atomic operation.

`send` first calls the non-blocking send path. On `Full`, it registers its lane
waiter, retries, then sleeps only if the ring is still full. `recv` does the
same around `Empty`. Both paths first execute up to 128 `spin_loop` retries,
which avoids kernel parking when the peer is active. The registration owns the
wait mutex across the second check, so notification cannot be lost between
that check and condition-variable sleep.

A blocking sender can publish data while holding its space registration. A
blocking receiver can release capacity while holding its data registration.
Those opposite-direction wakeups are deferred until after the current
registration is released. This prevents data-wait and space-wait lock-order
inversion. With no waiter, notification is one atomic operation and no lock.

## Drop

Sender drop closes and activates its lane, then decrements the live-sender
count. Earlier sender drops wake one receiver. The last sender drop wakes every
receiver. Receivers drain buffered values before retiring lanes; final lane
retirement also broadcasts when needed.

MPSC receiver drop marks the channel closed and closes installed and pending
consumers. MPMC closes the channel when its last receiver drops. Earlier MPMC
receiver drops republish local work. Closing wakes blocked lane senders; later
sends return `Disconnected`.

## Ordering And Capacity

MPSC ordering is FIFO within one sender lane and relaxed across lanes. MPMC
ordering is fully relaxed. Capacity is per sender, like a per-pipe ZMQ HWM.
Adding a sender adds another ring and another capacity allocation. No shared
global credit counter appears on the send path. MPMC receiver queues and the
orphan queue hold prefetched values outside those ring HWMs, so the sum of ring
capacities is a nominal bound rather than an exact total resident-item bound.
