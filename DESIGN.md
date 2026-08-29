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

Registration allocates a yring, assigns a slot, and publishes its consumer to
the pending list. The receiver imports pending consumers when the registry
generation changes. A disconnected empty lane returns its slot to the free
list. The number of producers is limited only by available memory.

The MPMC registry stores idle consumers directly in lane slots. A ready page
activation moves matching consumers into a lock-free ready-lane queue. It also
tracks one work stealer per live receiver. Receiver creation, receiver drop,
and stealer-list refresh use the registry mutex.

## Send Path

1. Check that the receiver is alive.
2. Push into the sender-owned yring.
3. Flush the producer.
4. Mark the lane ready.

## Lane Readiness

Each lane has a padded two-state signal:

- `IDLE`: receiver is not tracking the lane
- `PENDING`: lane is queued or active

After flushing the yring, every publication atomically swaps the signal to
`PENDING`. Only an `IDLE`-to-`PENDING` transition sets the lane bit in its ready
page. A page transition from empty to nonempty publishes the page ID to a
lock-free queue. Later publications coalesce into the existing pending lane.

At visible empty, the receiver swaps the signal to `IDLE` and then prefetches
the yring again. A producer that published before the swap synchronizes through
that swap, so the recheck sees its tail. A producer that publishes after the
swap queues the lane. If the recheck finds data while the signal remains idle,
the receiver claims the lane locally. This closes the empty-boundary race
without scanning every registered lane.

## MPSC Receive Path

The receiver takes ready page IDs, swaps each page's bits to zero, and appends
those lanes to a local active deque. It serves up to 64 items from one lane
before rotating it to the back. Newly ready pages are polled at the same
interval, so an always-busy lane cannot hide new producers.

`yring::prefetch` caches all flushed items with one Acquire load. Pops are
non-atomic. Consumed capacity is released after `min(64, lane capacity)` items
or when the lane reaches visible empty. A full release batch can span several
prefetch windows. Empty-lane release prevents a producer and receiver from
parking while partial credits remain unpublished.

## MPMC Receive Path

Each receiver owns a FIFO work deque. A receive operation checks its local
deque, the shared injector, and other receivers' stealers before claiming a
ready sender lane.

After claiming a lane, it prefetches and removes at most 64 values. The first
value satisfies the current receive. Remaining values move to the receiver's
local deque and are immediately stealable in batches. The lane token is then
requeued or returned to the registry; no receiver retains a sender lane between
API calls.

Draining a receiver's local work before claiming another lane preserves
batching. Requeuing every sender lane after at most 64 values bounds domination
by an always-busy lane. Receiver drop moves its remaining local values to the
shared injector and wakes competitors. This is one topology for all receiver
counts; there is no single-consumer specialization.

MPMC ordering is relaxed. This permits batches from one sender lane to execute
concurrently and permits later sender batches to overtake earlier local work.

## Blocking Waits

Each sender lane has one producer space wait cell. MPSC has a single-waiter
receiver data cell; MPMC has a generation-counted multi-waiter data cell. Wait
cells combine atomics with a mutex and condition variable. Notifications first
advance the generation. With no registered waiter, notification stays atomic
only.

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
inversion. With no waiter, notification is one atomic exchange and no lock.

## Drop

Sender drop closes and activates its lane, decrements the live-sender count,
then wakes a blocked receiver. The receiver drains buffered values before
retiring the lane.

MPSC receiver drop marks the channel closed and closes installed and pending
consumers. MPMC closes the channel when its last receiver drops. Earlier MPMC
receiver drops republish local work. Closing wakes blocked lane senders; later
sends return `Disconnected`.

## Ordering And Capacity

MPSC ordering is FIFO within one sender lane and relaxed across lanes. MPMC
ordering is fully relaxed. Capacity is per sender, like a per-pipe ZMQ HWM.
Adding a sender adds another ring and another capacity allocation. No shared
global credit counter appears on the send path.
