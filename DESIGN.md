# Design

`fanring` assigns one SPSC `yring` to each registered sender. The sender owns
the producer. The receiver owns every consumer. Registration and retirement
use a mutex. Successful try operations use no locks unless they must wake an
already-registered blocking peer.

## Dynamic Registry

The shared registry contains:

- pending consumers awaiting receiver adoption
- reusable lane slots with generation counters
- ready pages covering 64 lane slots each

Registration allocates a yring, assigns a slot, and publishes its consumer to
the pending list. The receiver imports pending consumers when the registry
generation changes. A disconnected empty lane returns its slot to the free
list. The number of producers is limited only by available memory.

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

## Receive Path

The receiver takes ready page IDs, swaps each page's bits to zero, and appends
those lanes to a local active deque. It serves up to 64 items from one lane
before rotating it to the back. Newly ready pages are polled at the same
interval, so an always-busy lane cannot hide new producers.

`yring::prefetch` caches all flushed items with one Acquire load. Pops are
non-atomic. Consumed capacity is released after `min(64, lane capacity)` items
or when the lane reaches visible empty. A full release batch can span several
prefetch windows. Empty-lane release prevents a producer and receiver from
parking while partial credits remain unpublished.

## Blocking Waits

The channel has one receiver data wait cell. Each lane has one producer space
wait cell. A wait cell combines an atomic notification generation and waiter
bit with a mutex and condition variable. Notifications increment the
generation. Registration and clearing change only the waiter bit, so a later
notification cannot be erased by an earlier wakeup completing.

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

Receiver drop marks the channel closed and closes installed and pending
consumers. It wakes blocked lane senders; later sends return `Disconnected`.

## Ordering And Capacity

Ordering is FIFO within one sender lane. Ordering across lanes is relaxed.
Capacity is per sender, like a per-pipe ZMQ HWM. Adding a sender adds another
ring and another capacity allocation. No shared global credit counter appears
on the send path.
