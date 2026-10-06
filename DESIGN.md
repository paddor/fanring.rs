# Design

`fanring` builds typed channels from one bounded SPSC `yring` per sender.
The producer owns its write cursor. Receive-side ownership differs by channel:

| Component | MPSC | MPMC |
| --- | --- | --- |
| Ring consumers | Owned by the single receiver | Stored in the registry; exclusively claimed by receivers |
| Ready work | Receiver-local lane rotation | Registry lane tokens and receiver work queues |
| Received batches | Move directly to the caller | Can stage in private or stealable queues |
| Lane control | Receiver-owned pause state | No per-lane pause API |

Capacity belongs to each sender ring. Adding a sender adds another ring.
MPMC staging holds values outside those ring bounds, so their sum is not an
exact bound on total resident items. MPSC preserves per-lane FIFO; MPMC permits
batches from one lane to be processed concurrently and has relaxed ordering.

## Registration and identity

Registration allocates a ring and installs its consumer under the registry
mutex. MPSC publishes pending consumers for receiver adoption. MPMC keeps idle
consumers in registry slots until a receiver claims them.

A sender clone registers a new lane. The receiver retires a disconnected lane
after draining it, or explicitly closes it. Retirement makes its slot reusable.
Generation counters prevent stale readiness tokens
from naming a replacement sender. MPSC's `LaneId` additionally carries channel
identity, is `Copy`, and keeps no ring alive. Identity counters never wrap into
reuse.

MPSC refreshes its registry view before resolving newly claimed readiness.
MPMC publishes immutable topology snapshots and resolves newly added pages
through the current snapshot. Their referenced readiness state is shared.
Registration, retirement, and receiver membership are maintenance operations;
senders do not acquire the registry mutex to publish values.

## Publication and readiness

A send checks its lane's disconnect state, writes into the sender-owned ring,
and flushes publication. `yring` publishes with release ordering and prefetches
with acquire ordering; cached pops and pushes use endpoint-local cursors.

Lane signals coalesce publication into readiness:

- `IDLE`: the receive side is not tracking the lane.
- `PENDING`: the lane is queued or active.

Only the transition to `PENDING` adds a ready bit. Ready pages collect lane
bits; groups summarize nonempty pages. Receivers find newly ready lanes through
these summaries without scanning every registration.

On visible empty, the receive side clears readiness and rechecks publication.
A racing producer either becomes visible to that recheck or sets readiness
again. This handshake prevents a publication from becoming permanently hidden.
External signaling can use unsignaled sends together with explicit lane scans.

## MPSC drainage and lane control

The receiver adopts consumers and maintains a local rotation of ready lanes.
Ordinary receives serve bounded bursts, polling new readiness between bursts.
Fair receives rotate after each value. Bulk receives follow ordinary scheduling
and move accepted prefixes of prefetched windows into caller-owned storage.

Pause state belongs entirely to the receiver. Pausing removes a lane from
ordinary scheduling while leaving its ring and unread values intact. The
sender fills its existing ring and then encounters backpressure. A targeted
receive can drain a paused lane; resuming makes it eligible for ordinary
receives again.

The application owns any value it has already popped. Fanring stores no
application-side pending item. Tagged and plain receive views share one drain;
only the tagged view constructs source IDs.

Consumed slots are published in batches independently of lane rotation. Empty
lane handling publishes partial batches. `release_consumed()` explicitly
publishes consumed capacity across lanes. Bulk and async receives do so before
returning. Pausing and resuming do not free unread slots.

## MPMC work distribution

A receiver first checks its own staged work and the shared orphan queue. It
then claims a ready sender lane or steals a batch from another receiver.
Exclusive lane claims ensure only one receiver operates its ring consumer at
a time. Registry readiness and requeued tokens rotate to keep busy lanes and
pages from hiding other ready work.

With one receiver, prefetched values stage in a private deque. Cloning publishes
that deque before exposing the new receiver. With multiple receivers, staging
uses bounded synchronized queues. An immutable queue snapshot lets receivers
find stealable work without locking the registry.

Dropping a non-final receiver transfers its buffered values to the shared
orphan queue. That queue can exceed the sender-ring bounds during receiver
churn. Values stay reachable by the surviving receivers.

A publication tracker covers transfers that temporarily hide work: lane
claims, drainage, requeue, stealing, and receiver handoff. A final empty scan
reports `Disconnected` only when there are no senders, lanes, or transfers left
and the publication generation stayed stable. Otherwise bounded maintenance
can report transient `Empty`.

## Waiting

Each sender lane has one capacity wait cell. MPSC has one receiver data wait
cell; MPMC shares data notifications among receiver waiters.

Blocking operations try the queue, register a waiter, and recheck before
sleeping. The wait registration bridges the recheck and condition-variable
sleep. Opposite-direction notifications are delivered after releasing the
current wait registration, avoiding data/space lock inversion.

Wait strategy belongs to the endpoint. Default parking has a short retry
phase; bounded spinning uses the same registration protocol when its budget
expires. Timeout operations share this protocol and stop at their deadline.

Async MPSC uses per-sender capacity wakers and one receiver data waker.
Registration precedes a queue recheck. Futures remove retained registrations
on completion or cancellation; manual poll users cancel abandoned waits.

## Teardown policies

The teardown policy is a sealed type parameter fixed at construction and
inherited by all handles.

| Policy | Unread ring values after receiver closure | Send coordination |
| --- | --- | --- |
| `Deferred` (default) | May stay alive until the sender releases its ring | No cleanup handshake |
| `Coordinated` | Drained during closure or by an overlapping send when it resumes | Atomic ownership handshake |

Receiver closure includes MPSC `close_lane()`, MPSC receiver drop, and final
MPMC receiver drop. It disconnects the affected lanes and wakes blocked
senders. Subsequent sends return their values as disconnected. Receiver-local
staged values may be destroyed sooner than retained ring values.

Coordinated lanes have shared active/closed state and a consumer handoff slot.
The receiver drains its visible unread values and closes the lane. An active
send takes responsibility for any late publication; without an active send,
the receiver completes the drain. Receiver teardown never waits for a stalled producer thread. Ring allocations stay alive with their sender handles.

Payload destructors run outside the handoff mutex on the receiver or producer
thread. As with ordinary Rust destruction, a destructor can itself block or
panic. Successful send does not acknowledge processing; payload destruction
releases queued replies, permits, and owned buffers.

Sender drop closes and activates its lane. Receivers drain its backlog before
retirement. Last-sender drop wakes all receivers. Non-final MPMC receiver drop
preserves staged work for surviving receivers under both policies.

[Getting started](GETTING_STARTED.md#choose-a-teardown-policy) shows policy
selection. [Development](DEVELOPMENT.md#teardown-policy-comparisons) covers the
owned-payload probe and throughput measurements.
