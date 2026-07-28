# Design

`fanring` is one fixed SPSC ring per registered sender. The receiver owns all
consumers and polls ready shards.

## Shared State

- `unclaimed`: mutex-protected producer slots used only during registration.
- `claimed`: bitmask of ever-claimed shards.
- `live_senders`: count used for disconnect detection.
- `receiver_alive`: close flag checked by send and registration.
- `ready`: per-shard ready flags plus one global ready bitmask.

## Send Path

1. Check `receiver_alive`.
2. Push into this sender's `yring::Producer`.
3. Flush the producer.
4. Mark this shard ready.

Send returns `Full` when the shard ring has no capacity and `Disconnected` when
the receiver is gone.

## Ready Protocol

Each shard has a padded `AtomicBool`. Senders set it with `swap(true, AcqRel)`.
Only a false-to-true transition publishes the shard bit into the global
`AtomicU64`.

The receiver swaps the global mask into `ready_cache`, then polls ready bits with
`next_shard`. A shard is removed only after:

1. `prefetch()` finds no items.
2. The shard flag is cleared.
3. A second `prefetch()` still finds no items.

If the second prefetch finds data, the receiver restores the shard flag. This
closes the race with a sender that publishes while the receiver is clearing.

## Receive Path

The receiver serves prefetched items from `cached_available`. It calls
`release()` after `PREFETCH_LIMIT` popped items or when the prefetched window
drains. Capacity therefore returns to senders in batches.

Ordering is FIFO per shard. Cross-shard order is whatever the ready mask and
round-robin cursor produce.

## Drop

Sender drop closes its producer, marks the shard ready, then decrements
`live_senders`.

Receiver drop clears `receiver_alive` and closes all consumers.

`RecvError::Disconnected` is returned after `live_senders == 0` and all claimed
rings report disconnected.
