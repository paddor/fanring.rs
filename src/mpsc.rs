//! Dynamic MPSC channel built from one bounded SPSC ring per sender.
//!
//! Each sender owns its ring producer and the receiver owns every ring
//! consumer. Ordering is FIFO per sender and relaxed across senders.
//! [`Receiver::try_recv_fair`] opts into one-value rotation among ready senders.
//!
//! Single-value receives batch slot release, so a sender can still observe
//! `Full` after values have been received. [`Receiver::release_consumed`]
//! publishes those freed slots immediately. [`Receiver::recv_batch_into`]
//! receives a bounded batch into a caller-owned vector and releases consumed
//! slots before returning, waiting only for the first value.

use crate::teardown::{Deferred, Teardown};

use std::fmt;

use crate::compat::{Arc, AtomicBool, AtomicU64, AtomicUsize, Mutex, Ordering, lock};
use crate::config::{WaitStrategy, validate_capacity};
use crate::ready::{LANES_PER_PAGE, LaneSignal, PAGES_PER_GROUP, ReadyGroup, ReadyPage};
use crate::wait::WaitCell;

#[cfg(feature = "async")]
mod asynchronous;
mod receiver;
mod sender;

#[cfg(feature = "async")]
pub use asynchronous::SendFuture;
use receiver::Lane;
pub use receiver::{IntoIter, Iter, LaneReceiver, Receiver, TryIter};
pub use sender::Sender;

pub use crate::error::{
    ChannelError, RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError,
    TryRegisterBoundedError, TryRegisterError, TrySendError,
};

#[cfg(not(all(loom, target_pointer_width = "64")))]
static NEXT_CHANNEL_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(all(loom, target_pointer_width = "64"))]
loom::lazy_static! {
    static ref NEXT_CHANNEL_ID: AtomicU64 = AtomicU64::new(0);
}

// IDs outlive channels, so allocation addresses cannot establish identity.
// This counter is touched only at channel construction and never wraps.
fn allocate_channel_id(counter: &AtomicU64) -> u64 {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.checked_add(1).expect("MPSC channel IDs exhausted");
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return current,
            Err(observed) => current = observed,
        }
    }
}

/// Maximum per-sender capacity accepted by [`channel`] and [`try_channel`].
///
/// `yring` rounds capacity up to a power of two and requires the rounded value
/// to fit in half the cursor range.
pub const MAX_CAPACITY_PER_SENDER: usize = 1usize << (usize::BITS - 2);

const PREFETCH_LIMIT: usize = 64;
const READY_POLL_INTERVAL: usize = 64;
// Keep small rings' existing credit granularity. Smaller batches can turn
// multi-producer traffic into repeated space-wait/notify contention.
#[cfg(not(loom))]
const MIN_RELEASE_BATCH: usize = 64;
#[cfg(loom)]
const MIN_RELEASE_BATCH: usize = 2;
#[cfg(not(loom))]
const PARK_SPINS: usize = 128;
#[cfg(loom)]
const PARK_SPINS: usize = 0;

/// Create an MPSC channel with one bounded ring per sender.
///
/// Uses [`Deferred`] teardown. See [`channel_with_policy`] to opt into
/// cleanup independent of idle sender lifetimes.
///
/// `capacity_per_sender` must be between 1 and [`MAX_CAPACITY_PER_SENDER`] and is
/// rounded up by `yring` to the next power of two.
///
/// # Panics
///
/// Panics when `capacity_per_sender` is zero or exceeds
/// [`MAX_CAPACITY_PER_SENDER`].
#[must_use]
pub fn channel<T>(capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    try_channel(capacity_per_sender).unwrap_or_else(|error| panic!("{error}"))
}

/// Try to create an MPSC channel with one bounded ring per sender.
///
/// This is the fallible version of [`channel`]. It returns a [`ChannelError`]
/// instead of panicking when the configuration is invalid.
///
/// # Errors
///
/// Returns [`ChannelError`] when `capacity_per_sender` is zero or exceeds
/// [`MAX_CAPACITY_PER_SENDER`].
pub fn try_channel<T>(
    capacity_per_sender: usize,
) -> Result<(Sender<T>, Receiver<T>), ChannelError> {
    try_channel_with_policy::<T, Deferred>(capacity_per_sender)
}

/// Create a channel with an explicit teardown policy.
///
/// [`Deferred`] is the default used by [`channel`].
/// [`Coordinated`](crate::teardown::Coordinated) destroys unread payloads while
/// sender handles remain alive; an overlapping send may finish cleanup later.
/// The policy is inherited by all cloned handles.
///
/// # Panics
///
/// Panics when capacity is zero or exceeds [`MAX_CAPACITY_PER_SENDER`].
///
/// # Example
///
/// ```
/// use fanring::{mpsc, teardown::Coordinated};
/// let (mut tx, rx) = mpsc::channel_with_policy::<_, Coordinated>(4);
/// let (reply, response) = std::sync::mpsc::channel::<()>();
/// tx.try_send(reply).unwrap();
/// drop(rx);
/// assert_eq!(response.try_recv(), Err(std::sync::mpsc::TryRecvError::Disconnected));
/// drop(tx);
/// ```
#[must_use]
pub fn channel_with_policy<T, P: Teardown>(
    capacity_per_sender: usize,
) -> (Sender<T, P>, Receiver<T, P>) {
    try_channel_with_policy(capacity_per_sender).unwrap_or_else(|error| panic!("{error}"))
}

/// Fallible version of [`channel_with_policy`].
///
/// # Errors
///
/// Returns [`ChannelError`] when capacity is zero or exceeds
/// [`MAX_CAPACITY_PER_SENDER`].
#[allow(
    clippy::type_complexity,
    reason = "channel constructor returns its two endpoints"
)]
pub fn try_channel_with_policy<T, P: Teardown>(
    capacity_per_sender: usize,
) -> Result<(Sender<T, P>, Receiver<T, P>), ChannelError> {
    validate_capacity(capacity_per_sender, MAX_CAPACITY_PER_SENDER)?;
    Ok(build_channel(capacity_per_sender))
}

fn build_channel<T, P: Teardown>(capacity_per_sender: usize) -> (Sender<T, P>, Receiver<T, P>) {
    let (producer, consumer) = crate::ring::spsc(capacity_per_sender);
    let group = Arc::new(ReadyGroup::new(0));
    let page = Arc::new(ReadyPage::new(0, group.clone()));
    let signal = Arc::new(LaneSignal::new(page.clone(), 0));
    let key = LaneKey {
        slot: 0,
        generation: 0,
    };
    let shared = Arc::new(Shared {
        registry: Mutex::new(Registry {
            pending: Vec::new(),
            free: Vec::new(),
            groups: vec![group.clone()],
            pages: vec![page.clone()],
            next_slot: 1,
        }),
        registry_generation: AtomicUsize::new(0),
        registered_lanes: AtomicUsize::new(1),
        live_senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        data_waiter: WaitCell::new(),
        capacity_per_sender,
        channel_id: allocate_channel_id(&NEXT_CHANNEL_ID),
    });

    (
        Sender {
            shared: shared.clone(),
            producer,
            key,
            signal: signal.clone(),
            wait_strategy: WaitStrategy::default(),
        },
        Receiver {
            shared,
            lanes: vec![Some(Lane::new(key, signal, consumer))],
            groups: vec![group],
            pages: vec![page],
            active: std::collections::VecDeque::with_capacity(1),
            paused: vec![false],
            ready_group_cursor: 0,
            seen_registry_generation: 0,
            items_until_ready_poll: READY_POLL_INTERVAL,
            capacity_per_sender: capacity_per_sender.next_power_of_two(),
            wait_strategy: WaitStrategy::default(),
        },
    )
}

struct Shared<T, P: Teardown> {
    registry: Mutex<Registry<T, P>>,
    registry_generation: AtomicUsize,
    registered_lanes: AtomicUsize,
    live_senders: AtomicUsize,
    receiver_alive: AtomicBool,
    data_waiter: WaitCell,
    capacity_per_sender: usize,
    channel_id: u64,
}

impl<T, P: Teardown> Shared<T, P> {
    #[allow(clippy::significant_drop_tightening)]
    #[allow(
        clippy::type_complexity,
        reason = "registration returns the lane key, readiness signal, and producer"
    )]
    fn register_sender(
        &self,
        max_lanes: usize,
        capacity: usize,
    ) -> Result<(LaneKey, Arc<LaneSignal>, crate::ring::Producer<T, P>), TryRegisterBoundedError>
    {
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterBoundedError::Disconnected);
        }

        let mut registry = lock(&self.registry);
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterBoundedError::Disconnected);
        }

        if self.registered_lanes.load(Ordering::Acquire) >= max_lanes {
            return Err(TryRegisterBoundedError::AtCapacity);
        }
        let (key, page) = registry.allocate_lane();
        let signal = Arc::new(LaneSignal::new(page, key.slot));
        let (producer, consumer) = crate::ring::spsc(capacity);
        registry.pending.push(PendingLane {
            key,
            signal: signal.clone(),
            consumer,
        });
        // Receiver teardown consumes the registry and counters together.
        self.registered_lanes.fetch_add(1, Ordering::Release);
        self.live_senders.fetch_add(1, Ordering::AcqRel);
        self.registry_generation.fetch_add(1, Ordering::Release);
        Ok((key, signal, producer))
    }

    #[inline]
    fn mark_ready(&self, signal: &LaneSignal) -> bool {
        signal.mark()
    }
}

impl<T, P: Teardown> fmt::Debug for Shared<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field(
                "registered_lanes",
                &self.registered_lanes.load(Ordering::Relaxed),
            )
            .field("live_senders", &self.live_senders.load(Ordering::Relaxed))
            .field(
                "receiver_alive",
                &self.receiver_alive.load(Ordering::Relaxed),
            )
            .field("capacity_per_sender", &self.capacity_per_sender)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LaneKey {
    slot: usize,
    generation: usize,
}

/// Opaque identity of one sender's lane in one channel.
///
/// Copying this ID retains no allocation. Channel identity and slot generation
/// never wrap, so old IDs cannot select a replacement lane or channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneId {
    key: LaneKey,
    channel_id: u64,
}

struct PendingLane<T, P: Teardown> {
    key: LaneKey,
    signal: Arc<LaneSignal>,
    consumer: crate::ring::Consumer<T, P>,
}

struct Registry<T, P: Teardown> {
    pending: Vec<PendingLane<T, P>>,
    free: Vec<LaneKey>,
    groups: Vec<Arc<ReadyGroup>>,
    pages: Vec<Arc<ReadyPage>>,
    next_slot: usize,
}

impl<T, P: Teardown> Registry<T, P> {
    fn allocate_lane(&mut self) -> (LaneKey, Arc<ReadyPage>) {
        let key = self.free.pop().unwrap_or_else(|| {
            let key = LaneKey {
                slot: self.next_slot,
                generation: 0,
            };
            self.next_slot += 1;
            key
        });
        let page_id = key.slot / LANES_PER_PAGE;
        while self.pages.len() <= page_id {
            let id = self.pages.len();
            let group_id = id / PAGES_PER_GROUP;
            while self.groups.len() <= group_id {
                self.groups
                    .push(Arc::new(ReadyGroup::new(self.groups.len())));
            }
            self.pages
                .push(Arc::new(ReadyPage::new(id, self.groups[group_id].clone())));
        }
        (key, self.pages[page_id].clone())
    }

    fn retire_lane(&mut self, key: LaneKey) {
        // Permanently retire an exhausted slot instead of aliasing an old ID.
        if let Some(generation) = key.generation.checked_add(1) {
            self.free.push(LaneKey {
                slot: key.slot,
                generation,
            });
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn channel_ids_never_wrap_after_exhaustion() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(allocate_channel_id(&counter), u64::MAX - 1);
        for _ in 0..2 {
            assert!(std::panic::catch_unwind(|| allocate_channel_id(&counter)).is_err());
            assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
        }
    }

    #[test]
    fn exhausted_lane_generation_is_not_reused() {
        let (_tx, rx) = channel::<()>(1);
        let mut registry = lock(&rx.shared.registry);
        registry.retire_lane(LaneKey {
            slot: 0,
            generation: usize::MAX - 1,
        });
        let (last, _) = registry.allocate_lane();
        assert_eq!(
            last,
            LaneKey {
                slot: 0,
                generation: usize::MAX
            }
        );
        registry.retire_lane(last);
        let (next, _) = registry.allocate_lane();
        assert_eq!(
            next,
            LaneKey {
                slot: 1,
                generation: 0
            }
        );
    }
}
