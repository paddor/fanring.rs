//! Dynamic MPSC channel built from one bounded SPSC ring per sender.
//!
//! Each sender owns its ring producer and the receiver owns every ring
//! consumer. Ordering is FIFO per sender and relaxed across senders.

use std::fmt;

use concurrent_queue::ConcurrentQueue;

use crate::compat::{Arc, AtomicBool, AtomicUsize, Mutex, Ordering, lock};
use crate::config::validate_capacity;
use crate::ready::{LANES_PER_PAGE, LaneSignal, ReadyPage};
use crate::wait::WaitCell;

mod receiver;
mod sender;

use receiver::Lane;
pub use receiver::{IntoIter, Iter, Receiver, TryIter};
pub use sender::Sender;

pub use crate::error::{
    ChannelError, RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError,
    TryRegisterError, TrySendError,
};

/// Maximum per-sender capacity accepted by [`channel`] and [`try_channel`].
///
/// `yring` rounds capacity up to a power of two and requires the rounded value
/// to fit in half the cursor range.
pub const MAX_CAPACITY_PER_SENDER: usize = 1usize << (usize::BITS - 2);

const PREFETCH_LIMIT: usize = 64;
const READY_POLL_INTERVAL: usize = 64;
#[cfg(not(loom))]
const PARK_SPINS: usize = 128;
#[cfg(loom)]
const PARK_SPINS: usize = 0;

/// Create an MPSC channel with one bounded ring per sender.
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
    validate_capacity(capacity_per_sender, MAX_CAPACITY_PER_SENDER)?;
    Ok(build_channel(capacity_per_sender))
}

fn build_channel<T>(capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    let (producer, consumer) = yring::spsc(capacity_per_sender);
    let page = Arc::new(ReadyPage::new(0));
    let signal = Arc::new(LaneSignal::new(page.clone(), 0));
    let key = LaneKey {
        slot: 0,
        generation: 0,
    };
    let shared = Arc::new(Shared {
        registry: Mutex::new(Registry {
            pending: Vec::new(),
            free: Vec::new(),
            pages: vec![page.clone()],
            next_slot: 1,
        }),
        ready_pages: ConcurrentQueue::unbounded(),
        registry_generation: AtomicUsize::new(0),
        registered_lanes: AtomicUsize::new(1),
        live_senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        data_waiter: WaitCell::new(),
        capacity_per_sender,
    });

    (
        Sender {
            shared: shared.clone(),
            producer,
            key,
            signal: signal.clone(),
        },
        Receiver {
            shared,
            lanes: vec![Some(Lane::new(key, signal, consumer))],
            pages: vec![page],
            active: std::collections::VecDeque::new(),
            seen_registry_generation: 0,
            items_until_ready_poll: READY_POLL_INTERVAL,
            capacity_per_sender: capacity_per_sender.next_power_of_two(),
        },
    )
}

struct Shared<T> {
    registry: Mutex<Registry<T>>,
    ready_pages: ConcurrentQueue<usize>,
    registry_generation: AtomicUsize,
    registered_lanes: AtomicUsize,
    live_senders: AtomicUsize,
    receiver_alive: AtomicBool,
    data_waiter: WaitCell,
    capacity_per_sender: usize,
}

impl<T> Shared<T> {
    #[allow(clippy::significant_drop_tightening)]
    fn register_sender(
        &self,
    ) -> Result<(LaneKey, Arc<LaneSignal>, yring::Producer<T>), TryRegisterError> {
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterError::Disconnected);
        }

        let mut registry = lock(&self.registry);
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterError::Disconnected);
        }

        let (key, page) = registry.allocate_lane();
        let signal = Arc::new(LaneSignal::new(page, key.slot));
        let (producer, consumer) = yring::spsc(self.capacity_per_sender);
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
        let mark = signal.mark();
        if let Some(page) = mark.page {
            self.ready_pages
                .push(page)
                .expect("ready-page queue is never closed");
        }
        mark.activated
    }
}

impl<T> fmt::Debug for Shared<T> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneKey {
    slot: usize,
    generation: usize,
}

struct PendingLane<T> {
    key: LaneKey,
    signal: Arc<LaneSignal>,
    consumer: yring::Consumer<T>,
}

struct Registry<T> {
    pending: Vec<PendingLane<T>>,
    free: Vec<LaneKey>,
    pages: Vec<Arc<ReadyPage>>,
    next_slot: usize,
}

impl<T> Registry<T> {
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
            self.pages.push(Arc::new(ReadyPage::new(self.pages.len())));
        }
        (key, self.pages[page_id].clone())
    }

    fn retire_lane(&mut self, key: LaneKey) {
        self.free.push(LaneKey {
            slot: key.slot,
            generation: key.generation.wrapping_add(1),
        });
    }
}
