//! Bounded sharded MPSC channel built from SPSC rings.
//!
//! `fanring` gives each registered sender its own bounded SPSC ring. The
//! receiver polls sender rings with a ready bitmask and round-robin cursor.
//! Senders never contend with each other on a shared queue tail.
//!
//! Ordering is FIFO per sender. Ordering across senders is intentionally
//! relaxed.

use std::fmt;

mod compat;
mod error;
mod ready;

use compat::{Arc, AtomicBool, AtomicU64, AtomicUsize, Mutex, Ordering};
use ready::{ReadyMask, ReadySet, shard_bit};

pub use error::{ChannelError, RecvError, SendError, TryRegisterError};

/// Maximum number of senders supported by the ready bitmask.
pub const MAX_SENDERS: usize = u64::BITS as usize;

/// Maximum per-sender capacity accepted by [`channel`] and [`try_channel`].
///
/// `yring` rounds capacity up to a power of two and requires the rounded value
/// to fit in half the cursor range.
pub const MAX_CAPACITY_PER_SENDER: usize = 1usize << (usize::BITS - 2);

const PREFETCH_LIMIT: usize = 64;

/// Create a bounded sharded MPSC channel.
///
/// `max_senders` includes the initial sender. It must be in
/// `1..=`[`MAX_SENDERS`]. `capacity_per_sender` must be in
/// `1..=`[`MAX_CAPACITY_PER_SENDER`] and is rounded up by `yring` to the next
/// power of two.
///
/// Cloning a sender is fallible: [`Sender::try_clone`] returns `None` when
/// all sender rings are already claimed.
pub fn channel<T>(max_senders: usize, capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    try_channel(max_senders, capacity_per_sender).unwrap_or_else(|error| panic!("{error}"))
}

/// Try to create a bounded sharded MPSC channel.
///
/// This is the fallible version of [`channel`]. It returns a [`ChannelError`]
/// instead of panicking when the configuration is invalid.
pub fn try_channel<T>(
    max_senders: usize,
    capacity_per_sender: usize,
) -> Result<(Sender<T>, Receiver<T>), ChannelError> {
    validate_channel_config(max_senders, capacity_per_sender)?;
    Ok(build_channel(max_senders, capacity_per_sender))
}

fn validate_channel_config(
    max_senders: usize,
    capacity_per_sender: usize,
) -> Result<(), ChannelError> {
    if max_senders == 0 {
        return Err(ChannelError::ZeroSenders);
    }
    if max_senders > MAX_SENDERS {
        return Err(ChannelError::TooManySenders {
            requested: max_senders,
            max: MAX_SENDERS,
        });
    }
    if capacity_per_sender == 0 {
        return Err(ChannelError::ZeroCapacity);
    }
    if capacity_per_sender > MAX_CAPACITY_PER_SENDER {
        return Err(ChannelError::CapacityTooLarge {
            requested: capacity_per_sender,
            max: MAX_CAPACITY_PER_SENDER,
        });
    }

    Ok(())
}

fn build_channel<T>(max_senders: usize, capacity_per_sender: usize) -> (Sender<T>, Receiver<T>) {
    let mut producers = Vec::with_capacity(max_senders);
    let mut consumers = Vec::with_capacity(max_senders);

    for _ in 0..max_senders {
        let (producer, consumer) = yring::spsc(capacity_per_sender);
        producers.push(Some(producer));
        consumers.push(consumer);
    }

    let producer = producers[0]
        .take()
        .expect("initial producer must be present");
    let shared = Arc::new(Shared {
        unclaimed: Mutex::new(producers),
        ready: ReadySet::new(max_senders),
        claimed: AtomicU64::new(1),
        live_senders: AtomicUsize::new(1),
        receiver_alive: AtomicBool::new(true),
        max_senders,
    });

    (
        Sender {
            shared: shared.clone(),
            producer,
            shard: 0,
        },
        Receiver {
            shared,
            consumers,
            cached_available: vec![0; max_senders],
            unreleased: vec![0; max_senders],
            ready_cache: ReadyMask::empty(),
            next_shard: 0,
        },
    )
}

struct Shared<T> {
    unclaimed: Mutex<Vec<Option<yring::Producer<T>>>>,
    ready: ReadySet,
    claimed: AtomicU64,
    live_senders: AtomicUsize,
    receiver_alive: AtomicBool,
    max_senders: usize,
}

impl<T> Shared<T> {
    fn claim_sender(
        &self,
        start_shard: usize,
    ) -> Result<(usize, yring::Producer<T>), TryRegisterError> {
        if !self.receiver_alive.load(Ordering::Acquire) {
            return Err(TryRegisterError::Disconnected);
        }

        let mut unclaimed = match self.unclaimed.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        for offset in 0..self.max_senders {
            let shard = start_shard.wrapping_add(offset) % self.max_senders;
            let Some(producer) = unclaimed[shard].take() else {
                continue;
            };
            if !self.receiver_alive.load(Ordering::Acquire) {
                unclaimed[shard] = Some(producer);
                return Err(TryRegisterError::Disconnected);
            }
            let bit = shard_bit(shard);
            self.claimed.fetch_or(bit, Ordering::Release);
            self.live_senders.fetch_add(1, Ordering::AcqRel);
            return Ok((shard, producer));
        }

        if self.receiver_alive.load(Ordering::Acquire) {
            Err(TryRegisterError::NoSenderSlot)
        } else {
            Err(TryRegisterError::Disconnected)
        }
    }
}

impl<T> fmt::Debug for Shared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("ready", &self.ready)
            .field("claimed", &self.claimed.load(Ordering::Relaxed))
            .field("live_senders", &self.live_senders.load(Ordering::Relaxed))
            .field(
                "receiver_alive",
                &self.receiver_alive.load(Ordering::Relaxed),
            )
            .field("max_senders", &self.max_senders)
            .finish_non_exhaustive()
    }
}

/// Sending half.
///
/// A `Sender` owns exactly one SPSC ring producer. It is `Send` when `T` is
/// `Send`, but it is not `Sync` and cannot be cloned infallibly. Use
/// [`try_clone`](Self::try_clone) or [`try_register`](Self::try_register) to
/// register another sender.
#[derive(Debug)]
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    producer: yring::Producer<T>,
    shard: usize,
}

impl<T> Sender<T> {
    /// Try to register another sender.
    ///
    /// Returns `None` when the receiver is gone or all sender rings are
    /// already claimed. Dropped sender slots are not reused in this initial
    /// design.
    pub fn try_clone(&self) -> Option<Self> {
        self.try_register().ok()
    }

    /// Try to register another sender and report why registration failed.
    ///
    /// Prefer this over [`try_clone`](Self::try_clone) when the caller needs to
    /// distinguish a closed receiver from an exhausted sender limit.
    pub fn try_register(&self) -> Result<Self, TryRegisterError> {
        let start = self.shard.wrapping_add(1);
        let (shard, producer) = self.shared.claim_sender(start)?;
        Ok(Self {
            shared: self.shared.clone(),
            producer,
            shard,
        })
    }

    /// Try to send one value.
    ///
    /// A successful send is immediately visible to the receiver. Internally,
    /// the value is pushed into this sender's SPSC ring and flushed.
    #[inline]
    pub fn try_send(&mut self, value: T) -> Result<(), SendError<T>> {
        if !self.shared.receiver_alive.load(Ordering::Acquire) {
            return Err(SendError::Disconnected(value));
        }

        match self.producer.push(value) {
            Ok(()) => {
                self.producer.flush();
                self.mark_ready();
                Ok(())
            }
            Err(value) => {
                if !self.shared.receiver_alive.load(Ordering::Acquire)
                    || self.producer.is_consumer_dropped()
                {
                    Err(SendError::Disconnected(value))
                } else {
                    Err(SendError::Full(value))
                }
            }
        }
    }

    /// Return this sender's shard index.
    #[inline]
    pub fn shard(&self) -> usize {
        self.shard
    }

    /// Return this sender's per-shard capacity after `yring` rounding.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.producer.capacity()
    }

    #[inline]
    fn mark_ready(&self) {
        self.shared.ready.mark_ready(self.shard);
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.producer.close();
        self.mark_ready();
        self.shared.live_senders.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Receiving half.
///
/// The receiver owns every SPSC consumer and polls ready shards
/// round-robin. It is `Send` when `T` is `Send`, but it is not `Sync`.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    consumers: Vec<yring::Consumer<T>>,
    cached_available: Vec<usize>,
    unreleased: Vec<usize>,
    ready_cache: ReadyMask,
    next_shard: usize,
}

impl<T> Receiver<T> {
    /// Try to receive one value.
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        loop {
            if self.ready_cache.is_empty() {
                self.ready_cache = self.shared.ready.take_ready();
                if self.ready_cache.is_empty() {
                    return if self.is_disconnected() {
                        Err(RecvError::Disconnected)
                    } else {
                        Err(RecvError::Empty)
                    };
                }
            }

            let len = self.consumers.len();
            for _ in 0..len {
                let shard = self.next_shard;
                self.next_shard += 1;
                if self.next_shard == len {
                    self.next_shard = 0;
                }

                if !self.ready_cache.contains(shard) {
                    continue;
                }

                if let Some(value) = self.try_recv_from_shard(shard) {
                    return Ok(value);
                }
            }

            self.ready_cache = ReadyMask::empty();
        }
    }

    #[inline]
    fn try_recv_from_shard(&mut self, shard: usize) -> Option<T> {
        if self.cached_available[shard] > 0 {
            return Some(self.pop_cached_from_shard(shard));
        }

        let prefetched = self.consumers[shard].prefetch();
        if prefetched > 0 {
            self.cached_available[shard] += prefetched;
            return Some(self.pop_cached_from_shard(shard));
        }

        self.shared.ready.clear_shard(shard);

        let prefetched = self.consumers[shard].prefetch();
        if prefetched > 0 {
            self.shared.ready.restore_shard(shard);
            self.cached_available[shard] += prefetched;
            return Some(self.pop_cached_from_shard(shard));
        }

        self.ready_cache.remove(shard);
        None
    }

    #[inline]
    fn pop_cached_from_shard(&mut self, shard: usize) -> T {
        let value = self.consumers[shard]
            .pop()
            .expect("cached_available guarantees prefetched data");
        self.cached_available[shard] -= 1;
        self.unreleased[shard] += 1;
        if self.unreleased[shard] == PREFETCH_LIMIT || self.cached_available[shard] == 0 {
            self.consumers[shard].release();
            self.unreleased[shard] = 0;
        }
        value
    }

    /// Return whether all live senders are gone and all claimed rings drained.
    #[inline]
    pub fn is_disconnected(&self) -> bool {
        if self.shared.live_senders.load(Ordering::Acquire) != 0 {
            return false;
        }

        let claimed = self.shared.claimed.load(Ordering::Acquire);
        for shard in 0..self.consumers.len() {
            if claimed & shard_bit(shard) != 0 && !self.consumers[shard].is_disconnected() {
                return false;
            }
        }
        true
    }

    /// Return the configured sender limit.
    #[inline]
    pub fn max_senders(&self) -> usize {
        self.consumers.len()
    }

    /// Return per-shard capacity after `yring` rounding.
    #[inline]
    pub fn capacity_per_sender(&self) -> usize {
        self.consumers[0].capacity()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_alive.store(false, Ordering::Release);
        for consumer in &mut self.consumers {
            consumer.close();
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("ready_cache", &self.ready_cache)
            .field("next_shard", &self.next_shard)
            .field("max_senders", &self.max_senders())
            .field("capacity_per_sender", &self.capacity_per_sender())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_send_recv() {
        let (mut tx, mut rx) = channel(4, 4);

        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(RecvError::Empty));
    }

    #[test]
    fn try_channel_validates_config() {
        assert_eq!(
            try_channel::<u8>(0, 1).unwrap_err(),
            ChannelError::ZeroSenders
        );
        assert_eq!(
            try_channel::<u8>(MAX_SENDERS + 1, 1).unwrap_err(),
            ChannelError::TooManySenders {
                requested: MAX_SENDERS + 1,
                max: MAX_SENDERS,
            }
        );
        assert_eq!(
            try_channel::<u8>(1, 0).unwrap_err(),
            ChannelError::ZeroCapacity
        );
        assert_eq!(
            try_channel::<u8>(1, MAX_CAPACITY_PER_SENDER + 1).unwrap_err(),
            ChannelError::CapacityTooLarge {
                requested: MAX_CAPACITY_PER_SENDER + 1,
                max: MAX_CAPACITY_PER_SENDER,
            }
        );
    }

    #[test]
    fn capacity_reports_yring_rounding() {
        let (tx, rx) = try_channel::<u8>(1, 3).unwrap();

        assert_eq!(tx.capacity(), 4);
        assert_eq!(rx.capacity_per_sender(), 4);
    }

    #[test]
    fn try_register_reports_failure_cause() {
        let (tx, rx) = channel::<u8>(1, 1);
        assert_eq!(
            tx.try_register().unwrap_err(),
            TryRegisterError::NoSenderSlot
        );
        drop(rx);
        assert_eq!(
            tx.try_register().unwrap_err(),
            TryRegisterError::Disconnected
        );
        assert!(tx.try_clone().is_none());
    }

    #[test]
    fn public_errors_implement_std_error() {
        fn assert_error<E: std::error::Error>() {}

        assert_error::<ChannelError>();
        assert_error::<TryRegisterError>();
        assert_error::<SendError<u8>>();
        assert_error::<RecvError>();

        assert_eq!(
            ChannelError::ZeroSenders.to_string(),
            "max_senders must be > 0"
        );
        assert_eq!(
            TryRegisterError::NoSenderSlot.to_string(),
            "all sender slots are already claimed"
        );
        assert_eq!(SendError::Full(1).to_string(), "sender ring is full");
        assert_eq!(RecvError::Empty.to_string(), "channel is empty");
    }

    #[test]
    fn per_sender_fifo_relaxed_across_senders() {
        let (mut tx0, mut rx) = channel(2, 8);
        let mut tx1 = tx0.try_clone().unwrap();

        for i in 0..4 {
            tx0.try_send((0, i)).unwrap();
            tx1.try_send((1, i)).unwrap();
        }

        let mut seen = [0, 0];
        for _ in 0..8 {
            let (sender, seq) = rx.try_recv().unwrap();
            assert_eq!(seq, seen[sender]);
            seen[sender] += 1;
        }
        assert_eq!(seen, [4, 4]);
    }

    #[test]
    fn sender_limit_is_explicit() {
        let (tx0, _rx) = channel::<u8>(2, 4);
        let tx1 = tx0.try_clone().unwrap();

        assert!(tx0.try_clone().is_none());
        drop(tx1);
        assert!(tx0.try_clone().is_none());
    }

    #[test]
    fn full_is_per_sender() {
        let (mut tx0, mut rx) = channel(2, 2);
        let mut tx1 = tx0.try_clone().unwrap();

        tx0.try_send(1).unwrap();
        tx0.try_send(2).unwrap();
        assert_eq!(tx0.try_send(3), Err(SendError::Full(3)));

        tx1.try_send(10).unwrap();

        let mut values = [rx.try_recv().unwrap(), rx.try_recv().unwrap()];
        values.sort();
        assert_eq!(values, [1, 10]);
    }

    #[test]
    fn receive_releases_capacity_when_prefetched_batch_drains() {
        let (mut tx, mut rx) = channel(1, 4);

        for i in 0..4 {
            tx.try_send(i).unwrap();
        }

        assert_eq!(rx.try_recv(), Ok(0));
        assert_eq!(tx.try_send(4), Err(SendError::Full(4)));

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(3));
        tx.try_send(4).unwrap();
        assert_eq!(rx.try_recv(), Ok(4));
    }

    #[test]
    fn receive_releases_capacity_at_local_batch_limit() {
        let (mut tx, mut rx) = channel(1, 128);

        for i in 0..128 {
            tx.try_send(i).unwrap();
        }

        for i in 0..63 {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert_eq!(tx.try_send(128), Err(SendError::Full(128)));

        assert_eq!(rx.try_recv(), Ok(63));
        tx.try_send(128).unwrap();

        for i in 64..128 {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert_eq!(rx.try_recv(), Ok(128));
    }

    #[test]
    fn disconnects_after_draining() {
        let (mut tx, mut rx) = channel(1, 4);
        tx.try_send(7).unwrap();
        drop(tx);

        assert_eq!(rx.try_recv(), Ok(7));
        assert_eq!(rx.try_recv(), Err(RecvError::Disconnected));
    }

    #[test]
    fn send_fails_after_receiver_drop() {
        let (mut tx, rx) = channel(1, 4);
        drop(rx);

        assert_eq!(tx.try_send(7), Err(SendError::Disconnected(7)));
    }

    #[test]
    fn cross_thread_many_senders() {
        let threads = 4;
        let per_thread = 1_000;
        let (tx0, mut rx) = channel(threads, 64);
        let mut senders = vec![tx0];
        for _ in 1..threads {
            let tx = senders[0].try_clone().unwrap();
            senders.push(tx);
        }

        std::thread::scope(|scope| {
            for (sender_id, mut tx) in senders.into_iter().enumerate() {
                scope.spawn(move || {
                    for seq in 0..per_thread {
                        let mut value = (sender_id, seq);
                        loop {
                            match tx.try_send(value) {
                                Ok(()) => break,
                                Err(SendError::Full(returned)) => {
                                    value = returned;
                                    std::thread::yield_now();
                                }
                                Err(SendError::Disconnected(_)) => panic!("receiver dropped"),
                            }
                        }
                    }
                });
            }

            let mut seen = vec![0usize; threads];
            let mut received = 0;
            while received < threads * per_thread {
                match rx.try_recv() {
                    Ok((sender_id, seq)) => {
                        assert_eq!(seq, seen[sender_id]);
                        seen[sender_id] += 1;
                        received += 1;
                    }
                    Err(RecvError::Empty) => std::thread::yield_now(),
                    Err(RecvError::Disconnected) => panic!("senders disconnected early"),
                }
            }
            assert_eq!(seen, vec![per_thread; threads]);
        });
    }
}
