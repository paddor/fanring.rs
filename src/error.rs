use std::fmt;

/// Invalid channel configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelError {
    /// `max_senders` was zero.
    ZeroSenders,
    /// `max_senders` exceeded [`crate::MAX_SENDERS`].
    TooManySenders {
        /// Requested sender count.
        requested: usize,
        /// Maximum supported sender count.
        max: usize,
    },
    /// `capacity_per_sender` was zero.
    ZeroCapacity,
    /// `capacity_per_sender` exceeded [`crate::MAX_CAPACITY_PER_SENDER`].
    CapacityTooLarge {
        /// Requested capacity.
        requested: usize,
        /// Maximum supported capacity.
        max: usize,
    },
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::ZeroSenders => f.write_str("max_senders must be > 0"),
            Self::TooManySenders { max, .. } => {
                write!(f, "max_senders must be <= {max} for the ready bitmask")
            }
            Self::ZeroCapacity => f.write_str("capacity_per_sender must be > 0"),
            Self::CapacityTooLarge { max, .. } => {
                write!(f, "capacity_per_sender must be <= {max}")
            }
        }
    }
}

impl std::error::Error for ChannelError {}

/// Error from [`crate::Sender::try_register`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRegisterError {
    /// The receiver has been dropped.
    Disconnected,
    /// All sender rings are already claimed.
    NoSenderSlot,
}

impl fmt::Display for TryRegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => f.write_str("receiver has been dropped"),
            Self::NoSenderSlot => f.write_str("all sender slots are already claimed"),
        }
    }
}

impl std::error::Error for TryRegisterError {}

/// Error from [`crate::Sender::try_send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// This sender's ring is full.
    Full(T),
    /// The receiver has been dropped.
    Disconnected(T),
}

impl<T> SendError<T> {
    /// Return the unsent value.
    #[inline]
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(value) | Self::Disconnected(value) => value,
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("sender ring is full"),
            Self::Disconnected(_) => f.write_str("receiver has been dropped"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

/// Error from [`crate::Receiver::try_recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// No claimed sender ring has visible data right now.
    Empty,
    /// All senders are gone and all claimed rings are drained.
    Disconnected,
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("channel is empty"),
            Self::Disconnected => f.write_str("all senders have been dropped"),
        }
    }
}

impl std::error::Error for RecvError {}
