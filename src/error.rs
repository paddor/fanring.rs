use std::fmt;

/// Invalid channel configuration shared by both channel variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelError {
    /// `capacity_per_sender` was zero.
    ZeroCapacity,
    /// `capacity_per_sender` exceeded the reported maximum.
    CapacityTooLarge {
        /// Requested capacity.
        requested: usize,
        /// Maximum supported capacity.
        max: usize,
    },
}

impl ChannelError {
    /// Return whether capacity was zero.
    #[inline]
    #[must_use]
    pub const fn is_zero_capacity(&self) -> bool {
        matches!(self, Self::ZeroCapacity)
    }

    /// Return whether capacity exceeded the supported maximum.
    #[inline]
    #[must_use]
    pub const fn is_capacity_too_large(&self) -> bool {
        matches!(self, Self::CapacityTooLarge { .. })
    }
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::ZeroCapacity => f.write_str("capacity_per_sender must be > 0"),
            Self::CapacityTooLarge { max, .. } => {
                write!(f, "capacity_per_sender must be <= {max}")
            }
        }
    }
}

impl std::error::Error for ChannelError {}

/// Sender registration failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRegisterError {
    /// All receivers have been dropped.
    Disconnected,
}

impl TryRegisterError {
    /// Return whether every receiver was dropped.
    #[inline]
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}

impl fmt::Display for TryRegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => f.write_str("all receivers have been dropped"),
        }
    }
}

impl std::error::Error for TryRegisterError {}

/// Blocking send failed because the receiving side disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    /// Return the unsent value.
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Return whether every receiver was dropped.
    #[inline]
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        true
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("all receivers have been dropped")
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

/// Non-blocking send failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// This sender's ring is full.
    Full(T),
    /// All receivers have been dropped.
    Disconnected(T),
}

impl<T> TrySendError<T> {
    /// Return the unsent value.
    #[inline]
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(value) | Self::Disconnected(value) => value,
        }
    }

    /// Return whether this sender's ring was full.
    #[inline]
    #[must_use]
    pub const fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }

    /// Return whether every receiver was dropped.
    #[inline]
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("sender ring is full"),
            Self::Disconnected(_) => f.write_str("all receivers have been dropped"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {}

/// Timed send failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendTimeoutError<T> {
    /// The timeout elapsed while this sender's ring remained full.
    Timeout(T),
    /// All receivers were dropped.
    Disconnected(T),
}

impl<T> SendTimeoutError<T> {
    /// Return the unsent value.
    #[inline]
    pub fn into_inner(self) -> T {
        match self {
            Self::Timeout(value) | Self::Disconnected(value) => value,
        }
    }

    /// Return whether the timeout elapsed.
    #[inline]
    #[must_use]
    pub const fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    /// Return whether every receiver was dropped.
    #[inline]
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }
}

impl<T> fmt::Display for SendTimeoutError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(_) => f.write_str("timed out waiting for sender capacity"),
            Self::Disconnected(_) => f.write_str("all receivers have been dropped"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for SendTimeoutError<T> {}

/// Blocking receive failed because the sending side disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl RecvError {
    /// Return whether every sender was dropped.
    #[inline]
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        true
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("all senders have been dropped")
    }
}

impl std::error::Error for RecvError {}

/// Non-blocking receive failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value is visible right now.
    Empty,
    /// All senders are gone and every ring and staged queue is drained.
    Disconnected,
}

impl TryRecvError {
    /// Return whether no value was immediately available.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    /// Return whether every sender was dropped and buffered work was drained.
    #[inline]
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("channel is empty"),
            Self::Disconnected => f.write_str("all senders have been dropped"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Timed receive failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimeoutError {
    /// The timeout elapsed while the channel remained empty.
    Timeout,
    /// All senders are gone and every ring and staged queue is drained.
    Disconnected,
}

impl RecvTimeoutError {
    /// Return whether the timeout elapsed.
    #[inline]
    #[must_use]
    pub const fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }

    /// Return whether every sender was dropped and buffered work was drained.
    #[inline]
    #[must_use]
    pub const fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}

impl fmt::Display for RecvTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("timed out waiting for a message"),
            Self::Disconnected => f.write_str("all senders have been dropped"),
        }
    }
}

impl std::error::Error for RecvTimeoutError {}
