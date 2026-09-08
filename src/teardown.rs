//! Choose when unread payloads are destroyed after the last receiver drops.
//!
//! Both policies disconnect the channel and wake blocked senders. The policy
//! is fixed at construction and inherited by every cloned handle and lane.
//! It is part of the channel type, so deferred sends omit cleanup coordination.

use std::fmt::Debug;

mod sealed {
    pub trait Sealed {}
}

/// A channel's teardown policy. Only [`Deferred`] and [`Coordinated`] implement it.
pub trait Teardown: sealed::Sealed + Debug + Send + Sync + 'static {
    #[doc(hidden)]
    const COORDINATED: bool;
}

/// Defer destruction of unread ring payloads until their sender releases the ring.
///
/// This is the default. Dropping the last receiver disconnects the channel but
/// may retain unread payloads for as long as sender handles remain alive.
/// Receiver-local staged values can be destroyed earlier. Payload destructors
/// must tolerate this delay; queued reply senders and permits remain alive too.
/// No cleanup handshake is performed on sends.
#[derive(Debug, Clone, Copy, Default)]
pub struct Deferred;

/// Reclaim unread payloads without requiring sender handles to be dropped.
///
/// The last receiver drains unread values and hands any remaining cleanup to
/// an overlapping send. Late publications are destroyed when that send resumes.
/// Receiver teardown does not wait for a paused producer, so destruction of a
/// late publication can occur after receiver drop returns. Payload destructors
/// run on either the receiver or producer thread, outside the handoff mutex.
/// As with ordinary Rust destruction, a payload destructor itself can block or
/// panic. Ring allocations still live as long as their sender handles.
///
/// Sends pay for an atomic ownership handshake. Use this policy when reply
/// cancellation or resource release must not depend on idle sender lifetimes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Coordinated;

impl sealed::Sealed for Deferred {}
impl sealed::Sealed for Coordinated {}

impl Teardown for Deferred {
    const COORDINATED: bool = false;
}

impl Teardown for Coordinated {
    const COORDINATED: bool = true;
}
