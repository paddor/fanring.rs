//! Fast typed in-process MPSC and MPMC channels built from SPSC rings.
//!
//! Every sender owns one bounded ring. Producers therefore avoid contention on
//! a shared queue tail, while receiver-side batching amortizes synchronization.
//! Sender registration is dynamic and dropped lane slots are reused.
//!
//! # Choose a channel
//!
//! - [`mpsc`] has one receiver, preserves FIFO within each sender lane, and has
//!   the smallest receive-side overhead.
//! - [`mpmc`] has cloneable competing receivers and relaxed ordering. Receivers
//!   stage batches in stealable local FIFOs, so total resident work can exceed
//!   the sum of sender-ring capacities.
//!
//! Both variants provide nonblocking, blocking, timeout, and deadline APIs.
//! Capacity is a high-water mark per sender, not one shared channel bound.
//!
//! # Example
//!
//! ```
//! use fanring::mpsc;
//!
//! let (mut tx0, mut rx) = mpsc::channel(64);
//! let mut tx1 = tx0.try_clone().expect("receiver is alive");
//!
//! tx0.send("first").unwrap();
//! tx1.send("second").unwrap();
//!
//! let mut values = [rx.recv().unwrap(), rx.recv().unwrap()];
//! values.sort_unstable();
//! assert_eq!(values, ["first", "second"]);
//! ```
//!
//! See each channel module for its exact ordering, capacity, and disconnection
//! contracts.

mod compat;
mod config;
mod error;
pub mod mpmc;
pub mod mpsc;
mod publication;
mod ready;
mod wait;
