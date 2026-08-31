//! Fast typed in-process channels built from SPSC rings.

mod compat;
mod config;
mod error;
pub mod mpmc;
pub mod mpsc;
mod publication;
mod ready;
mod wait;
