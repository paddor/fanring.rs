//! Fast typed in-process channels built from SPSC rings.

mod compat;
mod error;
pub mod mpmc;
pub mod mpsc;
mod ready;
mod wait;
