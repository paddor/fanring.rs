//! Bulk receives, slot release, deferred sends, and scans on the MPSC
//! channel.

mod admission;
mod deferred;
mod recv_batch;
mod release;
mod scan;
mod support;
mod try_recv_batch;
