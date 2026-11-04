//! Harness code shared by the benchmark targets: run tags, environment
//! options, payloads, channel adapters, CPU pinning, saturation control,
//! sample statistics, and JSONL results.

mod adapters;
mod affinity;
mod context;
mod options;
mod payload;
mod results;
mod sampling;
mod saturation;
#[cfg(test)]
mod tests;

pub(crate) use adapters::{
    BenchReceiver, BenchSender, ConcurrentReceiver, ConcurrentSender, RecvAttempt, SendAttempt,
    clones,
};
pub(crate) use affinity::Affinity;
pub(crate) use context::RunContext;
pub(crate) use options::{
    Filter, Mode, Profile, bench_duration, capacity_per_sender, env_counts, producer_counts,
    total_capacity,
};
pub(crate) use payload::{Payload, PayloadRun, for_each_payload, selected_payload_count};
pub(crate) use results::JsonlResults;
pub(crate) use sampling::{Outcome, Sampling, median_and_relative_mad};
pub(crate) use saturation::Saturation;
