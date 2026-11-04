//! Sample counts, per-run outcomes, and robust sample statistics.

use std::time::Duration;

use super::options::{env_f64, env_usize};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Sampling {
    pub(crate) samples: usize,
    pub(crate) warmup: Duration,
}

/// Elapsed time and received item count of one timed run.
pub(crate) struct Outcome {
    pub(crate) elapsed: Duration,
    pub(crate) items: u64,
}

impl Sampling {
    pub(crate) fn from_env() -> Self {
        Self {
            samples: env_usize("FANRING_BENCH_SAMPLES", 5),
            warmup: Duration::from_secs_f64(env_f64("FANRING_BENCH_WARMUP_SECS", 0.25)),
        }
    }
}

pub(crate) fn median_and_relative_mad(values: impl IntoIterator<Item = f64>) -> (f64, f64) {
    let mut values = values.into_iter().collect::<Vec<_>>();
    assert!(!values.is_empty(), "benchmark sample set is empty");
    values.sort_by(f64::total_cmp);
    let median = median_sorted(&values);
    let mut deviations = values
        .into_iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let mad = median_sorted(&deviations);
    let relative_mad = if median == 0.0 {
        0.0
    } else {
        mad / median * 100.0
    };
    (median, relative_mad)
}

fn median_sorted(values: &[f64]) -> f64 {
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}
