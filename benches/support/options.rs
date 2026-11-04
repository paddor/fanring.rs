//! Benchmark options read from the environment.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Try,
    Blocking,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Profile {
    Uncontrolled,
    Saturated,
}

/// Comma-separated allow list. An unset variable matches every value.
#[derive(Debug)]
pub(crate) struct Filter {
    values: Option<Vec<String>>,
}

impl Mode {
    pub(crate) fn from_env() -> Self {
        match std::env::var("FANRING_BENCH_MODE").as_deref() {
            Ok("blocking") => Self::Blocking,
            Ok("try") | Err(_) => Self::Try,
            Ok(value) => panic!("invalid benchmark mode {value:?}; expected try or blocking"),
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Try => "try",
            Self::Blocking => "blocking",
        }
    }
}

impl Profile {
    pub(crate) fn from_env() -> Self {
        match std::env::var("FANRING_BENCH_PROFILE").as_deref() {
            Ok("saturated") => Self::Saturated,
            Ok("uncontrolled") | Err(_) => Self::Uncontrolled,
            Ok(value) => {
                panic!("invalid benchmark profile {value:?}; expected uncontrolled or saturated")
            }
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Uncontrolled => "uncontrolled",
            Self::Saturated => "saturated, 50-100% occupancy",
        }
    }
}

impl Filter {
    pub(crate) fn from_env(name: &str) -> Self {
        let values = std::env::var(name).ok().map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect()
        });
        Self { values }
    }

    /// Whether the variable was set, so the filter may select opt-in values.
    pub(crate) fn is_set(&self) -> bool {
        self.values.is_some()
    }

    pub(crate) fn matches(&self, value: &str) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.iter().any(|candidate| candidate == value))
    }
}

/// Measured duration of one sample, from `FANRING_BENCH_SECS`.
pub(crate) fn bench_duration() -> Duration {
    Duration::from_secs_f64(env_f64("FANRING_BENCH_SECS", 1.0))
}

/// Total channel capacity, split across senders, from
/// `FANRING_BENCH_CAPACITY`.
pub(crate) fn total_capacity() -> usize {
    env_usize("FANRING_BENCH_CAPACITY", 8192)
}

pub(crate) fn producer_counts() -> Vec<usize> {
    env_counts("FANRING_BENCH_PRODUCERS", &[1, 2, 4, 8])
}

pub(crate) fn capacity_per_sender(total_capacity: usize, producers: usize) -> usize {
    assert!(producers > 0, "producer count must be > 0");
    assert!(
        total_capacity.is_multiple_of(producers),
        "total capacity must divide producer count"
    );
    total_capacity / producers
}

/// Comma-separated counts from `name`, or `default` when it is unset.
pub(crate) fn env_counts(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| {
                    value
                        .parse()
                        .unwrap_or_else(|_| panic!("{name} must list integers"))
                })
                .collect()
        },
    )
}

pub(super) fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value != 0)
            .unwrap_or_else(|| panic!("{name} must be a positive integer")),
        Err(_) => default,
    }
}

pub(super) fn env_f64(name: &str, default: f64) -> f64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or_else(|| panic!("{name} must be a nonnegative finite number")),
        Err(_) => default,
    }
}
