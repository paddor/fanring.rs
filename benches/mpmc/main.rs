#![cfg_attr(test, allow(dead_code, unused_imports))]

mod adapters;
mod run;
#[path = "../support/mod.rs"]
mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use run::{bench_crossbeam, bench_crossfire, bench_fanring, bench_flume, bench_kanal};
use support::{Affinity, JsonlResults, Sampling, Saturation, median_and_relative_mad};

#[derive(Debug, Clone, Copy)]
struct Config {
    producers: usize,
    consumers: usize,
    capacity_per_sender: usize,
    duration: Duration,
    profile: Profile,
    sample: usize,
    samples: usize,
    expected_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Try,
    Blocking,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Profile {
    Uncontrolled,
    Saturated,
}

#[derive(Debug, Clone, Copy)]
struct Payload<T> {
    label: &'static str,
    value: T,
}

#[derive(Debug)]
struct Filter {
    values: Option<Vec<String>>,
}

#[derive(Debug)]
struct RunContext {
    run_id: String,
    cpu: String,
    affinity: Affinity,
}
impl RunContext {
    /// Tags this run with the current time, the CPU, and the pinning policy.
    fn from_env() -> Self {
        Self {
            run_id: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before Unix epoch")
                .as_nanos()
                .to_string(),
            cpu: cpu_name(),
            affinity: Affinity::from_env(),
        }
    }
}

/// Configurations, implementations, sampling, and mode selected for this run.
#[derive(Debug)]
struct Plan {
    configs: Vec<Config>,
    impl_filter: Filter,
    sampling: Sampling,
    mode: Mode,
}

/// One timed run of one implementation.
#[derive(Debug, Clone, Copy)]
struct Case<'a, T> {
    context: &'a RunContext,
    implementation: &'static str,
    config: Config,
    mode: Mode,
    payload: Payload<T>,
}

type BenchFn<T> = fn(Case<'_, T>) -> Row;

#[derive(Debug, Serialize)]
struct Row {
    run_id: String,
    cpu: String,
    affinity: String,
    mode: &'static str,
    implementation: &'static str,
    payload: &'static str,
    payload_bytes: usize,
    producers: usize,
    consumers: usize,
    capacity_per_sender: usize,
    nominal_capacity: usize,
    capacity_model: &'static str,
    throughput_profile: &'static str,
    low_watermark: u64,
    high_watermark: u64,
    seconds: f64,
    items: u64,
    items_per_sec: f64,
    sample: usize,
    samples: usize,
    expected_rows: usize,
}

struct Outcome {
    elapsed: Duration,
    items: u64,
}

#[cfg(all(test, debug_assertions))]
fn main() {}

#[cfg(not(all(test, debug_assertions)))]
fn main() {
    crossfire::detect_backoff_cfg();
    let duration = Duration::from_secs_f64(
        std::env::var("FANRING_BENCH_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1.0),
    );
    let sampling = Sampling::from_env();
    let mut results = JsonlResults::new("throughput-mpmc.jsonl");
    let payload_filter = Filter::from_env("FANRING_BENCH_PAYLOADS");
    let impl_filter = Filter::from_env("FANRING_BENCH_IMPLS");
    let mode = Mode::from_env();
    let profile = Profile::from_env();
    assert!(
        mode == Mode::Try || profile == Profile::Uncontrolled,
        "saturated profile requires nonblocking mode"
    );
    let context = RunContext::from_env();

    println!(
        "MPMC {} comparison ({}, {} x {:.2}s, {:.2}s warmup, capacity {}, affinity {}, results {})\n",
        profile.label(),
        mode.label(),
        sampling.samples,
        duration.as_secs_f64(),
        sampling.warmup.as_secs_f64(),
        total_capacity(),
        context.affinity.description(),
        results.root().display()
    );

    let expected_rows = producer_counts().len()
        * consumer_counts().len()
        * selected_payload_count(&payload_filter)
        * selected_implementation_count(&impl_filter)
        * sampling.samples;
    let plan = Plan {
        configs: configs(duration, profile, &sampling, expected_rows),
        impl_filter,
        sampling,
        mode,
    };
    run_payloads(&context, &mut results, &plan, &payload_filter);
    results.flush();
}

/// One configuration per producer and consumer count pair, each splitting
/// the total capacity evenly across its senders.
fn configs(
    duration: Duration,
    profile: Profile,
    sampling: &Sampling,
    expected_rows: usize,
) -> Vec<Config> {
    let total_capacity = total_capacity();
    let consumer_counts = consumer_counts();
    producer_counts()
        .into_iter()
        .flat_map(|producers| {
            consumer_counts
                .iter()
                .copied()
                .map(move |consumers| Config {
                    producers,
                    consumers,
                    capacity_per_sender: capacity_per_sender(total_capacity, producers),
                    duration,
                    profile,
                    sample: 0,
                    samples: sampling.samples,
                    expected_rows,
                })
        })
        .collect()
}

/// Runs the plan once per payload that the payload filter selects.
fn run_payloads(
    context: &RunContext,
    results: &mut JsonlResults,
    plan: &Plan,
    payload_filter: &Filter,
) {
    if payload_filter.matches("u64") {
        run_payload(
            context,
            results,
            plan,
            Payload {
                label: "u64",
                value: 0u64,
            },
        );
    }
    if payload_filter.matches("bytes64") {
        run_payload(
            context,
            results,
            plan,
            Payload {
                label: "bytes64",
                value: [0u64; 8],
            },
        );
    }
    if payload_filter.matches("bytes256") {
        run_payload(
            context,
            results,
            plan,
            Payload {
                label: "bytes256",
                value: [0u64; 32],
            },
        );
    }
}

fn run_payload<T>(
    context: &RunContext,
    results: &mut JsonlResults,
    plan: &Plan,
    payload: Payload<T>,
) where
    T: Copy + Send + 'static,
{
    let Plan {
        configs,
        impl_filter,
        sampling,
        mode,
    } = plan;
    let implementations = implementations::<T>(impl_filter);
    if implementations.is_empty() {
        return;
    }
    let case = |implementation, config| Case {
        context,
        implementation,
        config,
        mode: *mode,
        payload,
    };

    println!("--- {} ({} bytes) ---", payload.label, size_of::<T>());
    for &config in configs {
        println!(
            "  producers={:<2} consumers={:<2} capacity_per_sender={:<4}",
            config.producers, config.consumers, config.capacity_per_sender
        );
        if !sampling.warmup.is_zero() {
            let warmup = Config {
                duration: sampling.warmup,
                ..config
            };
            for &(implementation, bench) in &implementations {
                let _ = bench(case(implementation, warmup));
            }
        }

        let mut rows = Vec::new();
        for sample in 0..sampling.samples {
            let measured = Config { sample, ..config };
            let start = sample % implementations.len();
            for offset in 0..implementations.len() {
                let (implementation, bench) =
                    implementations[(start + offset) % implementations.len()];
                let row = bench(case(implementation, measured));
                println!(
                    "    sample={:<2} {:<18} {:>8.2}M items/s",
                    sample + 1,
                    row.implementation,
                    row.items_per_sec / 1_000_000.0
                );
                results.write(row.implementation, &row);
                rows.push(row);
            }
        }

        print_medians(&implementations, &rows);
        println!();
    }
}

fn implementations<T>(impl_filter: &Filter) -> Vec<(&'static str, BenchFn<T>)>
where
    T: Copy + Send + 'static,
{
    let mut implementations: Vec<(&'static str, BenchFn<T>)> = Vec::new();
    if impl_filter.matches("fanring-mpmc") {
        implementations.push((
            "fanring-mpmc",
            bench_fanring::<T, fanring::teardown::Deferred>,
        ));
    }
    if impl_filter.matches("fanring-coordinated-mpmc") {
        implementations.push((
            "fanring-coordinated-mpmc",
            bench_fanring::<T, fanring::teardown::Coordinated>,
        ));
    }
    if impl_filter.matches("crossbeam-channel") {
        implementations.push(("crossbeam-channel", bench_crossbeam::<T>));
    }
    if impl_filter.matches("crossfire-mpmc") {
        implementations.push(("crossfire-mpmc", bench_crossfire::<T>));
    }
    if impl_filter.matches("flume") {
        implementations.push(("flume", bench_flume::<T>));
    }
    if impl_filter.matches("kanal") {
        implementations.push(("kanal", bench_kanal::<T>));
    }
    implementations
}

fn print_medians<T>(implementations: &[(&'static str, BenchFn<T>)], rows: &[Row]) {
    for (implementation, _) in implementations {
        let (median, relative_mad) = median_and_relative_mad(
            rows.iter()
                .filter(|row| row.implementation == *implementation)
                .map(|row| row.items_per_sec),
        );
        println!(
            "    {:<18} median {:>8.2}M items/s  MAD {:>5.2}%",
            implementation,
            median / 1_000_000.0,
            relative_mad
        );
    }
}

fn row<T>(case: Case<'_, T>, outcome: Outcome, saturation: Option<&Saturation>) -> Row {
    let Case {
        context,
        implementation,
        config,
        mode,
        payload,
    } = case;
    let (throughput_profile, low_watermark, high_watermark) =
        saturation.map_or(("uncontrolled", 0, 0), |saturation| {
            (
                "saturated",
                saturation.low_watermark(),
                saturation.high_watermark(),
            )
        });

    Row {
        run_id: context.run_id.clone(),
        cpu: context.cpu.clone(),
        affinity: context.affinity.description().to_string(),
        mode: mode.label(),
        implementation,
        payload: payload.label,
        payload_bytes: size_of::<T>(),
        producers: config.producers,
        consumers: config.consumers,
        capacity_per_sender: config.capacity_per_sender,
        nominal_capacity: config.total_capacity(),
        capacity_model: if implementation.starts_with("fanring") {
            "per-ring-hwm-with-staging"
        } else {
            "shared-bound"
        },
        throughput_profile,
        low_watermark,
        high_watermark,
        seconds: outcome.elapsed.as_secs_f64(),
        items: outcome.items,
        items_per_sec: outcome.items as f64 / outcome.elapsed.as_secs_f64(),
        sample: config.sample,
        samples: config.samples,
        expected_rows: config.expected_rows,
    }
}

impl Config {
    fn total_capacity(self) -> usize {
        self.producers * self.capacity_per_sender
    }
}

impl Mode {
    fn from_env() -> Self {
        match std::env::var("FANRING_BENCH_MODE").as_deref() {
            Ok("blocking") => Self::Blocking,
            Ok("try") | Err(_) => Self::Try,
            Ok(value) => panic!("invalid benchmark mode {value:?}; expected try or blocking"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Try => "try",
            Self::Blocking => "blocking",
        }
    }
}

impl Profile {
    fn from_env() -> Self {
        match std::env::var("FANRING_BENCH_PROFILE").as_deref() {
            Ok("saturated") => Self::Saturated,
            Ok("uncontrolled") | Err(_) => Self::Uncontrolled,
            Ok(value) => {
                panic!("invalid benchmark profile {value:?}; expected uncontrolled or saturated")
            }
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Uncontrolled => "uncontrolled",
            Self::Saturated => "saturated, 50-100% occupancy",
        }
    }
}

impl Filter {
    fn from_env(name: &str) -> Self {
        Self {
            values: std::env::var(name).ok().map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect()
            }),
        }
    }

    fn matches(&self, value: &str) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.iter().any(|candidate| candidate == value))
    }
}

fn clones<T: Clone>(value: &T, count: usize) -> Vec<T> {
    (0..count).map(|_| value.clone()).collect()
}

fn producer_counts() -> Vec<usize> {
    counts_from_env("FANRING_BENCH_PRODUCERS", &[1, 2, 4, 8])
}

fn consumer_counts() -> Vec<usize> {
    counts_from_env("FANRING_BENCH_CONSUMERS", &[1, 2, 4, 8])
}

fn selected_payload_count(filter: &Filter) -> usize {
    ["u64", "bytes64", "bytes256"]
        .into_iter()
        .filter(|payload| filter.matches(payload))
        .count()
}

fn selected_implementation_count(filter: &Filter) -> usize {
    [
        "fanring-mpmc",
        "fanring-coordinated-mpmc",
        "crossbeam-channel",
        "crossfire-mpmc",
        "flume",
        "kanal",
    ]
    .into_iter()
    .filter(|implementation| filter.matches(implementation))
    .count()
}

fn counts_from_env(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.parse().expect("invalid thread count"))
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn total_capacity() -> usize {
    std::env::var("FANRING_BENCH_CAPACITY")
        .ok()
        .map(|value| value.parse().expect("invalid total capacity"))
        .unwrap_or(8192)
}

fn capacity_per_sender(total_capacity: usize, producers: usize) -> usize {
    assert!(producers > 0, "producer count must be > 0");
    assert!(
        total_capacity.is_multiple_of(producers),
        "total capacity must divide producer count"
    );
    total_capacity / producers
}

fn cpu_name() -> String {
    let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") else {
        return "unknown CPU".to_string();
    };
    cpuinfo
        .lines()
        .find_map(|line| line.strip_prefix("model name\t: "))
        .unwrap_or("unknown CPU")
        .to_string()
}
