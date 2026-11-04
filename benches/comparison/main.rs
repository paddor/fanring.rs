#![cfg_attr(test, allow(dead_code, unused_imports))]

mod run;
#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use serde::Serialize;

use run::{
    bench_concurrent_queue, bench_crossbeam_channel, bench_crossfire, bench_fanring, bench_flume,
    bench_kanal, bench_thingbuf,
};
use support::{
    Filter, JsonlResults, Mode, Outcome, Payload, PayloadRun, Profile, RunContext, Sampling,
    Saturation, bench_duration, capacity_per_sender, for_each_payload, median_and_relative_mad,
    producer_counts, selected_payload_count, total_capacity,
};

#[derive(Debug, Clone, Copy)]
struct Config {
    producers: usize,
    capacity_per_sender: usize,
    duration: Duration,
    profile: Profile,
    sample: usize,
    samples: usize,
    expected_rows: usize,
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

// Cargo sets `cfg(test)` for bench targets. Keep `cargo test --all-targets`
// from running the full benchmark, while normal optimized `cargo bench` runs.
#[cfg(all(test, debug_assertions))]
fn main() {}

#[cfg(not(all(test, debug_assertions)))]
fn main() {
    crossfire::detect_backoff_cfg();
    let duration = bench_duration();
    let sampling = Sampling::from_env();
    let mut results = JsonlResults::new("throughput-mpsc.jsonl");
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
        "MPSC {} comparison ({}, {} x {:.2}s, {:.2}s warmup, capacity {} items, affinity {}, results {})\n",
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
        * selected_payload_count(&payload_filter)
        * selected_implementation_count(&impl_filter, mode)
        * sampling.samples;
    let plan = Plan {
        configs: configs(duration, profile, &sampling, expected_rows),
        impl_filter,
        sampling,
        mode,
    };
    for_each_payload(
        &payload_filter,
        &mut Runner {
            context: &context,
            results: &mut results,
            plan: &plan,
        },
    );
    results.flush();
}

/// One configuration per producer count, each splitting the total capacity
/// evenly across its senders.
fn configs(
    duration: Duration,
    profile: Profile,
    sampling: &Sampling,
    expected_rows: usize,
) -> Vec<Config> {
    let total_capacity = total_capacity();
    producer_counts()
        .into_iter()
        .map(|producers| Config {
            producers,
            capacity_per_sender: capacity_per_sender(total_capacity, producers),
            duration,
            profile,
            sample: 0,
            samples: sampling.samples,
            expected_rows,
        })
        .collect()
}

/// Runs the plan for one payload type.
struct Runner<'a> {
    context: &'a RunContext,
    results: &'a mut JsonlResults,
    plan: &'a Plan,
}

impl PayloadRun for Runner<'_> {
    fn run<T: Copy + Default + Send + Sync + 'static>(&mut self, payload: Payload<T>) {
        run_payload(self.context, self.results, self.plan, payload);
    }
}

fn run_payload<T>(
    context: &RunContext,
    results: &mut JsonlResults,
    plan: &Plan,
    payload: Payload<T>,
) where
    T: Copy + Default + Send + Sync + 'static,
{
    let Plan {
        configs,
        impl_filter,
        sampling,
        mode,
    } = plan;
    let implementations = implementations::<T>(impl_filter, *mode);
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
            "  producers={:<2} capacity_per_sender={:<4} total_capacity={}",
            config.producers,
            config.capacity_per_sender,
            config.total_capacity()
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

fn implementations<T>(impl_filter: &Filter, mode: Mode) -> Vec<(&'static str, BenchFn<T>)>
where
    T: Copy + Default + Send + Sync + 'static,
{
    let mut implementations: Vec<(&'static str, BenchFn<T>)> = Vec::new();
    if impl_filter.matches("fanring") {
        implementations.push(("fanring", bench_fanring::<T, fanring::teardown::Deferred>));
    }
    if impl_filter.matches("fanring-coordinated") {
        implementations.push((
            "fanring-coordinated",
            bench_fanring::<T, fanring::teardown::Coordinated>,
        ));
    }
    if impl_filter.matches("crossbeam-channel") {
        implementations.push(("crossbeam-channel", bench_crossbeam_channel::<T>));
    }
    if impl_filter.matches("crossfire") {
        implementations.push(("crossfire", bench_crossfire::<T>));
    }
    if impl_filter.matches("flume") {
        implementations.push(("flume", bench_flume::<T>));
    }
    if impl_filter.matches("kanal") {
        implementations.push(("kanal", bench_kanal::<T>));
    }
    if mode == Mode::Try && impl_filter.matches("concurrent-queue") {
        implementations.push(("concurrent-queue", bench_concurrent_queue::<T>));
    }
    if impl_filter.matches("thingbuf") {
        implementations.push(("thingbuf", bench_thingbuf::<T>));
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
        capacity_per_sender: config.capacity_per_sender,
        nominal_capacity: config.total_capacity(),
        capacity_model: if implementation.starts_with("fanring") {
            "per-ring-hwm"
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

fn selected_implementation_count(filter: &Filter, mode: Mode) -> usize {
    [
        ("fanring", true),
        ("fanring-coordinated", true),
        ("crossbeam-channel", true),
        ("crossfire", true),
        ("flume", true),
        ("kanal", true),
        ("concurrent-queue", mode == Mode::Try),
        ("thingbuf", true),
    ]
    .into_iter()
    .filter(|(implementation, supported)| *supported && filter.matches(implementation))
    .count()
}
