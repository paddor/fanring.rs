#![cfg_attr(test, allow(dead_code, unused_imports))]

mod support;

use std::fs;
use std::hint::black_box;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use support::{Sampling, append_jsonl, median_and_relative_mad};

#[derive(Debug, Clone, Copy)]
struct Config {
    producers: usize,
    capacity_per_sender: usize,
    duration: Duration,
    sample: usize,
    samples: usize,
    expected_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Try,
    Blocking,
}

#[derive(Debug)]
struct RunContext {
    run_id: String,
    cpu: String,
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

#[derive(Debug, Serialize)]
struct Row {
    run_id: String,
    cpu: String,
    mode: &'static str,
    implementation: &'static str,
    payload: &'static str,
    payload_bytes: usize,
    producers: usize,
    capacity_per_sender: usize,
    total_capacity: usize,
    seconds: f64,
    items: u64,
    items_per_sec: f64,
    sample: usize,
    samples: usize,
    expected_rows: usize,
}

const TIME_CHECK_INTERVAL: u64 = 1024;

// Cargo sets `cfg(test)` for bench targets. Keep `cargo test --all-targets`
// from running the full benchmark, while normal optimized `cargo bench` runs.
#[cfg(all(test, debug_assertions))]
fn main() {}

#[cfg(not(all(test, debug_assertions)))]
fn main() {
    let duration = Duration::from_secs_f64(
        std::env::var("FANRING_BENCH_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1.0),
    );
    let sampling = Sampling::from_env();
    let out_path = std::env::var_os("FANRING_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fanring-bench/results.jsonl"));
    let payload_filter = Filter::from_env("FANRING_BENCH_PAYLOADS");
    let impl_filter = Filter::from_env("FANRING_BENCH_IMPLS");
    let mode = Mode::from_env();
    let context = RunContext {
        run_id: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
            .to_string(),
        cpu: cpu_name(),
    };

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).expect("create benchmark output dir");
    }
    let mut out = append_jsonl(&out_path);

    println!(
        "MPSC comparison ({}, {} x {:.2}s, {:.2}s warmup, capacity {} items, output {})\n",
        mode.label(),
        sampling.samples,
        duration.as_secs_f64(),
        sampling.warmup.as_secs_f64(),
        total_capacity(),
        out_path.display()
    );

    let total_capacity = total_capacity();
    let producer_counts = producer_counts();
    let expected_rows = producer_counts.len()
        * selected_payload_count(&payload_filter)
        * selected_implementation_count(&impl_filter, mode)
        * sampling.samples;
    let configs: Vec<Config> = producer_counts
        .into_iter()
        .map(|producers| Config {
            producers,
            capacity_per_sender: capacity_per_sender(total_capacity, producers),
            duration,
            sample: 0,
            samples: sampling.samples,
            expected_rows,
        })
        .collect();

    if payload_filter.matches("u64") {
        run_payload(
            &context,
            &mut out,
            &configs,
            &impl_filter,
            sampling,
            mode,
            Payload {
                label: "u64",
                value: 0u64,
            },
        );
    }
    if payload_filter.matches("bytes64") {
        run_payload(
            &context,
            &mut out,
            &configs,
            &impl_filter,
            sampling,
            mode,
            Payload {
                label: "bytes64",
                value: [0u64; 8],
            },
        );
    }
    if payload_filter.matches("bytes256") {
        run_payload(
            &context,
            &mut out,
            &configs,
            &impl_filter,
            sampling,
            mode,
            Payload {
                label: "bytes256",
                value: [0u64; 32],
            },
        );
    }

    out.flush().expect("flush benchmark JSONL");
}

fn run_payload<T>(
    context: &RunContext,
    out: &mut impl Write,
    configs: &[Config],
    impl_filter: &Filter,
    sampling: Sampling,
    mode: Mode,
    payload: Payload<T>,
) where
    T: Copy + Default + Send + Sync + 'static,
{
    type BenchFn<T> = fn(&RunContext, Config, Mode, Payload<T>) -> Row;

    let mut implementations: Vec<(&str, BenchFn<T>)> = Vec::new();
    if impl_filter.matches("fanring") {
        implementations.push(("fanring", bench_fanring::<T>));
    }
    if impl_filter.matches("crossbeam-channel") {
        implementations.push(("crossbeam-channel", bench_crossbeam_channel::<T>));
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
    if implementations.is_empty() {
        return;
    }

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
            for (_, bench) in &implementations {
                let _ = bench(context, warmup, mode, payload);
            }
        }

        let mut rows = Vec::new();
        for sample in 0..sampling.samples {
            let measured = Config { sample, ..config };
            let start = sample % implementations.len();
            for offset in 0..implementations.len() {
                let (_, bench) = implementations[(start + offset) % implementations.len()];
                let row = bench(context, measured, mode, payload);
                println!(
                    "    sample={:<2} {:<18} {:>8.2}M items/s",
                    sample + 1,
                    row.implementation,
                    row.items_per_sec / 1_000_000.0
                );
                serde_json::to_writer(&mut *out, &row).expect("write benchmark row");
                writeln!(out).expect("write benchmark newline");
                rows.push(row);
            }
        }

        for (implementation, _) in &implementations {
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
        println!();
    }
}

fn bench_fanring<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx0, mut rx) = fanring::mpsc::channel::<T>(config.capacity_per_sender);
    let mut senders = vec![tx0];
    for _ in 1..config.producers {
        let tx = senders[0].try_clone().expect("fanring sender slot");
        senders.push(tx);
    }

    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for mut tx in senders {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => match tx.try_send(value) {
                        Ok(()) => sent += 1,
                        Err(fanring::mpsc::TrySendError::Full(_)) => thread::yield_now(),
                        Err(fanring::mpsc::TrySendError::Disconnected(_)) => break,
                    },
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                        sent += 1;
                    }
                }
            }
            sent
        }));
    }

    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let mut polls = 0u64;
    while !reached_deadline(polls, deadline) {
        polls = polls.wrapping_add(1);
        match mode {
            Mode::Try => match rx.try_recv() {
                Ok(value) => {
                    black_box(value);
                    messages += 1;
                }
                Err(
                    fanring::mpsc::TryRecvError::Empty | fanring::mpsc::TryRecvError::Disconnected,
                ) => {
                    thread::yield_now();
                }
            },
            Mode::Blocking => match rx.recv() {
                Ok(value) => {
                    black_box(value);
                    messages += 1;
                }
                Err(_) => break,
            },
        }
    }
    stop.store(true, Ordering::Relaxed);
    while let Ok(value) = rx.recv() {
        black_box(value);
        messages += 1;
    }
    let sent = handles
        .into_iter()
        .map(|handle| handle.join().expect("fanring producer thread"))
        .sum::<u64>();
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "fanring lost messages");
    row(context, "fanring", mode, payload, config, elapsed, messages)
}

fn bench_crossbeam_channel<T>(
    context: &RunContext,
    config: Config,
    mode: Mode,
    payload: Payload<T>,
) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = crossbeam_channel::bounded::<T>(config.total_capacity());

    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => match tx.try_send(value) {
                        Ok(()) => sent += 1,
                        Err(crossbeam_channel::TrySendError::Full(_)) => thread::yield_now(),
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => break,
                    },
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                        sent += 1;
                    }
                }
            }
            sent
        }));
    }
    drop(tx);

    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let mut polls = 0u64;
    while !reached_deadline(polls, deadline) {
        polls = polls.wrapping_add(1);
        let result = match mode {
            Mode::Try => rx.try_recv().ok(),
            Mode::Blocking => rx.recv().ok(),
        };
        if let Some(value) = result {
            black_box(value);
            messages += 1;
        } else if mode == Mode::Try {
            thread::yield_now();
        } else {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    while let Ok(value) = rx.recv() {
        black_box(value);
        messages += 1;
    }
    let sent = handles
        .into_iter()
        .map(|handle| handle.join().expect("crossbeam producer thread"))
        .sum::<u64>();
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "crossbeam-channel lost messages");
    row(
        context,
        "crossbeam-channel",
        mode,
        payload,
        config,
        elapsed,
        messages,
    )
}

fn bench_flume<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = flume::bounded::<T>(config.total_capacity());

    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => match tx.try_send(value) {
                        Ok(()) => sent += 1,
                        Err(flume::TrySendError::Full(_)) => thread::yield_now(),
                        Err(flume::TrySendError::Disconnected(_)) => break,
                    },
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                        sent += 1;
                    }
                }
            }
            sent
        }));
    }
    drop(tx);

    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let mut polls = 0u64;
    while !reached_deadline(polls, deadline) {
        polls = polls.wrapping_add(1);
        let result = match mode {
            Mode::Try => rx.try_recv().ok(),
            Mode::Blocking => rx.recv().ok(),
        };
        if let Some(value) = result {
            black_box(value);
            messages += 1;
        } else if mode == Mode::Try {
            thread::yield_now();
        } else {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    while let Ok(value) = rx.recv() {
        black_box(value);
        messages += 1;
    }
    let sent = handles
        .into_iter()
        .map(|handle| handle.join().expect("flume producer thread"))
        .sum::<u64>();
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "flume lost messages");
    row(context, "flume", mode, payload, config, elapsed, messages)
}

fn bench_kanal<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = kanal::bounded::<T>(config.total_capacity());

    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => match tx.try_send(value) {
                        Ok(true) => sent += 1,
                        Ok(false) | Err(_) => thread::yield_now(),
                    },
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                        sent += 1;
                    }
                }
            }
            sent
        }));
    }
    drop(tx);

    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let mut polls = 0u64;
    while !reached_deadline(polls, deadline) {
        polls = polls.wrapping_add(1);
        let result = match mode {
            Mode::Try => rx.try_recv().ok().flatten(),
            Mode::Blocking => rx.recv().ok(),
        };
        if let Some(value) = result {
            black_box(value);
            messages += 1;
        } else if mode == Mode::Try {
            thread::yield_now();
        } else {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    while let Ok(value) = rx.recv() {
        black_box(value);
        messages += 1;
    }
    let sent = handles
        .into_iter()
        .map(|handle| handle.join().expect("kanal producer thread"))
        .sum::<u64>();
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "kanal lost messages");
    row(context, "kanal", mode, payload, config, elapsed, messages)
}

fn bench_concurrent_queue<T>(
    context: &RunContext,
    config: Config,
    mode: Mode,
    payload: Payload<T>,
) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let queue = Arc::new(concurrent_queue::ConcurrentQueue::bounded(
        config.total_capacity(),
    ));

    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let queue = queue.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match queue.push(value) {
                    Ok(()) => sent += 1,
                    Err(_) => thread::yield_now(),
                }
            }
            sent
        }));
    }

    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let mut polls = 0u64;
    while !reached_deadline(polls, deadline) {
        polls = polls.wrapping_add(1);
        match queue.pop() {
            Ok(value) => {
                black_box(value);
                messages += 1;
            }
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent = handles
        .into_iter()
        .map(|handle| handle.join().expect("concurrent-queue producer thread"))
        .sum::<u64>();
    while let Ok(value) = queue.pop() {
        black_box(value);
        messages += 1;
    }
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "concurrent-queue lost messages");
    row(
        context,
        "concurrent-queue",
        mode,
        payload,
        config,
        elapsed,
        messages,
    )
}

fn bench_thingbuf<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Default + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = thingbuf::mpsc::blocking::channel::<T>(config.total_capacity());

    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => match tx.try_send(value) {
                        Ok(()) => sent += 1,
                        Err(thingbuf::mpsc::errors::TrySendError::Full(_)) => thread::yield_now(),
                        Err(_) => break,
                    },
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                        sent += 1;
                    }
                }
            }
            sent
        }));
    }
    drop(tx);

    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let mut polls = 0u64;
    while !reached_deadline(polls, deadline) {
        polls = polls.wrapping_add(1);
        let result = match mode {
            Mode::Try => rx.try_recv().ok(),
            Mode::Blocking => rx.recv(),
        };
        if let Some(value) = result {
            black_box(value);
            messages += 1;
        } else if mode == Mode::Try {
            thread::yield_now();
        } else {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    while let Some(value) = rx.recv() {
        black_box(value);
        messages += 1;
    }
    let sent = handles
        .into_iter()
        .map(|handle| handle.join().expect("thingbuf producer thread"))
        .sum::<u64>();
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "thingbuf lost messages");
    row(
        context, "thingbuf", mode, payload, config, elapsed, messages,
    )
}

fn row<T>(
    context: &RunContext,
    implementation: &'static str,
    mode: Mode,
    payload: Payload<T>,
    config: Config,
    elapsed: Duration,
    items: u64,
) -> Row {
    Row {
        run_id: context.run_id.clone(),
        cpu: context.cpu.clone(),
        mode: mode.label(),
        implementation,
        payload: payload.label,
        payload_bytes: size_of::<T>(),
        producers: config.producers,
        capacity_per_sender: config.capacity_per_sender,
        total_capacity: config.total_capacity(),
        seconds: elapsed.as_secs_f64(),
        items,
        items_per_sec: items as f64 / elapsed.as_secs_f64(),
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

impl Filter {
    fn from_env(name: &str) -> Self {
        let values = std::env::var(name).ok().map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        });
        Self { values }
    }

    fn matches(&self, value: &str) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.iter().any(|v| v == value))
    }
}

fn reached_deadline(polls: u64, deadline: Instant) -> bool {
    polls & (TIME_CHECK_INTERVAL - 1) == 0 && Instant::now() >= deadline
}

fn producer_counts() -> Vec<usize> {
    std::env::var("FANRING_BENCH_PRODUCERS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.parse().expect("invalid producer count"))
                .collect()
        })
        .unwrap_or_else(|| vec![1, 2, 4, 8])
}

fn selected_payload_count(filter: &Filter) -> usize {
    ["u64", "bytes64", "bytes256"]
        .into_iter()
        .filter(|payload| filter.matches(payload))
        .count()
}

fn selected_implementation_count(filter: &Filter, mode: Mode) -> usize {
    [
        ("fanring", true),
        ("crossbeam-channel", true),
        ("flume", true),
        ("kanal", true),
        ("concurrent-queue", mode == Mode::Try),
        ("thingbuf", true),
    ]
    .into_iter()
    .filter(|(implementation, supported)| *supported && filter.matches(implementation))
    .count()
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
