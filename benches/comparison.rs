#![cfg_attr(test, allow(dead_code, unused_imports))]

use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone, Copy)]
struct Config {
    producers: usize,
    capacity_per_sender: usize,
    duration: Duration,
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
            .unwrap_or(2.0),
    );
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
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .expect("open benchmark JSONL");
    let mut out = BufWriter::new(file);

    println!(
        "MPSC comparison ({}, {:.2}s per config, capacity {} items, output {})\n",
        mode.label(),
        duration.as_secs_f64(),
        total_capacity(),
        out_path.display()
    );

    let total_capacity = total_capacity();
    let configs: Vec<Config> = producer_counts()
        .into_iter()
        .map(|producers| Config {
            producers,
            capacity_per_sender: capacity_per_sender(total_capacity, producers),
            duration,
        })
        .collect();

    if payload_filter.matches("u64") {
        run_payload(
            &context,
            &mut out,
            &configs,
            &impl_filter,
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
    mode: Mode,
    payload: Payload<T>,
) where
    T: Copy + Default + Send + Sync + 'static,
{
    println!("--- {} ({} bytes) ---", payload.label, size_of::<T>());
    for config in configs {
        let mut rows = Vec::new();
        if impl_filter.matches("fanring") {
            rows.push(bench_fanring(context, *config, mode, payload));
        }
        if impl_filter.matches("crossbeam-channel") {
            rows.push(bench_crossbeam_channel(context, *config, mode, payload));
        }
        if impl_filter.matches("flume") {
            rows.push(bench_flume(context, *config, mode, payload));
        }
        if impl_filter.matches("kanal") {
            rows.push(bench_kanal(context, *config, mode, payload));
        }
        if mode == Mode::Try && impl_filter.matches("concurrent-queue") {
            rows.push(bench_concurrent_queue(context, *config, mode, payload));
        }
        if impl_filter.matches("thingbuf") {
            rows.push(bench_thingbuf(context, *config, mode, payload));
        }

        println!(
            "  producers={:<2} capacity_per_sender={:<4} total_capacity={}",
            config.producers,
            config.capacity_per_sender,
            config.total_capacity()
        );
        for row in rows {
            println!(
                "    {:<18} {:>8.2}M items/s",
                row.implementation,
                row.items_per_sec / 1_000_000.0
            );
            serde_json::to_writer(&mut *out, &row).expect("write benchmark row");
            writeln!(out).expect("write benchmark newline");
        }
        println!();
    }
}

fn bench_fanring<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx0, mut rx) = fanring::channel::<T>(config.capacity_per_sender);
    let mut senders = vec![tx0];
    for _ in 1..config.producers {
        let tx = senders[0].try_clone().expect("fanring sender slot");
        senders.push(tx);
    }

    let mut handles = Vec::with_capacity(config.producers);
    for mut tx in senders {
        let stop = stop.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => {
                        if let Err(fanring::TrySendError::Full(_)) = tx.try_send(value) {
                            thread::yield_now();
                        }
                    }
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                    }
                }
            }
        }));
    }

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
                Err(fanring::TryRecvError::Empty | fanring::TryRecvError::Disconnected) => {
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
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    drop(rx);
    for handle in handles {
        handle.join().expect("fanring producer thread");
    }
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

    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => {
                        if tx.try_send(value).is_err() {
                            thread::yield_now();
                        }
                    }
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                    }
                }
            }
        }));
    }
    drop(tx);

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
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    drop(rx);
    for handle in handles {
        handle.join().expect("crossbeam producer thread");
    }
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

    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => {
                        if tx.try_send(value).is_err() {
                            thread::yield_now();
                        }
                    }
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                    }
                }
            }
        }));
    }
    drop(tx);

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
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    drop(rx);
    for handle in handles {
        handle.join().expect("flume producer thread");
    }
    row(context, "flume", mode, payload, config, elapsed, messages)
}

fn bench_kanal<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = kanal::bounded::<T>(config.total_capacity());

    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => match tx.try_send(value) {
                        Ok(true) => {}
                        Ok(false) | Err(_) => thread::yield_now(),
                    },
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                    }
                }
            }
        }));
    }
    drop(tx);

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
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    drop(rx);
    for handle in handles {
        handle.join().expect("kanal producer thread");
    }
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

    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let queue = queue.clone();
        let stop = stop.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if queue.push(value).is_err() {
                    thread::yield_now();
                }
            }
        }));
    }

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
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("concurrent-queue producer thread");
    }
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

    let mut handles = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        let tx = tx.clone();
        let stop = stop.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => {
                        if tx.try_send(value).is_err() {
                            thread::yield_now();
                        }
                    }
                    Mode::Blocking => {
                        if tx.send(value).is_err() {
                            break;
                        }
                    }
                }
            }
        }));
    }
    drop(tx);

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
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    drop(rx);
    for handle in handles {
        handle.join().expect("thingbuf producer thread");
    }
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
