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
    implementation: &'static str,
    payload: &'static str,
    payload_bytes: usize,
    producers: usize,
    capacity_per_sender: usize,
    total_capacity: usize,
    seconds: f64,
    messages: u64,
    msgs_per_sec: f64,
}

const TIME_CHECK_INTERVAL: u64 = 1024;

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
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos()
        .to_string();

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
        "MPSC comparison ({:.2}s per config, output {})\n",
        duration.as_secs_f64(),
        out_path.display()
    );

    let configs: Vec<Config> = producer_counts()
        .into_iter()
        .map(|producers| Config {
            producers,
            capacity_per_sender: 1024,
            duration,
        })
        .collect();

    if payload_filter.matches("u64") {
        run_payload(
            &run_id,
            &mut out,
            &configs,
            &impl_filter,
            Payload {
                label: "u64",
                value: 0u64,
            },
        );
    }
    if payload_filter.matches("bytes64") {
        run_payload(
            &run_id,
            &mut out,
            &configs,
            &impl_filter,
            Payload {
                label: "bytes64",
                value: [0u64; 8],
            },
        );
    }
    if payload_filter.matches("bytes256") {
        run_payload(
            &run_id,
            &mut out,
            &configs,
            &impl_filter,
            Payload {
                label: "bytes256",
                value: [0u64; 32],
            },
        );
    }

    out.flush().expect("flush benchmark JSONL");
}

fn run_payload<T>(
    run_id: &str,
    out: &mut impl Write,
    configs: &[Config],
    impl_filter: &Filter,
    payload: Payload<T>,
) where
    T: Copy + Default + Send + Sync + 'static,
{
    println!("--- {} ({} bytes) ---", payload.label, size_of::<T>());
    for config in configs {
        let mut rows = Vec::new();
        if impl_filter.matches("fanring") {
            rows.push(bench_fanring(run_id, *config, payload));
        }
        if impl_filter.matches("crossbeam-channel") {
            rows.push(bench_crossbeam_channel(run_id, *config, payload));
        }
        if impl_filter.matches("flume") {
            rows.push(bench_flume(run_id, *config, payload));
        }
        if impl_filter.matches("kanal") {
            rows.push(bench_kanal(run_id, *config, payload));
        }
        if impl_filter.matches("concurrent-queue") {
            rows.push(bench_concurrent_queue(run_id, *config, payload));
        }
        if impl_filter.matches("thingbuf") {
            rows.push(bench_thingbuf(run_id, *config, payload));
        }

        println!("  producers={:<2}", config.producers);
        for row in rows {
            println!(
                "    {:<18} {:>8.2}M msg/s",
                row.implementation,
                row.msgs_per_sec / 1_000_000.0
            );
            serde_json::to_writer(&mut *out, &row).expect("write benchmark row");
            writeln!(out).expect("write benchmark newline");
        }
        println!();
    }
}

fn bench_fanring<T>(run_id: &str, config: Config, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let (tx0, mut rx) = fanring::channel::<T>(config.producers, config.capacity_per_sender);
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
                if let Err(fanring::SendError::Full(_)) = tx.try_send(value) {
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
        match rx.try_recv() {
            Ok(value) => {
                black_box(value);
                messages += 1;
            }
            Err(fanring::RecvError::Empty | fanring::RecvError::Disconnected) => {
                thread::yield_now();
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("fanring producer thread");
    }
    row(
        run_id,
        "fanring",
        payload,
        config,
        start.elapsed(),
        messages,
    )
}

fn bench_crossbeam_channel<T>(run_id: &str, config: Config, payload: Payload<T>) -> Row
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
                if tx.try_send(value).is_err() {
                    thread::yield_now();
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
        match rx.try_recv() {
            Ok(value) => {
                black_box(value);
                messages += 1;
            }
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("crossbeam producer thread");
    }
    row(
        run_id,
        "crossbeam-channel",
        payload,
        config,
        start.elapsed(),
        messages,
    )
}

fn bench_flume<T>(run_id: &str, config: Config, payload: Payload<T>) -> Row
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
                if tx.try_send(value).is_err() {
                    thread::yield_now();
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
        match rx.try_recv() {
            Ok(value) => {
                black_box(value);
                messages += 1;
            }
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("flume producer thread");
    }
    row(run_id, "flume", payload, config, start.elapsed(), messages)
}

fn bench_kanal<T>(run_id: &str, config: Config, payload: Payload<T>) -> Row
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
                match tx.try_send(value) {
                    Ok(true) => {}
                    Ok(false) | Err(_) => thread::yield_now(),
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
        match rx.try_recv() {
            Ok(Some(value)) => {
                black_box(value);
                messages += 1;
            }
            Ok(None) | Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("kanal producer thread");
    }
    row(run_id, "kanal", payload, config, start.elapsed(), messages)
}

fn bench_concurrent_queue<T>(run_id: &str, config: Config, payload: Payload<T>) -> Row
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
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("concurrent-queue producer thread");
    }
    row(
        run_id,
        "concurrent-queue",
        payload,
        config,
        start.elapsed(),
        messages,
    )
}

fn bench_thingbuf<T>(run_id: &str, config: Config, payload: Payload<T>) -> Row
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
                if tx.try_send(value).is_err() {
                    thread::yield_now();
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
        match rx.try_recv() {
            Ok(value) => {
                black_box(value);
                messages += 1;
            }
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("thingbuf producer thread");
    }
    row(
        run_id,
        "thingbuf",
        payload,
        config,
        start.elapsed(),
        messages,
    )
}

fn row<T>(
    run_id: &str,
    implementation: &'static str,
    payload: Payload<T>,
    config: Config,
    elapsed: Duration,
    messages: u64,
) -> Row {
    Row {
        run_id: run_id.to_string(),
        implementation,
        payload: payload.label,
        payload_bytes: size_of::<T>(),
        producers: config.producers,
        capacity_per_sender: config.capacity_per_sender,
        total_capacity: config.total_capacity(),
        seconds: elapsed.as_secs_f64(),
        messages,
        msgs_per_sec: messages as f64 / elapsed.as_secs_f64(),
    }
}

impl Config {
    fn total_capacity(self) -> usize {
        self.producers * self.capacity_per_sender
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
