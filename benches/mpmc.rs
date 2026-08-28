#![cfg_attr(test, allow(dead_code, unused_imports))]

use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Clone, Copy)]
struct Config {
    producers: usize,
    consumers: usize,
    capacity_per_sender: usize,
    duration: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Try,
    Blocking,
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
    consumers: usize,
    capacity_per_sender: usize,
    total_capacity: usize,
    seconds: f64,
    items: u64,
    items_per_sec: f64,
}

enum SendAttempt {
    Sent,
    Full,
    Disconnected,
}

enum RecvAttempt<T> {
    Item(T),
    Empty,
    Disconnected,
}

trait BenchSender<T>: Send + 'static {
    fn try_send(&mut self, value: T) -> SendAttempt;
    fn send(&mut self, value: T) -> bool;
}

trait BenchReceiver<T>: Send + 'static {
    fn try_recv(&mut self) -> RecvAttempt<T>;
    fn recv(&mut self) -> Option<T>;
}

#[cfg(all(test, debug_assertions))]
fn main() {}

#[cfg(not(all(test, debug_assertions)))]
fn main() {
    let duration = Duration::from_secs_f64(
        std::env::var("FANRING_BENCH_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2.0),
    );
    let total_capacity = total_capacity();
    let out_path = std::env::var_os("FANRING_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fanring-bench/mpmc.jsonl"));
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
        "MPMC comparison ({}, {:.2}s per config, capacity {}, output {})\n",
        mode.label(),
        duration.as_secs_f64(),
        total_capacity,
        out_path.display()
    );

    let configs = producer_counts()
        .into_iter()
        .flat_map(|producers| {
            consumer_counts().into_iter().map(move |consumers| Config {
                producers,
                consumers,
                capacity_per_sender: capacity_per_sender(total_capacity, producers),
                duration,
            })
        })
        .collect::<Vec<_>>();

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
    T: Copy + Send + 'static,
{
    println!("--- {} ({} bytes) ---", payload.label, size_of::<T>());
    for &config in configs {
        let mut rows = Vec::new();
        if impl_filter.matches("fanring-mpmc") {
            rows.push(bench_fanring(context, config, mode, payload));
        }
        if impl_filter.matches("crossbeam-channel") {
            rows.push(bench_crossbeam(context, config, mode, payload));
        }
        if impl_filter.matches("flume") {
            rows.push(bench_flume(context, config, mode, payload));
        }
        if impl_filter.matches("kanal") {
            rows.push(bench_kanal(context, config, mode, payload));
        }

        println!(
            "  producers={:<2} consumers={:<2} capacity_per_sender={:<4}",
            config.producers, config.consumers, config.capacity_per_sender
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
    let (tx0, rx0) = fanring::mpmc::channel(config.capacity_per_sender);
    let mut senders = vec![tx0];
    for _ in 1..config.producers {
        senders.push(senders[0].try_clone().expect("fanring sender lane"));
    }
    let mut receivers = vec![rx0];
    for _ in 1..config.consumers {
        receivers.push(receivers[0].clone());
    }
    run_channel(
        context,
        "fanring-mpmc",
        config,
        mode,
        payload,
        senders,
        receivers,
    )
}

fn bench_crossbeam<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded(config.total_capacity());
    let senders = clones(&tx, config.producers);
    let receivers = clones(&rx, config.consumers);
    drop(tx);
    drop(rx);
    run_channel(
        context,
        "crossbeam-channel",
        config,
        mode,
        payload,
        senders,
        receivers,
    )
}

fn bench_flume<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = flume::bounded(config.total_capacity());
    let senders = clones(&tx, config.producers);
    let receivers = clones(&rx, config.consumers);
    drop(tx);
    drop(rx);
    run_channel(context, "flume", config, mode, payload, senders, receivers)
}

fn bench_kanal<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = kanal::bounded(config.total_capacity());
    let senders = clones(&tx, config.producers);
    let receivers = clones(&rx, config.consumers);
    drop(tx);
    drop(rx);
    run_channel(context, "kanal", config, mode, payload, senders, receivers)
}

fn run_channel<T, S, R>(
    context: &RunContext,
    implementation: &'static str,
    config: Config,
    mode: Mode,
    payload: Payload<T>,
    senders: Vec<S>,
    receivers: Vec<R>,
) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(config.producers + config.consumers + 1));
    let mut sender_handles = Vec::with_capacity(config.producers);
    let mut receiver_handles = Vec::with_capacity(config.consumers);

    for mut sender in senders {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let value = payload.value;
        sender_handles.push(thread::spawn(move || {
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match mode {
                    Mode::Try => match sender.try_send(value) {
                        SendAttempt::Sent => sent += 1,
                        SendAttempt::Full => thread::yield_now(),
                        SendAttempt::Disconnected => break,
                    },
                    Mode::Blocking => {
                        if !sender.send(value) {
                            break;
                        }
                        sent += 1;
                    }
                }
            }
            sent
        }));
    }

    for mut receiver in receivers {
        let barrier = barrier.clone();
        receiver_handles.push(thread::spawn(move || {
            barrier.wait();
            let mut received = 0u64;
            loop {
                match mode {
                    Mode::Try => match receiver.try_recv() {
                        RecvAttempt::Item(value) => {
                            black_box(value);
                            received += 1;
                        }
                        RecvAttempt::Empty => thread::yield_now(),
                        RecvAttempt::Disconnected => break,
                    },
                    Mode::Blocking => match receiver.recv() {
                        Some(value) => {
                            black_box(value);
                            received += 1;
                        }
                        None => break,
                    },
                }
            }
            received
        }));
    }

    barrier.wait();
    let start = Instant::now();
    thread::sleep(config.duration);
    stop.store(true, Ordering::Relaxed);
    let elapsed = start.elapsed();

    let sent = sender_handles
        .into_iter()
        .map(|handle| handle.join().expect("producer thread"))
        .sum::<u64>();
    let received = receiver_handles
        .into_iter()
        .map(|handle| handle.join().expect("consumer thread"))
        .sum::<u64>();
    assert_eq!(sent, received, "{implementation} lost messages");

    Row {
        run_id: context.run_id.clone(),
        cpu: context.cpu.clone(),
        mode: mode.label(),
        implementation,
        payload: payload.label,
        payload_bytes: size_of::<T>(),
        producers: config.producers,
        consumers: config.consumers,
        capacity_per_sender: config.capacity_per_sender,
        total_capacity: config.total_capacity(),
        seconds: elapsed.as_secs_f64(),
        items: received,
        items_per_sec: received as f64 / elapsed.as_secs_f64(),
    }
}

impl<T> BenchSender<T> for fanring::mpmc::Sender<T>
where
    T: Send + 'static,
{
    fn try_send(&mut self, value: T) -> SendAttempt {
        match self.try_send(value) {
            Ok(()) => SendAttempt::Sent,
            Err(fanring::mpmc::TrySendError::Full(_)) => SendAttempt::Full,
            Err(fanring::mpmc::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    fn send(&mut self, value: T) -> bool {
        self.send(value).is_ok()
    }
}

impl<T> BenchReceiver<T> for fanring::mpmc::Receiver<T>
where
    T: Send + 'static,
{
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match self.try_recv() {
            Ok(value) => RecvAttempt::Item(value),
            Err(fanring::mpmc::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(fanring::mpmc::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    fn recv(&mut self) -> Option<T> {
        self.recv().ok()
    }
}

impl<T> BenchSender<T> for crossbeam_channel::Sender<T>
where
    T: Send + 'static,
{
    fn try_send(&mut self, value: T) -> SendAttempt {
        match crossbeam_channel::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(crossbeam_channel::TrySendError::Full(_)) => SendAttempt::Full,
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    fn send(&mut self, value: T) -> bool {
        crossbeam_channel::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for crossbeam_channel::Receiver<T>
where
    T: Send + 'static,
{
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match crossbeam_channel::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(crossbeam_channel::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(crossbeam_channel::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    fn recv(&mut self) -> Option<T> {
        crossbeam_channel::Receiver::recv(self).ok()
    }
}

impl<T> BenchSender<T> for flume::Sender<T>
where
    T: Send + 'static,
{
    fn try_send(&mut self, value: T) -> SendAttempt {
        match flume::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(flume::TrySendError::Full(_)) => SendAttempt::Full,
            Err(flume::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    fn send(&mut self, value: T) -> bool {
        flume::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for flume::Receiver<T>
where
    T: Send + 'static,
{
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match flume::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(flume::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(flume::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    fn recv(&mut self) -> Option<T> {
        flume::Receiver::recv(self).ok()
    }
}

impl<T> BenchSender<T> for kanal::Sender<T>
where
    T: Send + 'static,
{
    fn try_send(&mut self, value: T) -> SendAttempt {
        match kanal::Sender::try_send(self, value) {
            Ok(true) => SendAttempt::Sent,
            Ok(false) => SendAttempt::Full,
            Err(_) => SendAttempt::Disconnected,
        }
    }

    fn send(&mut self, value: T) -> bool {
        kanal::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for kanal::Receiver<T>
where
    T: Send + 'static,
{
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match kanal::Receiver::try_recv(self) {
            Ok(Some(value)) => RecvAttempt::Item(value),
            Ok(None) => RecvAttempt::Empty,
            Err(_) => RecvAttempt::Disconnected,
        }
    }

    fn recv(&mut self) -> Option<T> {
        kanal::Receiver::recv(self).ok()
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
