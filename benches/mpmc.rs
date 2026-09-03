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

use support::{Affinity, Sampling, Saturation, append_jsonl, median_and_relative_mad};

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
    let total_capacity = total_capacity();
    let out_path = std::env::var_os("FANRING_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fanring-bench/mpmc.jsonl"));
    let payload_filter = Filter::from_env("FANRING_BENCH_PAYLOADS");
    let impl_filter = Filter::from_env("FANRING_BENCH_IMPLS");
    let mode = Mode::from_env();
    let profile = Profile::from_env();
    assert!(
        mode == Mode::Try || profile == Profile::Uncontrolled,
        "saturated profile requires nonblocking mode"
    );
    let context = RunContext {
        run_id: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
            .to_string(),
        cpu: cpu_name(),
        affinity: Affinity::from_env(),
    };

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).expect("create benchmark output dir");
    }
    let mut out = append_jsonl(&out_path);

    println!(
        "MPMC {} comparison ({}, {} x {:.2}s, {:.2}s warmup, capacity {}, affinity {}, output {})\n",
        profile.label(),
        mode.label(),
        sampling.samples,
        duration.as_secs_f64(),
        sampling.warmup.as_secs_f64(),
        total_capacity,
        context.affinity.description(),
        out_path.display()
    );

    let producer_counts = producer_counts();
    let consumer_counts = consumer_counts();
    let expected_rows = producer_counts.len()
        * consumer_counts.len()
        * selected_payload_count(&payload_filter)
        * selected_implementation_count(&impl_filter)
        * sampling.samples;
    let configs = producer_counts
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
        .collect::<Vec<_>>();

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
    T: Copy + Send + 'static,
{
    type BenchFn<T> = fn(&RunContext, Config, Mode, Payload<T>) -> Row;

    let mut implementations: Vec<(&str, BenchFn<T>)> = Vec::new();
    if impl_filter.matches("fanring-mpmc") {
        implementations.push(("fanring-mpmc", bench_fanring::<T>));
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
    if implementations.is_empty() {
        return;
    }

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

fn bench_crossfire<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = crossfire::mpmc::bounded_blocking(config.total_capacity());
    let senders = clones(&tx, config.producers);
    let receivers = clones(&rx, config.consumers);
    drop(tx);
    drop(rx);
    run_channel(
        context,
        "crossfire-mpmc",
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
    match config.profile {
        Profile::Saturated => {
            run_saturated_channel(context, implementation, config, payload, senders, receivers)
        }
        Profile::Uncontrolled => run_uncontrolled_channel(
            context,
            implementation,
            config,
            mode,
            payload,
            senders,
            receivers,
        ),
    }
}

fn run_saturated_channel<T, S, R>(
    context: &RunContext,
    implementation: &'static str,
    config: Config,
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
    let saturation = Saturation::new(config.producers, config.total_capacity());
    let barrier = Arc::new(Barrier::new(config.producers + config.consumers + 1));
    let start_consuming = Arc::new(Barrier::new(config.consumers + 1));
    let phase_barrier = Arc::new(Barrier::new(config.consumers));
    let mut sender_handles = Vec::with_capacity(config.producers);
    let mut receiver_handles = Vec::with_capacity(config.consumers);

    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let affinity = context.affinity.clone();
        let value = payload.value;
        let mut full = saturation.producer(index);
        sender_handles.push(thread::spawn(move || {
            affinity.pin(config.consumers + index);
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match sender.try_send(value) {
                    SendAttempt::Sent => sent += 1,
                    SendAttempt::Full => {
                        full.mark_full();
                        thread::yield_now();
                    }
                    SendAttempt::Disconnected => break,
                }
            }
            sent
        }));
    }

    let burst_size = usize::try_from(saturation.high_watermark() - saturation.low_watermark())
        .expect("benchmark capacity fits usize");
    assert!(
        burst_size >= config.consumers,
        "saturated burst must cover every consumer"
    );
    for (index, mut receiver) in receivers.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let start_consuming = start_consuming.clone();
        let phase_barrier = phase_barrier.clone();
        let saturation = saturation.clone();
        let affinity = context.affinity.clone();
        receiver_handles.push(thread::spawn(move || {
            affinity.pin(index);
            barrier.wait();
            start_consuming.wait();
            let deadline = Instant::now() + config.duration;
            let mut received = 0u64;
            let mut remaining =
                burst_size / config.consumers + usize::from(index < burst_size % config.consumers);
            let mut measuring = true;
            loop {
                match receiver.try_recv() {
                    RecvAttempt::Item(value) => {
                        black_box(value);
                        received += 1;
                        if measuring {
                            remaining -= 1;
                        }
                    }
                    RecvAttempt::Empty => thread::yield_now(),
                    RecvAttempt::Disconnected => break,
                }
                if !measuring || remaining != 0 {
                    continue;
                }

                let leader = phase_barrier.wait().is_leader();
                if leader {
                    if Instant::now() >= deadline {
                        stop.store(true, Ordering::Relaxed);
                    } else {
                        saturation.advance();
                    }
                }
                phase_barrier.wait();
                if stop.load(Ordering::Relaxed) {
                    measuring = false;
                    continue;
                }
                assert!(saturation.wait_until_full(Some(&stop)));
                phase_barrier.wait();
                remaining = burst_size / config.consumers
                    + usize::from(index < burst_size % config.consumers);
            }
            received
        }));
    }

    barrier.wait();
    assert!(saturation.wait_until_full(None));
    let start = Instant::now();
    start_consuming.wait();

    let sent = sender_handles
        .into_iter()
        .map(|handle| handle.join().expect("producer thread"))
        .sum::<u64>();
    let received = receiver_handles
        .into_iter()
        .map(|handle| handle.join().expect("consumer thread"))
        .sum::<u64>();
    let elapsed = start.elapsed();
    assert_eq!(sent, received, "{implementation} lost messages");

    row(
        context,
        implementation,
        Mode::Try,
        payload,
        config,
        Outcome {
            elapsed,
            items: received,
        },
        Some(&saturation),
    )
}

fn run_uncontrolled_channel<T, S, R>(
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
    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let affinity = context.affinity.clone();
        let value = payload.value;
        sender_handles.push(thread::spawn(move || {
            affinity.pin(config.consumers + index);
            barrier.wait();
            let mut sent = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let attempt = match mode {
                    Mode::Try => sender.try_send(value),
                    Mode::Blocking => {
                        if sender.send(value) {
                            SendAttempt::Sent
                        } else {
                            SendAttempt::Disconnected
                        }
                    }
                };
                match attempt {
                    SendAttempt::Sent => sent += 1,
                    SendAttempt::Full => thread::yield_now(),
                    SendAttempt::Disconnected => break,
                }
            }
            sent
        }));
    }
    for (index, mut receiver) in receivers.into_iter().enumerate() {
        let barrier = barrier.clone();
        let affinity = context.affinity.clone();
        receiver_handles.push(thread::spawn(move || {
            affinity.pin(index);
            barrier.wait();
            let mut received = 0u64;
            loop {
                let attempt = match mode {
                    Mode::Try => receiver.try_recv(),
                    Mode::Blocking => receiver
                        .recv()
                        .map_or(RecvAttempt::Disconnected, RecvAttempt::Item),
                };
                match attempt {
                    RecvAttempt::Item(value) => {
                        black_box(value);
                        received += 1;
                    }
                    RecvAttempt::Empty => thread::yield_now(),
                    RecvAttempt::Disconnected => break,
                }
            }
            received
        }));
    }

    barrier.wait();
    let start = Instant::now();
    thread::sleep(config.duration);
    stop.store(true, Ordering::Relaxed);
    let sent = sender_handles
        .into_iter()
        .map(|handle| handle.join().expect("producer thread"))
        .sum::<u64>();
    let received = receiver_handles
        .into_iter()
        .map(|handle| handle.join().expect("consumer thread"))
        .sum::<u64>();
    let elapsed = start.elapsed();
    assert_eq!(sent, received, "{implementation} lost messages");

    row(
        context,
        implementation,
        mode,
        payload,
        config,
        Outcome {
            elapsed,
            items: received,
        },
        None,
    )
}

fn row<T>(
    context: &RunContext,
    implementation: &'static str,
    mode: Mode,
    payload: Payload<T>,
    config: Config,
    outcome: Outcome,
    saturation: Option<&Saturation>,
) -> Row {
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
        capacity_model: if implementation == "fanring-mpmc" {
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

impl<T> BenchSender<T> for fanring::mpmc::Sender<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match self.try_send(value) {
            Ok(()) => SendAttempt::Sent,
            Err(fanring::mpmc::TrySendError::Full(_)) => SendAttempt::Full,
            Err(fanring::mpmc::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        self.send(value).is_ok()
    }
}

impl<T> BenchReceiver<T> for fanring::mpmc::Receiver<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match self.try_recv() {
            Ok(value) => RecvAttempt::Item(value),
            Err(fanring::mpmc::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(fanring::mpmc::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        self.recv().ok()
    }
}

impl<T> BenchSender<T> for crossbeam_channel::Sender<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match crossbeam_channel::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(crossbeam_channel::TrySendError::Full(_)) => SendAttempt::Full,
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        crossbeam_channel::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for crossbeam_channel::Receiver<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match crossbeam_channel::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(crossbeam_channel::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(crossbeam_channel::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        crossbeam_channel::Receiver::recv(self).ok()
    }
}

impl<T> BenchSender<T> for crossfire::MTx<crossfire::mpmc::Array<T>>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match crossfire::BlockingTxTrait::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(crossfire::TrySendError::Full(_)) => SendAttempt::Full,
            Err(crossfire::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        crossfire::BlockingTxTrait::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for crossfire::MRx<crossfire::mpmc::Array<T>>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match crossfire::BlockingRxTrait::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(crossfire::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(crossfire::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        crossfire::BlockingRxTrait::recv(self).ok()
    }
}

impl<T> BenchSender<T> for flume::Sender<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match flume::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(flume::TrySendError::Full(_)) => SendAttempt::Full,
            Err(flume::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        flume::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for flume::Receiver<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match flume::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(flume::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(flume::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        flume::Receiver::recv(self).ok()
    }
}

impl<T> BenchSender<T> for kanal::Sender<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match kanal::Sender::try_send(self, value) {
            Ok(true) => SendAttempt::Sent,
            Ok(false) => SendAttempt::Full,
            Err(_) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        kanal::Sender::send(self, value).is_ok()
    }
}

impl<T> BenchReceiver<T> for kanal::Receiver<T>
where
    T: Send + 'static,
{
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match kanal::Receiver::try_recv(self) {
            Ok(Some(value)) => RecvAttempt::Item(value),
            Ok(None) => RecvAttempt::Empty,
            Err(_) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
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
