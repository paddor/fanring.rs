#![cfg_attr(test, allow(dead_code, unused_imports))]

mod support;

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use support::{Affinity, JsonlResults, Sampling, Saturation, median_and_relative_mad};

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

#[derive(Debug)]
struct RunContext {
    run_id: String,
    cpu: String,
    affinity: Affinity,
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

const TIME_CHECK_INTERVAL: u64 = 1024;
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

trait BenchReceiver<T> {
    fn try_recv(&mut self) -> RecvAttempt<T>;
    fn recv(&mut self) -> Option<T>;
    fn close_for_drain(&mut self) {}
}

struct ConcurrentSender<T>(Arc<concurrent_queue::ConcurrentQueue<T>>);
struct ConcurrentReceiver<T>(Arc<concurrent_queue::ConcurrentQueue<T>>);

struct Outcome {
    elapsed: Duration,
    items: u64,
}

// Cargo sets `cfg(test)` for bench targets. Keep `cargo test --all-targets`
// from running the full benchmark, while normal optimized `cargo bench` runs.
#[cfg(all(test, debug_assertions))]
fn main() {}

#[cfg(not(all(test, debug_assertions)))]
fn main() {
    crossfire::detect_backoff_cfg();
    let duration = Duration::from_secs_f64(
        std::env::var("FANRING_BENCH_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1.0),
    );
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
    let context = RunContext {
        run_id: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
            .to_string(),
        cpu: cpu_name(),
        affinity: Affinity::from_env(),
    };

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
            profile,
            sample: 0,
            samples: sampling.samples,
            expected_rows,
        })
        .collect();

    if payload_filter.matches("u64") {
        run_payload(
            &context,
            &mut results,
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
            &mut results,
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
            &mut results,
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

    results.flush();
}

fn run_payload<T>(
    context: &RunContext,
    results: &mut JsonlResults,
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
                results.write(row.implementation, &row);
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
    let (tx0, rx) = fanring::mpsc::channel::<T>(config.capacity_per_sender);
    let mut senders = vec![tx0];
    for _ in 1..config.producers {
        let tx = senders[0].try_clone().expect("fanring sender slot");
        senders.push(tx);
    }
    run_channel(context, "fanring", config, mode, payload, senders, rx)
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
    let (tx, rx) = crossbeam_channel::bounded::<T>(config.total_capacity());
    let senders = clones(&tx, config.producers);
    drop(tx);
    run_channel(
        context,
        "crossbeam-channel",
        config,
        mode,
        payload,
        senders,
        rx,
    )
}

fn bench_crossfire<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = crossfire::mpsc::bounded_blocking(config.total_capacity());
    let senders = clones(&tx, config.producers);
    drop(tx);
    run_channel(context, "crossfire", config, mode, payload, senders, rx)
}

fn bench_flume<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = flume::bounded::<T>(config.total_capacity());
    let senders = clones(&tx, config.producers);
    drop(tx);
    run_channel(context, "flume", config, mode, payload, senders, rx)
}

fn bench_kanal<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = kanal::bounded::<T>(config.total_capacity());
    let senders = clones(&tx, config.producers);
    drop(tx);
    run_channel(context, "kanal", config, mode, payload, senders, rx)
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
    let queue = Arc::new(concurrent_queue::ConcurrentQueue::bounded(
        config.total_capacity(),
    ));
    let senders = (0..config.producers)
        .map(|_| ConcurrentSender(queue.clone()))
        .collect();
    run_channel(
        context,
        "concurrent-queue",
        config,
        mode,
        payload,
        senders,
        ConcurrentReceiver(queue),
    )
}

fn bench_thingbuf<T>(context: &RunContext, config: Config, mode: Mode, payload: Payload<T>) -> Row
where
    T: Copy + Default + Send + Sync + 'static,
{
    let (tx, rx) = thingbuf::mpsc::blocking::channel::<T>(config.total_capacity());
    let senders = clones(&tx, config.producers);
    drop(tx);
    run_channel(context, "thingbuf", config, mode, payload, senders, rx)
}

fn run_channel<T, S, R>(
    context: &RunContext,
    implementation: &'static str,
    config: Config,
    mode: Mode,
    payload: Payload<T>,
    senders: Vec<S>,
    receiver: R,
) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    match config.profile {
        Profile::Saturated => {
            run_saturated_channel(context, implementation, config, payload, senders, receiver)
        }
        Profile::Uncontrolled => run_uncontrolled_channel(
            context,
            implementation,
            config,
            mode,
            payload,
            senders,
            receiver,
        ),
    }
}

fn run_saturated_channel<T, S, R>(
    context: &RunContext,
    implementation: &'static str,
    config: Config,
    payload: Payload<T>,
    senders: Vec<S>,
    mut receiver: R,
) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let saturation = Saturation::new(config.producers, config.total_capacity());
    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);

    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let affinity = context.affinity.clone();
        let value = payload.value;
        let mut full = saturation.producer(index);
        handles.push(thread::spawn(move || {
            affinity.pin(index + 1);
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

    context.affinity.pin(0);
    barrier.wait();
    assert!(saturation.wait_until_full(None));
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let burst_size = usize::try_from(saturation.high_watermark() - saturation.low_watermark())
        .expect("benchmark capacity fits usize");
    let mut remaining = burst_size;
    loop {
        match receiver.try_recv() {
            RecvAttempt::Item(value) => {
                black_box(value);
                messages += 1;
                remaining -= 1;
                if remaining == 0 {
                    if Instant::now() >= deadline {
                        break;
                    }
                    saturation.advance();
                    assert!(saturation.wait_until_full(None));
                    remaining = burst_size;
                }
            }
            RecvAttempt::Empty => thread::yield_now(),
            RecvAttempt::Disconnected => break,
        }
    }
    stop.store(true, Ordering::Relaxed);

    let sent = join_counts(handles, implementation);
    receiver.close_for_drain();
    loop {
        match receiver.try_recv() {
            RecvAttempt::Item(value) => {
                black_box(value);
                messages += 1;
            }
            RecvAttempt::Empty => thread::yield_now(),
            RecvAttempt::Disconnected => break,
        }
    }

    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "{implementation} lost messages");
    row(
        context,
        implementation,
        Mode::Try,
        payload,
        config,
        Outcome {
            elapsed,
            items: messages,
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
    mut receiver: R,
) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let affinity = context.affinity.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            affinity.pin(index + 1);
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

    context.affinity.pin(0);
    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut messages = 0u64;
    let mut polls = 0u64;
    while !reached_deadline(polls, deadline) {
        polls = polls.wrapping_add(1);
        let attempt = match mode {
            Mode::Try => receiver.try_recv(),
            Mode::Blocking => receiver
                .recv()
                .map_or(RecvAttempt::Disconnected, RecvAttempt::Item),
        };
        match attempt {
            RecvAttempt::Item(value) => {
                black_box(value);
                messages += 1;
            }
            RecvAttempt::Empty => thread::yield_now(),
            RecvAttempt::Disconnected => break,
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent = if mode == Mode::Blocking {
        while let Some(value) = receiver.recv() {
            black_box(value);
            messages += 1;
        }
        join_counts(handles, implementation)
    } else {
        let sent = join_counts(handles, implementation);
        receiver.close_for_drain();
        loop {
            match receiver.try_recv() {
                RecvAttempt::Item(value) => {
                    black_box(value);
                    messages += 1;
                }
                RecvAttempt::Empty => thread::yield_now(),
                RecvAttempt::Disconnected => break,
            }
        }
        sent
    };
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "{implementation} lost messages");
    row(
        context,
        implementation,
        mode,
        payload,
        config,
        Outcome {
            elapsed,
            items: messages,
        },
        None,
    )
}

fn join_counts(handles: Vec<thread::JoinHandle<u64>>, implementation: &str) -> u64 {
    handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .unwrap_or_else(|_| panic!("{implementation} producer thread panicked"))
        })
        .sum()
}

impl<T: Send + 'static> BenchSender<T> for fanring::mpsc::Sender<T> {
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match self.try_send(value) {
            Ok(()) => SendAttempt::Sent,
            Err(fanring::mpsc::TrySendError::Full(_)) => SendAttempt::Full,
            Err(fanring::mpsc::TrySendError::Disconnected(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        self.send(value).is_ok()
    }
}

impl<T> BenchReceiver<T> for fanring::mpsc::Receiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match self.try_recv() {
            Ok(value) => RecvAttempt::Item(value),
            Err(fanring::mpsc::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(fanring::mpsc::TryRecvError::Disconnected) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        self.recv().ok()
    }
}

impl<T: Send + 'static> BenchSender<T> for crossbeam_channel::Sender<T> {
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

impl<T> BenchReceiver<T> for crossbeam_channel::Receiver<T> {
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

impl<T: Send + 'static> BenchSender<T> for crossfire::MTx<crossfire::mpsc::Array<T>> {
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

impl<T: Send + 'static> BenchReceiver<T> for crossfire::Rx<crossfire::mpsc::Array<T>> {
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

impl<T: Send + 'static> BenchSender<T> for flume::Sender<T> {
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

impl<T> BenchReceiver<T> for flume::Receiver<T> {
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

impl<T: Send + 'static> BenchSender<T> for kanal::Sender<T> {
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

impl<T> BenchReceiver<T> for kanal::Receiver<T> {
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

impl<T: Send + 'static> BenchSender<T> for ConcurrentSender<T> {
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match self.0.push(value) {
            Ok(()) => SendAttempt::Sent,
            Err(concurrent_queue::PushError::Full(_)) => SendAttempt::Full,
            Err(concurrent_queue::PushError::Closed(_)) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, _value: T) -> bool {
        unreachable!("concurrent-queue has no blocking benchmark")
    }
}

impl<T> BenchReceiver<T> for ConcurrentReceiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match self.0.pop() {
            Ok(value) => RecvAttempt::Item(value),
            Err(concurrent_queue::PopError::Empty) => RecvAttempt::Empty,
            Err(concurrent_queue::PopError::Closed) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        unreachable!("concurrent-queue has no blocking benchmark")
    }

    fn close_for_drain(&mut self) {
        self.0.close();
    }
}

impl<T: Copy + Default + Send + Sync + 'static> BenchSender<T>
    for thingbuf::mpsc::blocking::Sender<T>
{
    #[inline(always)]
    fn try_send(&mut self, value: T) -> SendAttempt {
        match thingbuf::mpsc::blocking::Sender::try_send(self, value) {
            Ok(()) => SendAttempt::Sent,
            Err(thingbuf::mpsc::errors::TrySendError::Full(_)) => SendAttempt::Full,
            Err(thingbuf::mpsc::errors::TrySendError::Closed(_)) => SendAttempt::Disconnected,
            Err(_) => SendAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn send(&mut self, value: T) -> bool {
        thingbuf::mpsc::blocking::Sender::send(self, value).is_ok()
    }
}

impl<T: Clone + Default> BenchReceiver<T> for thingbuf::mpsc::blocking::Receiver<T> {
    #[inline(always)]
    fn try_recv(&mut self) -> RecvAttempt<T> {
        match thingbuf::mpsc::blocking::Receiver::try_recv(self) {
            Ok(value) => RecvAttempt::Item(value),
            Err(thingbuf::mpsc::errors::TryRecvError::Empty) => RecvAttempt::Empty,
            Err(thingbuf::mpsc::errors::TryRecvError::Closed) => RecvAttempt::Disconnected,
            Err(_) => RecvAttempt::Disconnected,
        }
    }

    #[inline(always)]
    fn recv(&mut self) -> Option<T> {
        thingbuf::mpsc::blocking::Receiver::recv(self)
    }
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
        capacity_per_sender: config.capacity_per_sender,
        nominal_capacity: config.total_capacity(),
        capacity_model: if implementation == "fanring" {
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

fn clones<T: Clone>(value: &T, count: usize) -> Vec<T> {
    (0..count).map(|_| value.clone()).collect()
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
