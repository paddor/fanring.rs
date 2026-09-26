#![cfg_attr(test, allow(dead_code, unused_imports))]
//! MPSC receive-path comparison: repeated `try_recv` against bulk receives.
//!
//! The `prefilled` profile measures receive cost alone. Producers fill their
//! rings, stop, and the consumer drains everything while the clock runs. The
//! `stream` profile keeps producers sending and reports end-to-end throughput.

mod support;

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fanring::mpsc::{Receiver, Sender, TryRecvError, TrySendError, channel};
use serde::Serialize;

use support::{Affinity, JsonlResults, Sampling, median_and_relative_mad};

const DEADLINE_CHECK_INTERVAL: u64 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Receive {
    TryRecv,
    RecvBatchInto,
    TryRecvBatchInto,
    TryRecvBatchIntoWhile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Profile {
    Prefilled,
    Stream,
}

#[derive(Debug, Clone, Copy)]
struct Config {
    profile: Profile,
    producers: usize,
    capacity_per_sender: usize,
    batch_limit: usize,
    duration: Duration,
    sample: usize,
    samples: usize,
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
    profile: &'static str,
    receive: &'static str,
    payload: &'static str,
    payload_bytes: usize,
    producers: usize,
    capacity_per_sender: usize,
    batch_limit: usize,
    seconds: f64,
    items: u64,
    items_per_sec: f64,
    ns_per_item: f64,
    rounds: u64,
    sample: usize,
    samples: usize,
}

enum Batch {
    Items(usize),
    Empty,
    Disconnected,
}

// Cargo sets `cfg(test)` for bench targets. Keep `cargo test --all-targets`
// from running the full benchmark, while normal optimized `cargo bench` runs.
#[cfg(all(test, debug_assertions))]
fn main() {}

#[cfg(not(all(test, debug_assertions)))]
fn main() {
    let duration = Duration::from_secs_f64(env_f64("FANRING_BENCH_SECS", 1.0));
    let sampling = Sampling::from_env();
    let mut results = JsonlResults::new("batch-mpsc.jsonl");
    let payload_filter = Filter::from_env("FANRING_BENCH_PAYLOADS");
    let receive_filter = Filter::from_env("FANRING_BENCH_RECEIVES");
    let profile_filter = Filter::from_env("FANRING_BENCH_PROFILE");
    let context = RunContext {
        run_id: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
            .to_string(),
        cpu: cpu_name(),
        affinity: Affinity::from_env(),
    };
    let total_capacity = env_usize("FANRING_BENCH_CAPACITY", 8192);
    let producer_counts = env_list("FANRING_BENCH_PRODUCERS", &[1, 2, 4, 8]);
    let batch_limits = env_list("FANRING_BENCH_BATCH", &[64, 1024]);
    let receives = Receive::ALL
        .into_iter()
        .filter(|receive| receive_filter.matches(receive.label()))
        .collect::<Vec<_>>();
    let profiles = Profile::ALL
        .into_iter()
        .filter(|profile| profile_filter.matches(profile.label()))
        .collect::<Vec<_>>();
    assert!(!receives.is_empty(), "no receive mode selected");
    assert!(!profiles.is_empty(), "no profile selected");

    println!(
        "MPSC batch receive comparison ({} x {:.2}s, {:.2}s warmup, capacity {} items, affinity {}, results {})\n",
        sampling.samples,
        duration.as_secs_f64(),
        sampling.warmup.as_secs_f64(),
        total_capacity,
        context.affinity.description(),
        results.root().display()
    );

    let mut configs = Vec::new();
    for &profile in &profiles {
        for &producers in &producer_counts {
            for &batch_limit in &batch_limits {
                configs.push(Config {
                    profile,
                    producers,
                    capacity_per_sender: (total_capacity / producers).max(1),
                    batch_limit,
                    duration,
                    sample: 0,
                    samples: sampling.samples,
                });
            }
        }
    }

    if payload_filter.matches("u64") {
        run_payload(
            &context,
            &mut results,
            &configs,
            &receives,
            sampling,
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
            &receives,
            sampling,
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
            &receives,
            sampling,
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
    receives: &[Receive],
    sampling: Sampling,
    payload: Payload<T>,
) where
    T: Copy + Send + 'static,
{
    println!("--- {} ({} bytes) ---", payload.label, size_of::<T>());
    for &config in configs {
        println!(
            "  profile={:<9} producers={:<2} capacity_per_sender={:<4} batch={}",
            config.profile.label(),
            config.producers,
            config.capacity_per_sender,
            config.batch_limit
        );
        if !sampling.warmup.is_zero() {
            let warmup = Config {
                duration: sampling.warmup,
                ..config
            };
            for &receive in receives {
                let _ = run_channel(context, warmup, receive, payload);
            }
        }

        let mut rows = Vec::new();
        for sample in 0..sampling.samples {
            let measured = Config { sample, ..config };
            let start = sample % receives.len();
            for offset in 0..receives.len() {
                let receive = receives[(start + offset) % receives.len()];
                let row = run_channel(context, measured, receive, payload);
                println!(
                    "    sample={:<2} {:<20} {:>8.2}M items/s  {:>6.2} ns/item",
                    sample + 1,
                    row.receive,
                    row.items_per_sec / 1_000_000.0,
                    row.ns_per_item
                );
                results.write("fanring", &row);
                rows.push(row);
            }
        }

        for &receive in receives {
            let (median, relative_mad) = median_and_relative_mad(
                rows.iter()
                    .filter(|row| row.receive == receive.label())
                    .map(|row| row.items_per_sec),
            );
            println!(
                "    {:<20} median {:>8.2}M items/s  {:>6.2} ns/item  MAD {:>5.2}%",
                receive.label(),
                median / 1_000_000.0,
                1e9 / median,
                relative_mad
            );
        }
        println!();
    }
}

fn run_channel<T>(
    context: &RunContext,
    config: Config,
    receive: Receive,
    payload: Payload<T>,
) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx0, rx) = channel::<T>(config.capacity_per_sender);
    let mut senders = vec![tx0];
    for _ in 1..config.producers {
        let tx = senders[0].try_clone().expect("fanring sender slot");
        senders.push(tx);
    }
    match config.profile {
        Profile::Prefilled => run_prefilled(context, config, receive, payload, senders, rx),
        Profile::Stream => run_stream(context, config, receive, payload, senders, rx),
    }
}

fn run_prefilled<T>(
    context: &RunContext,
    config: Config,
    receive: Receive,
    payload: Payload<T>,
    senders: Vec<Sender<T>>,
    mut rx: Receiver<T>,
) -> Row
where
    T: Copy + Send + 'static,
{
    let capacity = rx.capacity_per_sender();
    let per_round = capacity * config.producers;
    let stop = Arc::new(AtomicBool::new(false));
    let start_barrier = Arc::new(Barrier::new(config.producers + 1));
    let filled_barrier = Arc::new(Barrier::new(config.producers + 1));
    let mut handles = Vec::with_capacity(config.producers);
    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let start_barrier = start_barrier.clone();
        let filled_barrier = filled_barrier.clone();
        let affinity = context.affinity.clone();
        let value = payload.value;
        handles.push(thread::spawn(move || {
            affinity.pin(index + 1);
            loop {
                start_barrier.wait();
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let mut filled = 0;
                while sender.try_send(value).is_ok() {
                    filled += 1;
                }
                assert_eq!(filled, capacity, "producer could not refill its ring");
                filled_barrier.wait();
            }
        }));
    }

    context.affinity.pin(0);
    let mut batch = Vec::with_capacity(config.batch_limit);
    let deadline = Instant::now() + config.duration;
    let mut drain = Duration::ZERO;
    let mut items = 0u64;
    let mut rounds = 0u64;
    loop {
        if Instant::now() >= deadline {
            stop.store(true, Ordering::Relaxed);
            start_barrier.wait();
            break;
        }
        start_barrier.wait();
        filled_barrier.wait();
        let started = Instant::now();
        let mut received = 0;
        while received < per_round {
            match receive_batch(receive, &mut rx, &mut batch, config.batch_limit) {
                Batch::Items(count) => {
                    received += count;
                    black_box(&batch);
                    batch.clear();
                }
                Batch::Empty => panic!("prefilled rings reported empty"),
                Batch::Disconnected => panic!("prefilled rings reported disconnected"),
            }
        }
        drain += started.elapsed();
        items += per_round as u64;
        rounds += 1;
    }
    for handle in handles {
        handle.join().expect("producer thread panicked");
    }
    assert!(rounds > 0, "no prefilled round completed");
    row(context, config, receive, payload, drain, items, rounds)
}

fn run_stream<T>(
    context: &RunContext,
    config: Config,
    receive: Receive,
    payload: Payload<T>,
    senders: Vec<Sender<T>>,
    mut rx: Receiver<T>,
) -> Row
where
    T: Copy + Send + 'static,
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
                match sender.try_send(value) {
                    Ok(()) => sent += 1,
                    Err(TrySendError::Full(_)) => thread::yield_now(),
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }
            sent
        }));
    }

    context.affinity.pin(0);
    let mut batch = Vec::with_capacity(config.batch_limit);
    barrier.wait();
    let start = Instant::now();
    let deadline = start + config.duration;
    let mut items = 0u64;
    let mut polls = 0u64;
    loop {
        if polls.is_multiple_of(DEADLINE_CHECK_INTERVAL) && Instant::now() >= deadline {
            break;
        }
        polls = polls.wrapping_add(1);
        match receive_batch(receive, &mut rx, &mut batch, config.batch_limit) {
            Batch::Items(count) => {
                items += count as u64;
                black_box(&batch);
                batch.clear();
            }
            Batch::Empty => thread::yield_now(),
            Batch::Disconnected => break,
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent: u64 = handles
        .into_iter()
        .map(|handle| handle.join().expect("producer thread panicked"))
        .sum();
    loop {
        match receive_batch(receive, &mut rx, &mut batch, config.batch_limit) {
            Batch::Items(count) => {
                items += count as u64;
                black_box(&batch);
                batch.clear();
            }
            Batch::Empty => thread::yield_now(),
            Batch::Disconnected => break,
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(sent, items, "stream lost messages");
    row(context, config, receive, payload, elapsed, items, 0)
}

#[inline(never)]
fn receive_batch<T>(
    receive: Receive,
    rx: &mut Receiver<T>,
    batch: &mut Vec<T>,
    limit: usize,
) -> Batch {
    match receive {
        Receive::TryRecv => {
            let mut count = 0;
            let mut disconnected = false;
            while count < limit {
                match rx.try_recv() {
                    Ok(value) => {
                        batch.push(value);
                        count += 1;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }
            rx.release_consumed();
            if count != 0 {
                Batch::Items(count)
            } else if disconnected {
                Batch::Disconnected
            } else {
                Batch::Empty
            }
        }
        Receive::RecvBatchInto => match rx.recv_batch_into(batch, limit) {
            Ok(count) => Batch::Items(count),
            Err(_) => Batch::Disconnected,
        },
        Receive::TryRecvBatchInto => match rx.try_recv_batch_into(batch, limit) {
            Ok(count) => Batch::Items(count),
            Err(TryRecvError::Empty) => Batch::Empty,
            Err(TryRecvError::Disconnected) => Batch::Disconnected,
        },
        Receive::TryRecvBatchIntoWhile => {
            // A counting budget that admits exactly `limit` values measures
            // the per-value admission check without changing the batch shape.
            let mut budget = limit;
            let result = rx.try_recv_batch_into_while(batch, limit, |_| {
                if budget == 0 {
                    return false;
                }
                budget -= 1;
                true
            });
            match result {
                Ok(0) => Batch::Empty,
                Ok(count) => Batch::Items(count),
                Err(TryRecvError::Empty) => Batch::Empty,
                Err(TryRecvError::Disconnected) => Batch::Disconnected,
            }
        }
    }
}

fn row<T>(
    context: &RunContext,
    config: Config,
    receive: Receive,
    payload: Payload<T>,
    elapsed: Duration,
    items: u64,
    rounds: u64,
) -> Row {
    let seconds = elapsed.as_secs_f64();
    let items_per_sec = items as f64 / seconds;
    Row {
        run_id: context.run_id.clone(),
        cpu: context.cpu.clone(),
        affinity: context.affinity.description().to_string(),
        profile: config.profile.label(),
        receive: receive.label(),
        payload: payload.label,
        payload_bytes: size_of::<T>(),
        producers: config.producers,
        capacity_per_sender: config.capacity_per_sender,
        batch_limit: config.batch_limit,
        seconds,
        items,
        items_per_sec,
        ns_per_item: 1e9 / items_per_sec,
        rounds,
        sample: config.sample + 1,
        samples: config.samples,
    }
}

impl Receive {
    const ALL: [Self; 4] = [
        Self::TryRecv,
        Self::RecvBatchInto,
        Self::TryRecvBatchInto,
        Self::TryRecvBatchIntoWhile,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::TryRecv => "try_recv",
            Self::RecvBatchInto => "recv_batch_into",
            Self::TryRecvBatchInto => "try_recv_batch_into",
            Self::TryRecvBatchIntoWhile => "try_recv_batch_into_while",
        }
    }
}

impl Profile {
    const ALL: [Self; 2] = [Self::Prefilled, Self::Stream];

    const fn label(self) -> &'static str {
        match self {
            Self::Prefilled => "prefilled",
            Self::Stream => "stream",
        }
    }
}

impl Filter {
    fn from_env(name: &str) -> Self {
        Self {
            values: std::env::var(name).ok().map(|value| {
                value
                    .split(',')
                    .map(|item| item.trim().to_string())
                    .filter(|item| !item.is_empty())
                    .collect()
            }),
        }
    }

    fn matches(&self, value: &str) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.iter().any(|item| item == value))
    }
}

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |value| {
            value
                .split(',')
                .map(|item| item.trim().parse().expect("numeric list entry"))
                .collect()
        },
    )
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn cpu_name() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split(':').nth(1))
                .map(|name| name.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}
