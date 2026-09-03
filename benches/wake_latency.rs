#![cfg_attr(test, allow(dead_code, unused_imports))]

use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[derive(Debug, Serialize)]
struct Row {
    run_id: String,
    cpu: String,
    implementation: &'static str,
    operation: &'static str,
    capacity: usize,
    rounds: usize,
    settle_mode: &'static str,
    settle_ns: u64,
    mean_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

trait BlockingSender: Send {
    fn send(&mut self, value: u64) -> bool;
}

trait BlockingReceiver: Send {
    fn recv(&mut self) -> Option<u64>;
}

impl BlockingSender for fanring::mpsc::Sender<u64> {
    fn send(&mut self, value: u64) -> bool {
        fanring::mpsc::Sender::send(self, value).is_ok()
    }
}

impl BlockingReceiver for fanring::mpsc::Receiver<u64> {
    fn recv(&mut self) -> Option<u64> {
        fanring::mpsc::Receiver::recv(self).ok()
    }
}

impl BlockingSender for fanring::mpmc::Sender<u64> {
    fn send(&mut self, value: u64) -> bool {
        fanring::mpmc::Sender::send(self, value).is_ok()
    }
}

impl BlockingReceiver for fanring::mpmc::Receiver<u64> {
    fn recv(&mut self) -> Option<u64> {
        fanring::mpmc::Receiver::recv(self).ok()
    }
}

impl BlockingSender for crossbeam_channel::Sender<u64> {
    fn send(&mut self, value: u64) -> bool {
        crossbeam_channel::Sender::send(self, value).is_ok()
    }
}

impl BlockingReceiver for crossbeam_channel::Receiver<u64> {
    fn recv(&mut self) -> Option<u64> {
        crossbeam_channel::Receiver::recv(self).ok()
    }
}

impl BlockingSender for crossfire::MTx<crossfire::mpsc::Array<u64>> {
    fn send(&mut self, value: u64) -> bool {
        crossfire::BlockingTxTrait::send(self, value).is_ok()
    }
}

impl BlockingReceiver for crossfire::Rx<crossfire::mpsc::Array<u64>> {
    fn recv(&mut self) -> Option<u64> {
        crossfire::BlockingRxTrait::recv(self).ok()
    }
}

impl BlockingSender for crossfire::MTx<crossfire::mpmc::Array<u64>> {
    fn send(&mut self, value: u64) -> bool {
        crossfire::BlockingTxTrait::send(self, value).is_ok()
    }
}

impl BlockingReceiver for crossfire::MRx<crossfire::mpmc::Array<u64>> {
    fn recv(&mut self) -> Option<u64> {
        crossfire::BlockingRxTrait::recv(self).ok()
    }
}

impl BlockingSender for flume::Sender<u64> {
    fn send(&mut self, value: u64) -> bool {
        flume::Sender::send(self, value).is_ok()
    }
}

impl BlockingReceiver for flume::Receiver<u64> {
    fn recv(&mut self) -> Option<u64> {
        flume::Receiver::recv(self).ok()
    }
}

impl BlockingSender for kanal::Sender<u64> {
    fn send(&mut self, value: u64) -> bool {
        kanal::Sender::send(self, value).is_ok()
    }
}

impl BlockingReceiver for kanal::Receiver<u64> {
    fn recv(&mut self) -> Option<u64> {
        kanal::Receiver::recv(self).ok()
    }
}

impl BlockingSender for thingbuf::mpsc::blocking::Sender<u64> {
    fn send(&mut self, value: u64) -> bool {
        thingbuf::mpsc::blocking::Sender::send(self, value).is_ok()
    }
}

impl BlockingReceiver for thingbuf::mpsc::blocking::Receiver<u64> {
    fn recv(&mut self) -> Option<u64> {
        thingbuf::mpsc::blocking::Receiver::recv(self)
    }
}

#[cfg(all(test, debug_assertions))]
fn main() {}

#[cfg(not(all(test, debug_assertions)))]
fn main() {
    crossfire::detect_backoff_cfg();
    let rounds = env_usize("FANRING_WAKE_ROUNDS", 10_000);
    let warmup = env_usize("FANRING_WAKE_WARMUP", 200);
    let settle = Duration::from_nanos(env_u64("FANRING_WAKE_SETTLE_NS", 50_000));
    let settle_mode = SettleMode::from_env();
    let out_path = std::env::var_os("FANRING_WAKE_OUT").map_or_else(
        || PathBuf::from("target/fanring-bench/wake-latency.jsonl"),
        PathBuf::from,
    );
    let filter = Filter::from_env("FANRING_BENCH_IMPLS");
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos()
        .to_string();
    let cpu = cpu_name();

    assert!(rounds > 0, "wake rounds must be > 0");
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).expect("create benchmark output dir");
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .expect("open wake benchmark JSONL");
    let mut out = BufWriter::new(file);

    println!(
        "Wake latency ({rounds} samples, {warmup} warmup, {} ns {} settle, output {})\n",
        settle.as_nanos(),
        settle_mode.label(),
        out_path.display()
    );

    if filter.matches("fanring") {
        run_implementation(
            &run_id,
            &cpu,
            "fanring",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || fanring::mpsc::channel(1),
        );
    }
    if filter.matches("fanring-mpmc") {
        run_implementation(
            &run_id,
            &cpu,
            "fanring-mpmc",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || fanring::mpmc::channel(1),
        );
    }
    if filter.matches("crossbeam-channel") {
        run_implementation(
            &run_id,
            &cpu,
            "crossbeam-channel",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || crossbeam_channel::bounded(1),
        );
    }
    if filter.matches("crossfire") {
        run_implementation(
            &run_id,
            &cpu,
            "crossfire",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || crossfire::mpsc::bounded_blocking(1),
        );
    }
    if filter.matches("crossfire-mpmc") {
        run_implementation(
            &run_id,
            &cpu,
            "crossfire-mpmc",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || crossfire::mpmc::bounded_blocking(1),
        );
    }
    if filter.matches("flume") {
        run_implementation(
            &run_id,
            &cpu,
            "flume",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || flume::bounded(1),
        );
    }
    if filter.matches("kanal") {
        run_implementation(
            &run_id,
            &cpu,
            "kanal",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || kanal::bounded(1),
        );
    }
    if filter.matches("thingbuf") {
        run_implementation(
            &run_id,
            &cpu,
            "thingbuf",
            rounds,
            warmup,
            settle,
            settle_mode,
            &mut out,
            || thingbuf::mpsc::blocking::channel(1),
        );
    }

    out.flush().expect("flush wake benchmark JSONL");
}

#[allow(clippy::too_many_arguments)]
fn run_implementation<S, R, F>(
    run_id: &str,
    cpu: &str,
    implementation: &'static str,
    rounds: usize,
    warmup: usize,
    settle: Duration,
    settle_mode: SettleMode,
    out: &mut impl Write,
    make_channel: F,
) where
    S: BlockingSender + 'static,
    R: BlockingReceiver + 'static,
    F: Fn() -> (S, R),
{
    let (sender, receiver) = make_channel();
    let recv_samples = measure_recv_wake(sender, receiver, rounds, warmup, settle, settle_mode);
    write_row(
        out,
        &summarize(
            run_id,
            cpu,
            implementation,
            "recv_wake",
            settle,
            settle_mode,
            recv_samples,
        ),
    );

    let (sender, receiver) = make_channel();
    let send_samples = measure_send_wake(sender, receiver, rounds, warmup, settle, settle_mode);
    write_row(
        out,
        &summarize(
            run_id,
            cpu,
            implementation,
            "send_wake",
            settle,
            settle_mode,
            send_samples,
        ),
    );
    println!();
}

fn measure_recv_wake<S, R>(
    mut sender: S,
    mut receiver: R,
    rounds: usize,
    warmup: usize,
    settle: Duration,
    settle_mode: SettleMode,
) -> Vec<u64>
where
    S: BlockingSender,
    R: BlockingReceiver + 'static,
{
    let total = rounds + warmup;
    let clock = Instant::now();
    let (ready_tx, ready_rx) = sync_channel(0);
    let (sample_tx, sample_rx) = sync_channel(0);
    let receiver = thread::spawn(move || {
        for _ in 0..total {
            ready_tx.send(()).expect("signal receiver ready");
            let sent = receiver.recv().expect("receive wake payload");
            sample_tx
                .send(elapsed_ns(clock).saturating_sub(sent))
                .expect("report receiver wake sample");
        }
    });

    let mut samples = Vec::with_capacity(rounds);
    for round in 0..total {
        ready_rx.recv().expect("wait for receiver ready");
        settle_mode.wait(settle);
        assert!(sender.send(elapsed_ns(clock)), "receiver disconnected");
        let sample = sample_rx.recv().expect("receive receiver wake sample");
        if round >= warmup {
            samples.push(sample);
        }
    }
    drop(sender);
    receiver.join().expect("receiver wake thread");
    samples
}

fn measure_send_wake<S, R>(
    mut sender: S,
    mut receiver: R,
    rounds: usize,
    warmup: usize,
    settle: Duration,
    settle_mode: SettleMode,
) -> Vec<u64>
where
    S: BlockingSender + 'static,
    R: BlockingReceiver,
{
    let total = rounds + warmup;
    let clock = Instant::now();
    assert!(sender.send(0), "receiver disconnected");
    let (ready_tx, ready_rx) = sync_channel(0);
    let (sample_tx, sample_rx) = sync_channel(0);
    let sender = thread::spawn(move || {
        for value in 1..=total as u64 {
            ready_tx.send(()).expect("signal sender ready");
            assert!(sender.send(value), "receiver disconnected");
            sample_tx
                .send(elapsed_ns(clock))
                .expect("report sender wake sample");
        }
    });

    let mut samples = Vec::with_capacity(rounds);
    for round in 0..total {
        ready_rx.recv().expect("wait for sender ready");
        settle_mode.wait(settle);
        let started = elapsed_ns(clock);
        receiver.recv().expect("receive buffered payload");
        let completed = sample_rx.recv().expect("receive sender wake sample");
        if round >= warmup {
            samples.push(completed.saturating_sub(started));
        }
    }
    drop(receiver);
    sender.join().expect("sender wake thread");
    samples
}

fn summarize(
    run_id: &str,
    cpu: &str,
    implementation: &'static str,
    operation: &'static str,
    settle: Duration,
    settle_mode: SettleMode,
    mut samples: Vec<u64>,
) -> Row {
    samples.sort_unstable();
    let sum = samples.iter().map(|&value| u128::from(value)).sum::<u128>();
    let sample_count = u128::try_from(samples.len()).expect("sample count fits u128");
    let row = Row {
        run_id: run_id.to_string(),
        cpu: cpu.to_string(),
        implementation,
        operation,
        capacity: 1,
        rounds: samples.len(),
        settle_mode: settle_mode.label(),
        settle_ns: duration_ns(settle),
        mean_ns: u64::try_from(sum / sample_count).expect("mean of u64 samples fits u64"),
        p50_ns: percentile(&samples, 500),
        p95_ns: percentile(&samples, 950),
        p99_ns: percentile(&samples, 990),
        p999_ns: percentile(&samples, 999),
        max_ns: *samples.last().expect("wake samples are nonempty"),
    };
    println!(
        "  {:<18} {:<9} mean={:>8}ns p50={:>8}ns p95={:>8}ns p99={:>8}ns p99.9={:>8}ns max={:>8}ns",
        row.implementation,
        row.operation,
        row.mean_ns,
        row.p50_ns,
        row.p95_ns,
        row.p99_ns,
        row.p999_ns,
        row.max_ns,
    );
    row
}

fn percentile(samples: &[u64], permille: usize) -> u64 {
    let index = (samples.len() - 1) * permille / 1_000;
    samples[index]
}

fn write_row(out: &mut impl Write, row: &Row) {
    serde_json::to_writer(&mut *out, row).expect("write wake benchmark row");
    writeln!(out).expect("write wake benchmark newline");
}

fn elapsed_ns(clock: Instant) -> u64 {
    duration_ns(clock.elapsed())
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map_or(default, |value| {
        value.parse().expect("invalid usize environment value")
    })
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().map_or(default, |value| {
        value.parse().expect("invalid u64 environment value")
    })
}

#[derive(Debug, Clone, Copy)]
enum SettleMode {
    Sleep,
    Spin,
}

impl SettleMode {
    fn from_env() -> Self {
        match std::env::var("FANRING_WAKE_SETTLE_MODE").as_deref() {
            Ok("spin") => Self::Spin,
            Ok("sleep") | Err(_) => Self::Sleep,
            Ok(value) => panic!("invalid settle mode {value:?}; expected sleep or spin"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Sleep => "sleep",
            Self::Spin => "spin",
        }
    }

    fn wait(self, duration: Duration) {
        match self {
            Self::Sleep => thread::sleep(duration),
            Self::Spin => {
                let deadline = Instant::now() + duration;
                while Instant::now() < deadline {
                    std::hint::spin_loop();
                }
            }
        }
    }
}

#[derive(Debug)]
struct Filter {
    values: Option<Vec<String>>,
}

impl Filter {
    fn from_env(name: &str) -> Self {
        let values = std::env::var(name).ok().map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect()
        });
        Self { values }
    }

    fn matches(&self, value: &str) -> bool {
        self.values
            .as_ref()
            .is_none_or(|values| values.iter().any(|candidate| candidate == value))
    }
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
