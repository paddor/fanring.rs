#![cfg_attr(test, allow(dead_code, unused_imports))]

mod support;

use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;

use support::{Affinity, BenchReceiver, BenchSender, Filter, JsonlResults, RunContext};

#[derive(Debug, Serialize)]
struct Row {
    run_id: String,
    cpu: String,
    affinity: String,
    implementation: &'static str,
    operation: &'static str,
    spin_ns: Option<u64>,
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

/// Round counts and settle behavior shared by every wake measurement.
#[derive(Debug, Clone, Copy)]
struct Wake {
    rounds: usize,
    warmup: usize,
    settle: Duration,
    settle_mode: SettleMode,
}

/// One measured implementation and its spin budget, if any.
#[derive(Debug, Clone, Copy)]
struct Series {
    implementation: &'static str,
    spin_ns: Option<u64>,
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
    let spin_ns = env_optional_u64("FANRING_WAKE_SPIN_NS");
    let mut runner = Runner {
        mpsc: JsonlResults::new("latency-mpsc.jsonl"),
        mpmc: JsonlResults::new("latency-mpmc.jsonl"),
        filter: Filter::from_env("FANRING_BENCH_IMPLS"),
        context: RunContext::from_env(),
        wake: Wake {
            rounds,
            warmup,
            settle,
            settle_mode,
        },
    };

    assert!(rounds > 0, "wake rounds must be > 0");
    println!(
        "Wake latency ({rounds} samples, {warmup} warmup, {} ns {} settle, affinity {}, results {})\n",
        settle.as_nanos(),
        settle_mode.label(),
        runner.context.affinity.description(),
        runner.mpsc.root().display()
    );

    run_fanring(&mut runner, spin_ns);
    run_external(&mut runner);
    runner.mpsc.flush();
    runner.mpmc.flush();
}

/// Run settings and result files shared by every measured implementation.
struct Runner {
    mpsc: JsonlResults,
    mpmc: JsonlResults,
    filter: Filter,
    context: RunContext,
    wake: Wake,
}

/// Measures the fanring channels, plus their spin-wait variants when
/// `spin_ns` is set.
fn run_fanring(runner: &mut Runner, spin_ns: Option<u64>) {
    let Runner {
        mpsc,
        mpmc,
        filter,
        context,
        wake,
    } = runner;
    if filter.matches("fanring") {
        run_implementation(
            context,
            *wake,
            Series::new("fanring", None),
            &mut [mpsc],
            || fanring::mpsc::channel(1),
        );
    }
    if filter.matches("fanring-mpmc") {
        run_implementation(
            context,
            *wake,
            Series::new("fanring-mpmc", None),
            &mut [mpmc],
            || fanring::mpmc::channel(1),
        );
    }
    let Some(spin_ns) = spin_ns else {
        return;
    };
    let strategy = fanring::WaitStrategy::SpinFor(Duration::from_nanos(spin_ns));
    if filter.matches("fanring-spin") {
        run_implementation(
            context,
            *wake,
            Series::new("fanring-spin", Some(spin_ns)),
            &mut [mpsc],
            || {
                let (mut sender, mut receiver) = fanring::mpsc::channel(1);
                sender.set_wait_strategy(strategy);
                receiver.set_wait_strategy(strategy);
                (sender, receiver)
            },
        );
    }
    if filter.matches("fanring-spin-mpmc") {
        run_implementation(
            context,
            *wake,
            Series::new("fanring-spin-mpmc", Some(spin_ns)),
            &mut [mpmc],
            || {
                let (mut sender, mut receiver) = fanring::mpmc::channel(1);
                sender.set_wait_strategy(strategy);
                receiver.set_wait_strategy(strategy);
                (sender, receiver)
            },
        );
    }
}

/// Measures the other channel crates.
fn run_external(runner: &mut Runner) {
    let Runner {
        mpsc,
        mpmc,
        filter,
        context,
        wake,
    } = runner;
    let wake = *wake;
    if filter.matches("crossbeam-channel") {
        let series = Series::new("crossbeam-channel", None);
        run_implementation(context, wake, series, &mut [mpsc, mpmc], || {
            crossbeam_channel::bounded(1)
        });
    }
    if filter.matches("crossfire") {
        let series = Series::new("crossfire", None);
        run_implementation(context, wake, series, &mut [mpsc], || {
            crossfire::mpsc::bounded_blocking(1)
        });
    }
    if filter.matches("crossfire-mpmc") {
        let series = Series::new("crossfire-mpmc", None);
        run_implementation(context, wake, series, &mut [mpmc], || {
            crossfire::mpmc::bounded_blocking(1)
        });
    }
    if filter.matches("flume") {
        let series = Series::new("flume", None);
        run_implementation(context, wake, series, &mut [mpsc, mpmc], || {
            flume::bounded(1)
        });
    }
    if filter.matches("kanal") {
        let series = Series::new("kanal", None);
        run_implementation(context, wake, series, &mut [mpsc, mpmc], || {
            kanal::bounded(1)
        });
    }
    if filter.matches("thingbuf") {
        let series = Series::new("thingbuf", None);
        run_implementation(context, wake, series, &mut [mpsc], || {
            thingbuf::mpsc::blocking::channel(1)
        });
    }
}

impl Series {
    const fn new(implementation: &'static str, spin_ns: Option<u64>) -> Self {
        Self {
            implementation,
            spin_ns,
        }
    }
}

fn run_implementation<S, R, F>(
    context: &RunContext,
    wake: Wake,
    series: Series,
    outputs: &mut [&mut JsonlResults],
    make_channel: F,
) where
    S: BenchSender<u64>,
    R: BenchReceiver<u64>,
    F: Fn() -> (S, R),
{
    let (sender, receiver) = make_channel();
    let recv_samples = measure_recv_wake(&context.affinity, sender, receiver, wake);
    write_row(
        outputs,
        &summarize(context, series, "recv_wake", wake, recv_samples),
    );

    let (sender, receiver) = make_channel();
    let send_samples = measure_send_wake(&context.affinity, sender, receiver, wake);
    write_row(
        outputs,
        &summarize(context, series, "send_wake", wake, send_samples),
    );
    println!();
}

fn measure_recv_wake<S, R>(
    affinity: &Affinity,
    mut sender: S,
    mut receiver: R,
    wake: Wake,
) -> Vec<u64>
where
    S: BenchSender<u64>,
    R: BenchReceiver<u64>,
{
    let Wake {
        rounds,
        warmup,
        settle,
        settle_mode,
    } = wake;
    let total = rounds + warmup;
    affinity.pin(0);
    let affinity = affinity.clone();
    let clock = Instant::now();
    let (ready_tx, ready_rx) = sync_channel(0);
    let (sample_tx, sample_rx) = sync_channel(0);
    let receiver = thread::spawn(move || {
        affinity.pin(1);
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
    affinity: &Affinity,
    mut sender: S,
    mut receiver: R,
    wake: Wake,
) -> Vec<u64>
where
    S: BenchSender<u64>,
    R: BenchReceiver<u64>,
{
    let Wake {
        rounds,
        warmup,
        settle,
        settle_mode,
    } = wake;
    let total = rounds + warmup;
    affinity.pin(0);
    let affinity = affinity.clone();
    let clock = Instant::now();
    assert!(sender.send(0), "receiver disconnected");
    let (ready_tx, ready_rx) = sync_channel(0);
    let (sample_tx, sample_rx) = sync_channel(0);
    let sender = thread::spawn(move || {
        affinity.pin(1);
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
    context: &RunContext,
    series: Series,
    operation: &'static str,
    wake: Wake,
    mut samples: Vec<u64>,
) -> Row {
    samples.sort_unstable();
    let sum = samples.iter().map(|&value| u128::from(value)).sum::<u128>();
    let sample_count = u128::try_from(samples.len()).expect("sample count fits u128");
    let row = Row {
        run_id: context.run_id.clone(),
        cpu: context.cpu.clone(),
        affinity: context.affinity.description().to_string(),
        implementation: series.implementation,
        operation,
        spin_ns: series.spin_ns,
        capacity: 1,
        rounds: samples.len(),
        settle_mode: wake.settle_mode.label(),
        settle_ns: duration_ns(wake.settle),
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

fn write_row(outputs: &mut [&mut JsonlResults], row: &Row) {
    for output in outputs {
        output.write(row.implementation, row);
    }
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

fn env_optional_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect("invalid u64 environment value"))
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
