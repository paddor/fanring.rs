//! Timed MPMC runs: per-implementation setup and the measured producer and
//! consumer threads.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use crate::adapters::{BenchReceiver, BenchSender, RecvAttempt, SendAttempt};
use crate::support::Saturation;
use crate::{Case, Config, Mode, Outcome, Profile, Row, clones, row};

/// Stop flag, saturation phases, and barriers shared by saturated workers.
struct SaturatedSync {
    stop: Arc<AtomicBool>,
    saturation: Saturation,
    barrier: Arc<Barrier>,
    start_consuming: Arc<Barrier>,
    phase_barrier: Arc<Barrier>,
}

pub(crate) fn bench_fanring<T, P: fanring::teardown::Teardown>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let config = case.config;
    let (tx0, rx0) = fanring::mpmc::channel_with_policy::<T, P>(config.capacity_per_sender);
    let mut senders = vec![tx0];
    for _ in 1..config.producers {
        senders.push(senders[0].try_clone().expect("fanring sender lane"));
    }
    let mut receivers = vec![rx0];
    for _ in 1..config.consumers {
        receivers.push(receivers[0].clone());
    }
    run_channel(case, senders, receivers)
}

pub(crate) fn bench_crossbeam<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    let receivers = clones(&rx, case.config.consumers);
    drop(tx);
    drop(rx);
    run_channel(case, senders, receivers)
}

pub(crate) fn bench_crossfire<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = crossfire::mpmc::bounded_blocking(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    let receivers = clones(&rx, case.config.consumers);
    drop(tx);
    drop(rx);
    run_channel(case, senders, receivers)
}

pub(crate) fn bench_flume<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = flume::bounded(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    let receivers = clones(&rx, case.config.consumers);
    drop(tx);
    drop(rx);
    run_channel(case, senders, receivers)
}

pub(crate) fn bench_kanal<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = kanal::bounded(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    let receivers = clones(&rx, case.config.consumers);
    drop(tx);
    drop(rx);
    run_channel(case, senders, receivers)
}

fn run_channel<T, S, R>(case: Case<'_, T>, senders: Vec<S>, receivers: Vec<R>) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    match case.config.profile {
        // Saturated runs always use nonblocking sends and receives.
        Profile::Saturated => run_saturated_channel(
            Case {
                mode: Mode::Try,
                ..case
            },
            senders,
            receivers,
        ),
        Profile::Uncontrolled => run_uncontrolled_channel(case, senders, receivers),
    }
}

fn run_saturated_channel<T, S, R>(case: Case<'_, T>, senders: Vec<S>, receivers: Vec<R>) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    let Case {
        implementation,
        config,
        ..
    } = case;
    let sync = SaturatedSync::new(config);
    let sender_handles = spawn_saturated_producers(case, senders, &sync);
    let SaturatedSync {
        saturation,
        barrier,
        start_consuming,
        ..
    } = &sync;

    let burst_size = usize::try_from(saturation.high_watermark() - saturation.low_watermark())
        .expect("benchmark capacity fits usize");
    assert!(
        burst_size >= config.consumers,
        "saturated burst must cover every consumer"
    );
    let receiver_handles = spawn_saturated_consumers(case, receivers, &sync, burst_size);

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
        case,
        Outcome {
            elapsed,
            items: received,
        },
        Some(saturation),
    )
}

impl SaturatedSync {
    fn new(config: Config) -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            saturation: Saturation::new(config.producers, config.total_capacity()),
            barrier: Arc::new(Barrier::new(config.producers + config.consumers + 1)),
            start_consuming: Arc::new(Barrier::new(config.consumers + 1)),
            phase_barrier: Arc::new(Barrier::new(config.consumers)),
        }
    }
}

fn spawn_saturated_producers<T, S>(
    case: Case<'_, T>,
    senders: Vec<S>,
    sync: &SaturatedSync,
) -> Vec<thread::JoinHandle<u64>>
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
{
    let config = case.config;
    let mut sender_handles = Vec::with_capacity(config.producers);
    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = sync.stop.clone();
        let barrier = sync.barrier.clone();
        let affinity = case.context.affinity.clone();
        let value = case.payload.value;
        let mut full = sync.saturation.producer(index);
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
    sender_handles
}

fn spawn_saturated_consumers<T, R>(
    case: Case<'_, T>,
    receivers: Vec<R>,
    sync: &SaturatedSync,
    burst_size: usize,
) -> Vec<thread::JoinHandle<u64>>
where
    T: Copy + Send + 'static,
    R: BenchReceiver<T>,
{
    let config = case.config;
    let mut receiver_handles = Vec::with_capacity(config.consumers);
    for (index, mut receiver) in receivers.into_iter().enumerate() {
        let stop = sync.stop.clone();
        let barrier = sync.barrier.clone();
        let start_consuming = sync.start_consuming.clone();
        let phase_barrier = sync.phase_barrier.clone();
        let saturation = sync.saturation.clone();
        let affinity = case.context.affinity.clone();
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
    receiver_handles
}

fn run_uncontrolled_channel<T, S, R>(case: Case<'_, T>, senders: Vec<S>, receivers: Vec<R>) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    let Case {
        implementation,
        config,
        ..
    } = case;
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(config.producers + config.consumers + 1));
    let sender_handles = spawn_uncontrolled_producers(case, senders, &stop, &barrier);
    let receiver_handles = spawn_uncontrolled_consumers(case, receivers, &barrier);

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
        case,
        Outcome {
            elapsed,
            items: received,
        },
        None,
    )
}

fn spawn_uncontrolled_producers<T, S>(
    case: Case<'_, T>,
    senders: Vec<S>,
    stop: &Arc<AtomicBool>,
    barrier: &Arc<Barrier>,
) -> Vec<thread::JoinHandle<u64>>
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
{
    let Case { config, mode, .. } = case;
    let mut sender_handles = Vec::with_capacity(config.producers);
    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let affinity = case.context.affinity.clone();
        let value = case.payload.value;
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
    sender_handles
}

fn spawn_uncontrolled_consumers<T, R>(
    case: Case<'_, T>,
    receivers: Vec<R>,
    barrier: &Arc<Barrier>,
) -> Vec<thread::JoinHandle<u64>>
where
    T: Copy + Send + 'static,
    R: BenchReceiver<T>,
{
    let Case { config, mode, .. } = case;
    let mut receiver_handles = Vec::with_capacity(config.consumers);
    for (index, mut receiver) in receivers.into_iter().enumerate() {
        let barrier = barrier.clone();
        let affinity = case.context.affinity.clone();
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
    receiver_handles
}
