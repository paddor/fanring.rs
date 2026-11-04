//! Timed MPSC runs: per-implementation setup, producer threads, and the
//! measured receive loop.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use crate::adapters::{
    BenchReceiver, BenchSender, ConcurrentReceiver, ConcurrentSender, RecvAttempt, SendAttempt,
};
use crate::support::Saturation;
use crate::{Case, Mode, Outcome, Profile, Row, clones, row};

// Producer stop checks must not share a cache line with queue allocations.
#[derive(Debug)]
#[repr(align(128))]
struct Stop(AtomicBool);

const TIME_CHECK_INTERVAL: u64 = 1024;

pub(crate) fn bench_fanring<T, P: fanring::teardown::Teardown>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let config = case.config;
    let (tx0, rx) = fanring::mpsc::channel_with_policy::<T, P>(config.capacity_per_sender);
    let mut senders = vec![tx0];
    for _ in 1..config.producers {
        let tx = senders[0].try_clone().expect("fanring sender slot");
        senders.push(tx);
    }
    run_channel(case, senders, rx)
}

pub(crate) fn bench_crossbeam_channel<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = crossbeam_channel::bounded::<T>(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    drop(tx);
    run_channel(case, senders, rx)
}

pub(crate) fn bench_crossfire<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = crossfire::mpsc::bounded_blocking(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    drop(tx);
    run_channel(case, senders, rx)
}

pub(crate) fn bench_flume<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = flume::bounded::<T>(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    drop(tx);
    run_channel(case, senders, rx)
}

pub(crate) fn bench_kanal<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let (tx, rx) = kanal::bounded::<T>(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    drop(tx);
    run_channel(case, senders, rx)
}

pub(crate) fn bench_concurrent_queue<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Send + 'static,
{
    let queue = Arc::new(concurrent_queue::ConcurrentQueue::bounded(
        case.config.total_capacity(),
    ));
    let senders = (0..case.config.producers)
        .map(|_| ConcurrentSender(queue.clone()))
        .collect();
    run_channel(case, senders, ConcurrentReceiver(queue))
}

pub(crate) fn bench_thingbuf<T>(case: Case<'_, T>) -> Row
where
    T: Copy + Default + Send + Sync + 'static,
{
    let (tx, rx) = thingbuf::mpsc::blocking::channel::<T>(case.config.total_capacity());
    let senders = clones(&tx, case.config.producers);
    drop(tx);
    run_channel(case, senders, rx)
}

fn run_channel<T, S, R>(case: Case<'_, T>, senders: Vec<S>, receiver: R) -> Row
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
            receiver,
        ),
        Profile::Uncontrolled => run_uncontrolled_channel(case, senders, receiver),
    }
}

fn run_saturated_channel<T, S, R>(case: Case<'_, T>, senders: Vec<S>, mut receiver: R) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    let Case {
        context,
        implementation,
        config,
        ..
    } = case;
    let stop = Arc::new(Stop(AtomicBool::new(false)));
    let saturation = Saturation::new(config.producers, config.total_capacity());
    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let handles = spawn_saturated_producers(case, senders, &stop, &barrier, &saturation);

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
    stop.0.store(true, Ordering::Relaxed);

    let sent = join_and_drain(handles, implementation, &mut receiver, &mut messages);
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "{implementation} lost messages");
    row(
        case,
        Outcome {
            elapsed,
            items: messages,
        },
        Some(&saturation),
    )
}

fn spawn_saturated_producers<T, S>(
    case: Case<'_, T>,
    senders: Vec<S>,
    stop: &Arc<Stop>,
    barrier: &Arc<Barrier>,
    saturation: &Saturation,
) -> Vec<thread::JoinHandle<u64>>
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
{
    let mut handles = Vec::with_capacity(case.config.producers);

    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let affinity = case.context.affinity.clone();
        let value = case.payload.value;
        let mut full = saturation.producer(index);
        handles.push(thread::spawn(move || {
            affinity.pin(index + 1);
            barrier.wait();
            let mut sent = 0u64;
            while !stop.0.load(Ordering::Relaxed) {
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
    handles
}

fn run_uncontrolled_channel<T, S, R>(case: Case<'_, T>, senders: Vec<S>, mut receiver: R) -> Row
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
    R: BenchReceiver<T>,
{
    let Case {
        context,
        implementation,
        config,
        mode,
        ..
    } = case;
    let stop = Arc::new(Stop(AtomicBool::new(false)));
    let barrier = Arc::new(Barrier::new(config.producers + 1));
    let handles = spawn_uncontrolled_producers(case, senders, &stop, &barrier);

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
    stop.0.store(true, Ordering::Relaxed);
    let sent = if mode == Mode::Blocking {
        while let Some(value) = receiver.recv() {
            black_box(value);
            messages += 1;
        }
        join_counts(handles, implementation)
    } else {
        join_and_drain(handles, implementation, &mut receiver, &mut messages)
    };
    let elapsed = start.elapsed();
    assert_eq!(sent, messages, "{implementation} lost messages");
    row(
        case,
        Outcome {
            elapsed,
            items: messages,
        },
        None,
    )
}

fn spawn_uncontrolled_producers<T, S>(
    case: Case<'_, T>,
    senders: Vec<S>,
    stop: &Arc<Stop>,
    barrier: &Arc<Barrier>,
) -> Vec<thread::JoinHandle<u64>>
where
    T: Copy + Send + 'static,
    S: BenchSender<T>,
{
    let mode = case.mode;
    let mut handles = Vec::with_capacity(case.config.producers);
    for (index, mut sender) in senders.into_iter().enumerate() {
        let stop = stop.clone();
        let barrier = barrier.clone();
        let affinity = case.context.affinity.clone();
        let value = case.payload.value;
        handles.push(thread::spawn(move || {
            affinity.pin(index + 1);
            barrier.wait();
            let mut sent = 0u64;
            while !stop.0.load(Ordering::Relaxed) {
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
    handles
}

/// Joins stopped producers, then drains what they left queued.
///
/// Returns the producers' sent count. Received values add to `messages`.
/// Inlined so the timed drain compiles as it did in place.
#[inline(always)]
fn join_and_drain<T, R: BenchReceiver<T>>(
    handles: Vec<thread::JoinHandle<u64>>,
    implementation: &str,
    receiver: &mut R,
    messages: &mut u64,
) -> u64 {
    let sent = join_counts(handles, implementation);
    receiver.close_for_drain();
    loop {
        match receiver.try_recv() {
            RecvAttempt::Item(value) => {
                black_box(value);
                *messages += 1;
            }
            RecvAttempt::Empty => thread::yield_now(),
            RecvAttempt::Disconnected => break,
        }
    }
    sent
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

fn reached_deadline(polls: u64, deadline: Instant) -> bool {
    polls & (TIME_CHECK_INTERVAL - 1) == 0 && Instant::now() >= deadline
}
