use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
struct Config {
    duration: Duration,
    cpus: [core_affinity::CoreId; 2],
}

impl Config {
    fn pin(self, index: usize) {
        assert!(
            core_affinity::set_for_current(self.cpus[index]),
            "CPU pin failed"
        );
    }
}

fn cpu_pair() -> [core_affinity::CoreId; 2] {
    let mut available = core_affinity::get_core_ids().expect("CPU affinity unavailable");
    available.sort_unstable_by_key(|cpu| cpu.id);
    assert!(available.len() >= 2, "comparison needs two available CPUs");
    if let Ok(ids) = std::env::var("YRING_BENCH_CPUS") {
        let ids = ids
            .split(',')
            .map(|id| id.parse::<usize>().expect("invalid CPU ID"))
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2, "YRING_BENCH_CPUS needs two CPU IDs");
        assert_ne!(ids[0], ids[1], "producer and consumer need different CPUs");
        let pair = [ids[0], ids[1]].map(|id| {
            *available
                .iter()
                .find(|cpu| cpu.id == id)
                .expect("CPU unavailable")
        });
        if let (Some(a), Some(b)) = (physical_core(pair[0].id), physical_core(pair[1].id)) {
            assert_ne!(a, b, "producer and consumer need different physical cores");
        }
        return pair;
    }
    let first = available[0];
    let second = available
        .iter()
        .copied()
        .find(|cpu| {
            cpu.id != first.id
                && match (physical_core(first.id), physical_core(cpu.id)) {
                    (Some(a), Some(b)) => a != b,
                    _ => true,
                }
        })
        .expect("comparison needs two physical cores");
    [first, second]
}

fn physical_core(cpu: usize) -> Option<(String, String)> {
    let base = PathBuf::from(format!("/sys/devices/system/cpu/cpu{cpu}/topology"));
    Some((
        fs::read_to_string(base.join("physical_package_id")).ok()?,
        fs::read_to_string(base.join("core_id")).ok()?,
    ))
}

fn rate(received: u64, sent: u64, start: Instant) -> f64 {
    assert_eq!(received, sent, "sent/received counts differ");
    received as f64 / start.elapsed().as_secs_f64()
}

fn yring_bench<T: Copy + Send + 'static>(
    config: Config,
    cap: usize,
    batch_size: usize,
    val: T,
) -> f64 {
    config.pin(1);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let (mut producer, mut consumer) = yring::spsc::<T>(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        config.pin(0);
        barrier2.wait();
        let mut sent = 0u64;
        let mut pending = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            if producer.push(val).is_ok() {
                sent += 1;
                pending += 1;
                if pending >= batch_size as u64 {
                    producer.flush();
                    pending = 0;
                }
            } else {
                producer.flush();
                thread::yield_now();
            }
        }
        producer.flush();
        sent
    });

    barrier.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < config.duration {
        if consumer.prefetch() > 0 {
            while consumer.pop().is_some() {
                received += 1;
            }
            consumer.release();
        } else {
            thread::yield_now();
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent = sender.join().unwrap();
    while consumer.prefetch() > 0 {
        while consumer.pop().is_some() {
            received += 1;
        }
        consumer.release();
    }
    rate(received, sent, start)
}

fn rtrb_per_item<T: Copy + Send + 'static>(
    config: Config,
    cap: usize,
    _batch_size: usize,
    val: T,
) -> f64 {
    config.pin(1);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let (mut producer, mut consumer) = rtrb::RingBuffer::<T>::new(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        config.pin(0);
        barrier2.wait();
        let mut sent = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            if producer.push(val).is_ok() {
                sent += 1;
            } else {
                thread::yield_now();
            }
        }
        sent
    });

    barrier.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < config.duration {
        match consumer.pop() {
            Ok(_) => received += 1,
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent = sender.join().unwrap();
    while consumer.pop().is_ok() {
        received += 1;
    }
    rate(received, sent, start)
}

fn rtrb_chunked<T: Copy + Send + 'static>(
    config: Config,
    cap: usize,
    batch_size: usize,
    val: T,
) -> f64 {
    config.pin(1);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let (mut producer, mut consumer) = rtrb::RingBuffer::<T>::new(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        config.pin(0);
        barrier2.wait();
        let mut sent = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            match producer.write_chunk_uninit(batch_size) {
                Ok(chunk) => {
                    chunk.fill_from_iter(std::iter::repeat_n(val, batch_size));
                    sent += batch_size as u64;
                }
                Err(_) => thread::yield_now(),
            }
        }
        sent
    });

    barrier.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < config.duration {
        let avail = consumer.slots();
        if avail > 0 {
            if let Ok(chunk) = consumer.read_chunk(avail) {
                received += chunk.len() as u64;
                chunk.commit_all();
            }
        } else {
            thread::yield_now();
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent = sender.join().unwrap();
    while consumer.slots() > 0 {
        let chunk = consumer.read_chunk(consumer.slots()).unwrap();
        received += chunk.len() as u64;
        chunk.commit_all();
    }
    rate(received, sent, start)
}

fn crossbeam_bounded<T: Copy + Send + 'static>(
    config: Config,
    cap: usize,
    _batch_size: usize,
    val: T,
) -> f64 {
    config.pin(1);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = crossbeam_channel::bounded::<T>(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        config.pin(0);
        barrier2.wait();
        let mut sent = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            if tx.try_send(val).is_ok() {
                sent += 1;
            } else {
                thread::yield_now();
            }
        }
        sent
    });

    barrier.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < config.duration {
        match rx.try_recv() {
            Ok(_) => received += 1,
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent = sender.join().unwrap();
    while rx.try_recv().is_ok() {
        received += 1;
    }
    rate(received, sent, start)
}

fn flume_bounded<T: Copy + Send + 'static>(
    config: Config,
    cap: usize,
    _batch_size: usize,
    val: T,
) -> f64 {
    config.pin(1);
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = barrier.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = flume::bounded::<T>(cap);

    let stop2 = stop.clone();
    let sender = thread::spawn(move || {
        config.pin(0);
        barrier2.wait();
        let mut sent = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            if tx.try_send(val).is_ok() {
                sent += 1;
            } else {
                thread::yield_now();
            }
        }
        sent
    });

    barrier.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < config.duration {
        match rx.try_recv() {
            Ok(_) => received += 1,
            Err(_) => thread::yield_now(),
        }
    }
    stop.store(true, Ordering::Relaxed);
    let sent = sender.join().unwrap();
    while rx.try_recv().is_ok() {
        received += 1;
    }
    rate(received, sent, start)
}

struct Run {
    config: Config,
    samples: usize,
    warmup: Duration,
    id: String,
    source_revision: String,
    results: BufWriter<std::fs::File>,
}

fn run_suite<T: Copy + Send + 'static>(run: &mut Run, payload: &str, val: T) {
    type Bench<T> = fn(Config, usize, usize, T) -> f64;
    let cases: [(&str, Bench<T>); 6] = [
        ("yring (batch=1)", yring_bench),
        ("yring (batch=64)", yring_bench),
        ("rtrb per-item", rtrb_per_item),
        ("rtrb chunked", rtrb_chunked),
        ("crossbeam bounded", crossbeam_bounded),
        ("flume bounded", flume_bounded),
    ];
    println!("--- {payload} ---");
    let batch_for = |index| if index == 1 || index == 3 { 64 } else { 1 };
    if !run.warmup.is_zero() {
        for (index, (_, bench)) in cases.iter().enumerate() {
            bench(
                Config {
                    duration: run.warmup,
                    ..run.config
                },
                1024,
                batch_for(index),
                val,
            );
        }
    }
    for sample in 0..run.samples {
        for offset in 0..cases.len() {
            let index = (offset + sample) % cases.len();
            let (channel, bench) = cases[index];
            let throughput = bench(run.config, 1024, batch_for(index), val);
            println!(
                "  {channel:<20} sample {}: {:>7.1}M items/s",
                sample + 1,
                throughput / 1_000_000.0
            );
            let row = serde_json::json!({
                "run_id": run.id,
                "source_revision": run.source_revision,
                "channel": channel,
                "payload": payload,
                "payload_bytes": std::mem::size_of::<T>(),
                "capacity": 1024,
                "batch_size": batch_for(index),
                "sample": sample,
                "samples": run.samples,
                "expected_rows": 24 * run.samples,
                "duration_secs": run.config.duration.as_secs_f64(),
                "warmup_secs": run.warmup.as_secs_f64(),
                "producer_cpu": run.config.cpus[0].id,
                "consumer_cpu": run.config.cpus[1].id,
                "throughput_items_per_sec": throughput,
            });
            serde_json::to_writer(&mut run.results, &row).expect("write benchmark row");
            writeln!(run.results).expect("write benchmark newline");
            run.results.flush().expect("flush benchmark results");
        }
    }
}

fn seconds(name: &str, default: f64) -> Duration {
    let seconds =
        std::env::var(name).map_or(default, |value| value.parse().expect("invalid duration"));
    assert!(seconds.is_finite() && seconds >= 0.0, "invalid duration");
    Duration::from_secs_f64(seconds)
}

fn main() {
    let duration = seconds("YRING_BENCH_SECS", 2.0);
    assert!(!duration.is_zero(), "measurement duration must be positive");
    let samples = std::env::var("YRING_BENCH_SAMPLES")
        .map_or(5, |value| value.parse().expect("invalid sample count"));
    assert!(samples > 0, "sample count must be positive");
    let cpus = cpu_pair();
    let path = std::env::var_os("YRING_BENCH_RESULTS").map_or_else(
        || {
            PathBuf::from(
                std::env::var_os("HOME").expect("set YRING_BENCH_RESULTS when HOME is absent"),
            )
            .join(".cache/yring/comparison.jsonl")
        },
        PathBuf::from,
    );
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).expect("create result directory");
    }
    let results = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open results"),
    );
    let source_revision = std::env::var("YRING_BENCH_SOURCE_REVISION").unwrap_or_else(|_| {
        let output = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("read source revision");
        assert!(
            output.status.success(),
            "set YRING_BENCH_SOURCE_REVISION outside a Git checkout"
        );
        String::from_utf8(output.stdout)
            .expect("Git revision encoding")
            .trim()
            .to_owned()
    });
    println!(
        "SPSC comparison ({samples} x {:.2}s, cap=1024, producer CPU {}, consumer CPU {}, results {})",
        duration.as_secs_f64(),
        cpus[0].id,
        cpus[1].id,
        path.display()
    );
    let mut run = Run {
        config: Config { duration, cpus },
        samples,
        warmup: seconds("YRING_BENCH_WARMUP_SECS", 0.25),
        id: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
            .to_string(),
        source_revision,
        results,
    };
    run_suite(&mut run, "u64", 0u64);
    run_suite(&mut run, "[u8; 32]", [0u8; 32]);
    run_suite(&mut run, "[u8; 64]", [0u8; 64]);
    run_suite(&mut run, "[u8; 128]", [0u8; 128]);
}
