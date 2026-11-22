use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const DURATION: Duration = Duration::from_secs(2);

/// A fixed ring of `cap` slots, or with `max`, a ring that starts at `cap`
/// and may grow to `max`.
#[derive(Clone, Copy)]
struct Shape {
    cap: usize,
    max: Option<usize>,
}

fn bench<T: Copy + Send + 'static>(shape: Shape, batch_size: usize, val: T) {
    let size = std::mem::size_of::<T>();
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));

    let (mut producer, mut consumer) = match shape.max {
        Some(max) => yring::spsc_growing::<T>(shape.cap, max),
        None => yring::spsc::<T>(shape.cap),
    };
    let cap = shape.cap;
    let grows = shape
        .max
        .map_or(String::new(), |max| format!(" grows to {max}"));

    let stop2 = stop.clone();
    let sent2 = sent.clone();
    let sender = thread::spawn(move || {
        let mut count = 0u64;
        let mut pending = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            if producer.push(val).is_ok() {
                count += 1;
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
        sent2.store(count, Ordering::Relaxed);
    });

    let start = Instant::now();
    let mut received = 0u64;
    while start.elapsed() < DURATION {
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
    sender.join().unwrap();

    let elapsed = start.elapsed();
    let items_per_sec = received as f64 / elapsed.as_secs_f64();
    let mb_per_sec = received as f64 * size as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    println!(
        "  cap={cap:<6} batch={batch_size:<4} {:>7.1}M items/s  {:>5.0} MB/s{grows}",
        items_per_sec / 1_000_000.0,
        mb_per_sec,
    );
}

fn section<T: Copy + Send + 'static>(title: &str, val: T, shapes: &[Shape]) {
    println!("--- {title} ---");
    for &shape in shapes {
        for batch in [1, 16, 64, 256] {
            bench(shape, batch, val);
        }
        println!();
    }
}

fn main() {
    println!("yring throughput (2s per config)\n");

    let fixed = [
        Shape {
            cap: 1024,
            max: None,
        },
        Shape {
            cap: 4096,
            max: None,
        },
    ];
    section("u64 (8 bytes)", 0u64, &fixed);
    section("[u8; 64] (64 bytes)", [0u8; 64], &fixed);
    section("[u8; 128] (128 bytes)", [0u8; 128], &fixed);

    // Starts at 1024 slots and may grow to 1M: a consumer that keeps up
    // never leaves the first segment and checks for a link on each prefetch.
    let growing = [Shape {
        cap: 1024,
        max: Some(1 << 20),
    }];
    section("u64 (8 bytes), growing", 0u64, &growing);
    section("[u8; 64] (64 bytes), growing", [0u8; 64], &growing);
}
