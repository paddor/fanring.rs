use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const DURATION: Duration = Duration::from_secs(2);

fn bench<T: Copy + Send + 'static>(cap: usize, batch_size: usize, val: T) {
    let size = std::mem::size_of::<T>();
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));

    let (mut producer, mut consumer) = yring::spsc::<T>(cap);

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
        "  cap={cap:<6} batch={batch_size:<4} {:>7.1}M items/s  {:>5.0} MB/s",
        items_per_sec / 1_000_000.0,
        mb_per_sec,
    );
}

fn main() {
    println!("yring throughput (2s per config)\n");

    println!("--- u64 (8 bytes) ---");
    for cap in [1024, 4096] {
        for batch in [1, 16, 64, 256] {
            bench(cap, batch, 0u64);
        }
        println!();
    }

    println!("--- [u8; 64] (64 bytes) ---");
    for cap in [1024, 4096] {
        for batch in [1, 16, 64, 256] {
            bench(cap, batch, [0u8; 64]);
        }
        println!();
    }

    println!("--- [u8; 128] (128 bytes) ---");
    for cap in [1024, 4096] {
        for batch in [1, 16, 64, 256] {
            bench(cap, batch, [0u8; 128]);
        }
        println!();
    }
}
