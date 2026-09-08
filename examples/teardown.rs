//! Sequential payload-lifetime probe for the versions pinned in Cargo.lock.
//! This checks destruction timing, not concurrent teardown progress guarantees.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Debug, Default)]
struct Payload(Option<Arc<AtomicUsize>>);

impl Drop for Payload {
    fn drop(&mut self) {
        if let Some(drops) = &self.0 {
            drops.fetch_add(1, Ordering::Relaxed);
        }
    }
}

const QUEUED: usize = 4;

fn probe<S, R>(name: &str, build: impl FnOnce(Payload) -> (S, R)) {
    let drops = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = build(Payload(Some(drops.clone())));
    assert_eq!(
        drops.load(Ordering::Relaxed),
        0,
        "{name}: unexpected early drop"
    );
    drop(receiver);
    let at_receiver_drop = drops.load(Ordering::Relaxed);
    drop(sender);
    let at_sender_drop = drops.load(Ordering::Relaxed);
    assert_eq!(at_sender_drop, QUEUED, "{name}: exactly-once destruction");
    println!("| {name} | {at_receiver_drop}/{QUEUED} | {at_sender_drop}/{QUEUED} |");
}

macro_rules! channel_probe {
    ($name:expr, $channel:expr) => {
        probe($name, |payload| {
            #[allow(unused_mut)]
            let (mut tx, rx) = $channel;
            for _ in 1..QUEUED {
                tx.try_send(payload.clone()).unwrap();
            }
            tx.try_send(payload).unwrap();
            (tx, rx)
        });
    };
}

fn queue_probe(name: &str, queue: concurrent_queue::ConcurrentQueue<Payload>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = Arc::new(queue);
    for _ in 0..QUEUED {
        queue.push(Payload(Some(drops.clone()))).unwrap();
    }
    let remaining_owner = queue.clone();
    queue.close();
    drop(queue);
    let at_close = drops.load(Ordering::Relaxed);
    drop(remaining_owner);
    let at_last_owner_drop = drops.load(Ordering::Relaxed);
    assert_eq!(
        at_last_owner_drop, QUEUED,
        "{name}: exactly-once destruction"
    );
    println!("| {name} | {at_close}/{QUEUED} | {at_last_owner_drop}/{QUEUED} |");
}

fn main() {
    println!("| Channel | Destroyed after last receiver drop, sender alive | After sender drop |");
    println!("|---|---:|---:|");
    channel_probe!("fanring MPSC Deferred", fanring::mpsc::channel(QUEUED));
    channel_probe!("fanring MPMC Deferred", fanring::mpmc::channel(QUEUED));
    channel_probe!(
        "fanring MPSC Coordinated",
        fanring::mpsc::channel_with_policy::<_, fanring::teardown::Coordinated>(QUEUED)
    );
    channel_probe!(
        "fanring MPMC Coordinated",
        fanring::mpmc::channel_with_policy::<_, fanring::teardown::Coordinated>(QUEUED)
    );
    channel_probe!(
        "crossbeam-channel bounded",
        crossbeam_channel::bounded(QUEUED)
    );
    channel_probe!(
        "crossbeam-channel unbounded",
        crossbeam_channel::unbounded()
    );
    channel_probe!(
        "crossfire MPSC bounded",
        crossfire::mpsc::bounded_blocking(QUEUED)
    );
    channel_probe!(
        "crossfire MPMC bounded",
        crossfire::mpmc::bounded_blocking(QUEUED)
    );
    channel_probe!(
        "crossfire MPSC unbounded",
        crossfire::mpsc::unbounded_blocking()
    );
    channel_probe!(
        "crossfire MPMC unbounded",
        crossfire::mpmc::unbounded_blocking()
    );
    channel_probe!("flume bounded", flume::bounded(QUEUED));
    channel_probe!("flume unbounded", flume::unbounded());
    channel_probe!("kanal bounded", kanal::bounded(QUEUED));
    channel_probe!("kanal unbounded", kanal::unbounded());
    channel_probe!(
        "thingbuf blocking",
        thingbuf::mpsc::blocking::channel(QUEUED)
    );
    probe("yring SPSC", |payload| {
        let (mut tx, rx) = yring::spsc(QUEUED);
        for _ in 1..QUEUED {
            tx.push(payload.clone()).unwrap();
        }
        tx.push(payload).unwrap();
        tx.flush();
        (tx, rx)
    });

    println!();
    println!(
        "| Queue | Destroyed after close and one Arc drop, another alive | After last Arc drop |"
    );
    println!("|---|---:|---:|");
    queue_probe(
        "concurrent-queue bounded",
        concurrent_queue::ConcurrentQueue::bounded(QUEUED),
    );
    queue_probe(
        "concurrent-queue unbounded",
        concurrent_queue::ConcurrentQueue::unbounded(),
    );
}
