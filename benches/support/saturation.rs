//! Saturated-profile control: producers report full rings, and the consumer
//! waits for every producer before each measured burst.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

const LOW_WATERMARK_PERCENT: usize = 50;
const HIGH_WATERMARK_PERCENT: usize = 100;
const SPINS_BEFORE_YIELD: usize = 64;

#[derive(Clone)]
pub(crate) struct Saturation {
    generation: Arc<AtomicU64>,
    full_generation: Counters,
    low_watermark: u64,
    high_watermark: u64,
}

#[derive(Clone)]
struct Counters(Arc<[PaddedCounter]>);

#[repr(align(64))]
struct PaddedCounter(AtomicU64);

pub(crate) struct FullSignal {
    generation: Arc<AtomicU64>,
    full_generation: Counters,
    index: usize,
    acknowledged: u64,
}

impl Saturation {
    pub(crate) fn new(producers: usize, total_capacity: usize) -> Self {
        assert!(producers > 0, "producer count must be > 0");
        assert!(total_capacity > 0, "total capacity must be > 0");

        let low_watermark = percentage(total_capacity, LOW_WATERMARK_PERCENT).max(1);
        let high_watermark = percentage(total_capacity, HIGH_WATERMARK_PERCENT)
            .max(low_watermark)
            .min(total_capacity as u64);

        Self {
            generation: Arc::new(AtomicU64::new(1)),
            full_generation: Counters::new(producers),
            low_watermark,
            high_watermark,
        }
    }

    pub(crate) fn producer(&self, index: usize) -> FullSignal {
        assert!(
            index < self.full_generation.0.len(),
            "producer index out of bounds"
        );
        FullSignal {
            generation: self.generation.clone(),
            full_generation: self.full_generation.clone(),
            index,
            acknowledged: 0,
        }
    }

    pub(crate) fn advance(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn wait_until_full(&self, stop: Option<&AtomicBool>) -> bool {
        let generation = self.generation.load(Ordering::Relaxed);
        let mut spins = 0;
        while !self.full_generation.all_equal(generation) {
            if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
                return false;
            }
            if spins < SPINS_BEFORE_YIELD {
                std::hint::spin_loop();
                spins += 1;
            } else {
                thread::yield_now();
                spins = 0;
            }
        }
        true
    }

    pub(crate) const fn low_watermark(&self) -> u64 {
        self.low_watermark
    }

    pub(crate) const fn high_watermark(&self) -> u64 {
        self.high_watermark
    }
}

impl Counters {
    fn new(count: usize) -> Self {
        Self(
            (0..count)
                .map(|_| PaddedCounter(AtomicU64::new(0)))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    fn all_equal(&self, expected: u64) -> bool {
        self.0
            .iter()
            .all(|counter| counter.0.load(Ordering::Acquire) == expected)
    }
}

impl FullSignal {
    #[inline]
    pub(crate) fn mark_full(&mut self) {
        let generation = self.generation.load(Ordering::Relaxed);
        if self.acknowledged == generation {
            return;
        }
        self.full_generation.0[self.index]
            .0
            .store(generation, Ordering::Release);
        self.acknowledged = generation;
    }
}

fn percentage(value: usize, percent: usize) -> u64 {
    u64::try_from(value / 100 * percent + value % 100 * percent / 100)
        .expect("benchmark capacity fits u64")
}
