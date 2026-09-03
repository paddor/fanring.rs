use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use serde::Serialize;

const LOW_WATERMARK_PERCENT: usize = 50;
const HIGH_WATERMARK_PERCENT: usize = 100;
const SPINS_BEFORE_YIELD: usize = 64;

pub(crate) struct JsonlResults {
    root: PathBuf,
    file_name: &'static str,
    writers: BTreeMap<String, BufWriter<File>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Sampling {
    pub(crate) samples: usize,
    pub(crate) warmup: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct Affinity {
    cores: Arc<[core_affinity::CoreId]>,
    description: Arc<str>,
}

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

impl JsonlResults {
    pub(crate) fn new(file_name: &'static str) -> Self {
        Self {
            root: results_root(),
            file_name,
            writers: BTreeMap::new(),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn write<T: Serialize>(&mut self, implementation: &str, row: &T) {
        let directory = implementation_directory(implementation);
        let path = result_path(&self.root, implementation, self.file_name);
        let writer = self
            .writers
            .entry(directory.to_string())
            .or_insert_with(|| {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).expect("create benchmark result directory");
                }
                append_jsonl(&path)
            });
        serde_json::to_writer(&mut *writer, row).expect("write benchmark row");
        writeln!(writer).expect("write benchmark newline");
    }

    pub(crate) fn flush(&mut self) {
        for writer in self.writers.values_mut() {
            writer.flush().expect("flush benchmark JSONL");
        }
    }
}

fn implementation_directory(implementation: &str) -> &str {
    implementation
        .strip_suffix("-mpmc")
        .unwrap_or(implementation)
}

fn result_path(root: &Path, implementation: &str, file_name: &str) -> PathBuf {
    root.join(implementation_directory(implementation))
        .join(file_name)
}

fn results_root() -> PathBuf {
    std::env::var_os("FANRING_BENCH_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(
                std::env::var_os("HOME")
                    .expect("HOME must be set unless FANRING_BENCH_CACHE_DIR is set"),
            )
            .join(".cache/fanring")
        })
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

impl Affinity {
    pub(crate) fn from_env() -> Self {
        match std::env::var("FANRING_BENCH_AFFINITY").as_deref() {
            Ok("off") => Self {
                cores: Arc::from([]),
                description: Arc::from("off"),
            },
            Ok("auto") | Err(_) => {
                let cores = core_affinity::get_core_ids().unwrap_or_else(|| {
                    panic!(
                        "CPU affinity is unavailable; set FANRING_BENCH_AFFINITY=off to continue"
                    )
                });
                assert!(!cores.is_empty(), "CPU affinity returned no available CPUs");
                let (cores, topology) = ordered_cores(cores);
                let ids = cores
                    .iter()
                    .map(|core| core.id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                Self {
                    cores: cores.into(),
                    description: format!("{topology}:{ids}").into(),
                }
            }
            Ok(value) => {
                panic!("invalid FANRING_BENCH_AFFINITY={value:?}; expected auto or off")
            }
        }
    }

    pub(crate) fn pin(&self, index: usize) {
        if self.cores.is_empty() {
            return;
        }
        let core = self.cores[index % self.cores.len()];
        assert!(
            core_affinity::set_for_current(core),
            "failed to pin benchmark thread to CPU {}",
            core.id
        );
    }

    pub(crate) fn description(&self) -> &str {
        &self.description
    }
}

fn ordered_cores(
    mut cores: Vec<core_affinity::CoreId>,
) -> (Vec<core_affinity::CoreId>, &'static str) {
    cores.sort_unstable_by_key(|core| core.id);
    let mut groups = BTreeMap::<(u32, u32), Vec<core_affinity::CoreId>>::new();
    for core in &cores {
        let Some(package) = topology_id(core.id, "physical_package_id") else {
            return (cores, "logical-order");
        };
        let Some(core_id) = topology_id(core.id, "core_id") else {
            return (cores, "logical-order");
        };
        groups.entry((package, core_id)).or_default().push(*core);
    }
    let groups = groups.into_values().collect::<Vec<_>>();
    (sibling_major(&groups), "physical-first")
}

fn sibling_major(groups: &[Vec<core_affinity::CoreId>]) -> Vec<core_affinity::CoreId> {
    let sibling_count = groups.iter().map(Vec::len).max().unwrap_or(0);
    (0..sibling_count)
        .flat_map(|sibling| {
            groups
                .iter()
                .filter_map(move |group| group.get(sibling).copied())
        })
        .collect()
}

fn topology_id(cpu: usize, name: &str) -> Option<u32> {
    std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{cpu}/topology/{name}"))
        .ok()?
        .trim()
        .parse()
        .ok()
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

impl Sampling {
    pub(crate) fn from_env() -> Self {
        Self {
            samples: env_usize("FANRING_BENCH_SAMPLES", 5),
            warmup: Duration::from_secs_f64(env_f64("FANRING_BENCH_WARMUP_SECS", 0.25)),
        }
    }
}

fn append_jsonl(path: &Path) -> BufWriter<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .expect("open benchmark JSONL");
    let length = file.metadata().expect("read benchmark metadata").len();
    if length != 0 {
        file.seek(SeekFrom::End(-1))
            .expect("seek benchmark JSONL tail");
        let mut last = [0];
        file.read_exact(&mut last)
            .expect("read benchmark JSONL tail");
        if last[0] != b'\n' {
            file.write_all(b"\n")
                .expect("terminate partial benchmark row");
        }
    }
    BufWriter::new(file)
}

pub(crate) fn median_and_relative_mad(values: impl IntoIterator<Item = f64>) -> (f64, f64) {
    let mut values = values.into_iter().collect::<Vec<_>>();
    assert!(!values.is_empty(), "benchmark sample set is empty");
    values.sort_by(f64::total_cmp);
    let median = median_sorted(&values);
    let mut deviations = values
        .into_iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let mad = median_sorted(&deviations);
    let relative_mad = if median == 0.0 {
        0.0
    } else {
        mad / median * 100.0
    };
    (median, relative_mad)
}

fn median_sorted(values: &[f64]) -> f64 {
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value != 0)
            .unwrap_or_else(|| panic!("{name} must be a positive integer")),
        Err(_) => default,
    }
}

fn env_f64(name: &str, default: f64) -> f64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or_else(|| panic!("{name} must be a nonnegative finite number")),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{result_path, sibling_major};
    use core_affinity::CoreId;

    fn six_smt_cores() -> Vec<Vec<CoreId>> {
        (0..6)
            .map(|id| vec![CoreId { id }, CoreId { id: id + 6 }])
            .collect()
    }

    #[test]
    fn sibling_major_uses_each_physical_core_before_smt_siblings() {
        let ids = sibling_major(&six_smt_cores())
            .into_iter()
            .map(|core| core.id)
            .collect::<Vec<_>>();

        assert_eq!(ids, (0..12).collect::<Vec<_>>());
    }

    #[test]
    fn result_path_uses_topology_file_in_implementation_directory() {
        assert_eq!(
            result_path(
                Path::new("/cache/fanring"),
                "crossfire-mpmc",
                "throughput-mpmc.jsonl"
            ),
            Path::new("/cache/fanring/crossfire/throughput-mpmc.jsonl")
        );
    }
}
