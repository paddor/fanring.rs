use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Sampling {
    pub(crate) samples: usize,
    pub(crate) warmup: Duration,
}

impl Sampling {
    pub(crate) fn from_env() -> Self {
        Self {
            samples: env_usize("FANRING_BENCH_SAMPLES", 5),
            warmup: Duration::from_secs_f64(env_f64("FANRING_BENCH_WARMUP_SECS", 0.25)),
        }
    }
}

pub(crate) fn append_jsonl(path: &Path) -> BufWriter<File> {
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
