//! Render benchmark JSONL as MPSC or MPMC SVG heatmaps.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use plotters::prelude::*;
use serde::Deserialize;

#[path = "chart/hardware.rs"]
mod hardware;
#[path = "chart/latency.rs"]
mod latency;
#[path = "chart/render.rs"]
mod render;
#[path = "chart/summary.rs"]
mod summary;
#[path = "chart/support.rs"]
mod support;

use render::draw_chart;
use summary::draw_summary_chart;
use support::{arg_value, has_arg, run_is_complete};

const BACKGROUND_COLOR: RGBColor = RGBColor(0, 0, 0);
const GRID_COLOR: RGBColor = RGBColor(55, 65, 81);
const TEXT_COLOR: RGBColor = RGBColor(229, 231, 235);
const MUTED_TEXT_COLOR: RGBColor = RGBColor(156, 163, 175);
const DEFAULT_CHART_MODE: &str = "try";

type ChartResult<T> = Result<T, ChartError>;

const SERIES: &[(&str, &str)] = &[
    ("fanring", "fanring"),
    ("fanring-mpmc", "fanring"),
    ("crossbeam-channel", "crossbeam-channel 0.5.16"),
    ("concurrent-queue", "concurrent-queue 2.5.0"),
    ("thingbuf", "thingbuf 0.1.6"),
    ("flume", "flume 0.12.0"),
    ("kanal", "kanal 0.1.1"),
];

#[derive(Clone, Debug, Deserialize)]
struct Row {
    run_id: String,
    #[serde(default = "render::unknown_cpu")]
    cpu: String,
    #[serde(default = "legacy_affinity")]
    affinity: String,
    #[serde(default = "render::default_mode")]
    mode: String,
    implementation: String,
    payload: String,
    payload_bytes: usize,
    producers: usize,
    #[serde(default)]
    consumers: Option<usize>,
    #[serde(alias = "total_capacity")]
    nominal_capacity: usize,
    #[serde(default)]
    capacity_model: Option<String>,
    #[serde(default = "legacy_profile")]
    throughput_profile: String,
    #[serde(default)]
    low_watermark: u64,
    #[serde(default)]
    high_watermark: u64,
    #[serde(alias = "msgs_per_sec")]
    items_per_sec: f64,
    #[serde(default)]
    sample: usize,
    #[serde(default = "render::one_sample")]
    samples: usize,
    #[serde(default)]
    expected_rows: usize,
}

impl Row {
    fn affinity_label(&self) -> &str {
        match self.affinity.split_once(':') {
            Some(("physical-first", _)) => "all workers pinned, physical cores first",
            Some(("logical-order", _)) => "all workers pinned in logical CPU order",
            _ => &self.affinity,
        }
    }

    fn throughput_profile_label(&self) -> String {
        if self.throughput_profile == "saturated" && self.nominal_capacity > 0 {
            let low = self.low_watermark.saturating_mul(100) / self.nominal_capacity as u64;
            let high = self.high_watermark.saturating_mul(100) / self.nominal_capacity as u64;
            format!("saturated occupancy {low}-{high}%")
        } else {
            match self.throughput_profile.as_str() {
                "uncontrolled" => "natural queue occupancy".to_string(),
                _ => self.throughput_profile.clone(),
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BenchmarkShape {
    cpu: String,
    affinity: String,
    mode: String,
    nominal_capacity: usize,
    throughput_profile: String,
    low_watermark: u64,
    high_watermark: u64,
    samples: usize,
    configurations: BTreeSet<(String, usize, usize, Option<usize>)>,
}

#[derive(Debug, Clone, Copy)]
struct Measurement {
    median: f64,
    relative_mad: f64,
}

#[derive(Debug)]
enum ChartError {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Draw(String),
    NoRows {
        path: PathBuf,
    },
    NoRenderableRows,
    NoCompleteRun,
    RunNotFound(String),
    RunIncomplete(String),
    InvalidTopology(String),
}

impl fmt::Display for ChartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Self::Draw(error) => write!(f, "draw chart: {error}"),
            Self::NoRows { path } => write!(f, "no benchmark rows in {}", path.display()),
            Self::NoRenderableRows => f.write_str("no benchmark rows to render"),
            Self::NoCompleteRun => f.write_str("no complete benchmark run found"),
            Self::RunNotFound(run_id) => write!(f, "benchmark run id not found: {run_id}"),
            Self::RunIncomplete(run_id) => write!(f, "benchmark run is incomplete: {run_id}"),
            Self::InvalidTopology(topology) => {
                write!(
                    f,
                    "invalid latency topology {topology:?}; expected mpsc or mpmc"
                )
            }
        }
    }
}

impl Error for ChartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

trait DrawResultExt<T> {
    fn chart(self) -> ChartResult<T>;
}

impl<T, E> DrawResultExt<T> for Result<T, E>
where
    E: fmt::Debug,
{
    fn chart(self) -> ChartResult<T> {
        self.map_err(|error| ChartError::Draw(format!("{error:?}")))
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> ChartResult<()> {
    if let Some(topology) = arg_value("--latency") {
        return run_latency(&topology);
    }
    if has_arg("--summary") {
        return run_summary();
    }

    run_detail()
}

fn run_latency(topology: &str) -> ChartResult<()> {
    let input = arg_value("--input").map_or_else(
        || PathBuf::from("target/fanring-bench/wake-latency.jsonl"),
        PathBuf::from,
    );
    let output = arg_value("--output").map_or_else(
        || PathBuf::from(format!("doc/charts/latency-{topology}.svg")),
        PathBuf::from,
    );

    prepare_output(&output)?;
    latency::draw_latency_chart(&input, &output, topology, arg_value("--run"))?;
    println!("wrote {}", output.display());
    Ok(())
}

fn run_detail() -> ChartResult<()> {
    let input = arg_value("--input").map_or_else(
        || PathBuf::from("target/fanring-bench/results.jsonl"),
        PathBuf::from,
    );
    let output = arg_value("--output").map_or_else(
        || PathBuf::from("doc/charts/throughput-mpsc.svg"),
        PathBuf::from,
    );

    let rows = read_rows(&input)?;
    if rows.is_empty() {
        return Err(ChartError::NoRows { path: input });
    }

    let rows = select_run_with_default_mode(rows, arg_value("--run"), Some(DEFAULT_CHART_MODE))?;
    prepare_output(&output)?;
    draw_chart(&rows, &output)?;
    println!("wrote {}", output.display());
    Ok(())
}

fn run_summary() -> ChartResult<()> {
    let mpsc_input = arg_value("--mpsc-input").map_or_else(
        || PathBuf::from("target/fanring-bench/results.jsonl"),
        PathBuf::from,
    );
    let mpmc_input = arg_value("--mpmc-input").map_or_else(
        || PathBuf::from("target/fanring-bench/mpmc.jsonl"),
        PathBuf::from,
    );
    let output = arg_value("--output").map_or_else(
        || PathBuf::from("doc/charts/throughput-summary.svg"),
        PathBuf::from,
    );

    let mpsc_rows = read_rows_for_run(
        &mpsc_input,
        arg_value("--mpsc-run"),
        Some(DEFAULT_CHART_MODE),
    )?;
    let mpmc_rows = read_rows_for_run(
        &mpmc_input,
        arg_value("--mpmc-run"),
        Some(DEFAULT_CHART_MODE),
    )?;
    prepare_output(&output)?;
    draw_summary_chart(&mpsc_rows, &mpmc_rows, &output)?;
    println!("wrote {}", output.display());
    Ok(())
}

fn read_rows_for_run(
    path: &Path,
    requested_run: Option<String>,
    default_mode: Option<&str>,
) -> ChartResult<Vec<Row>> {
    let rows = read_rows(path)?;
    if rows.is_empty() {
        return Err(ChartError::NoRows {
            path: path.to_path_buf(),
        });
    }
    select_run_with_default_mode(rows, requested_run, default_mode)
}

fn select_run_with_default_mode(
    rows: Vec<Row>,
    requested_run: Option<String>,
    default_mode: Option<&str>,
) -> ChartResult<Vec<Row>> {
    if requested_run.is_none() {
        return combine_latest_runs(&rows, default_mode).ok_or(ChartError::NoCompleteRun);
    }
    let run_id = requested_run.expect("checked above");
    let selected: Vec<Row> = rows
        .into_iter()
        .filter(|row| row.run_id == run_id)
        .collect();
    if selected.is_empty() {
        return Err(ChartError::RunNotFound(run_id));
    }
    if !run_is_complete(&selected.iter().collect::<Vec<_>>()) {
        return Err(ChartError::RunIncomplete(run_id));
    }
    Ok(selected)
}

fn combine_latest_runs(rows: &[Row], mode: Option<&str>) -> Option<Vec<Row>> {
    let mut seen = BTreeSet::new();
    let run_ids = rows
        .iter()
        .filter(|row| mode.is_none_or(|mode| row.mode == mode))
        .filter_map(|row| {
            seen.insert(row.run_id.as_str())
                .then_some(row.run_id.as_str())
        })
        .collect::<Vec<_>>();
    let complete_runs = run_ids
        .into_iter()
        .filter_map(|run_id| {
            let run_rows = rows
                .iter()
                .filter(|row| row.run_id == run_id && mode.is_none_or(|mode| row.mode == mode))
                .collect::<Vec<_>>();
            run_is_complete(&run_rows).then_some(run_rows)
        })
        .collect::<Vec<_>>();

    let mut reference = None;
    for run_rows in &complete_runs {
        for group in implementation_groups(run_rows) {
            let shape = benchmark_shape(&group)?;
            if reference.as_ref().is_none_or(|current: &BenchmarkShape| {
                shape.configurations.len() >= current.configurations.len()
            }) {
                reference = Some(shape);
            }
        }
    }
    let reference = reference?;
    let implementations = complete_runs
        .iter()
        .flat_map(|run_rows| implementation_groups(run_rows))
        .filter(|group| benchmark_shape(group).as_ref() == Some(&reference))
        .map(|group| group[0].implementation.as_str())
        .collect::<BTreeSet<_>>();

    let mut combined = Vec::new();
    for implementation in implementations {
        let group = complete_runs.iter().rev().find_map(|run_rows| {
            let group = run_rows
                .iter()
                .copied()
                .filter(|row| row.implementation == implementation)
                .collect::<Vec<_>>();
            (benchmark_shape(&group).as_ref() == Some(&reference)).then_some(group)
        })?;
        combined.extend(group.into_iter().cloned());
    }
    Some(combined)
}

fn implementation_groups<'a>(rows: &[&'a Row]) -> Vec<Vec<&'a Row>> {
    let mut groups = BTreeMap::<&str, Vec<&Row>>::new();
    for row in rows {
        groups
            .entry(row.implementation.as_str())
            .or_default()
            .push(*row);
    }
    groups.into_values().collect()
}

fn benchmark_shape(rows: &[&Row]) -> Option<BenchmarkShape> {
    let first = *rows.first()?;
    if rows.iter().any(|row| {
        row.cpu != first.cpu
            || row.affinity != first.affinity
            || row.mode != first.mode
            || row.nominal_capacity != first.nominal_capacity
            || row.throughput_profile != first.throughput_profile
            || row.low_watermark != first.low_watermark
            || row.high_watermark != first.high_watermark
            || row.samples != first.samples
    }) {
        return None;
    }
    Some(BenchmarkShape {
        cpu: first.cpu.clone(),
        affinity: first.affinity.clone(),
        mode: first.mode.clone(),
        nominal_capacity: first.nominal_capacity,
        throughput_profile: first.throughput_profile.clone(),
        low_watermark: first.low_watermark,
        high_watermark: first.high_watermark,
        samples: first.samples,
        configurations: rows
            .iter()
            .map(|row| {
                (
                    row.payload.clone(),
                    row.payload_bytes,
                    row.producers,
                    row.consumers,
                )
            })
            .collect(),
    })
}

fn legacy_profile() -> String {
    "uncontrolled".to_string()
}

fn legacy_affinity() -> String {
    "off".to_string()
}

fn prepare_output(output: &Path) -> ChartResult<()> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ChartError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

fn read_rows(path: &Path) -> ChartResult<Vec<Row>> {
    let file = File::open(path).map_err(|source| ChartError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut rows = Vec::new();
    for (i, line) in BufReader::new(file).lines().enumerate() {
        let line_number = i + 1;
        let line = line.map_err(|source| ChartError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(row) => rows.push(row),
            Err(error) => {
                let recovered = line
                    .match_indices("{\"run_id\"")
                    .filter_map(|(offset, _)| serde_json::from_str(&line[offset..]).ok())
                    .last();
                if let Some(row) = recovered {
                    eprintln!(
                        "warning: recovered appended benchmark row at {}:{line_number}",
                        path.display()
                    );
                    rows.push(row);
                } else {
                    eprintln!(
                        "warning: ignored incomplete benchmark row at {}:{line_number}: {error}",
                        path.display()
                    );
                }
            }
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::{Row, select_run_with_default_mode};

    #[test]
    fn chart_default_ignores_newer_blocking_run() {
        let rows = vec![
            row("try-run", "try", "fanring", 1, 1),
            row("blocking-run", "blocking", "fanring", 1, 1),
        ];

        let selected = select_run_with_default_mode(rows, None, Some("try")).unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].run_id, "try-run");
    }

    #[test]
    fn chart_default_combines_latest_full_run_per_implementation() {
        let rows = vec![
            row("old", "try", "fanring", 1, 4),
            row("old", "try", "fanring", 2, 4),
            row("old", "try", "crossbeam", 1, 4),
            row("old", "try", "crossbeam", 2, 4),
            row("new", "try", "fanring", 1, 2),
            row("new", "try", "fanring", 2, 2),
        ];

        let selected = select_run_with_default_mode(rows, None, Some("try")).unwrap();

        assert_eq!(selected.len(), 4);
        assert!(
            selected
                .iter()
                .any(|row| { row.implementation == "fanring" && row.run_id == "new" })
        );
        assert!(
            selected
                .iter()
                .any(|row| { row.implementation == "crossbeam" && row.run_id == "old" })
        );
    }

    #[test]
    fn chart_default_ignores_complete_targeted_run() {
        let rows = vec![
            row("full", "try", "fanring", 1, 4),
            row("full", "try", "fanring", 2, 4),
            row("full", "try", "crossbeam", 1, 4),
            row("full", "try", "crossbeam", 2, 4),
            row("targeted", "try", "fanring", 8, 1),
        ];

        let selected = select_run_with_default_mode(rows, None, Some("try")).unwrap();

        assert_eq!(selected.len(), 4);
        assert!(selected.iter().all(|row| row.run_id == "full"));
    }

    #[test]
    fn chart_default_does_not_mix_throughput_profiles() {
        let mut rows = vec![
            row("old", "try", "fanring", 1, 4),
            row("old", "try", "fanring", 2, 4),
            row("old", "try", "crossbeam", 1, 4),
            row("old", "try", "crossbeam", 2, 4),
        ];
        for row in &mut rows {
            row.throughput_profile = "uncontrolled".to_string();
            row.low_watermark = 0;
            row.high_watermark = 0;
        }
        rows.extend([
            row("new", "try", "fanring", 1, 2),
            row("new", "try", "fanring", 2, 2),
        ]);

        let selected = select_run_with_default_mode(rows, None, Some("try")).unwrap();

        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|row| row.run_id == "new"));
    }

    #[test]
    fn chart_default_does_not_mix_affinity_policies() {
        let mut rows = vec![
            row("old", "try", "fanring", 1, 2),
            row("old", "try", "fanring", 2, 2),
        ];
        for row in &mut rows {
            row.affinity = "off".to_string();
        }
        rows.extend([
            row("new", "try", "fanring", 1, 2),
            row("new", "try", "fanring", 2, 2),
        ]);

        let selected = select_run_with_default_mode(rows, None, Some("try")).unwrap();

        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|row| row.run_id == "new"));
    }

    fn row(
        run_id: &str,
        mode: &str,
        implementation: &str,
        producers: usize,
        expected_rows: usize,
    ) -> Row {
        Row {
            run_id: run_id.to_string(),
            cpu: "cpu".to_string(),
            affinity: "physical-first:0,1".to_string(),
            mode: mode.to_string(),
            implementation: implementation.to_string(),
            payload: "u64".to_string(),
            payload_bytes: 8,
            producers,
            consumers: None,
            nominal_capacity: 1,
            capacity_model: Some("per-ring-hwm".to_string()),
            throughput_profile: "saturated".to_string(),
            low_watermark: 1,
            high_watermark: 1,
            items_per_sec: 1.0,
            sample: 0,
            samples: 1,
            expected_rows,
        }
    }
}
