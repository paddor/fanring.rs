//! Render benchmark JSONL as MPSC or MPMC SVG heatmaps.

use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use plotters::prelude::*;
use serde::Deserialize;

#[path = "chart/hardware.rs"]
mod hardware;
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

#[derive(Debug, Deserialize)]
struct Row {
    run_id: String,
    #[serde(default = "render::unknown_cpu")]
    cpu: String,
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
    #[serde(alias = "msgs_per_sec")]
    items_per_sec: f64,
    #[serde(default)]
    sample: usize,
    #[serde(default = "render::one_sample")]
    samples: usize,
    #[serde(default)]
    expected_rows: usize,
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
    if has_arg("--summary") {
        return run_summary();
    }

    run_detail()
}

fn run_detail() -> ChartResult<()> {
    let input = arg_value("--input").map_or_else(
        || PathBuf::from("target/fanring-bench/results.jsonl"),
        PathBuf::from,
    );
    let output =
        arg_value("--output").map_or_else(|| PathBuf::from("doc/charts/mpsc.svg"), PathBuf::from);

    let rows = read_rows(&input)?;
    if rows.is_empty() {
        return Err(ChartError::NoRows { path: input });
    }

    let rows = select_run(rows, arg_value("--run"))?;
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
    let output = arg_value("--output")
        .map_or_else(|| PathBuf::from("doc/charts/summary.svg"), PathBuf::from);

    let mpsc_rows = read_rows_for_run(&mpsc_input, arg_value("--mpsc-run"), Some("try"))?;
    let mpmc_rows = read_rows_for_run(&mpmc_input, arg_value("--mpmc-run"), Some("try"))?;
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

fn select_run(rows: Vec<Row>, requested_run: Option<String>) -> ChartResult<Vec<Row>> {
    select_run_with_default_mode(rows, requested_run, None)
}

fn select_run_with_default_mode(
    rows: Vec<Row>,
    requested_run: Option<String>,
    default_mode: Option<&str>,
) -> ChartResult<Vec<Row>> {
    let run_id = if let Some(run_id) = requested_run {
        run_id
    } else {
        rows.iter()
            .rev()
            .map(|row| row.run_id.as_str())
            .find(|run_id| {
                let run_rows = rows
                    .iter()
                    .filter(|row| row.run_id == **run_id)
                    .collect::<Vec<_>>();
                run_is_complete(&run_rows)
                    && default_mode.is_none_or(|mode| run_rows.iter().all(|row| row.mode == mode))
            })
            .map(str::to_owned)
            .ok_or(ChartError::NoCompleteRun)?
    };
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
    fn summary_default_ignores_newer_blocking_run() {
        let rows = vec![row("try-run", "try"), row("blocking-run", "blocking")];

        let selected = select_run_with_default_mode(rows, None, Some("try")).unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].run_id, "try-run");
    }

    fn row(run_id: &str, mode: &str) -> Row {
        Row {
            run_id: run_id.to_string(),
            cpu: "cpu".to_string(),
            mode: mode.to_string(),
            implementation: "fanring".to_string(),
            payload: "u64".to_string(),
            payload_bytes: 8,
            producers: 1,
            consumers: None,
            nominal_capacity: 1,
            capacity_model: Some("per-ring-hwm".to_string()),
            items_per_sec: 1.0,
            sample: 0,
            samples: 1,
            expected_rows: 1,
        }
    }
}
