//! Render benchmark JSONL as MPSC or MPMC SVG heatmaps.

use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use plotters::prelude::*;
use serde::Deserialize;

#[path = "chart/render.rs"]
mod render;
#[path = "chart/support.rs"]
mod support;

use render::draw_chart;
use support::{arg_value, run_is_complete};

const BACKGROUND_COLOR: RGBColor = RGBColor(0, 0, 0);
const GRID_COLOR: RGBColor = RGBColor(55, 65, 81);
const TEXT_COLOR: RGBColor = RGBColor(229, 231, 235);
const MUTED_TEXT_COLOR: RGBColor = RGBColor(156, 163, 175);

type ChartResult<T> = Result<T, ChartError>;

const SERIES: &[(&str, &str)] = &[
    ("fanring", "fanring"),
    ("fanring-mpmc", "fanring"),
    ("crossbeam-channel", "crossbeam-channel"),
    ("concurrent-queue", "concurrent-queue"),
    ("thingbuf", "thingbuf"),
    ("flume", "flume"),
    ("kanal", "kanal"),
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

    let requested_run = arg_value("--run");
    let run_id = if let Some(run_id) = requested_run {
        run_id
    } else {
        rows.iter()
            .rev()
            .map(|row| row.run_id.as_str())
            .find(|run_id| {
                run_is_complete(
                    &rows
                        .iter()
                        .filter(|row| row.run_id == **run_id)
                        .collect::<Vec<_>>(),
                )
            })
            .map(str::to_owned)
            .ok_or(ChartError::NoCompleteRun)?
    };
    let rows: Vec<Row> = rows
        .into_iter()
        .filter(|row| row.run_id == run_id)
        .collect();
    if rows.is_empty() {
        return Err(ChartError::RunNotFound(run_id));
    }
    if !run_is_complete(&rows.iter().collect::<Vec<_>>()) {
        return Err(ChartError::RunIncomplete(run_id));
    }

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ChartError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    draw_chart(&rows, &output)?;
    println!("wrote {}", output.display());
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
