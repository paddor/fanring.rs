use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};
use serde::Deserialize;

const BACKGROUND_COLOR: RGBColor = RGBColor(0, 0, 0);
const GRID_COLOR: RGBColor = RGBColor(55, 65, 81);
const AXIS_COLOR: RGBColor = RGBColor(156, 163, 175);
const TEXT_COLOR: RGBColor = RGBColor(229, 231, 235);
const MUTED_TEXT_COLOR: RGBColor = RGBColor(156, 163, 175);

type ChartResult<T> = Result<T, ChartError>;

const SERIES: &[(&str, &str, RGBColor)] = &[
    ("fanring", "fanring", RGBColor(250, 204, 21)),
    ("fanring-mpmc", "fanring", RGBColor(250, 204, 21)),
    (
        "crossbeam-channel",
        "crossbeam-channel",
        RGBColor(96, 165, 250),
    ),
    (
        "concurrent-queue",
        "concurrent-queue",
        RGBColor(248, 113, 113),
    ),
    ("thingbuf", "thingbuf", RGBColor(251, 146, 60)),
    ("flume", "flume", RGBColor(167, 139, 250)),
    ("kanal", "kanal", RGBColor(52, 211, 153)),
];

#[derive(Debug, Deserialize)]
struct Row {
    run_id: String,
    #[serde(default = "unknown_cpu")]
    cpu: String,
    #[serde(default = "default_mode")]
    mode: String,
    implementation: String,
    payload: String,
    payload_bytes: usize,
    producers: usize,
    #[serde(default)]
    consumers: Option<usize>,
    total_capacity: usize,
    #[serde(alias = "msgs_per_sec")]
    items_per_sec: f64,
}

#[derive(Debug)]
enum ChartError {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Json {
        path: PathBuf,
        line: usize,
        source: serde_json::Error,
    },
    Draw(String),
    MissingRunId,
    NoRows {
        path: PathBuf,
    },
    NoRenderableRows,
    RunNotFound(String),
}

impl fmt::Display for ChartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Self::Json { path, line, source } => {
                write!(f, "{}:{line}: {source}", path.display())
            }
            Self::Draw(error) => write!(f, "draw chart: {error}"),
            Self::MissingRunId => f.write_str("missing benchmark run id"),
            Self::NoRows { path } => write!(f, "no benchmark rows in {}", path.display()),
            Self::NoRenderableRows => f.write_str("no benchmark rows to render"),
            Self::RunNotFound(run_id) => write!(f, "benchmark run id not found: {run_id}"),
        }
    }
}

impl Error for ChartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Json { source, .. } => Some(source),
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
    let input = arg_value("--input")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/fanring-bench/results.jsonl"));
    let output = arg_value("--output")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("doc/charts/mpsc.svg"));

    let rows = read_rows(&input)?;
    if rows.is_empty() {
        return Err(ChartError::NoRows { path: input });
    }

    let run_id = arg_value("--run")
        .or_else(|| rows.last().map(|row| row.run_id.clone()))
        .ok_or(ChartError::MissingRunId)?;
    let rows: Vec<Row> = rows
        .into_iter()
        .filter(|row| row.run_id == run_id)
        .collect();
    if rows.is_empty() {
        return Err(ChartError::RunNotFound(run_id));
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
        rows.push(
            serde_json::from_str(&line).map_err(|source| ChartError::Json {
                path: path.to_path_buf(),
                line: line_number,
                source,
            })?,
        );
    }
    Ok(rows)
}

fn draw_chart(rows: &[Row], output: &Path) -> ChartResult<()> {
    if rows.is_empty() {
        return Err(ChartError::NoRenderableRows);
    }

    let mut payloads: Vec<(String, usize)> = rows
        .iter()
        .map(|row| (row.payload.clone(), row.payload_bytes))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect();
    payloads.sort_by_key(|(_, bytes)| *bytes);
    let payloads: Vec<String> = payloads.into_iter().map(|(payload, _)| payload).collect();
    let is_mpmc = rows.iter().any(|row| row.consumers.is_some());
    let topologies: Vec<(usize, usize)> = rows
        .iter()
        .map(|row| (row.producers, row.consumers.unwrap_or(0)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let topology_labels = topologies
        .iter()
        .map(|(producers, consumers)| {
            if is_mpmc {
                format!("{producers}/{consumers}")
            } else {
                producers.to_string()
            }
        })
        .collect::<Vec<_>>();

    let width = if is_mpmc { 1440 } else { 1024 };
    let panel_h = 250;
    let header_h = 56;
    let legend_h = 74;
    let total_h = header_h + panel_h * payloads.len() as u32 + legend_h;

    let root = SVGBackend::new(output, (width, total_h)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR).chart()?;

    let title_style = ("sans-serif", 15)
        .into_font()
        .color(&TEXT_COLOR)
        .pos(Pos::new(HPos::Center, VPos::Center));
    let subtitle_style = ("sans-serif", 10)
        .into_font()
        .color(&MUTED_TEXT_COLOR)
        .pos(Pos::new(HPos::Center, VPos::Center));

    root.draw(&Text::new(
        if is_mpmc {
            "fanring MPMC comparison"
        } else {
            "fanring MPSC comparison"
        },
        (width as i32 / 2, 17),
        title_style,
    ))
    .chart()?;
    root.draw(&Text::new(
        chart_subtitle(rows),
        (width as i32 / 2, 35),
        subtitle_style,
    ))
    .chart()?;

    let (_, body) = root.split_vertically(header_h);
    let (chart_area, legend_area) = body.split_vertically(panel_h * payloads.len() as u32);
    let panels = chart_area.split_evenly((payloads.len(), 1));

    for (area, payload) in panels.iter().zip(payloads.iter()) {
        let payload_rows: Vec<&Row> = rows.iter().filter(|row| &row.payload == payload).collect();
        let y_raw = payload_rows
            .iter()
            .map(|row| row.items_per_sec / 1_000_000.0)
            .fold(0.0_f64, f64::max)
            .max(1.0);
        let (y_max, y_ticks) = nice_axis(y_raw, 5);
        let x_max = topologies.len().max(1) as f64;
        let present_series: Vec<(&str, &str, RGBColor)> = SERIES
            .iter()
            .copied()
            .filter(|(key, _, _)| payload_rows.iter().any(|row| row.implementation == *key))
            .collect();

        let mut chart = ChartBuilder::on(area)
            .margin_top(22)
            .margin_bottom(36)
            .margin_left(76)
            .margin_right(22)
            .caption(
                payload_title(&payload_rows),
                ("sans-serif", 12).into_font().color(&TEXT_COLOR),
            )
            .build_cartesian_2d(0.0..x_max, 0.0..y_max)
            .chart()?;

        chart
            .configure_mesh()
            .x_labels(topologies.len() + 1)
            .y_labels(y_ticks + 1)
            .light_line_style(TRANSPARENT)
            .bold_line_style(GRID_COLOR)
            .axis_style(AXIS_COLOR)
            .label_style(("sans-serif", 0).into_font().color(&TEXT_COLOR))
            .x_label_formatter(&|_| String::new())
            .y_label_formatter(&|y| fmt_mmsgs(*y))
            .draw()
            .chart()?;

        draw_axis_labels(
            area,
            &topology_labels,
            if is_mpmc {
                "producer / consumer threads"
            } else {
                "producer threads"
            },
            y_max,
            y_ticks,
        )?;

        let group_pad = 0.12;
        let inner_gap = 0.012;
        let slot_width = (1.0 - group_pad * 2.0) / present_series.len().max(1) as f64;

        for (series_index, (key, _, color)) in present_series.iter().enumerate() {
            let by_topology: BTreeMap<(usize, usize), f64> = payload_rows
                .iter()
                .filter(|row| row.implementation == *key)
                .map(|row| {
                    (
                        (row.producers, row.consumers.unwrap_or(0)),
                        row.items_per_sec / 1_000_000.0,
                    )
                })
                .collect();

            let bars = topologies
                .iter()
                .enumerate()
                .filter_map(|(group_index, topology)| {
                    by_topology.get(topology).map(|value| {
                        let x0 = group_index as f64
                            + group_pad
                            + series_index as f64 * slot_width
                            + inner_gap;
                        let x1 =
                            group_index as f64 + group_pad + (series_index + 1) as f64 * slot_width
                                - inner_gap;
                        Rectangle::new([(x0, 0.0), (x1, *value)], color.filled())
                    })
                });

            chart.draw_series(bars).chart()?;
        }
    }

    draw_legend(&legend_area, rows)?;
    Ok(())
}

fn draw_axis_labels(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    labels: &[String],
    x_axis_title: &str,
    y_max: f64,
    y_ticks: usize,
) -> ChartResult<()> {
    let tick_style = ("sans-serif", 11)
        .into_font()
        .color(&TEXT_COLOR)
        .pos(Pos::new(HPos::Center, VPos::Center));
    let right_tick_style = ("sans-serif", 11)
        .into_font()
        .color(&TEXT_COLOR)
        .pos(Pos::new(HPos::Right, VPos::Center));
    let axis_style = ("sans-serif", 11)
        .into_font()
        .color(&MUTED_TEXT_COLOR)
        .pos(Pos::new(HPos::Center, VPos::Center));
    let y_axis_style = ("sans-serif", 11)
        .into_font()
        .color(&MUTED_TEXT_COLOR)
        .pos(Pos::new(HPos::Left, VPos::Center));

    let width = area.dim_in_pixel().0 as i32;
    let plot_left = 76;
    let plot_right = width - 23;
    let plot_top = 42;
    let plot_bottom = 213;
    let plot_w = plot_right - plot_left;
    let plot_h = plot_bottom - plot_top;
    let x_count = labels.len().max(1);

    for (i, label) in labels.iter().enumerate() {
        let x = plot_left + plot_w * (2 * i + 1) as i32 / (2 * x_count) as i32;
        area.draw(&Text::new(
            label.as_str(),
            (x, plot_bottom + 17),
            tick_style.clone(),
        ))
        .chart()?;
    }
    area.draw(&Text::new(
        x_axis_title,
        ((plot_left + plot_right) / 2, plot_bottom + 34),
        axis_style,
    ))
    .chart()?;

    for tick in 0..=y_ticks {
        let y = plot_bottom - plot_h * tick as i32 / y_ticks.max(1) as i32;
        let value = y_max * tick as f64 / y_ticks.max(1) as f64;
        area.draw(&Text::new(
            fmt_mmsgs(value),
            (plot_left - 9, y),
            right_tick_style.clone(),
        ))
        .chart()?;
    }
    area.draw(&Text::new("M items/s", (12, plot_top - 14), y_axis_style))
        .chart()?;
    Ok(())
}

fn payload_title(rows: &[&Row]) -> String {
    if let Some(row) = rows.first() {
        format!("{} ({} B)", row.payload, row.payload_bytes)
    } else {
        "payload".to_string()
    }
}

fn chart_subtitle(rows: &[Row]) -> String {
    let Some(row) = rows.first() else {
        return "unknown CPU".to_string();
    };

    format!(
        "{}; {} operations; capacity {} items",
        simplify_cpu_name(&row.cpu),
        row.mode,
        row.total_capacity
    )
}

fn simplify_cpu_name(cpu: &str) -> String {
    cpu.replace("(R)", "")
        .replace("(TM)", "")
        .replace("CPU ", "")
        .replace(" @ 3.20GHz", "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn unknown_cpu() -> String {
    "unknown CPU".to_string()
}

fn default_mode() -> String {
    "try".to_string()
}

fn draw_legend(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[Row],
) -> ChartResult<()> {
    let present: Vec<(&str, &str, RGBColor)> = SERIES
        .iter()
        .copied()
        .filter(|(key, _, _)| rows.iter().any(|row| row.implementation == *key))
        .collect();

    let label_style = ("sans-serif", 11).into_font().color(&TEXT_COLOR);
    let dim_style = ("sans-serif", 10).into_font().color(&MUTED_TEXT_COLOR);

    area.draw_text("legend", &dim_style, (46, 2)).chart()?;

    for (i, (_, label, color)) in present.iter().enumerate() {
        let col = i % 3;
        let row = i / 3;
        let x = 46 + col as i32 * 300;
        let y = 22 + row as i32 * 21;
        area.draw(&Rectangle::new(
            [(x, y + 2), (x + 18, y + 14)],
            color.filled(),
        ))
        .chart()?;
        area.draw_text(label, &label_style, (x + 26, y)).chart()?;
    }
    Ok(())
}

fn fmt_mmsgs(v: f64) -> String {
    if (v - v.round()).abs() < 0.05 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
    }
}

fn nice_step(max_val: f64, target_lines: usize) -> f64 {
    if max_val <= 0.0 {
        return 1.0;
    }

    let raw = max_val / target_lines as f64;
    let mag = 10.0_f64.powf(raw.log10().floor());
    for step in [1.0, 2.0, 2.5, 5.0, 10.0].map(|s| s * mag) {
        if max_val / step <= target_lines as f64 + 1.0 {
            return step;
        }
    }
    mag * 10.0
}

fn nice_axis(max_val: f64, target_lines: usize) -> (f64, usize) {
    let step = nice_step(max_val, target_lines);
    let ticks = (max_val / step).ceil().max(1.0) as usize;
    (step * ticks as f64, ticks)
}

fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
    }
    None
}
