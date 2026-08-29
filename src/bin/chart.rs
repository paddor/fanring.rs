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
    let width = if is_mpmc { 1440 } else { 1024 };
    let section_h = 236;
    let header_h = 58;
    let total_h = header_h + section_h * payloads.len() as u32;

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

    for (index, payload) in payloads.iter().enumerate() {
        let payload_rows: Vec<&Row> = rows.iter().filter(|row| &row.payload == payload).collect();
        let raw_max = payload_rows
            .iter()
            .map(|row| row.items_per_sec / 1_000_000.0)
            .fold(0.0_f64, f64::max)
            .max(1.0);
        let scale_max = nice_axis(raw_max, 5).0;
        let section_y = header_h as i32 + index as i32 * section_h as i32;

        if is_mpmc {
            draw_mpmc_heatmap(&root, &payload_rows, section_y, width as i32, scale_max)?;
        } else {
            draw_mpsc_heatmap(&root, &payload_rows, section_y, width as i32, scale_max)?;
        }
    }

    root.present().chart()?;
    Ok(())
}

fn draw_mpsc_heatmap(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Row],
    y: i32,
    width: i32,
    scale_max: f64,
) -> ChartResult<()> {
    let series = present_series(rows);
    let producers: Vec<usize> = rows
        .iter()
        .map(|row| row.producers)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let values = values_by_series(rows);
    let winners = winners_by_topology(rows);
    let table_left = 184;
    let table_right = 24;
    let cell_width = (width - table_left - table_right) / producers.len().max(1) as i32;
    let row_height = 25;
    let rows_top = y + 68;

    draw_section_title(area, rows, y, scale_max, "")?;
    draw_text(
        area,
        "implementation",
        (28, y + 46),
        label_style(MUTED_TEXT_COLOR, HPos::Left),
    )?;
    draw_text(
        area,
        "producer threads",
        ((table_left + width - table_right) / 2, y + 38),
        label_style(MUTED_TEXT_COLOR, HPos::Center),
    )?;

    for (column, producer) in producers.iter().enumerate() {
        let x = table_left + column as i32 * cell_width;
        draw_text(
            area,
            producer.to_string(),
            (x + cell_width / 2, y + 56),
            label_style(TEXT_COLOR, HPos::Center),
        )?;
    }

    for (row_index, (key, label)) in series.iter().enumerate() {
        let row_y = rows_top + row_index as i32 * row_height;
        let color = if label == &"fanring" {
            RGBColor(250, 204, 21)
        } else {
            TEXT_COLOR
        };
        draw_text(
            area,
            *label,
            (28, row_y + row_height / 2),
            label_style(color, HPos::Left),
        )?;

        for (column, producer) in producers.iter().enumerate() {
            let topology = (*producer, 0);
            let value = values.get(&(*key, topology)).copied();
            let winner = value
                .zip(winners.get(&topology).copied())
                .is_some_and(|(value, best)| value >= best);
            draw_heat_cell(
                area,
                table_left + column as i32 * cell_width,
                row_y,
                cell_width,
                row_height,
                value,
                scale_max,
                winner,
            )?;
        }
    }

    draw_separator(area, y + 235, width)?;
    Ok(())
}

fn draw_mpmc_heatmap(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Row],
    y: i32,
    width: i32,
    scale_max: f64,
) -> ChartResult<()> {
    let series = present_series(rows);
    let producers: Vec<usize> = rows
        .iter()
        .map(|row| row.producers)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let consumers: Vec<usize> = rows
        .iter()
        .filter_map(|row| row.consumers)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let values = values_by_series(rows);
    let winners = winners_by_topology(rows);
    let table_left = 184;
    let table_right = 24;
    let group_gap = 8;
    let topology_count = producers.len() * consumers.len();
    let total_gaps = group_gap * producers.len().saturating_sub(1) as i32;
    let cell_width = (width - table_left - table_right - total_gaps) / topology_count.max(1) as i32;
    let row_height = 29;
    let rows_top = y + 89;

    draw_section_title(area, rows, y, scale_max, "")?;
    draw_text(
        area,
        "producer threads",
        (table_left - 12, y + 39),
        label_style(MUTED_TEXT_COLOR, HPos::Right),
    )?;
    draw_text(
        area,
        "consumer threads",
        (table_left - 12, y + 61),
        label_style(MUTED_TEXT_COLOR, HPos::Right),
    )?;
    draw_text(
        area,
        "implementation",
        (28, y + 78),
        label_style(MUTED_TEXT_COLOR, HPos::Left),
    )?;

    for (producer_index, producer) in producers.iter().enumerate() {
        let group_start =
            table_left + producer_index as i32 * (consumers.len() as i32 * cell_width + group_gap);
        let group_width = consumers.len() as i32 * cell_width;
        draw_text(
            area,
            producer.to_string(),
            (group_start + group_width / 2, y + 39),
            label_style(TEXT_COLOR, HPos::Center),
        )?;

        for (consumer_index, consumer) in consumers.iter().enumerate() {
            draw_text(
                area,
                consumer.to_string(),
                (
                    group_start + consumer_index as i32 * cell_width + cell_width / 2,
                    y + 61,
                ),
                label_style(TEXT_COLOR, HPos::Center),
            )?;
        }
    }

    for (row_index, (key, label)) in series.iter().enumerate() {
        let row_y = rows_top + row_index as i32 * row_height;
        let color = if label == &"fanring" {
            RGBColor(250, 204, 21)
        } else {
            TEXT_COLOR
        };
        draw_text(
            area,
            *label,
            (28, row_y + row_height / 2),
            label_style(color, HPos::Left),
        )?;

        for (producer_index, producer) in producers.iter().enumerate() {
            let group_start = table_left
                + producer_index as i32 * (consumers.len() as i32 * cell_width + group_gap);
            for (consumer_index, consumer) in consumers.iter().enumerate() {
                let topology = (*producer, *consumer);
                let value = values.get(&(*key, topology)).copied();
                let winner = value
                    .zip(winners.get(&topology).copied())
                    .is_some_and(|(value, best)| value >= best);
                draw_heat_cell(
                    area,
                    group_start + consumer_index as i32 * cell_width,
                    row_y,
                    cell_width,
                    row_height,
                    value,
                    scale_max,
                    winner,
                )?;
            }
        }
    }

    draw_separator(area, y + 235, width)?;
    Ok(())
}

type SeriesValues<'a> = BTreeMap<(&'a str, (usize, usize)), f64>;

fn values_by_series<'a>(rows: &[&'a Row]) -> SeriesValues<'a> {
    rows.iter()
        .map(|row| {
            (
                (
                    row.implementation.as_str(),
                    (row.producers, row.consumers.unwrap_or(0)),
                ),
                row.items_per_sec / 1_000_000.0,
            )
        })
        .collect()
}

fn winners_by_topology(rows: &[&Row]) -> BTreeMap<(usize, usize), f64> {
    let mut winners = BTreeMap::new();
    for row in rows {
        let topology = (row.producers, row.consumers.unwrap_or(0));
        let value = row.items_per_sec / 1_000_000.0;
        winners
            .entry(topology)
            .and_modify(|best: &mut f64| *best = best.max(value))
            .or_insert(value);
    }
    winners
}

fn present_series(rows: &[&Row]) -> Vec<(&'static str, &'static str)> {
    SERIES
        .iter()
        .copied()
        .filter(|(key, _)| rows.iter().any(|row| row.implementation == *key))
        .collect()
}

fn draw_section_title(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Row],
    y: i32,
    scale_max: f64,
    suffix: &str,
) -> ChartResult<()> {
    let title = format!(
        "{}; M items/s; darker to lighter = 0-{}{suffix}; white outline = winner",
        payload_title(rows),
        fmt_mmsgs(scale_max)
    );
    draw_text(
        area,
        title,
        (area.dim_in_pixel().0 as i32 / 2, y + 15),
        ("sans-serif", 11)
            .into_font()
            .color(&TEXT_COLOR)
            .pos(Pos::new(HPos::Center, VPos::Center)),
    )
}

#[allow(clippy::too_many_arguments)]
fn draw_heat_cell(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
    value: Option<f64>,
    scale_max: f64,
    winner: bool,
) -> ChartResult<()> {
    let x0 = x + 2;
    let y0 = y + 2;
    let x1 = x + width - 2;
    let y1 = y + height - 2;
    let intensity = value.map_or(0.0, |value| (value / scale_max).clamp(0.0, 1.0));
    let fill = value.map_or(RGBColor(17, 24, 39), |_| heat_color(intensity));

    area.draw(&Rectangle::new([(x0, y0), (x1, y1)], fill.filled()))
        .chart()?;
    if winner {
        area.draw(&Rectangle::new(
            [(x0, y0), (x1, y1)],
            ShapeStyle::from(&TEXT_COLOR).stroke_width(2),
        ))
        .chart()?;
    }

    let text = value.map_or_else(|| "-".to_string(), |value| format!("{value:.1}"));
    let text_color = if intensity >= 0.68 {
        BACKGROUND_COLOR
    } else {
        TEXT_COLOR
    };
    draw_text(
        area,
        text,
        ((x0 + x1) / 2, (y0 + y1) / 2),
        ("sans-serif", 11)
            .into_font()
            .color(&text_color)
            .pos(Pos::new(HPos::Center, VPos::Center)),
    )
}

fn heat_color(intensity: f64) -> RGBColor {
    const LOW: RGBColor = RGBColor(24, 32, 48);
    const HIGH: RGBColor = RGBColor(96, 165, 250);
    let mix = intensity.clamp(0.0, 1.0);
    RGBColor(
        lerp(LOW.0, HIGH.0, mix),
        lerp(LOW.1, HIGH.1, mix),
        lerp(LOW.2, HIGH.2, mix),
    )
}

fn lerp(from: u8, to: u8, mix: f64) -> u8 {
    (from as f64 + (to as f64 - from as f64) * mix).round() as u8
}

fn draw_text<T: std::borrow::Borrow<str>>(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    text: T,
    position: (i32, i32),
    style: TextStyle<'_>,
) -> ChartResult<()> {
    area.draw(&Text::new(text, position, style)).chart()
}

fn label_style(color: RGBColor, horizontal: HPos) -> TextStyle<'static> {
    ("sans-serif", 11)
        .into_font()
        .color(&color)
        .pos(Pos::new(horizontal, VPos::Center))
}

fn draw_separator(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    y: i32,
    width: i32,
) -> ChartResult<()> {
    area.draw(&PathElement::new(
        vec![(24, y), (width - 24, y)],
        ShapeStyle::from(&GRID_COLOR).stroke_width(1),
    ))
    .chart()
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
