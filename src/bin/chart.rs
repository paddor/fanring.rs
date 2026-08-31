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
    #[serde(default)]
    sample: usize,
    #[serde(default = "one_sample")]
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
    let width: u32 = if is_mpmc { 1440 } else { 1024 };
    let section_h: u32 = 236;
    let header_h: u32 = 58;
    let payload_count = u32::try_from(payloads.len()).expect("payload count fits u32");
    let total_h = section_h
        .checked_mul(payload_count)
        .and_then(|height| header_h.checked_add(height))
        .expect("chart height fits u32");
    let width_i32 = u32_to_i32(width);
    let section_h_i32 = u32_to_i32(section_h);
    let header_h_i32 = u32_to_i32(header_h);

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
        (width_i32 / 2, 17),
        title_style,
    ))
    .chart()?;
    root.draw(&Text::new(
        chart_subtitle(rows),
        (width_i32 / 2, 35),
        subtitle_style,
    ))
    .chart()?;

    for (index, payload) in payloads.iter().enumerate() {
        let payload_rows: Vec<&Row> = rows.iter().filter(|row| &row.payload == payload).collect();
        let raw_max = values_by_series(&payload_rows)
            .values()
            .map(|measurement| measurement.median)
            .fold(0.0_f64, f64::max)
            .max(1.0);
        let scale_max = nice_axis_max(raw_max, 5);
        let section_y = header_h_i32 + usize_to_i32(index) * section_h_i32;

        if is_mpmc {
            draw_mpmc_heatmap(&root, &payload_rows, section_y, width_i32, scale_max)?;
        } else {
            draw_mpsc_heatmap(&root, &payload_rows, section_y, width_i32, scale_max)?;
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
    let winners = winners_by_topology(&values);
    let table_left = 184;
    let table_right = 24;
    let cell_width = (width - table_left - table_right) / usize_to_i32(producers.len().max(1));
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
        let x = table_left + usize_to_i32(column) * cell_width;
        draw_text(
            area,
            producer.to_string(),
            (x + cell_width / 2, y + 56),
            label_style(TEXT_COLOR, HPos::Center),
        )?;
    }

    for (row_index, (key, label)) in series.iter().enumerate() {
        let row_y = rows_top + usize_to_i32(row_index) * row_height;
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
            let measurement = values.get(&(*key, topology)).copied();
            let winner = measurement
                .map(|measurement| measurement.median)
                .zip(winners.get(&topology).copied())
                .is_some_and(|(value, best)| value >= best);
            draw_heat_cell(
                area,
                table_left + usize_to_i32(column) * cell_width,
                row_y,
                cell_width,
                row_height,
                measurement,
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
    let winners = winners_by_topology(&values);
    let table_left = 184;
    let table_right = 24;
    let group_gap = 8;
    let topology_count = producers.len().saturating_mul(consumers.len());
    let consumer_count = usize_to_i32(consumers.len());
    let total_gaps = group_gap * usize_to_i32(producers.len().saturating_sub(1));
    let cell_width =
        (width - table_left - table_right - total_gaps) / usize_to_i32(topology_count.max(1));
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
            table_left + usize_to_i32(producer_index) * (consumer_count * cell_width + group_gap);
        let group_width = consumer_count * cell_width;
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
                    group_start + usize_to_i32(consumer_index) * cell_width + cell_width / 2,
                    y + 61,
                ),
                label_style(TEXT_COLOR, HPos::Center),
            )?;
        }
    }

    for (row_index, (key, label)) in series.iter().enumerate() {
        let row_y = rows_top + usize_to_i32(row_index) * row_height;
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
                + usize_to_i32(producer_index) * (consumer_count * cell_width + group_gap);
            for (consumer_index, consumer) in consumers.iter().enumerate() {
                let topology = (*producer, *consumer);
                let measurement = values.get(&(*key, topology)).copied();
                let winner = measurement
                    .map(|measurement| measurement.median)
                    .zip(winners.get(&topology).copied())
                    .is_some_and(|(value, best)| value >= best);
                draw_heat_cell(
                    area,
                    group_start + usize_to_i32(consumer_index) * cell_width,
                    row_y,
                    cell_width,
                    row_height,
                    measurement,
                    scale_max,
                    winner,
                )?;
            }
        }
    }

    draw_separator(area, y + 235, width)?;
    Ok(())
}

type SeriesValues<'a> = BTreeMap<(&'a str, (usize, usize)), Measurement>;

fn values_by_series<'a>(rows: &[&'a Row]) -> SeriesValues<'a> {
    let mut samples = BTreeMap::<(&str, (usize, usize)), Vec<f64>>::new();
    for row in rows {
        samples
            .entry((
                row.implementation.as_str(),
                (row.producers, row.consumers.unwrap_or(0)),
            ))
            .or_default()
            .push(row.items_per_sec / 1_000_000.0);
    }
    samples
        .into_iter()
        .map(|(key, values)| (key, measurement(values)))
        .collect()
}

fn winners_by_topology(values: &SeriesValues<'_>) -> BTreeMap<(usize, usize), f64> {
    let mut winners = BTreeMap::new();
    for ((_, topology), measurement) in values {
        winners
            .entry(*topology)
            .and_modify(|best: &mut f64| *best = best.max(measurement.median))
            .or_insert(measurement.median);
    }
    winners
}

fn present_series<'a>(rows: &[&'a Row]) -> Vec<(&'a str, &'a str)> {
    let present = rows
        .iter()
        .map(|row| row.implementation.as_str())
        .collect::<BTreeSet<_>>();
    let mut series = SERIES
        .iter()
        .copied()
        .filter(|(key, _)| present.contains(key))
        .collect::<Vec<_>>();
    let known = SERIES.iter().map(|(key, _)| *key).collect::<BTreeSet<_>>();
    series.extend(
        present
            .into_iter()
            .filter(|implementation| !known.contains(implementation))
            .map(|implementation| (implementation, implementation)),
    );
    series
}

fn draw_section_title(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Row],
    y: i32,
    scale_max: f64,
    suffix: &str,
) -> ChartResult<()> {
    let title = format!(
        "{}; median M items/s (+/- MAD); color scale 0-{}{suffix}; white outline = winner",
        payload_title(rows),
        fmt_mmsgs(scale_max)
    );
    draw_text(
        area,
        title,
        (u32_to_i32(area.dim_in_pixel().0) / 2, y + 15),
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
    measurement: Option<Measurement>,
    scale_max: f64,
    winner: bool,
) -> ChartResult<()> {
    let x0 = x + 2;
    let y0 = y + 2;
    let x1 = x + width - 2;
    let y1 = y + height - 2;
    let intensity = measurement.map_or(0.0, |measurement| {
        (measurement.median / scale_max).clamp(0.0, 1.0)
    });
    let fill = measurement.map_or(RGBColor(17, 24, 39), |_| heat_color(intensity));

    area.draw(&Rectangle::new([(x0, y0), (x1, y1)], fill.filled()))
        .chart()?;
    if winner {
        area.draw(&Rectangle::new(
            [(x0, y0), (x1, y1)],
            ShapeStyle::from(&TEXT_COLOR).stroke_width(2),
        ))
        .chart()?;
    }

    let text_color = if intensity >= 0.68 {
        BACKGROUND_COLOR
    } else {
        TEXT_COLOR
    };
    let center = (i32::midpoint(x0, x1), i32::midpoint(y0, y1));
    if let Some(measurement) = measurement {
        draw_text(
            area,
            format!("{:.1}", measurement.median),
            (center.0, center.1 - 4),
            ("sans-serif", 10)
                .into_font()
                .color(&text_color)
                .pos(Pos::new(HPos::Center, VPos::Center)),
        )?;
        draw_text(
            area,
            format!("+/-{:.1}%", measurement.relative_mad),
            (center.0, center.1 + 7),
            ("sans-serif", 7)
                .into_font()
                .color(&text_color)
                .pos(Pos::new(HPos::Center, VPos::Center)),
        )
    } else {
        draw_text(
            area,
            "-",
            center,
            ("sans-serif", 11)
                .into_font()
                .color(&text_color)
                .pos(Pos::new(HPos::Center, VPos::Center)),
        )
    }
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

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn lerp(from: u8, to: u8, mix: f64) -> u8 {
    // Both endpoints and the clamped interpolation remain in u8 range.
    (f64::from(to) - f64::from(from))
        .mul_add(mix, f64::from(from))
        .round() as u8
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
        "{}; {} operations; {} samples; capacity {} items",
        simplify_cpu_name(&row.cpu),
        row.mode,
        row.samples,
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

fn one_sample() -> usize {
    1
}

fn fmt_mmsgs(v: f64) -> String {
    if (v - v.round()).abs() < 0.05 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
    }
}

fn nice_step(max_val: f64, target_lines: u32) -> f64 {
    if max_val <= 0.0 {
        return 1.0;
    }

    let raw = max_val / f64::from(target_lines);
    let mag = 10.0_f64.powf(raw.log10().floor());
    for step in [1.0, 2.0, 2.5, 5.0, 10.0].map(|s| s * mag) {
        if max_val / step <= f64::from(target_lines) + 1.0 {
            return step;
        }
    }
    mag * 10.0
}

fn nice_axis_max(max_val: f64, target_lines: u32) -> f64 {
    let step = nice_step(max_val, target_lines);
    step * (max_val / step).ceil().max(1.0)
}

fn measurement(mut values: Vec<f64>) -> Measurement {
    values.sort_by(f64::total_cmp);
    let median = median_sorted(&values);
    let mut deviations = values
        .into_iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    let mad = median_sorted(&deviations);
    Measurement {
        median,
        relative_mad: if median == 0.0 {
            0.0
        } else {
            mad / median * 100.0
        },
    }
}

fn median_sorted(values: &[f64]) -> f64 {
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

fn usize_to_i32(value: usize) -> i32 {
    i32::try_from(value).expect("chart dimension fits i32")
}

fn u32_to_i32(value: u32) -> i32 {
    i32::try_from(value).expect("chart dimension fits i32")
}

fn run_is_complete(rows: &[&Row]) -> bool {
    if rows.is_empty() {
        return false;
    }
    let expected_rows = rows[0].expected_rows;
    if expected_rows != 0
        && (rows.len() != expected_rows
            || rows.iter().any(|row| row.expected_rows != expected_rows))
    {
        return false;
    }
    let mut groups =
        BTreeMap::<(&str, &str, usize, Option<usize>), (usize, BTreeSet<usize>)>::new();
    for row in rows {
        let (_, samples) = groups
            .entry((
                row.implementation.as_str(),
                row.payload.as_str(),
                row.producers,
                row.consumers,
            ))
            .or_insert_with(|| (row.samples, BTreeSet::new()));
        samples.insert(row.sample);
    }
    groups.values().all(|(expected, samples)| {
        *expected != 0 && samples.len() == *expected && samples.iter().copied().eq(0..*expected)
    })
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

#[cfg(test)]
mod tests {
    use super::{Row, measurement, present_series, run_is_complete};

    #[test]
    fn measurement_uses_median_and_relative_mad() {
        let measurement = measurement(vec![10.0, 12.0, 14.0]);
        assert_eq!(measurement.median, 12.0);
        assert!((measurement.relative_mad - 16.666_666).abs() < 0.000_001);
    }

    #[test]
    fn complete_run_requires_every_sample() {
        let first = row("fanring", 0, 2);
        let second = row("fanring", 1, 2);
        assert!(run_is_complete(&[&first, &second]));
        assert!(!run_is_complete(&[&first]));
    }

    #[test]
    fn unknown_implementations_are_rendered() {
        let known = row("fanring", 0, 1);
        let unknown = row("new-channel", 0, 1);
        assert_eq!(
            present_series(&[&known, &unknown]),
            vec![("fanring", "fanring"), ("new-channel", "new-channel")]
        );
    }

    fn row(implementation: &str, sample: usize, samples: usize) -> Row {
        Row {
            run_id: "run".to_string(),
            cpu: "cpu".to_string(),
            mode: "try".to_string(),
            implementation: implementation.to_string(),
            payload: "u64".to_string(),
            payload_bytes: 8,
            producers: 1,
            consumers: None,
            total_capacity: 1,
            items_per_sec: 1.0,
            sample,
            samples,
            expected_rows: samples,
        }
    }
}
