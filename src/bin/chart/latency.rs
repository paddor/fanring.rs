use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use plotters::prelude::*;
use plotters::style::text_anchor::HPos;
use serde::Deserialize;

use super::hardware::chart_hardware;
use super::plot::{
    AXIS, Area, BG, Bar, Font, GRID, MUTED, Plot, TEXT, finish_svg, line, px, rect, text,
};
use super::{ChartError, ChartResult, DrawResultExt};

const MPSC_SERIES: &[Series] = &[
    Series::new("fanring", "fanring", RGBColor(0xf8, 0x71, 0x71)),
    Series::new("fanring-spin", "fanring spin", RGBColor(0xc9, 0x4b, 0x4b)),
    Series::new(
        "crossbeam-channel",
        "crossbeam-channel 0.5.17",
        RGBColor(0x60, 0xa5, 0xfa),
    ),
    Series::new("crossfire", "crossfire 3.1.20", RGBColor(0x22, 0xd3, 0xee)),
    Series::new("thingbuf", "thingbuf 0.1.6", RGBColor(0xa7, 0x8b, 0xfa)),
    Series::new("flume", "flume 0.12.0", RGBColor(0xf4, 0x72, 0xb6)),
    Series::new("kanal", "kanal 0.1.1*", RGBColor(0xf5, 0x9e, 0x0b)),
];

const MPMC_SERIES: &[Series] = &[
    Series::new("fanring-mpmc", "fanring", RGBColor(0xf8, 0x71, 0x71)),
    Series::new(
        "fanring-spin-mpmc",
        "fanring spin",
        RGBColor(0xc9, 0x4b, 0x4b),
    ),
    Series::new(
        "crossbeam-channel",
        "crossbeam-channel 0.5.17",
        RGBColor(0x60, 0xa5, 0xfa),
    ),
    Series::new(
        "crossfire-mpmc",
        "crossfire 3.1.20",
        RGBColor(0x22, 0xd3, 0xee),
    ),
    Series::new("flume", "flume 0.12.0", RGBColor(0xf4, 0x72, 0xb6)),
    Series::new("kanal", "kanal 0.1.1*", RGBColor(0xf5, 0x9e, 0x0b)),
];

const OPERATIONS: &[(&str, &str)] = &[
    ("recv_wake", "blocked receiver"),
    ("send_wake", "blocked sender"),
];

#[derive(Clone, Copy)]
struct Series {
    key: &'static str,
    label: &'static str,
    color: RGBColor,
}

impl Series {
    const fn new(key: &'static str, label: &'static str, color: RGBColor) -> Self {
        Self { key, label, color }
    }
}

#[derive(Debug, Deserialize)]
struct LatencyRow {
    run_id: String,
    cpu: String,
    #[serde(default)]
    affinity: String,
    implementation: String,
    operation: String,
    #[serde(default)]
    spin_ns: Option<u64>,
    #[serde(default = "one")]
    capacity: usize,
    rounds: usize,
    settle_mode: String,
    settle_ns: u64,
    p50_ns: u64,
    p99_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LatencyShape {
    cpu: String,
    affinity: String,
    capacity: usize,
    rounds: usize,
    settle_mode: String,
    settle_ns: u64,
}

struct ImplementationRun<'a> {
    implementation: &'static str,
    shape: LatencyShape,
    rows: Vec<&'a LatencyRow>,
}

pub(super) fn draw_latency_chart(
    inputs: &[PathBuf],
    source: &Path,
    output: &Path,
    topology: &str,
    requested_run: Option<String>,
) -> ChartResult<()> {
    let series = match topology {
        "mpsc" => MPSC_SERIES,
        "mpmc" => MPMC_SERIES,
        _ => return Err(ChartError::InvalidTopology(topology.to_string())),
    };
    let mut rows = Vec::new();
    for input in inputs {
        rows.extend(read_rows(input)?);
    }
    if rows.is_empty() {
        return Err(ChartError::NoRows {
            path: source.to_path_buf(),
        });
    }
    let rows = select_run(&rows, series, requested_run)?;
    draw(&rows, series, topology, output)
}

fn read_rows(path: &Path) -> ChartResult<Vec<LatencyRow>> {
    let file = File::open(path).map_err(|source| ChartError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut rows = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|source| ChartError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(row) => rows.push(row),
            Err(error) => eprintln!(
                "warning: ignored incomplete wake-latency row at {}:{}: {error}",
                path.display(),
                index + 1
            ),
        }
    }
    Ok(rows)
}

fn select_run<'a>(
    rows: &'a [LatencyRow],
    series: &[Series],
    requested_run: Option<String>,
) -> ChartResult<Vec<&'a LatencyRow>> {
    if let Some(run_id) = requested_run {
        let selected = rows
            .iter()
            .filter(|row| row.run_id == run_id)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Err(ChartError::RunNotFound(run_id));
        }
        return complete_rows(&selected, series).ok_or(ChartError::RunIncomplete(run_id));
    }

    combine_latest_runs(rows, series).ok_or(ChartError::NoCompleteRun)
}

fn combine_latest_runs<'a>(
    rows: &'a [LatencyRow],
    series: &[Series],
) -> Option<Vec<&'a LatencyRow>> {
    let mut seen = BTreeSet::new();
    let mut run_ids = rows
        .iter()
        .filter_map(|row| {
            seen.insert(row.run_id.as_str())
                .then_some(row.run_id.as_str())
        })
        .collect::<Vec<_>>();
    run_ids.sort_by_key(|run_id| run_id.parse::<u128>().unwrap_or(0));

    let mut runs = Vec::new();
    for run_id in run_ids {
        for candidate in series {
            let selected = rows
                .iter()
                .filter(|row| row.run_id == run_id && row.implementation == candidate.key)
                .collect::<Vec<_>>();
            if let Some(shape) = complete_implementation(&selected) {
                runs.push(ImplementationRun {
                    implementation: candidate.key,
                    shape,
                    rows: selected,
                });
            }
        }
    }

    for reference in runs.iter().rev().map(|run| &run.shape) {
        let mut combined = Vec::new();
        for candidate in series {
            let Some(run) = runs
                .iter()
                .rev()
                .find(|run| run.implementation == candidate.key && run.shape == *reference)
            else {
                combined.clear();
                break;
            };
            combined.extend(run.rows.iter().copied());
        }
        if !combined.is_empty() {
            return Some(combined);
        }
    }
    None
}

fn complete_implementation(rows: &[&LatencyRow]) -> Option<LatencyShape> {
    let first = *rows.first()?;
    if rows.len() != OPERATIONS.len()
        || rows
            .iter()
            .any(|row| latency_shape(row) != latency_shape(first))
        || !OPERATIONS
            .iter()
            .all(|(operation, _)| rows.iter().any(|row| row.operation == *operation))
    {
        return None;
    }
    Some(latency_shape(first))
}

fn latency_shape(row: &LatencyRow) -> LatencyShape {
    LatencyShape {
        cpu: row.cpu.clone(),
        affinity: row.affinity.clone(),
        capacity: row.capacity,
        rounds: row.rounds,
        settle_mode: row.settle_mode.clone(),
        settle_ns: row.settle_ns,
    }
}

fn complete_rows<'a>(rows: &[&'a LatencyRow], series: &[Series]) -> Option<Vec<&'a LatencyRow>> {
    let first = rows.iter().copied().find(|row| {
        series
            .iter()
            .any(|candidate| candidate.key == row.implementation)
    })?;
    let mut selected = BTreeMap::new();
    for row in rows {
        if !series
            .iter()
            .any(|candidate| candidate.key == row.implementation)
        {
            continue;
        }
        if row.cpu != first.cpu
            || row.affinity != first.affinity
            || row.rounds != first.rounds
            || row.capacity != first.capacity
            || row.settle_mode != first.settle_mode
            || row.settle_ns != first.settle_ns
            || !OPERATIONS
                .iter()
                .any(|(operation, _)| *operation == row.operation)
        {
            return None;
        }
        if selected
            .insert((row.implementation.as_str(), row.operation.as_str()), *row)
            .is_some()
        {
            return None;
        }
    }
    let complete = series.iter().all(|candidate| {
        OPERATIONS
            .iter()
            .all(|(operation, _)| selected.contains_key(&(candidate.key, *operation)))
    });
    complete.then(|| selected.into_values().collect())
}

fn draw(rows: &[&LatencyRow], series: &[Series], topology: &str, output: &Path) -> ChartResult<()> {
    let width = 1024;
    let height = 445;
    let area = SVGBackend::new(output, (width, height)).into_drawing_area();
    area.fill(&BG).chart()?;

    let first = rows.first().copied().ok_or(ChartError::NoRenderableRows)?;
    let middle = i32::try_from(width / 2).expect("chart width fits i32");
    draw_header(&area, first, topology, middle)?;

    let max_value = rows
        .iter()
        .map(|row| row.p99_ns as f64)
        .fold(0.0_f64, f64::max);
    let plot = Plot {
        left: 70.0,
        right: 1000.0,
        top: 62.0,
        bottom: 296.0,
        y_max: (max_value * 1.15).max(1.0),
    };
    draw_y_grid(&area, plot)?;
    draw_bars(&area, plot, series, rows)?;
    draw_legend(&area, series, rows, 40.0, 350.0)?;
    draw_footnotes(&area, middle)?;
    area.present().chart()?;
    drop(area);
    finish_svg(output, width, height)
}

fn draw_header(
    area: &Area<'_>,
    first: &LatencyRow,
    topology: &str,
    middle: i32,
) -> ChartResult<()> {
    text(
        area,
        format!(
            "{} blocking wake latency (lower is better)",
            topology.to_uppercase()
        ),
        (middle, 20),
        Font::bold(15, TEXT, HPos::Center),
    )?;
    text(
        area,
        format!(
            "{}; capacity {}; {} rounds; {} {} settle",
            chart_hardware(&first.cpu),
            first.capacity,
            first.rounds,
            format_duration(first.settle_ns as f64),
            first.settle_mode
        ),
        (middle, 40),
        Font::regular(10, MUTED, HPos::Center),
    )
}

fn draw_bars(
    area: &Area<'_>,
    plot: Plot,
    series: &[Series],
    rows: &[&LatencyRow],
) -> ChartResult<()> {
    let values = rows
        .iter()
        .map(|row| ((row.implementation.as_str(), row.operation.as_str()), *row))
        .collect::<BTreeMap<_, _>>();
    let group_width = (plot.right - plot.left) / OPERATIONS.len() as f64;
    let bar_gap = 8.0;
    let minimum_group_gap = 36.0;
    let preferred_bar_width: f64 = 44.0;
    let available_bar_width =
        (group_width - minimum_group_gap - series.len().saturating_sub(1) as f64 * bar_gap)
            / series.len() as f64;
    let bar_width = preferred_bar_width.min(available_bar_width);
    let cluster_width =
        series.len() as f64 * bar_width + series.len().saturating_sub(1) as f64 * bar_gap;
    for (operation_index, (operation, label)) in OPERATIONS.iter().enumerate() {
        let cluster_x =
            plot.left + operation_index as f64 * group_width + (group_width - cluster_width) / 2.0;
        for (series_index, candidate) in series.iter().enumerate() {
            let row = values
                .get(&(candidate.key, *operation))
                .ok_or(ChartError::NoRenderableRows)?;
            let bar = Bar {
                x: cluster_x + series_index as f64 * (bar_width + bar_gap),
                width: bar_width,
                color: candidate.color,
            };
            draw_bar(area, plot, bar, row)?;
        }
        text(
            area,
            *label,
            (px(cluster_x + cluster_width / 2.0), px(plot.bottom + 22.0)),
            Font::bold(12, TEXT, HPos::Center),
        )?;
    }
    Ok(())
}

fn draw_footnotes(area: &Area<'_>, middle: i32) -> ChartResult<()> {
    for (footnote, y) in [
        (
            "Solid bars reach p50; translucent extensions reach p99.",
            400,
        ),
        (
            "* Kanal performs up to 256 sched_yield calls before parking",
            420,
        ),
    ] {
        text(
            area,
            footnote,
            (middle, y),
            Font::regular(9, MUTED, HPos::Center),
        )?;
    }
    Ok(())
}

fn draw_bar(area: &Area<'_>, plot: Plot, bar: Bar, row: &LatencyRow) -> ChartResult<()> {
    let p99_y = plot.y(row.p99_ns as f64);
    translucent_rect(
        area,
        (bar.x, p99_y),
        (bar.x + bar.width, plot.bottom),
        bar.color,
    )?;
    let p50_y = plot.y(row.p50_ns as f64);
    rect(
        area,
        (bar.x, p50_y),
        (bar.x + bar.width, plot.bottom),
        bar.color,
    )
}

fn translucent_rect(
    area: &Area<'_>,
    (x1, y1): (f64, f64),
    (x2, y2): (f64, f64),
    color: RGBColor,
) -> ChartResult<()> {
    let corners = [(px(x1), px(y1)), (px(x2), px(y2))];
    area.draw(&Rectangle::new(corners, color.mix(0.3).filled()))
        .chart()?;
    area.draw(&Rectangle::new(corners, color.mix(0.7).stroke_width(1)))
        .chart()
}

fn draw_y_grid(area: &Area<'_>, plot: Plot) -> ChartResult<()> {
    let step = nice_step(plot.y_max, 5);
    let mut value = step;
    while value <= plot.y_max {
        let y = plot.y(value);
        line(area, (plot.left, y), (plot.right, y), GRID, 1)?;
        text(
            area,
            format_duration(value),
            (px(plot.left - 8.0), px(y)),
            Font::regular(10, MUTED, HPos::Right),
        )?;
        value += step;
    }
    line(
        area,
        (plot.left, plot.bottom),
        (plot.right, plot.bottom),
        AXIS,
        2,
    )
}

fn draw_legend(
    area: &Area<'_>,
    series: &[Series],
    rows: &[&LatencyRow],
    x: f64,
    y: f64,
) -> ChartResult<()> {
    for (index, candidate) in series.iter().enumerate() {
        let column = index % 4;
        let row = index / 4;
        let legend_x = x + column as f64 * 240.0;
        let legend_y = y + row as f64 * 20.0;
        rect(
            area,
            (legend_x, legend_y - 6.0),
            (legend_x + 12.0, legend_y + 6.0),
            candidate.color,
        )?;
        let label = rows
            .iter()
            .find(|row| {
                row.implementation == candidate.key
                    && row.implementation.starts_with("fanring-spin")
            })
            .and_then(|row| row.spin_ns)
            .map_or_else(
                || candidate.label.to_string(),
                |spin_ns| {
                    let duration = format_duration(spin_ns as f64)
                        .replace('\u{b5}', "\u{3bc}")
                        .replace(' ', "");
                    format!("{} ({duration})", candidate.label)
                },
            );
        text(
            area,
            label,
            (px(legend_x + 18.0), px(legend_y)),
            Font::regular(10, TEXT, HPos::Left),
        )?;
    }
    Ok(())
}

fn nice_step(max_value: f64, target_lines: usize) -> f64 {
    let raw = max_value / target_lines as f64;
    let magnitude = 10.0_f64.powf(raw.max(1e-9).log10().floor());
    [1.0, 2.0, 5.0, 10.0]
        .into_iter()
        .map(|multiplier| multiplier * magnitude)
        .find(|step| max_value / step <= target_lines as f64 + 1.0)
        .unwrap_or(magnitude * 10.0)
}

fn format_duration(ns: f64) -> String {
    if ns >= 1_000_000.0 {
        format!("{:.0} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.0} \u{b5}s", ns / 1_000.0)
    } else {
        format!("{ns:.0} ns")
    }
}

const fn one() -> usize {
    1
}

#[cfg(test)]
mod tests {
    use super::{LatencyRow, MPSC_SERIES, select_run};

    #[test]
    fn selects_latest_complete_run() {
        let mut rows = complete_run("old");
        rows.extend(complete_run("new"));
        rows.push(row("partial", "fanring", "recv_wake"));

        let selected = select_run(&rows, MPSC_SERIES, None).unwrap();

        assert!(selected.iter().all(|row| row.run_id == "new"));
    }

    #[test]
    fn combines_latest_compatible_run_for_each_implementation() {
        let mut rows = complete_run("1");
        rows.extend([
            row("2", "crossfire", "recv_wake"),
            row("2", "crossfire", "send_wake"),
        ]);

        let selected = select_run(&rows, MPSC_SERIES, None).unwrap();

        assert_eq!(selected.len(), MPSC_SERIES.len() * 2);
        assert!(selected.iter().all(|row| {
            row.run_id
                == if row.implementation == "crossfire" {
                    "2"
                } else {
                    "1"
                }
        }));
    }

    #[test]
    fn requested_partial_run_is_rejected() {
        let rows = vec![row("partial", "fanring", "recv_wake")];

        let error = select_run(&rows, MPSC_SERIES, Some("partial".to_string())).unwrap_err();

        assert!(error.to_string().contains("incomplete"));
    }

    #[test]
    fn different_affinity_runs_are_not_combined() {
        let mut rows = complete_run("1");
        let mut pinned = [
            row("2", "crossfire", "recv_wake"),
            row("2", "crossfire", "send_wake"),
        ];
        for row in &mut pinned {
            row.affinity = "physical-first:0,1".to_string();
        }
        rows.extend(pinned);

        let selected = select_run(&rows, MPSC_SERIES, None).unwrap();
        assert!(selected.iter().all(|row| row.run_id == "1"));

        rows[0].affinity = "physical-first:0,1".to_string();
        assert!(select_run(&rows, MPSC_SERIES, Some("1".to_string())).is_err());
    }

    fn complete_run(run_id: &str) -> Vec<LatencyRow> {
        MPSC_SERIES
            .iter()
            .flat_map(|series| {
                ["recv_wake", "send_wake"].map(|operation| row(run_id, series.key, operation))
            })
            .collect()
    }

    fn row(run_id: &str, implementation: &str, operation: &str) -> LatencyRow {
        LatencyRow {
            run_id: run_id.to_string(),
            cpu: "cpu".to_string(),
            affinity: String::new(),
            implementation: implementation.to_string(),
            operation: operation.to_string(),
            spin_ns: implementation.contains("spin").then_some(50_000),
            capacity: 1,
            rounds: 100,
            settle_mode: "sleep".to_string(),
            settle_ns: 50_000,
            p50_ns: 1_000,
            p99_ns: 2_000,
        }
    }
}
