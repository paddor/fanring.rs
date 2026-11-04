use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use plotters::drawing::{DrawingArea, IntoDrawingArea};
use plotters::element::{PathElement, Rectangle, Text};
use plotters::prelude::{Color, IntoFont, RGBColor, SVGBackend, ShapeStyle, TextStyle};
use plotters::style::text_anchor::{HPos, Pos, VPos};

use super::hardware::chart_hardware;
use super::{
    BACKGROUND_COLOR, ChartError, ChartResult, DrawResultExt, GRID_COLOR, MUTED_TEXT_COLOR,
    Measurement, Row, SERIES, TEXT_COLOR,
};

const TABLE_LEFT: i32 = 184;
const TABLE_RIGHT: i32 = 24;

/// Pixel size of the whole chart and of its header and payload sections.
struct ChartLayout {
    width: u32,
    height: u32,
    header_height: u32,
    section_height: u32,
}

/// Column and row geometry of one heatmap table.
///
/// Columns form one group per producer count with one cell per consumer
/// count. MPSC tables use a single consumer column of 0 per group.
struct HeatTable {
    producers: Vec<usize>,
    consumers: Vec<usize>,
    cell_width: i32,
    group_gap: i32,
    rows_top: i32,
    row_height: i32,
}

/// Pixel rectangle of one heatmap cell.
#[derive(Clone, Copy)]
struct Cell {
    x: i32,
    y: i32,
    width: i32,
    height: i32,
}

pub(super) fn draw_chart(rows: &[Row], output: &Path) -> ChartResult<()> {
    if rows.is_empty() {
        return Err(ChartError::NoRenderableRows);
    }

    let payloads = payloads_by_size(rows);
    let is_mpmc = rows.iter().any(|row| row.consumers.is_some());
    let layout = ChartLayout::new(rows, is_mpmc, payloads.len());
    let width_i32 = u32_to_i32(layout.width);

    let root = SVGBackend::new(output, (layout.width, layout.height)).into_drawing_area();
    root.fill(&BACKGROUND_COLOR).chart()?;
    draw_chart_title(&root, rows, is_mpmc, width_i32 / 2)?;

    for (index, payload) in payloads.iter().enumerate() {
        let payload_rows: Vec<&Row> = rows.iter().filter(|row| &row.payload == payload).collect();
        let raw_max = values_by_series(&payload_rows)
            .values()
            .map(|measurement| measurement.median)
            .fold(0.0_f64, f64::max)
            .max(1.0);
        let scale_max = nice_axis_max(raw_max, 5);
        let section_y = layout.section_y(index);

        if is_mpmc {
            draw_mpmc_heatmap(&root, &payload_rows, section_y, width_i32, scale_max)?;
        } else {
            draw_mpsc_heatmap(&root, &payload_rows, section_y, width_i32, scale_max)?;
        }
    }

    root.draw(&Text::new(
        capacity_footnote(rows, is_mpmc),
        (width_i32 / 2, u32_to_i32(layout.height) - 9),
        ("sans-serif", 9)
            .into_font()
            .color(&MUTED_TEXT_COLOR)
            .pos(Pos::new(HPos::Center, VPos::Center)),
    ))
    .chart()?;

    root.present().chart()?;
    Ok(())
}

/// Returns payload names ordered by payload size.
fn payloads_by_size(rows: &[Row]) -> Vec<String> {
    let mut payloads: Vec<(String, usize)> = rows
        .iter()
        .map(|row| (row.payload.clone(), row.payload_bytes))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect();
    payloads.sort_by_key(|(_, bytes)| *bytes);
    payloads.into_iter().map(|(payload, _)| payload).collect()
}

impl ChartLayout {
    fn new(rows: &[Row], is_mpmc: bool, payload_count: usize) -> Self {
        let width: u32 = if is_mpmc { 1440 } else { 1024 };
        let series_count = present_series(&rows.iter().collect::<Vec<_>>()).len();
        let (section_header, row_height) = if is_mpmc { (100, 29) } else { (78, 25) };
        let section_height: u32 = section_header
            + row_height * u32::try_from(series_count).expect("series count fits u32");
        let header_height: u32 = 58;
        let footer_height: u32 = 24;
        let payload_count = u32::try_from(payload_count).expect("payload count fits u32");
        let height = section_height
            .checked_mul(payload_count)
            .and_then(|height| header_height.checked_add(height))
            .and_then(|height| footer_height.checked_add(height))
            .expect("chart height fits u32");
        Self {
            width,
            height,
            header_height,
            section_height,
        }
    }

    /// Returns the top pixel row of the payload section at `index`.
    fn section_y(&self, index: usize) -> i32 {
        u32_to_i32(self.header_height) + usize_to_i32(index) * u32_to_i32(self.section_height)
    }
}

fn draw_chart_title(
    root: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[Row],
    is_mpmc: bool,
    center: i32,
) -> ChartResult<()> {
    let title_style = ("sans-serif", 15)
        .into_font()
        .color(&TEXT_COLOR)
        .pos(Pos::new(HPos::Center, VPos::Center));
    let subtitle_style = ("sans-serif", 10)
        .into_font()
        .color(&MUTED_TEXT_COLOR)
        .pos(Pos::new(HPos::Center, VPos::Center));

    let profile = if rows
        .first()
        .is_some_and(|row| row.throughput_profile == "saturated")
    {
        "saturated throughput"
    } else {
        "throughput"
    };
    root.draw(&Text::new(
        format!(
            "fanring {} {profile} comparison",
            if is_mpmc { "MPMC" } else { "MPSC" }
        ),
        (center, 17),
        title_style,
    ))
    .chart()?;
    root.draw(&Text::new(
        chart_subtitle(rows),
        (center, 35),
        subtitle_style,
    ))
    .chart()
}

fn draw_mpsc_heatmap(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Row],
    y: i32,
    width: i32,
    scale_max: f64,
) -> ChartResult<()> {
    let table = HeatTable::mpsc(rows, y, width);

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
        ((TABLE_LEFT + width - TABLE_RIGHT) / 2, y + 38),
        label_style(MUTED_TEXT_COLOR, HPos::Center),
    )?;

    for (column, producer) in table.producers.iter().enumerate() {
        draw_text(
            area,
            producer.to_string(),
            (table.group_x(column) + table.cell_width / 2, y + 56),
            label_style(TEXT_COLOR, HPos::Center),
        )?;
    }

    draw_series_rows(area, rows, &table, scale_max)?;
    draw_separator(area, y + 260, width)?;
    Ok(())
}

fn draw_mpmc_heatmap(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Row],
    y: i32,
    width: i32,
    scale_max: f64,
) -> ChartResult<()> {
    let table = HeatTable::mpmc(rows, y, width);

    draw_section_title(area, rows, y, scale_max, "")?;
    draw_text(
        area,
        "producer threads",
        (TABLE_LEFT - 12, y + 39),
        label_style(MUTED_TEXT_COLOR, HPos::Right),
    )?;
    draw_text(
        area,
        "consumer threads",
        (TABLE_LEFT - 12, y + 61),
        label_style(MUTED_TEXT_COLOR, HPos::Right),
    )?;
    draw_text(
        area,
        "implementation",
        (28, y + 78),
        label_style(MUTED_TEXT_COLOR, HPos::Left),
    )?;

    for (producer_index, producer) in table.producers.iter().enumerate() {
        draw_text(
            area,
            producer.to_string(),
            (
                table.group_x(producer_index) + table.group_width() / 2,
                y + 39,
            ),
            label_style(TEXT_COLOR, HPos::Center),
        )?;

        for (consumer_index, consumer) in table.consumers.iter().enumerate() {
            draw_text(
                area,
                consumer.to_string(),
                (
                    table.cell_x(producer_index, consumer_index) + table.cell_width / 2,
                    y + 61,
                ),
                label_style(TEXT_COLOR, HPos::Center),
            )?;
        }
    }

    draw_series_rows(area, rows, &table, scale_max)?;
    draw_separator(area, y + 235, width)?;
    Ok(())
}

/// Draws one labeled row of heat cells per present series.
fn draw_series_rows(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    rows: &[&Row],
    table: &HeatTable,
    scale_max: f64,
) -> ChartResult<()> {
    let series = present_series(rows);
    let values = values_by_series(rows);
    let winners = winners_by_topology(&values);
    for (row_index, (key, label)) in series.iter().enumerate() {
        let row_y = table.rows_top + usize_to_i32(row_index) * table.row_height;
        let color = if key.starts_with("fanring") {
            RGBColor(250, 204, 21)
        } else {
            TEXT_COLOR
        };
        draw_text(
            area,
            *label,
            (28, row_y + table.row_height / 2),
            label_style(color, HPos::Left),
        )?;

        for (producer_index, producer) in table.producers.iter().enumerate() {
            for (consumer_index, consumer) in table.consumers.iter().enumerate() {
                let topology = (*producer, *consumer);
                let measurement = values.get(&(*key, topology)).copied();
                let winner = measurement
                    .map(|measurement| measurement.median)
                    .zip(winners.get(&topology).copied())
                    .is_some_and(|(value, best)| value >= best);
                let cell = Cell {
                    x: table.cell_x(producer_index, consumer_index),
                    y: row_y,
                    width: table.cell_width,
                    height: table.row_height,
                };
                draw_heat_cell(area, cell, measurement, scale_max, winner)?;
            }
        }
    }
    Ok(())
}

impl HeatTable {
    fn mpsc(rows: &[&Row], y: i32, width: i32) -> Self {
        let producers = distinct_producers(rows);
        let cell_width = (width - TABLE_LEFT - TABLE_RIGHT) / usize_to_i32(producers.len().max(1));
        Self {
            producers,
            consumers: vec![0],
            cell_width,
            group_gap: 0,
            rows_top: y + 68,
            row_height: 25,
        }
    }

    fn mpmc(rows: &[&Row], y: i32, width: i32) -> Self {
        let producers = distinct_producers(rows);
        let consumers: Vec<usize> = rows
            .iter()
            .filter_map(|row| row.consumers)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let group_gap = 8;
        let topology_count = producers.len().saturating_mul(consumers.len());
        let total_gaps = group_gap * usize_to_i32(producers.len().saturating_sub(1));
        let cell_width =
            (width - TABLE_LEFT - TABLE_RIGHT - total_gaps) / usize_to_i32(topology_count.max(1));
        Self {
            producers,
            consumers,
            cell_width,
            group_gap,
            rows_top: y + 89,
            row_height: 29,
        }
    }

    fn group_width(&self) -> i32 {
        usize_to_i32(self.consumers.len()) * self.cell_width
    }

    /// Returns the left pixel column of the producer group at `producer_index`.
    fn group_x(&self, producer_index: usize) -> i32 {
        TABLE_LEFT + usize_to_i32(producer_index) * (self.group_width() + self.group_gap)
    }

    fn cell_x(&self, producer_index: usize, consumer_index: usize) -> i32 {
        self.group_x(producer_index) + usize_to_i32(consumer_index) * self.cell_width
    }
}

fn distinct_producers(rows: &[&Row]) -> Vec<usize> {
    rows.iter()
        .map(|row| row.producers)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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

pub(super) fn present_series<'a>(rows: &[&'a Row]) -> Vec<(&'a str, &'a str)> {
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

fn draw_heat_cell(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    cell: Cell,
    measurement: Option<Measurement>,
    scale_max: f64,
    winner: bool,
) -> ChartResult<()> {
    let x0 = cell.x + 2;
    let y0 = cell.y + 2;
    let x1 = cell.x + cell.width - 2;
    let y1 = cell.y + cell.height - 2;
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
        "{}; {}; {} operations; {} samples; nominal capacity {} items; {}",
        chart_hardware(&row.cpu),
        row.affinity_label(),
        row.mode,
        row.samples,
        row.nominal_capacity,
        row.throughput_profile_label()
    )
}

fn capacity_footnote(rows: &[Row], is_mpmc: bool) -> String {
    let models = rows
        .iter()
        .filter_map(|row| row.capacity_model.as_deref())
        .collect::<BTreeSet<_>>();
    if models.is_empty() {
        return "Legacy results: capacity model was not recorded".to_string();
    }
    if rows
        .iter()
        .all(|row| row.implementation.starts_with("fanring"))
    {
        return format!(
            "Deferred retains unread ring payloads; Coordinated reclaims them during close or an overlapping send. Capacity: per-ring HWM{}",
            if is_mpmc {
                " plus receiver staging"
            } else {
                ""
            }
        );
    }
    if is_mpmc {
        "Capacity: fanring uses per-ring HWM plus receiver staging; others use one shared bound"
            .to_string()
    } else {
        "Capacity: fanring uses per-ring HWM; others use one shared bound".to_string()
    }
}

pub(super) fn unknown_cpu() -> String {
    "unknown CPU".to_string()
}

pub(super) fn default_mode() -> String {
    "try".to_string()
}

pub(super) fn one_sample() -> usize {
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

pub(super) fn measurement(mut values: Vec<f64>) -> Measurement {
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
