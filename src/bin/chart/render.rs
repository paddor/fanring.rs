use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use plotters::drawing::{DrawingArea, IntoDrawingArea};
use plotters::element::{PathElement, Rectangle, Text};
use plotters::prelude::{Color, IntoFont, RGBColor, SVGBackend, ShapeStyle, TextStyle};
use plotters::style::text_anchor::{HPos, Pos, VPos};

use super::{
    BACKGROUND_COLOR, ChartError, ChartResult, DrawResultExt, GRID_COLOR, MUTED_TEXT_COLOR,
    Measurement, Row, SERIES, TEXT_COLOR,
};

pub(super) fn draw_chart(rows: &[Row], output: &Path) -> ChartResult<()> {
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
    let footer_h: u32 = 24;
    let payload_count = u32::try_from(payloads.len()).expect("payload count fits u32");
    let total_h = section_h
        .checked_mul(payload_count)
        .and_then(|height| header_h.checked_add(height))
        .and_then(|height| footer_h.checked_add(height))
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

    root.draw(&Text::new(
        capacity_footnote(rows, is_mpmc),
        (width_i32 / 2, u32_to_i32(total_h) - 9),
        ("sans-serif", 9)
            .into_font()
            .color(&MUTED_TEXT_COLOR)
            .pos(Pos::new(HPos::Center, VPos::Center)),
    ))
    .chart()?;

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
        "{}; {} operations; {} samples; nominal capacity {} items",
        simplify_cpu_name(&row.cpu),
        row.mode,
        row.samples,
        row.nominal_capacity
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
    if is_mpmc {
        "Capacity: fanring uses per-ring HWM plus receiver staging; others use one shared bound"
            .to_string()
    } else {
        "Capacity: fanring uses per-ring HWM; others use one shared bound".to_string()
    }
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
