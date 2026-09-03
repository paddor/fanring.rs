use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use plotters::coord::Shift;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use super::hardware::chart_hardware;
use super::render::measurement;
use super::{ChartError, ChartResult, DrawResultExt, Row};

const BG: RGBColor = RGBColor(0x0d, 0x11, 0x17);
const GRID: RGBColor = RGBColor(0x21, 0x26, 0x2d);
const AXIS: RGBColor = RGBColor(0x30, 0x36, 0x3d);
const TEXT: RGBColor = RGBColor(0xe6, 0xed, 0xf3);
const MUTED: RGBColor = RGBColor(0x7d, 0x85, 0x90);
const FONT_BUMP: u32 = 1;

const SERIES: &[Series] = &[
    Series {
        mpsc_key: "fanring",
        mpmc_key: Some("fanring-mpmc"),
        label: "fanring",
        color: RGBColor(0xf8, 0x71, 0x71),
    },
    Series {
        mpsc_key: "crossbeam-channel",
        mpmc_key: Some("crossbeam-channel"),
        label: "crossbeam-channel 0.5.16",
        color: RGBColor(0x60, 0xa5, 0xfa),
    },
    Series {
        mpsc_key: "concurrent-queue",
        mpmc_key: None,
        label: "concurrent-queue 2.5.0",
        color: RGBColor(0x4a, 0xde, 0x80),
    },
    Series {
        mpsc_key: "thingbuf",
        mpmc_key: None,
        label: "thingbuf 0.1.6",
        color: RGBColor(0xa7, 0x8b, 0xfa),
    },
    Series {
        mpsc_key: "flume",
        mpmc_key: Some("flume"),
        label: "flume 0.12.0",
        color: RGBColor(0xf4, 0x72, 0xb6),
    },
    Series {
        mpsc_key: "kanal",
        mpmc_key: Some("kanal"),
        label: "kanal 0.1.1",
        color: RGBColor(0xf5, 0x9e, 0x0b),
    },
];

#[derive(Clone, Copy)]
struct Series {
    mpsc_key: &'static str,
    mpmc_key: Option<&'static str>,
    label: &'static str,
    color: RGBColor,
}

type Area<'a> = DrawingArea<SVGBackend<'a>, Shift>;

pub(super) fn draw_summary_chart(
    mpsc_rows: &[Row],
    mpmc_rows: &[Row],
    output: &Path,
) -> ChartResult<()> {
    let nominal_capacity = summary_capacity(mpsc_rows, mpmc_rows)?;
    let groups = [
        ("MPSC: 4 producers", summary_values(mpsc_rows, false)),
        (
            "MPMC: 4 producers / 4 consumers",
            summary_values(mpmc_rows, true),
        ),
    ];
    let expected_mpmc_series = SERIES
        .iter()
        .filter(|series| series.mpmc_key.is_some())
        .count();
    if groups[0].1.len() != SERIES.len() || groups[1].1.len() != expected_mpmc_series {
        return Err(ChartError::NoRenderableRows);
    }

    let width = 850;
    let height = 430;
    let x_left = 70.0;
    let x_right = 830.0;
    let plot_width = x_right - x_left;
    let plot_top = 62.0;
    let plot_bottom = 318.0;
    let max_value = groups
        .iter()
        .flat_map(|(_, values)| values.values().copied())
        .fold(0.0_f64, f64::max);
    let y_max = (max_value * 1.15).max(1.0);

    let area = root(output, width, height)?;
    let title =
        format!("u64 channel throughput with capacity={nominal_capacity} (higher is better)");
    let subtitle = mpsc_rows.first().map(|row| {
        format!(
            "{}, {}, {}",
            chart_hardware(&row.cpu),
            row.affinity_label(),
            row.throughput_profile_label()
        )
    });
    chart_header(&area, width, &title, subtitle.as_deref(), 22)?;
    vtext(
        &area,
        "million items/s",
        22,
        px((plot_top + plot_bottom) / 2.0),
        11,
        TEXT,
    )?;
    draw_y_grid(&area, x_left, x_right, plot_top, plot_bottom, y_max)?;

    let group_width = plot_width / groups.len() as f64;
    let bar_width = 44.0;
    let bar_gap = 2.0;
    for (group_index, (group, values)) in groups.iter().enumerate() {
        let active_series = SERIES
            .iter()
            .filter(|series| values.contains_key(series.label))
            .collect::<Vec<_>>();
        let cluster_width = active_series.len() as f64 * bar_width
            + active_series.len().saturating_sub(1) as f64 * bar_gap;
        let group_x =
            x_left + group_index as f64 * group_width + (group_width - cluster_width) / 2.0;
        for (series_index, series) in active_series.iter().enumerate() {
            let Some(value) = values.get(series.label) else {
                continue;
            };
            draw_bar(
                &area,
                group_x + series_index as f64 * (bar_width + bar_gap),
                bar_width,
                plot_top,
                plot_bottom,
                y_max,
                *value,
                series.color,
            )?;
        }
        text(
            &area,
            *group,
            px(group_x + cluster_width / 2.0),
            px(plot_bottom + 20.0),
            11,
            TEXT,
            HPos::Center,
            true,
        )?;
    }

    draw_legend(&area, 65.0, plot_bottom + 52.0, nominal_capacity)?;

    area.present().chart()?;
    drop(area);
    finish_svg(output, width, height)
}

fn summary_capacity(mpsc_rows: &[Row], mpmc_rows: &[Row]) -> ChartResult<usize> {
    let profiles = mpsc_rows
        .iter()
        .chain(mpmc_rows)
        .map(|row| {
            (
                row.affinity.as_str(),
                row.throughput_profile.as_str(),
                row.low_watermark,
                row.high_watermark,
            )
        })
        .collect::<BTreeSet<_>>();
    let capacities = mpsc_rows
        .iter()
        .chain(mpmc_rows)
        .filter(|row| row.payload == "u64" && row.producers == 4)
        .map(|row| row.nominal_capacity)
        .collect::<BTreeSet<_>>();
    if profiles.len() == 1 && capacities.len() == 1 {
        let capacity = *capacities.first().expect("one capacity exists");
        if capacity.is_multiple_of(4) {
            return Ok(capacity);
        }
    }
    Err(ChartError::NoRenderableRows)
}

fn summary_values(rows: &[Row], mpmc: bool) -> BTreeMap<&'static str, f64> {
    SERIES
        .iter()
        .filter_map(|series| {
            let key = if mpmc {
                series.mpmc_key?
            } else {
                series.mpsc_key
            };
            let values = rows
                .iter()
                .filter(|row| {
                    row.implementation == key
                        && row.payload == "u64"
                        && row.producers == 4
                        && if mpmc {
                            row.consumers == Some(4)
                        } else {
                            row.consumers.is_none()
                        }
                })
                .map(|row| row.items_per_sec / 1_000_000.0)
                .collect::<Vec<_>>();
            (!values.is_empty()).then(|| (series.label, measurement(values).median))
        })
        .collect()
}

fn root(path: &Path, width: u32, height: u32) -> ChartResult<Area<'_>> {
    let area = SVGBackend::new(path, (width, height)).into_drawing_area();
    area.fill(&BG).chart()?;
    Ok(area)
}

fn finish_svg(path: &Path, width: u32, height: u32) -> ChartResult<()> {
    let mut svg = std::fs::read_to_string(path).map_err(|source| ChartError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    svg = svg.replacen(
        &format!("<svg width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\""),
        &format!("<svg viewBox=\"0 0 {width} {height}\""),
        1,
    );
    svg = svg.replacen(
        "xmlns=\"http://www.w3.org/2000/svg\"",
        "xmlns=\"http://www.w3.org/2000/svg\" font-family=\"system-ui, -apple-system, sans-serif\"",
        1,
    );
    std::fs::write(path, svg).map_err(|source| ChartError::Io {
        path: path.to_path_buf(),
        source,
    })
}

#[allow(clippy::too_many_arguments)]
fn text(
    area: &Area<'_>,
    value: impl Into<String>,
    x: i32,
    y: i32,
    size: u32,
    color: RGBColor,
    horizontal: HPos,
    bold: bool,
) -> ChartResult<()> {
    let mut font = ("sans-serif", size + FONT_BUMP).into_font();
    if bold {
        font = font.style(FontStyle::Bold);
    }
    let style = TextStyle::from(font)
        .color(&color)
        .pos(Pos::new(horizontal, VPos::Center));
    area.draw(&Text::new(value.into(), (x, y), style)).chart()
}

fn vtext(
    area: &Area<'_>,
    value: &str,
    x: i32,
    y: i32,
    size: u32,
    color: RGBColor,
) -> ChartResult<()> {
    let font = ("sans-serif", size + FONT_BUMP)
        .into_font()
        .style(FontStyle::Bold)
        .transform(FontTransform::Rotate270);
    let style = TextStyle::from(font)
        .color(&color)
        .pos(Pos::new(HPos::Center, VPos::Center));
    area.draw(&Text::new(value.to_string(), (x, y), style))
        .chart()
}

fn rect(area: &Area<'_>, x1: f64, y1: f64, x2: f64, y2: f64, color: RGBColor) -> ChartResult<()> {
    area.draw(&Rectangle::new(
        [(px(x1), px(y1)), (px(x2), px(y2))],
        ShapeStyle::from(&color).filled(),
    ))
    .chart()
}

fn line(
    area: &Area<'_>,
    x1: f64,
    y1: f64,
    x2: f64,
    y2: f64,
    color: RGBColor,
    width: u32,
) -> ChartResult<()> {
    area.draw(&PathElement::new(
        vec![(px(x1), px(y1)), (px(x2), px(y2))],
        color.stroke_width(width),
    ))
    .chart()
}

#[allow(clippy::too_many_arguments)]
fn draw_bar(
    area: &Area<'_>,
    x: f64,
    width: f64,
    plot_top: f64,
    plot_bottom: f64,
    y_max: f64,
    value: f64,
    color: RGBColor,
) -> ChartResult<()> {
    let y = plot_bottom - (value / y_max) * (plot_bottom - plot_top);
    rect(area, x, y, x + width, plot_bottom, color)
}

fn draw_y_grid(
    area: &Area<'_>,
    x_left: f64,
    x_right: f64,
    plot_top: f64,
    plot_bottom: f64,
    y_max: f64,
) -> ChartResult<()> {
    let map_y = |value: f64| plot_bottom - (value / y_max) * (plot_bottom - plot_top);
    let step = nice_step(y_max, 5);
    let mut value = step;
    while value <= y_max {
        let y = map_y(value);
        line(area, x_left, y, x_right, y, GRID, 1)?;
        text(
            area,
            format!("{value:.0}"),
            px(x_left - 8.0),
            px(y),
            10,
            MUTED,
            HPos::Right,
            false,
        )?;
        value += step;
    }
    line(area, x_left, plot_bottom, x_right, plot_bottom, AXIS, 2)
}

fn chart_header(
    area: &Area<'_>,
    width: u32,
    title: &str,
    hardware: Option<&str>,
    y: i32,
) -> ChartResult<()> {
    let middle = i32::try_from(width / 2).expect("chart width fits i32");
    text(area, title, middle, y, 14, TEXT, HPos::Center, true)?;
    if let Some(hardware) = hardware {
        text(
            area,
            hardware,
            middle,
            y + 18,
            10,
            MUTED,
            HPos::Center,
            false,
        )?;
    }
    Ok(())
}

fn draw_legend(area: &Area<'_>, x: f64, y: f64, nominal_capacity: usize) -> ChartResult<()> {
    for (index, series) in SERIES.iter().enumerate() {
        let column = index % 3;
        let row = index / 3;
        let legend_x = x + column as f64 * 230.0;
        let legend_y = y + row as f64 * 20.0;
        rect(
            area,
            legend_x,
            legend_y - 6.0,
            legend_x + 12.0,
            legend_y + 6.0,
            series.color,
        )?;
        text(
            area,
            if series.mpsc_key == "fanring" {
                format!("fanring (4 x {}-item rings)", nominal_capacity / 4)
            } else {
                series.label.to_string()
            },
            px(legend_x + 18.0),
            px(legend_y),
            10,
            TEXT,
            HPos::Left,
            false,
        )?;
    }
    Ok(())
}

fn nice_step(max_value: f64, target_lines: usize) -> f64 {
    if max_value <= 0.0 {
        return 1.0;
    }
    let raw = max_value / target_lines as f64;
    let magnitude = 10.0_f64.powf(raw.max(1e-9).log10().floor());
    for multiplier in [1.0, 2.0, 5.0, 10.0] {
        let step = multiplier * magnitude;
        if max_value / step <= target_lines as f64 + 1.0 {
            return step;
        }
    }
    magnitude * 10.0
}

fn px(value: f64) -> i32 {
    value.round() as i32
}

#[cfg(test)]
mod tests {
    use super::{SERIES, summary_capacity, summary_values};
    use crate::Row;

    #[test]
    fn summary_uses_same_series_for_both_groups() {
        let mpsc = SERIES.iter().map(|series| row(series.mpsc_key, None));
        let mpmc = SERIES
            .iter()
            .filter_map(|series| series.mpmc_key.map(|key| row(key, Some(4))));

        let mpsc = summary_values(&mpsc.collect::<Vec<_>>(), false);
        let mpmc = summary_values(&mpmc.collect::<Vec<_>>(), true);
        assert_eq!(mpsc.len(), SERIES.len());
        assert_eq!(mpmc.len(), 4);
        for series in SERIES {
            assert_eq!(mpsc.get(series.label), Some(&1.0));
            assert_eq!(mpmc.get(series.label), series.mpmc_key.map(|_| &1.0));
        }
    }

    #[test]
    fn summary_rejects_mixed_throughput_profiles() {
        let mpsc = vec![row("fanring", None)];
        let mut mpmc = vec![row("fanring-mpmc", Some(4))];
        mpmc[0].throughput_profile = "uncontrolled".to_string();
        mpmc[0].low_watermark = 0;
        mpmc[0].high_watermark = 0;

        assert!(summary_capacity(&mpsc, &mpmc).is_err());
    }

    #[test]
    fn summary_rejects_mixed_affinity_policies() {
        let mpsc = vec![row("fanring", None)];
        let mut mpmc = vec![row("fanring-mpmc", Some(4))];
        mpmc[0].affinity = "off".to_string();

        assert!(summary_capacity(&mpsc, &mpmc).is_err());
    }

    fn row(implementation: &str, consumers: Option<usize>) -> Row {
        Row {
            run_id: "run".to_string(),
            cpu: "cpu".to_string(),
            affinity: "physical-first:0,1".to_string(),
            mode: "try".to_string(),
            implementation: implementation.to_string(),
            payload: "u64".to_string(),
            payload_bytes: 8,
            producers: 4,
            consumers,
            nominal_capacity: 8192,
            capacity_model: Some("per-ring-hwm".to_string()),
            throughput_profile: "saturated".to_string(),
            low_watermark: 4096,
            high_watermark: 8192,
            items_per_sec: 1_000_000.0,
            sample: 0,
            samples: 1,
            expected_rows: 1,
        }
    }
}
