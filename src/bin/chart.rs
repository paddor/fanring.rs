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
    (
        "crossbeam-channel",
        "crossbeam-channel",
        RGBColor(96, 165, 250),
    ),
    ("flume", "flume", RGBColor(167, 139, 250)),
    ("kanal", "kanal", RGBColor(52, 211, 153)),
    (
        "concurrent-queue",
        "concurrent-queue",
        RGBColor(248, 113, 113),
    ),
    ("thingbuf", "thingbuf", RGBColor(251, 146, 60)),
];

#[derive(Debug, Deserialize)]
struct Row {
    run_id: String,
    implementation: String,
    payload: String,
    payload_bytes: usize,
    producers: usize,
    capacity_per_sender: usize,
    total_capacity: usize,
    seconds: f64,
    msgs_per_sec: f64,
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
    let Some(first_row) = rows.first() else {
        return Err(ChartError::NoRenderableRows);
    };

    let mut payloads: Vec<(String, usize)> = rows
        .iter()
        .map(|row| (row.payload.clone(), row.payload_bytes))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect();
    payloads.sort_by_key(|(_, bytes)| *bytes);
    let payloads: Vec<String> = payloads.into_iter().map(|(payload, _)| payload).collect();
    let producers: Vec<usize> = rows
        .iter()
        .map(|row| row.producers)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let width = 950;
    let panel_h = 250;
    let header_h = 66;
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
        "fanring MPSC comparison",
        (width as i32 / 2, 17),
        title_style,
    ))
    .chart()?;

    let cap = first_row.capacity_per_sender;
    root.draw(&Text::new(
        format!(
            "X-axis: producer threads. Y-axis: million messages/sec. Higher is better. Capacity: {cap}/sender.",
        ),
        (width as i32 / 2, 35),
        subtitle_style.clone(),
    ))
    .chart()?;
    root.draw(&Text::new(
        format!(
            "run: {}   shared-queue baselines use total capacity = producers * {cap}",
            first_row.run_id
        ),
        (width as i32 / 2, 51),
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
            .map(|row| row.msgs_per_sec / 1_000_000.0)
            .fold(0.0_f64, f64::max)
            .max(1.0);
        let (y_max, y_ticks) = nice_axis(y_raw, 5);
        let x_max = (producers.len().saturating_sub(1)).max(1) as f64;

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
            .x_labels(producers.len())
            .y_labels(y_ticks + 1)
            .light_line_style(TRANSPARENT)
            .bold_line_style(GRID_COLOR)
            .axis_style(AXIS_COLOR)
            .label_style(("sans-serif", 0).into_font().color(&TEXT_COLOR))
            .x_label_formatter(&|x| {
                producers
                    .get(x.round() as usize)
                    .map_or(String::new(), usize::to_string)
            })
            .y_label_formatter(&|y| fmt_mmsgs(*y))
            .draw()
            .chart()?;

        draw_axis_labels(area, &producers, y_max, y_ticks)?;

        for (key, label, color) in SERIES {
            let by_producer: BTreeMap<usize, f64> = payload_rows
                .iter()
                .filter(|row| row.implementation == *key)
                .map(|row| (row.producers, row.msgs_per_sec / 1_000_000.0))
                .collect();
            if by_producer.is_empty() {
                continue;
            }
            let points: Vec<(f64, f64)> = producers
                .iter()
                .enumerate()
                .filter_map(|(i, producer)| by_producer.get(producer).map(|v| (i as f64, *v)))
                .collect();

            chart
                .draw_series(LineSeries::new(points.clone(), color.stroke_width(3)))
                .chart()?
                .label(*label)
                .legend(|(x, y)| {
                    PathElement::new(vec![(x, y), (x + 18, y)], color.stroke_width(3))
                });
            chart
                .draw_series(
                    points
                        .into_iter()
                        .map(|point| Circle::new(point, 2, color.filled())),
                )
                .chart()?;
        }
    }

    draw_legend(&legend_area, rows)?;
    Ok(())
}

fn draw_axis_labels(
    area: &DrawingArea<SVGBackend<'_>, plotters::coord::Shift>,
    producers: &[usize],
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

    let plot_left = 76;
    let plot_right = 927;
    let plot_top = 42;
    let plot_bottom = 213;
    let plot_w = plot_right - plot_left;
    let plot_h = plot_bottom - plot_top;
    let x_count = producers.len().saturating_sub(1).max(1);

    for (i, producer) in producers.iter().enumerate() {
        let x = plot_left + plot_w * i as i32 / x_count as i32;
        area.draw(&Text::new(
            producer.to_string(),
            (x, plot_bottom + 17),
            tick_style.clone(),
        ))
        .chart()?;
    }
    area.draw(&Text::new(
        "producer threads",
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
    area.draw(&Text::new("M msg/s", (12, plot_top - 14), y_axis_style))
        .chart()?;
    Ok(())
}

fn payload_title(rows: &[&Row]) -> String {
    if let Some(row) = rows.first() {
        let max_total_capacity = rows
            .iter()
            .map(|row| row.total_capacity)
            .max()
            .unwrap_or(row.total_capacity);
        format!(
            "{} payload ({} B), {:.2}s/sample, max total capacity {}",
            row.payload, row.payload_bytes, row.seconds, max_total_capacity
        )
    } else {
        "payload".to_string()
    }
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
        area.draw(&PathElement::new(
            vec![(x, y + 6), (x + 18, y + 6)],
            color.stroke_width(3),
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
