//! Drawing primitives shared by the latency and summary bar charts.

use std::path::Path;

use plotters::coord::Shift;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use super::{ChartError, ChartResult, DrawResultExt};

pub(super) const BG: RGBColor = RGBColor(0x0d, 0x11, 0x17);
pub(super) const GRID: RGBColor = RGBColor(0x21, 0x26, 0x2d);
pub(super) const AXIS: RGBColor = RGBColor(0x30, 0x36, 0x3d);
pub(super) const TEXT: RGBColor = RGBColor(0xe6, 0xed, 0xf3);
pub(super) const MUTED: RGBColor = RGBColor(0x7d, 0x85, 0x90);

pub(super) type Area<'a> = DrawingArea<SVGBackend<'a>, Shift>;

/// Pixel bounds of the plot region and the value at its top edge.
#[derive(Clone, Copy)]
pub(super) struct Plot {
    pub(super) left: f64,
    pub(super) right: f64,
    pub(super) top: f64,
    pub(super) bottom: f64,
    pub(super) y_max: f64,
}

impl Plot {
    /// Returns the pixel row of `value`.
    pub(super) fn y(self, value: f64) -> f64 {
        self.bottom - value / self.y_max * (self.bottom - self.top)
    }
}

/// Horizontal extent and color of one bar.
#[derive(Clone, Copy)]
pub(super) struct Bar {
    pub(super) x: f64,
    pub(super) width: f64,
    pub(super) color: RGBColor,
}

/// Size, color, horizontal anchor, and weight of a text label.
#[derive(Clone, Copy)]
pub(super) struct Font {
    pub(super) size: u32,
    pub(super) color: RGBColor,
    pub(super) horizontal: HPos,
    pub(super) bold: bool,
}

impl Font {
    pub(super) const fn regular(size: u32, color: RGBColor, horizontal: HPos) -> Self {
        Self {
            size,
            color,
            horizontal,
            bold: false,
        }
    }

    pub(super) const fn bold(size: u32, color: RGBColor, horizontal: HPos) -> Self {
        Self {
            size,
            color,
            horizontal,
            bold: true,
        }
    }
}

pub(super) fn text(
    area: &Area<'_>,
    value: impl Into<String>,
    position: (i32, i32),
    font: Font,
) -> ChartResult<()> {
    let mut style = ("sans-serif", font.size).into_font();
    if font.bold {
        style = style.style(FontStyle::Bold);
    }
    area.draw(&Text::new(
        value.into(),
        position,
        TextStyle::from(style)
            .color(&font.color)
            .pos(Pos::new(font.horizontal, VPos::Center)),
    ))
    .chart()
}

pub(super) fn rect(
    area: &Area<'_>,
    (x1, y1): (f64, f64),
    (x2, y2): (f64, f64),
    color: RGBColor,
) -> ChartResult<()> {
    area.draw(&Rectangle::new(
        [(px(x1), px(y1)), (px(x2), px(y2))],
        ShapeStyle::from(&color).filled(),
    ))
    .chart()
}

pub(super) fn line(
    area: &Area<'_>,
    (x1, y1): (f64, f64),
    (x2, y2): (f64, f64),
    color: RGBColor,
    width: u32,
) -> ChartResult<()> {
    area.draw(&PathElement::new(
        vec![(px(x1), px(y1)), (px(x2), px(y2))],
        color.stroke_width(width),
    ))
    .chart()
}

pub(super) fn finish_svg(path: &Path, width: u32, height: u32) -> ChartResult<()> {
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

pub(super) fn px(value: f64) -> i32 {
    value.round() as i32
}
