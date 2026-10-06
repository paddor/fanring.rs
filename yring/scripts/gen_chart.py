#!/usr/bin/env python3
"""Generate SPSC comparison SVG chart from benchmark results."""

import math
import argparse
import json
import statistics
from html import escape
import sys
from pathlib import Path

YRING_DIR = Path(__file__).resolve().parent.parent

CHANNEL_ORDER = [
    "yring (batch=1)",
    "yring (batch=64)",
    "rtrb per-item",
    "rtrb chunked",
    "crossbeam bounded",
    "flume bounded",
]

COLORS = {
    "yring (batch=1)":    ("#e85d04", "#b34803"),
    "yring (batch=64)":   ("#dc2626", "#a81e1e"),
    "rtrb per-item":      ("#2563eb", "#1d4ed8"),
    "rtrb chunked":       ("#7c3aed", "#6d28d9"),
    "crossbeam bounded":  ("#525c68", "#3d454f"),
    "flume bounded":      ("#16a34a", "#15803d"),
}

LABELS = {
    "yring (batch=1)":    "yring (flush every item)",
    "yring (batch=64)":   "yring (batch=64)",
    "rtrb per-item":      "rtrb v0.3 (per-item)",
    "rtrb chunked":       "rtrb v0.3 (chunk API, batch=64)",
    "crossbeam bounded":  "crossbeam-channel v0.5 (bounded MPMC)",
    "flume bounded":      "flume v0.12 (bounded MPMC)",
}

def load_results(path):
    runs = {}
    for line in path.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            runs.setdefault(row["run_id"], []).append(row)
    for run_id in sorted(runs, key=int, reverse=True):
        rows = runs[run_id]
        first = rows[0]
        expected = first["expected_rows"]
        samples = first["samples"]
        if (len(rows) != expected or expected != 24 * samples
                or samples <= 0 or first["producer_cpu"] == first["consumer_cpu"]):
            continue
        fields = ("expected_rows", "samples", "capacity", "duration_secs",
                  "warmup_secs", "producer_cpu", "consumer_cpu", "source_revision")
        if any(any(row[field] != first[field] for field in fields) for row in rows):
            continue
        groups = {}
        valid = True
        for payload in ("u64", "[u8; 32]", "[u8; 64]", "[u8; 128]"):
            groups[payload] = {}
            for index, channel in enumerate(CHANNEL_ORDER):
                selected = [row for row in rows if row["payload"] == payload
                            and row["channel"] == channel]
                if (len(selected) != samples
                        or {row["sample"] for row in selected} != set(range(samples))
                        or any(row["batch_size"] != (64 if index in (1, 3) else 1)
                               for row in selected)
                        or any(not math.isfinite(row["throughput_items_per_sec"])
                               or row["throughput_items_per_sec"] <= 0 for row in selected)):
                    valid = False
                    break
                groups[payload][channel] = statistics.median(
                    row["throughput_items_per_sec"] / 1_000_000 for row in selected)
            if not valid:
                break
        if valid:
            return groups, first
    raise ValueError("no complete, compatible SPSC comparison run")


def hardware_label():
    config = YRING_DIR.parent / ".chart_hw"
    if config.exists():
        fields = dict(line.split("=", 1) for line in config.read_text().splitlines()
                      if "=" in line)
        return ", ".join(fields[key] for key in ("prefix", "postfix") if fields.get(key))
    return ""


def nice_step(max_val, target_lines):
    raw = max_val / target_lines
    mag = 10 ** int(f"{raw:.0e}".split("e")[1])
    for s in [1, 2, 5, 10]:
        step = s * mag
        if max_val / step <= target_lines + 1:
            return step
    return mag * 10


def generate_chart(results, subtitle, hardware):
    groups = list(results)
    n_groups = len(groups)
    n_bars = len(CHANNEL_ORDER)

    svg_w = 900
    x_left, x_right = 70, 880
    plot_w = x_right - x_left

    top_margin = 74
    plot_h = 250
    p_top = top_margin
    p_bot = p_top + plot_h

    all_vals = [v for g in results.values() for v in g.values()]
    y_max = max(all_vals) * 1.15

    def y(v):
        return p_bot - (v / y_max) * plot_h

    group_w = plot_w / n_groups
    bar_w = min(group_w * 0.7 / n_bars, 50)
    inner_gap = bar_w * 0.15
    total_bars_w = n_bars * bar_w + (n_bars - 1) * inner_gap

    mid_x = svg_w / 2

    legend_items = [(k, LABELS[k]) for k in CHANNEL_ORDER]
    leg_row_h = 18
    leg_cols = 2
    leg_rows = math.ceil(len(legend_items) / leg_cols)

    svg_h = int(p_bot + 40 + leg_rows * leg_row_h + 20)

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="#0d1117"/>')

    # title
    L.append(
        f'  <text x="{mid_x}" y="22" text-anchor="middle" fill="#e6edf3"'
        f' font-size="14" font-weight="700">'
        f'SPSC channel throughput (M items/s, higher is better)'
        f'</text>'
    )
    # subtitle
    L.append(
        f'  <text x="{mid_x}" y="38" text-anchor="middle" fill="#7d8590"'
        f' font-size="10">{escape(subtitle)}</text>'
    )

    L.append(
        f'  <text x="{mid_x}" y="54" text-anchor="middle" fill="#7d8590"'
        f' font-size="10">{escape(hardware)}</text>'
    )

    # y-axis label
    y_mid = (p_top + p_bot) / 2
    L.append(
        f'  <text x="22" y="{y_mid}" text-anchor="middle" fill="#e6edf3"'
        f' font-size="11" font-weight="600"'
        f' transform="rotate(-90,22,{y_mid})">M items/s</text>'
    )

    # gridlines
    step = nice_step(y_max, 6)
    v = step
    while v <= y_max:
        yy = y(v)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#21262d" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#7d8590" font-size="10">'
            f'{v:.0f}</text>'
        )
        v += step

    # baseline
    L.append(
        f'  <line x1="{x_left}" y1="{p_bot}" x2="{x_right}" y2="{p_bot}"'
        f' stroke="#30363d" stroke-width="1.5"/>'
    )

    # bars
    for gi, group in enumerate(groups):
        gx = x_left + gi * group_w
        bar_start = gx + (group_w - total_bars_w) / 2

        for bi, channel in enumerate(CHANNEL_ORDER):
            val = results[group][channel]
            main_c, _ = COLORS[channel]
            bx = bar_start + bi * (bar_w + inner_gap)
            bh = (val / y_max) * plot_h
            by = p_bot - bh

            L.append(
                f'  <rect x="{bx:.1f}" y="{by:.1f}"'
                f' width="{bar_w:.1f}" height="{bh:.1f}"'
                f' fill="{main_c}" rx="1"/>'
            )

            # value label above bar
            label_y = by - 5
            if val >= 100:
                label = f"{val:.0f}"
            else:
                label = f"{val:.1f}"
            L.append(
                f'  <text x="{bx + bar_w / 2:.1f}" y="{label_y:.1f}"'
                f' text-anchor="middle" fill="#e6edf3" font-size="8"'
                f' font-weight="600">{label}</text>'
            )

        # group label
        gcx = gx + group_w / 2
        L.append(
            f'  <text x="{gcx:.1f}" y="{p_bot + 16}" text-anchor="middle"'
            f' fill="#e6edf3" font-size="11" font-weight="600">{group}</text>'
        )

    # legend
    leg_y = p_bot + 38
    leg_col_x = [mid_x - 220, mid_x + 30]
    for i, (key, label) in enumerate(legend_items):
        col = i // leg_rows
        row = i % leg_rows
        if col >= leg_cols:
            break
        lx = leg_col_x[col]
        ly = leg_y + row * leg_row_h
        main_c, _ = COLORS[key]
        L.append(
            f'  <rect x="{lx:.0f}" y="{ly - 5}" width="12" height="12"'
            f' fill="{main_c}" rx="2"/>'
        )
        L.append(
            f'  <text x="{lx + 18:.0f}" y="{ly + 5}" fill="#e6edf3"'
            f' font-size="10" font-weight="500">{label}</text>'
        )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path,
                        default=Path.home() / ".cache/yring/comparison.jsonl")
    parser.add_argument("--output", type=Path,
                        default=YRING_DIR / "doc/spsc_comparison.svg")
    args = parser.parse_args()
    results, row = load_results(args.results)
    subtitle = (f"cap={row['capacity']}, median of {row['samples']} x "
                f"{row['duration_secs']:g}s, producer CPU {row['producer_cpu']}, "
                f"consumer CPU {row['consumer_cpu']}")
    svg = generate_chart(results, subtitle, hardware_label())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(svg)
    print(f"Written: {args.output} (run {row['run_id']}, "
          f"source {row['source_revision']})", file=sys.stderr)


if __name__ == "__main__":
    main()
