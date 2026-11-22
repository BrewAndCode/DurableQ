#!/usr/bin/env python3
"""Generate deterministic SVG charts from DurableQ benchmark results."""

from __future__ import annotations

import argparse
import html
import json
import math
import statistics
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS = PROJECT_ROOT / "benchmarks" / "results.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "docs" / "assets" / "benchmark-charts"

INK = "#172b4d"
MUTED = "#526581"
GRID = "#d8e0eb"
BLUE = "#2864dc"
BLUE_LIGHT = "#dce8ff"
ORANGE = "#db6b26"
GREEN = "#21845b"
PURPLE = "#7253b5"
WHITE = "#ffffff"


def _number(value: float) -> str:
    """Format SVG coordinates consistently for byte-identical output."""
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _text(x: float, y: float, value: str, *, size: int = 16, fill: str = INK,
          anchor: str = "start", weight: str = "normal") -> str:
    return (
        f'<text x="{_number(x)}" y="{_number(y)}" font-size="{size}" '
        f'fill="{fill}" text-anchor="{anchor}" font-weight="{weight}">'
        f"{html.escape(value)}</text>"
    )


def _circle(x: float, y: float, radius: float, fill: str, *, stroke: str = WHITE) -> str:
    return (
        f'<circle cx="{_number(x)}" cy="{_number(y)}" r="{_number(radius)}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2"/>'
    )


def _base_svg(width: int, height: int, title: str, description: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
        f'height="{height}" viewBox="0 0 {width} {height}" role="img" '
        f'aria-labelledby="title desc">',
        '<title id="title">' + html.escape(title) + "</title>",
        '<desc id="desc">' + html.escape(description) + "</desc>",
        f'<rect width="{width}" height="{height}" rx="16" fill="{WHITE}"/>',
        '<style>text{font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}</style>',
    ]


def _finish_svg(parts: list[str]) -> str:
    return "\n".join([*parts, "</svg>", ""])


def _require_runs(group: dict, metric: str, section: str) -> list[float]:
    runs = group.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError(f"{section} entry has no raw runs")
    values = []
    for run in runs:
        value = run.get(metric)
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{section} run is missing a finite {metric}")
        values.append(float(value))
    return values


def _require_latency_runs(group: dict, percentile: str) -> list[float]:
    runs = group.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError("worker entry has no raw runs")
    values = []
    for run in runs:
        latency = run.get("latency_ms")
        value = latency.get(percentile) if isinstance(latency, dict) else None
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(
                f"worker run is missing a finite {percentile} latency value"
            )
        values.append(float(value))
    return values


def _load_results(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read benchmark results from {path}: {exc}") from exc
    for section in ("enqueue_throughput", "worker_concurrency"):
        if not isinstance(data.get(section), list) or not data[section]:
            raise ValueError(f"Benchmark results must contain {section} entries")
    return data


def _enqueue_chart(data: dict) -> str:
    groups = sorted(data["enqueue_throughput"], key=lambda group: group["jobs"])
    values = [(int(group["jobs"]), _require_runs(group, "jobs_per_second", "enqueue"))
              for group in groups]
    max_value = max(max(runs) for _, runs in values)
    axis_max = math.ceil(max_value / 100) * 100

    width, height = 1120, 560
    left, right, top, bottom = 130, 1000, 135, 440
    plot_width = right - left
    parts = _base_svg(
        width,
        height,
        "DurableQ enqueue throughput",
        "Median jobs enqueued per second by batch size. Dots show individual measured repetitions.",
    )
    parts.extend([
        _text(60, 52, "Enqueue throughput", size=28, weight="bold"),
        _text(60, 82, "Public enqueue API · median of measured runs; dots show each run", size=16, fill=MUTED),
    ])

    tick = 100
    for value in range(0, axis_max + 1, tick):
        x = left + plot_width * value / axis_max
        parts.append(f'<line x1="{_number(x)}" y1="{top}" x2="{_number(x)}" y2="{bottom}" stroke="{GRID}"/>')
        parts.append(_text(x, bottom + 28, str(value), size=14, fill=MUTED, anchor="middle"))

    row_gap = (bottom - top) / len(values)
    for index, (jobs, runs) in enumerate(values):
        y = top + row_gap * (index + 0.5)
        median = statistics.median(runs)
        bar_width = plot_width * median / axis_max
        parts.append(_text(left - 20, y + 6, f"{jobs:,} jobs", size=17, anchor="end", weight="bold"))
        parts.append(
            f'<rect x="{left}" y="{_number(y - 21)}" width="{_number(bar_width)}" '
            f'height="42" rx="8" fill="{BLUE_LIGHT}"/>'
        )
        for run_index, run_value in enumerate(runs):
            x = left + plot_width * run_value / axis_max
            offset = (run_index - (len(runs) - 1) / 2) * 12
            parts.append(_circle(x, y + offset, 6, BLUE))
        median_x = left + plot_width * median / axis_max
        parts.append(f'<line x1="{_number(median_x)}" y1="{_number(y - 23)}" x2="{_number(median_x)}" y2="{_number(y + 23)}" stroke="{PURPLE}" stroke-width="3"/>')
        parts.append(_text(min(median_x + 15, right + 10), y + 6, f"median {median:.2f}", size=15, fill=PURPLE, weight="bold"))

    parts.extend([
        _text((left + right) / 2, 500, "Enqueue throughput (jobs/second)", size=17, fill=INK, anchor="middle", weight="bold"),
        _circle(330, 535, 5, BLUE),
        _text(344, 540, "individual run", size=14, fill=MUTED),
        f'<line x1="500" y1="529" x2="500" y2="541" stroke="{PURPLE}" stroke-width="3"/>',
        _text(512, 540, "median", size=14, fill=MUTED),
    ])
    return _finish_svg(parts)


def _worker_chart(data: dict) -> str:
    groups = sorted(data["worker_concurrency"], key=lambda group: group["workers"])
    workers = [int(group["workers"]) for group in groups]
    throughputs = [_require_runs(group, "completed_jobs_per_second", "worker") for group in groups]
    latency = {
        percentile: [
            statistics.median(_require_latency_runs(group, percentile))
            for group in groups
        ]
        for percentile in ("p50", "p95", "p99")
    }

    width, height = 1240, 700
    parts = _base_svg(
        width,
        height,
        "DurableQ worker throughput and latency",
        "Median completed jobs per second and median per-run p50, p95, and p99 completion latency by worker thread count.",
    )
    parts.extend([
        _text(55, 50, "Worker throughput and latency", size=27, weight="bold"),
        _text(55, 80, "Worker instances run as threads in one process · medians across repetitions", size=15, fill=MUTED),
    ])

    # Left panel: median worker throughput, with raw repetitions overlaid.
    left_x0, left_x1, plot_y0, plot_y1 = 90, 570, 145, 520
    throughput_max = math.ceil(max(max(values) for values in throughputs) / 100) * 100
    for value in range(0, throughput_max + 1, 100):
        y = plot_y1 - (plot_y1 - plot_y0) * value / throughput_max
        parts.append(f'<line x1="{left_x0}" y1="{_number(y)}" x2="{left_x1}" y2="{_number(y)}" stroke="{GRID}"/>')
        parts.append(_text(left_x0 - 12, y + 5, str(value), size=12, fill=MUTED, anchor="end"))
    parts.append(_text((left_x0 + left_x1) / 2, 118, "Completed-job throughput", size=18, weight="bold", anchor="middle"))
    parts.append(_text((left_x0 + left_x1) / 2, 560, "Worker threads", size=15, fill=MUTED, anchor="middle"))
    parts.append(f'<text x="24" y="{_number((plot_y0 + plot_y1) / 2)}" transform="rotate(-90 24 {_number((plot_y0 + plot_y1) / 2)})" font-size="15" fill="{MUTED}" text-anchor="middle">Jobs/second</text>')
    x_step = (left_x1 - left_x0) / len(workers)
    for index, (count, runs) in enumerate(zip(workers, throughputs, strict=True)):
        x_center = left_x0 + x_step * (index + 0.5)
        median = statistics.median(runs)
        bar_height = (plot_y1 - plot_y0) * median / throughput_max
        parts.append(f'<rect x="{_number(x_center - 27)}" y="{_number(plot_y1 - bar_height)}" width="54" height="{_number(bar_height)}" rx="6" fill="{BLUE_LIGHT}"/>')
        for run_index, run_value in enumerate(runs):
            x = x_center + (run_index - (len(runs) - 1) / 2) * 10
            y = plot_y1 - (plot_y1 - plot_y0) * run_value / throughput_max
            parts.append(_circle(x, y, 5, BLUE))
        median_y = plot_y1 - (plot_y1 - plot_y0) * median / throughput_max
        parts.append(_text(x_center, median_y - 12, f"{median:.1f}", size=13, fill=PURPLE, anchor="middle", weight="bold"))
        parts.append(_text(x_center, plot_y1 + 23, str(count), size=14, fill=MUTED, anchor="middle"))

    # Right panel: median of per-run latency percentiles.
    right_x0, right_x1 = 720, 1160
    latency_max = math.ceil(max(max(values) for values in latency.values()) / 5000) * 5000
    for value in range(0, latency_max + 1, 5000):
        y = plot_y1 - (plot_y1 - plot_y0) * value / latency_max
        parts.append(f'<line x1="{right_x0}" y1="{_number(y)}" x2="{right_x1}" y2="{_number(y)}" stroke="{GRID}"/>')
        parts.append(_text(right_x0 - 12, y + 5, f"{value // 1000}k", size=12, fill=MUTED, anchor="end"))
    parts.append(_text((right_x0 + right_x1) / 2, 118, "Enqueue-to-completion latency", size=18, weight="bold", anchor="middle"))
    parts.append(_text((right_x0 + right_x1) / 2, 560, "Worker threads", size=15, fill=MUTED, anchor="middle"))
    parts.append(f'<text x="650" y="{_number((plot_y0 + plot_y1) / 2)}" transform="rotate(-90 650 {_number((plot_y0 + plot_y1) / 2)})" font-size="15" fill="{MUTED}" text-anchor="middle">Latency (milliseconds)</text>')
    x_step = (right_x1 - right_x0) / max(1, len(workers) - 1)
    x_positions = [right_x0 + x_step * index for index in range(len(workers))]
    for x, count in zip(x_positions, workers, strict=True):
        parts.append(_text(x, plot_y1 + 23, str(count), size=14, fill=MUTED, anchor="middle"))
    series = (("p50", BLUE), ("p95", ORANGE), ("p99", GREEN))
    for percentile, color in series:
        points = [
            (x, plot_y1 - (plot_y1 - plot_y0) * value / latency_max)
            for x, value in zip(x_positions, latency[percentile], strict=True)
        ]
        point_list = " ".join(f"{_number(x)},{_number(y)}" for x, y in points)
        parts.append(f'<polyline points="{point_list}" fill="none" stroke="{color}" stroke-width="3" stroke-linejoin="round"/>')
        for x, y in points:
            parts.append(_circle(x, y, 5, color))

    legend_y = 620
    legend_x = 395
    for label, color, gap in (("p50", BLUE, 0), ("p95", ORANGE, 110), ("p99", GREEN, 220)):
        parts.append(f'<line x1="{legend_x + gap}" y1="{legend_y - 5}" x2="{legend_x + gap + 24}" y2="{legend_y - 5}" stroke="{color}" stroke-width="3"/>')
        parts.append(_text(legend_x + gap + 32, legend_y, label, size=14, fill=MUTED))
    parts.append(_circle(840, legend_y - 5, 5, BLUE))
    parts.append(_text(854, legend_y, "throughput run", size=14, fill=MUTED))
    parts.append(_text(55, 665, "Latency uses PostgreSQL completed_at − created_at; the batch was enqueued before workers started.", size=13, fill=MUTED))
    return _finish_svg(parts)


def generate_charts(results_path: Path, output_dir: Path) -> tuple[Path, Path]:
    data = _load_results(results_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    enqueue_path = output_dir / "enqueue-throughput.svg"
    worker_path = output_dir / "worker-throughput-latency.svg"
    enqueue_path.write_text(_enqueue_chart(data), encoding="utf-8", newline="\n")
    worker_path.write_text(_worker_chart(data), encoding="utf-8", newline="\n")
    return enqueue_path, worker_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_RESULTS, help="Raw benchmark JSON input")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for generated SVG charts")
    args = parser.parse_args()
    try:
        generated = generate_charts(args.input, args.output_dir)
    except (KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))
    for path in generated:
        print(path)


if __name__ == "__main__":
    main()
