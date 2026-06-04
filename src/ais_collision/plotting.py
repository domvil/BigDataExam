from __future__ import annotations

from math import hypot
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

from .pipeline import CollisionResult


def _format_marker_label(prefix: str, point_index: int, total_points: int, timestamp: object) -> str:
    if hasattr(timestamp, "strftime"):
        time_label = timestamp.strftime("%H:%M:%S")
    else:
        time_label = str(timestamp)
    return f"{prefix} ({point_index}/{total_points})\n{time_label} UTC"


def _near_event(point_lon: float, point_lat: float, event_lon: float, event_lat: float) -> bool:
    return hypot(point_lon - event_lon, point_lat - event_lat) <= 0.004


def plot_collision(result: CollisionResult, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(11, 8.5), constrained_layout=True)
    fig.patch.set_facecolor("#eef7fb")
    ax.set_facecolor("#d8edf7")
    colors = ["#0f766e", "#c2410c"]
    line_effect = [pe.Stroke(linewidth=4.8, foreground="white", alpha=0.75), pe.Normal()]
    candidate = result.selected_candidate

    for index, (mmsi, points) in enumerate(sorted(result.trajectories.items())):
        if not points:
            continue
        color = colors[index % len(colors)]
        lons = [point.longitude for point in points]
        lats = [point.latitude for point in points]
        label = f"{points[0].vessel_name} ({mmsi})"
        (track_line,) = ax.plot(
            lons,
            lats,
            color=color,
            linewidth=2.3,
            marker="o",
            markersize=3.2,
            alpha=0.84,
            label=label,
            zorder=4,
        )
        track_line.set_path_effects(line_effect)

        if len(points) > 1:
            ax.plot(
                lons[:2],
                lats[:2],
                color=color,
                linewidth=4.4,
                alpha=0.95,
                solid_capstyle="round",
                zorder=5,
            )
            start_dx = lons[1] - lons[0]
            start_dy = lats[1] - lats[0]
            direction_arrow = ax.annotate(
                "",
                xy=(lons[1], lats[1]),
                xytext=(lons[0], lats[0]),
                arrowprops={
                    "arrowstyle": "-|>",
                    "color": color,
                    "linewidth": 2.8,
                    "mutation_scale": 22,
                    "shrinkA": 0,
                    "shrinkB": 0,
                },
                zorder=8,
            )
            if direction_arrow.arrow_patch is not None:
                direction_arrow.arrow_patch.set_path_effects(
                    [pe.Stroke(linewidth=5.2, foreground="white", alpha=0.92), pe.Normal()]
                )
        else:
            start_dx = 0.0
            start_dy = 0.0

        ax.scatter(
            lons[0],
            lats[0],
            color=color,
            marker="o",
            s=135,
            edgecolors="white",
            linewidths=1.4,
            zorder=7,
        )
        ax.scatter(
            lons[-1],
            lats[-1],
            color=color,
            marker="X",
            s=230,
            edgecolors="white",
            linewidths=1.8,
            zorder=9,
        )
        ax.scatter(
            lons[-1],
            lats[-1],
            facecolors="none",
            edgecolors="white",
            linewidths=2.2,
            s=360,
            alpha=0.9,
            zorder=8,
        )

        start_offset_x = 12 if start_dx >= 0 else -96
        start_offset_y = 14 if start_dy >= 0 else -28
        end_offset_x = 12 if len(points) == 1 or (lons[-1] - lons[-2]) >= 0 else -92
        end_offset_y = 12 if len(points) == 1 or (lats[-1] - lats[-2]) >= 0 else -26
        if _near_event(
            lons[-1],
            lats[-1],
            candidate.collision_longitude,
            candidate.collision_latitude,
        ):
            end_offset_x = -120 if end_offset_x > 0 else end_offset_x
            end_offset_y = -34 if end_offset_y > 0 else -38

        ax.annotate(
            _format_marker_label("Start", 1, len(points), points[0].timestamp),
            (lons[0], lats[0]),
            textcoords="offset points",
            xytext=(start_offset_x, start_offset_y),
            fontsize=9,
            fontweight="bold",
            color=color,
            bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": color, "alpha": 0.92},
            zorder=10,
        )
        ax.annotate(
            _format_marker_label("End", len(points), len(points), points[-1].timestamp),
            (lons[-1], lats[-1]),
            textcoords="offset points",
            xytext=(end_offset_x, end_offset_y),
            fontsize=9,
            fontweight="bold",
            color=color,
            bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": color, "alpha": 0.92},
            arrowprops={"arrowstyle": "->", "color": color, "linewidth": 1.2},
            zorder=10,
        )

    ax.scatter(
        [candidate.collision_longitude],
        [candidate.collision_latitude],
        facecolors="none",
        edgecolors="#ef4444",
        linewidths=2.2,
        s=420,
        alpha=0.5,
        zorder=10,
    )
    ax.scatter(
        [candidate.collision_longitude],
        [candidate.collision_latitude],
        color="white",
        edgecolors="#7f1d1d",
        linewidths=1.8,
        s=210,
        zorder=11,
    )
    ax.scatter(
        [candidate.collision_longitude],
        [candidate.collision_latitude],
        color="#b91c1c",
        marker="X",
        s=190,
        edgecolors="white",
        linewidths=1.4,
        label="Potential collision",
        zorder=12,
    )
    ax.annotate(
        "Potential collision\n"
        f"{candidate.collision_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        (candidate.collision_longitude, candidate.collision_latitude),
        textcoords="offset points",
        xytext=(16, 16),
        fontsize=10,
        color="#7f1d1d",
        fontweight="bold",
        bbox={"boxstyle": "round,pad=0.35", "fc": "#fff8f7", "ec": "#b45309", "alpha": 0.97},
        arrowprops={"arrowstyle": "->", "color": "#7f1d1d", "linewidth": 1.6},
        zorder=13,
    )

    ax.set_title(
        "AIS trajectories in a 20-minute window around the selected collision candidate",
        fontsize=15,
        fontweight="bold",
        pad=14,
        color="#17384d",
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, color="white", linewidth=1.0, alpha=0.72)
    ax.margins(x=0.06, y=0.06)
    for spine in ax.spines.values():
        spine.set_color("#8fb0c4")
        spine.set_linewidth(1.2)
    ax.tick_params(colors="#35586b")
    ax.text(
        0.015,
        0.985,
        "Large circle = start\nArrowhead = initial direction\nLarge X = end",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.3", "fc": "white", "ec": "#b8cfdd", "alpha": 0.94},
    )
    ax.legend(loc="best")

    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path
