from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from .pipeline import CollisionResult


def _format_marker_label(prefix: str, point_index: int, total_points: int, timestamp: object) -> str:
    if hasattr(timestamp, "strftime"):
        time_label = timestamp.strftime("%H:%M:%S")
    else:
        time_label = str(timestamp)
    return f"{prefix} ({point_index}/{total_points})\n{time_label} UTC"


def plot_collision(result: CollisionResult, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(11, 8.5), constrained_layout=True)
    colors = ["#005f73", "#bb3e03"]

    for index, (mmsi, points) in enumerate(sorted(result.trajectories.items())):
        if not points:
            continue
        color = colors[index % len(colors)]
        lons = [point.longitude for point in points]
        lats = [point.latitude for point in points]
        label = f"{points[0].vessel_name} ({mmsi})"
        ax.plot(
            lons,
            lats,
            color=color,
            linewidth=2.0,
            marker="o",
            markersize=3.0,
            alpha=0.8,
            label=label,
        )

        if len(points) > 1:
            ax.plot(
                lons[:2],
                lats[:2],
                color=color,
                linewidth=4.4,
                alpha=0.95,
                solid_capstyle="round",
            )
            start_dx = lons[1] - lons[0]
            start_dy = lats[1] - lats[0]
            ax.annotate(
                "",
                xy=(lons[1], lats[1]),
                xytext=(lons[0], lats[0]),
                arrowprops={
                    "arrowstyle": "-|>",
                    "color": color,
                    "linewidth": 2.0,
                    "mutation_scale": 16,
                    "shrinkA": 0,
                    "shrinkB": 0,
                },
                zorder=6,
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
            s=150,
            edgecolors="white",
            linewidths=1.2,
            zorder=7,
        )

        start_offset_x = 12 if start_dx >= 0 else -96
        start_offset_y = 14 if start_dy >= 0 else -28
        end_offset_x = 12 if len(points) == 1 or (lons[-1] - lons[-2]) >= 0 else -92
        end_offset_y = 12 if len(points) == 1 or (lats[-1] - lats[-2]) >= 0 else -26

        ax.annotate(
            _format_marker_label("Start", 1, len(points), points[0].timestamp),
            (lons[0], lats[0]),
            textcoords="offset points",
            xytext=(start_offset_x, start_offset_y),
            fontsize=9,
            fontweight="bold",
            color=color,
            bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": color, "alpha": 0.92},
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
        )

    candidate = result.selected_candidate
    ax.scatter(
        [candidate.collision_longitude],
        [candidate.collision_latitude],
        color="#ae2012",
        marker="X",
        s=120,
        label="Closest approach",
        zorder=5,
    )
    ax.annotate(
        candidate.collision_timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
        (candidate.collision_longitude, candidate.collision_latitude),
        textcoords="offset points",
        xytext=(10, 10),
        fontsize=10,
        bbox={"boxstyle": "round,pad=0.3", "fc": "white", "ec": "#999999"},
    )

    ax.set_title("AIS trajectories 20 minutes before and after the selected collision candidate")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, alpha=0.25)
    ax.text(
        0.015,
        0.985,
        "Large circle = start\nArrow = initial direction\nLarge X = end",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.3", "fc": "white", "ec": "#c8c8c8", "alpha": 0.92},
    )
    ax.legend(loc="best")

    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path
