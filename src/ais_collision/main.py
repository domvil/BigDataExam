from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import PipelineConfig, build_spark, run_pipeline, save_result_files
from .plotting import plot_collision


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect the closest AIS vessel encounter inside the assignment area using PySpark."
    )
    parser.add_argument(
        "--input-glob",
        default="Data/aisdk-2021-12-*.csv",
        help="Glob that matches the December AIS CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for JSON, CSV, and plot outputs.",
    )
    parser.add_argument("--master", default="local[4]", help="Spark master URL.")
    parser.add_argument(
        "--driver-memory",
        default="8g",
        help="Spark driver memory allocation, for example 4g or 8g.",
    )
    parser.add_argument(
        "--center-lat",
        type=float,
        default=55.225,
        help="Latitude of the assignment search center.",
    )
    parser.add_argument(
        "--center-lon",
        type=float,
        default=14.245,
        help="Longitude of the assignment search center.",
    )
    parser.add_argument(
        "--radius-nm",
        type=float,
        default=50.0,
        help="Search radius in nautical miles.",
    )
    parser.add_argument(
        "--moving-sog-knots",
        type=float,
        default=1.5,
        help="Minimum speed over ground to treat a position as moving.",
    )
    parser.add_argument(
        "--candidate-distance-m",
        type=float,
        default=100.0,
        help="Maximum observed separation for candidate encounters.",
    )
    parser.add_argument(
        "--gps-speed-cap-knots",
        type=float,
        default=70.0,
        help="Maximum implied speed used to filter GPS jumps.",
    )
    parser.add_argument(
        "--grid-size-m",
        type=float,
        default=150.0,
        help="Spatial bucket size used before the self-join.",
    )
    parser.add_argument(
        "--max-time-delta-s",
        type=int,
        default=30,
        help="Maximum timestamp gap allowed between paired AIS messages.",
    )
    parser.add_argument(
        "--top-n-candidates",
        type=int,
        default=12,
        help="How many ranked encounter candidates to evaluate with trajectory windows.",
    )
    parser.add_argument(
        "--trajectory-window-minutes",
        type=int,
        default=10,
        help="Minutes before and after the selected event to export and plot.",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=400,
        help="Spark shuffle partition count.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig(
        center_lat=args.center_lat,
        center_lon=args.center_lon,
        radius_nm=args.radius_nm,
        moving_sog_knots=args.moving_sog_knots,
        candidate_distance_m=args.candidate_distance_m,
        gps_speed_cap_knots=args.gps_speed_cap_knots,
        grid_size_m=args.grid_size_m,
        max_time_delta_s=args.max_time_delta_s,
        top_n_candidates=args.top_n_candidates,
        trajectory_window_minutes=args.trajectory_window_minutes,
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=args.driver_memory,
        master=args.master,
    )

    spark = build_spark(config)
    try:
        result = run_pipeline(spark, args.input_glob, config)
    finally:
        spark.stop()

    output_dir = Path(args.output_dir)
    files = save_result_files(output_dir, result)
    plot_path = plot_collision(result, output_dir / "collision_trajectory.png")

    candidate = result.selected_candidate
    movement_note = "passed" if result.movement_filter_passed else "not passed"
    print("Selected collision candidate")
    print(f"  MMSI pair: {candidate.mmsi_1} / {candidate.mmsi_2}")
    print(f"  Vessel names: {candidate.vessel_name_1} / {candidate.vessel_name_2}")
    print(f"  Collision timestamp (midpoint): {candidate.collision_timestamp.isoformat(sep=' ')}")
    print(
        "  Collision coordinates: "
        f"{candidate.collision_latitude:.6f}, {candidate.collision_longitude:.6f}"
    )
    print(f"  Closest observed distance: {candidate.distance_m:.2f} m")
    print(f"  Trajectory movement filter: {movement_note}")
    if result.notes:
        print(f"  Notes: {result.notes}")
    print(f"  Summary JSON: {files['summary']}")
    print(f"  Candidate CSV: {files['candidates']}")
    print(f"  Trajectory CSV: {files['trajectory']}")
    print(f"  Plot PNG: {plot_path}")


if __name__ == "__main__":
    main()
