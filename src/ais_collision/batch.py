from __future__ import annotations

import argparse
import csv
import glob
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from .pipeline import (
    CollisionResult,
    PipelineConfig,
    build_spark,
    run_pipeline_for_paths,
    save_result_files,
    serialize_candidate,
    serialize_result,
)
from .plotting import plot_collision


@dataclass(frozen=True)
class DailyRun:
    source_file: str
    result: CollisionResult


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the AIS collision detector one daily CSV at a time and aggregate the "
            "best December candidate."
        )
    )
    parser.add_argument(
        "--input-glob",
        default="Data/aisdk-2021-12-*.csv",
        help="Glob that matches the December AIS CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default="output/full-month",
        help="Directory for aggregated month-level outputs.",
    )
    parser.add_argument("--master", default="local[4]", help="Spark master URL.")
    parser.add_argument(
        "--driver-memory",
        default="8g",
        help="Spark driver memory allocation, for example 4g or 8g.",
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
        help="How many ranked encounter candidates to keep overall.",
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
    output_dir = Path(args.output_dir)
    cleanup_output_dir(output_dir)

    input_paths = sorted(glob.glob(args.input_glob))
    if not input_paths:
        raise FileNotFoundError(f"No AIS files matched input glob: {args.input_glob}")

    config = PipelineConfig(
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

    successful_runs: list[DailyRun] = []
    failures: list[dict[str, str]] = []
    spark = build_spark(config)
    try:
        for index, input_path in enumerate(input_paths, start=1):
            input_file = Path(input_path)
            print(f"[{index}/{len(input_paths)}] Processing {input_file.name}")

            try:
                result = run_pipeline_for_paths(spark, [str(input_file)], config)
            except RuntimeError as error:
                failures.append(
                    {
                        "file": input_file.name,
                        "reason": str(error),
                    }
                )
                continue
            except Exception as error:
                failures.append(
                    {
                        "file": input_file.name,
                        "reason": f"{type(error).__name__}: {error}",
                    }
                )
                continue

            successful_runs.append(DailyRun(source_file=input_file.name, result=result))
    finally:
        spark.stop()

    if not successful_runs:
        raise RuntimeError("No daily AIS runs completed successfully.")

    selected_run = min(successful_runs, key=run_sort_key)
    selected_result = selected_run.result
    combined_candidates = [
        dict(serialize_candidate(candidate), source_file=run.source_file)
        for run in successful_runs
        for candidate in run.result.top_candidates
    ]
    combined_candidates.sort(key=candidate_sort_key)
    combined_candidates = combined_candidates[: args.top_n_candidates]

    summary_payload = serialize_result(selected_result)
    summary_payload["top_candidates"] = combined_candidates
    summary_payload["processed_files"] = [run.source_file for run in successful_runs]
    summary_payload["failed_files"] = failures

    files = save_result_files(output_dir, selected_result)
    plot_collision(selected_result, output_dir / "collision_trajectory.png")

    (output_dir / "collision_summary.json").write_text(
        json.dumps(summary_payload, indent=2),
        encoding="utf-8",
    )
    write_candidates_csv(output_dir / "top_collision_candidates.csv", combined_candidates)

    selected_candidate = summary_payload["selected_candidate"]
    print("Selected collision candidate")
    print(
        f"  Source file: {selected_run.source_file}\n"
        f"  MMSI pair: {selected_candidate['mmsi_1']} / {selected_candidate['mmsi_2']}\n"
        f"  Vessel names: {selected_candidate['vessel_name_1']} / {selected_candidate['vessel_name_2']}\n"
        f"  Collision timestamp: {selected_candidate['collision_timestamp']}\n"
        f"  Collision coordinates: {selected_candidate['collision_latitude']}, {selected_candidate['collision_longitude']}\n"
        f"  Closest observed distance: {selected_candidate['distance_m']} m"
    )
    if failures:
        print(f"  Failed daily files: {len(failures)}")
    print(f"  Aggregated summary JSON: {output_dir / 'collision_summary.json'}")
    print(f"  Candidate CSV: {output_dir / 'top_collision_candidates.csv'}")
    print(f"  Plot PNG: {output_dir / 'collision_trajectory.png'}")


def cleanup_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for legacy_path in [output_dir / "daily", output_dir / "selected_day_summary.json"]:
        if legacy_path.is_dir():
            shutil.rmtree(legacy_path)
        elif legacy_path.exists():
            legacy_path.unlink()


def run_sort_key(run: DailyRun) -> tuple[float, str]:
    selected = run.result.selected_candidate
    return float(selected.distance_m), selected.collision_timestamp.isoformat()


def candidate_sort_key(candidate: dict) -> tuple[float, str]:
    return float(candidate["distance_m"]), candidate["collision_timestamp"]


def write_candidates_csv(output_path: Path, candidates: list[dict]) -> None:
    if not candidates:
        return
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = list(candidates[0].keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(candidate)


if __name__ == "__main__":
    main()