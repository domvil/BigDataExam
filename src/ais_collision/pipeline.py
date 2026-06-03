from __future__ import annotations

import csv
import glob
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from math import atan2, cos, degrees, radians, sin, sqrt
from pathlib import Path
from statistics import mean

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

EARTH_RADIUS_M = 6_371_000.0
METERS_PER_DEGREE_LAT = 111_320.0
KNOTS_PER_MPS = 1.9438444924406
NM_TO_M = 1_852.0
SERVICE_VESSEL_KEYWORDS = (
    "rescue",
    "kbv",
    "kyst",
    "coast",
    "pilot",
    "patrol",
    "sar",
)


@dataclass(frozen=True)
class PipelineConfig:
    center_lat: float = 55.225
    center_lon: float = 14.245
    radius_nm: float = 50.0
    moving_sog_knots: float = 1.5
    gps_speed_cap_knots: float = 70.0
    candidate_distance_m: float = 100.0
    grid_size_m: float = 150.0
    max_time_delta_s: int = 30
    top_n_candidates: int = 12
    trajectory_window_minutes: int = 10
    min_trajectory_points: int = 3
    min_window_path_m: float = 1_000.0
    min_window_displacement_m: float = 250.0
    synchronized_distance_m: float = 25.0
    synchronized_step_seconds: int = 5
    parallel_heading_delta_deg: float = 12.0
    parallel_distance_m: float = 30.0
    parallel_duration_s: int = 120
    route_overlap_distance_m: float = 35.0
    route_overlap_fraction: float = 0.6
    crossing_candidate_distance_m: float = 8.0
    crossing_time_delta_min_s: int = 15
    crossing_synchronized_distance_m: float = 100.0
    crossing_heading_delta_deg: float = 20.0
    crossing_route_overlap_fraction: float = 0.2
    crossing_min_combined_sog_knots: float = 8.0
    anomaly_track_end_seconds: int = 30
    anomaly_window_seconds: int = 180
    anomaly_min_pre_sog_knots: float = 4.0
    anomaly_speed_drop_knots: float = 3.0
    anomaly_speed_drop_fraction: float = 0.35
    shuffle_partitions: int = 400
    driver_memory: str = "8g"
    master: str = "local[4]"


@dataclass(frozen=True)
class CollisionCandidate:
    mmsi_1: int
    mmsi_2: int
    vessel_name_1: str | None
    vessel_name_2: str | None
    left_timestamp: datetime
    right_timestamp: datetime
    collision_timestamp: datetime
    collision_latitude: float
    collision_longitude: float
    distance_m: float
    time_delta_s: int
    sog_1: float | None
    sog_2: float | None
    status_1: str | None
    status_2: str | None


@dataclass(frozen=True)
class TrajectoryPoint:
    mmsi: int
    vessel_name: str | None
    timestamp: datetime
    latitude: float
    longitude: float
    sog: float | None


@dataclass(frozen=True)
class TrajectoryMetrics:
    mmsi: int
    point_count: int
    average_sog: float
    path_length_m: float
    displacement_m: float


@dataclass(frozen=True)
class EncounterAssessment:
    synchronized_min_distance_m: float
    synchronized_min_timestamp: datetime | None
    overlap_duration_s: int
    close_duration_s: int
    heading_delta_deg: float | None
    route_overlap_fraction: float
    track_end_anomaly_count: int
    speed_drop_anomaly_count: int
    post_event_anomaly: bool
    parallel_encounter: bool
    confirmed_collision_like: bool


@dataclass(frozen=True)
class CollisionResult:
    selected_candidate: CollisionCandidate
    top_candidates: list[CollisionCandidate]
    trajectories: dict[int, list[TrajectoryPoint]]
    metrics: dict[int, TrajectoryMetrics]
    movement_filter_passed: bool
    assessment: EncounterAssessment | None = None
    notes: str | None = None


def build_spark(config: PipelineConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName("ais-collision-detector")
        .master(config.master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", config.driver_memory)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_pipeline(
    spark: SparkSession,
    input_glob: str,
    config: PipelineConfig,
) -> CollisionResult:
    input_paths = resolve_input_paths(input_glob)
    if len(input_paths) == 1:
        return run_pipeline_for_paths(spark, input_paths, config)

    daily_results: list[CollisionResult] = []
    global_candidates: list[CollisionCandidate] = []
    for input_path in input_paths:
        try:
            day_result = run_pipeline_for_paths(spark, [input_path], config)
        except RuntimeError:
            continue
        daily_results.append(day_result)
        global_candidates.extend(day_result.top_candidates)

    if not daily_results:
        raise RuntimeError(
            "No collision candidates were found with the current filters. "
            "Try increasing --candidate-distance-m or reducing --moving-sog-knots."
        )

    candidate_pool = sorted(global_candidates, key=candidate_sort_key)[: config.top_n_candidates]
    ranked_results = [result for result in daily_results if result.movement_filter_passed]
    if not ranked_results:
        ranked_results = daily_results

    winning_result = min(ranked_results, key=result_sort_key)
    return CollisionResult(
        selected_candidate=winning_result.selected_candidate,
        top_candidates=candidate_pool,
        trajectories=winning_result.trajectories,
        metrics=winning_result.metrics,
        movement_filter_passed=winning_result.movement_filter_passed,
        notes=winning_result.notes,
    )


def run_pipeline_for_paths(
    spark: SparkSession,
    input_paths: list[str],
    config: PipelineConfig,
) -> CollisionResult:
    cleaned = prepare_clean_region_dataset(spark, input_paths[0], config)
    candidates_df = find_collision_candidates(cleaned, config)
    candidate_rows = candidates_df.limit(config.top_n_candidates).collect()

    if not candidate_rows:
        raise RuntimeError(
            "No collision candidates were found with the current filters. "
            "Try increasing --candidate-distance-m or reducing --moving-sog-knots."
        )

    candidates = [row_to_candidate(row) for row in candidate_rows]
    return select_best_candidate(cleaned, candidates, config)


def save_result_files(output_dir: Path, result: CollisionResult) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_path = output_dir / "collision_summary.json"
    candidates_path = output_dir / "top_collision_candidates.csv"
    trajectory_path = output_dir / (
        f"trajectory_{result.selected_candidate.mmsi_1}_"
        f"{result.selected_candidate.mmsi_2}.csv"
    )

    summary_payload = serialize_result(result)
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    with candidates_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = list(serialize_candidate(result.top_candidates[0]).keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for candidate in result.top_candidates:
            writer.writerow(serialize_candidate(candidate))

    with trajectory_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "mmsi",
                "vessel_name",
                "timestamp",
                "latitude",
                "longitude",
                "sog",
            ],
        )
        writer.writeheader()
        for mmsi in sorted(result.trajectories):
            for point in result.trajectories[mmsi]:
                writer.writerow(
                    {
                        "mmsi": point.mmsi,
                        "vessel_name": point.vessel_name,
                        "timestamp": point.timestamp.isoformat(sep=" "),
                        "latitude": f"{point.latitude:.6f}",
                        "longitude": f"{point.longitude:.6f}",
                        "sog": "" if point.sog is None else f"{point.sog:.2f}",
                    }
                )

    return {
        "summary": summary_path,
        "candidates": candidates_path,
        "trajectory": trajectory_path,
    }


def serialize_result(result: CollisionResult) -> dict[str, object]:
    return {
        "selected_candidate": serialize_candidate(result.selected_candidate),
        "movement_filter_passed": result.movement_filter_passed,
        "encounter_assessment": None if result.assessment is None else serialize_assessment(result.assessment),
        "notes": result.notes,
        "trajectory_metrics": {
            str(mmsi): asdict(metrics) for mmsi, metrics in result.metrics.items()
        },
        "top_candidates": [serialize_candidate(candidate) for candidate in result.top_candidates],
    }


def prepare_clean_region_dataset(
    spark: SparkSession,
    input_glob: str,
    config: PipelineConfig,
) -> DataFrame:
    raw = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(resolve_input_paths(input_glob))
    )
    radius_m = config.radius_nm * NM_TO_M
    lat_pad = radius_m / METERS_PER_DEGREE_LAT
    lon_pad = radius_m / (METERS_PER_DEGREE_LAT * safe_cosine(config.center_lat))

    typed = (
        raw.select(
            F.to_timestamp(F.col("# Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("event_ts"),
            normalize_string("Type of mobile").alias("mobile_type"),
            parse_long("MMSI").alias("mmsi"),
            parse_double("Latitude").alias("latitude"),
            parse_double("Longitude").alias("longitude"),
            normalize_string("Navigational status").alias("status"),
            parse_double("SOG").alias("sog"),
            parse_double("COG").alias("cog"),
            parse_double("Heading").alias("heading"),
            normalize_string("Callsign").alias("callsign"),
            normalize_string("Name").alias("name"),
            normalize_string("Ship type").alias("ship_type"),
        )
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("mmsi").isNotNull())
        .filter(F.col("latitude").isNotNull())
        .filter(F.col("longitude").isNotNull())
        .filter(F.col("mobile_type").isin("Class A", "Class B"))
        .filter(F.col("latitude").between(-90.0, 90.0))
        .filter(F.col("longitude").between(-180.0, 180.0))
        .filter(F.col("latitude").between(config.center_lat - lat_pad, config.center_lat + lat_pad))
        .filter(F.col("longitude").between(config.center_lon - lon_pad, config.center_lon + lon_pad))
        .withColumn(
            "distance_to_center_m",
            haversine_expr(
                F.col("latitude"),
                F.col("longitude"),
                F.lit(config.center_lat),
                F.lit(config.center_lon),
            ),
        )
        .filter(F.col("distance_to_center_m") <= F.lit(radius_m))
    )

    region = typed

    excluded_statuses = ["at anchor", "moored", "aground"]
    region = region.filter(
        ~F.lower(F.coalesce(F.col("status"), F.lit(""))).isin(*excluded_statuses)
    )

    vessel_window = Window.partitionBy("mmsi").orderBy("event_ts")
    with_jumps = (
        region.withColumn("event_epoch", F.col("event_ts").cast("long"))
        .withColumn("prev_ts", F.lag("event_ts").over(vessel_window))
        .withColumn("prev_epoch", F.lag("event_epoch").over(vessel_window))
        .withColumn("prev_latitude", F.lag("latitude").over(vessel_window))
        .withColumn("prev_longitude", F.lag("longitude").over(vessel_window))
        .withColumn(
            "jump_seconds",
            F.when(F.col("prev_epoch").isNull(), None).otherwise(
                F.col("event_epoch") - F.col("prev_epoch")
            ),
        )
        .withColumn(
            "jump_m",
            F.when(F.col("prev_latitude").isNull(), None).otherwise(
                haversine_expr(
                    F.col("latitude"),
                    F.col("longitude"),
                    F.col("prev_latitude"),
                    F.col("prev_longitude"),
                )
            ),
        )
        .withColumn(
            "implied_speed_knots",
            F.when(
                F.col("jump_seconds") > 0,
                F.col("jump_m") / F.col("jump_seconds") * F.lit(KNOTS_PER_MPS),
            ),
        )
    )

    return with_jumps.filter(
        F.col("implied_speed_knots").isNull()
        | (F.col("implied_speed_knots") <= F.lit(config.gps_speed_cap_knots))
    ).drop(
        "prev_ts",
        "prev_epoch",
        "prev_latitude",
        "prev_longitude",
        "jump_seconds",
        "jump_m",
        "implied_speed_knots",
    )


def find_collision_candidates(cleaned: DataFrame, config: PipelineConfig) -> DataFrame:
    lat_cell_size = config.grid_size_m / METERS_PER_DEGREE_LAT
    lon_cell_size = config.grid_size_m / (
        METERS_PER_DEGREE_LAT * safe_cosine(config.center_lat)
    )
    max_lat_diff = config.candidate_distance_m / METERS_PER_DEGREE_LAT
    max_lon_diff = config.candidate_distance_m / (
        METERS_PER_DEGREE_LAT * safe_cosine(config.center_lat)
    )

    moving = (
        cleaned.filter(F.col("sog").isNotNull())
        .filter(F.col("sog") >= F.lit(config.moving_sog_knots))
        .withColumn("event_minute", F.date_trunc("minute", F.col("event_ts")))
        .withColumn(
            "lat_bucket",
            F.floor((F.col("latitude") - F.lit(config.center_lat)) / F.lit(lat_cell_size)).cast(
                "int"
            ),
        )
        .withColumn(
            "lon_bucket",
            F.floor((F.col("longitude") - F.lit(config.center_lon)) / F.lit(lon_cell_size)).cast(
                "int"
            ),
        )
    )

    neighbors = F.array(
        *[
            F.struct(F.lit(lat_offset).alias("lat_offset"), F.lit(lon_offset).alias("lon_offset"))
            for lat_offset in (-1, 0, 1)
            for lon_offset in (-1, 0, 1)
        ]
    )

    left = moving.select(
        F.col("mmsi").alias("mmsi_1"),
        F.col("name").alias("vessel_name_1"),
        F.col("callsign").alias("callsign_1"),
        F.col("status").alias("status_1"),
        F.col("event_ts").alias("ts_1"),
        F.col("event_epoch").alias("epoch_1"),
        F.col("event_minute").alias("event_minute"),
        F.col("latitude").alias("lat_1"),
        F.col("longitude").alias("lon_1"),
        F.col("sog").alias("sog_1"),
        F.col("lat_bucket").alias("lat_bucket"),
        F.col("lon_bucket").alias("lon_bucket"),
    )
    right = (
        moving.select(
            F.col("mmsi").alias("mmsi_2"),
            F.col("name").alias("vessel_name_2"),
            F.col("callsign").alias("callsign_2"),
            F.col("status").alias("status_2"),
            F.col("event_ts").alias("ts_2"),
            F.col("event_epoch").alias("epoch_2"),
            F.col("event_minute").alias("event_minute_2"),
            F.col("latitude").alias("lat_2"),
            F.col("longitude").alias("lon_2"),
            F.col("sog").alias("sog_2"),
            F.col("lat_bucket").alias("lat_bucket_2"),
            F.col("lon_bucket").alias("lon_bucket_2"),
        )
        .withColumn("neighbor", F.explode(neighbors))
        .select(
            "mmsi_2",
            "vessel_name_2",
            "callsign_2",
            "status_2",
            "ts_2",
            "epoch_2",
            "event_minute_2",
            "lat_2",
            "lon_2",
            "sog_2",
            (F.col("lat_bucket_2") + F.col("neighbor.lat_offset")).alias("join_lat_bucket"),
            (F.col("lon_bucket_2") + F.col("neighbor.lon_offset")).alias("join_lon_bucket"),
        )
    )

    pairs = (
        left.join(
            right,
            (F.col("event_minute") == F.col("event_minute_2"))
            & (F.col("lat_bucket") == F.col("join_lat_bucket"))
            & (F.col("lon_bucket") == F.col("join_lon_bucket"))
            & (F.col("mmsi_1") < F.col("mmsi_2")),
            "inner",
        )
        .withColumn("time_delta_s", F.abs(F.col("epoch_1") - F.col("epoch_2")))
        .filter(F.col("time_delta_s") <= F.lit(config.max_time_delta_s))
        .filter(F.abs(F.col("lat_1") - F.col("lat_2")) <= F.lit(max_lat_diff))
        .filter(F.abs(F.col("lon_1") - F.col("lon_2")) <= F.lit(max_lon_diff))
        .withColumn(
            "distance_m",
            haversine_expr(
                F.col("lat_1"),
                F.col("lon_1"),
                F.col("lat_2"),
                F.col("lon_2"),
            ),
        )
        .filter(F.col("distance_m") <= F.lit(config.candidate_distance_m))
        .withColumn(
            "collision_epoch",
            ((F.col("epoch_1") + F.col("epoch_2")) / F.lit(2.0)).cast("long"),
        )
        .withColumn(
            "collision_ts",
            F.to_timestamp(F.from_unixtime(F.col("collision_epoch"))),
        )
        .withColumn("collision_latitude", (F.col("lat_1") + F.col("lat_2")) / F.lit(2.0))
        .withColumn("collision_longitude", (F.col("lon_1") + F.col("lon_2")) / F.lit(2.0))
        .withColumn(
            "service_score",
            service_name_score_expr("vessel_name_1", "callsign_1")
            + service_name_score_expr("vessel_name_2", "callsign_2"),
        )
        .withColumn(
            "combined_sog",
            F.coalesce(F.col("sog_1"), F.lit(0.0)) + F.coalesce(F.col("sog_2"), F.lit(0.0)),
        )
    )

    candidate_payload = F.struct(
        F.col("mmsi_1").alias("mmsi_1"),
        F.col("mmsi_2").alias("mmsi_2"),
        F.col("vessel_name_1").alias("vessel_name_1"),
        F.col("callsign_1").alias("callsign_1"),
        F.col("vessel_name_2").alias("vessel_name_2"),
        F.col("callsign_2").alias("callsign_2"),
        F.col("status_1").alias("status_1"),
        F.col("status_2").alias("status_2"),
        F.col("ts_1").alias("ts_1"),
        F.col("ts_2").alias("ts_2"),
        F.col("collision_ts").alias("collision_ts"),
        F.col("collision_latitude").alias("collision_latitude"),
        F.col("collision_longitude").alias("collision_longitude"),
        F.col("distance_m").alias("distance_m"),
        F.col("time_delta_s").alias("time_delta_s"),
        F.col("sog_1").alias("sog_1"),
        F.col("sog_2").alias("sog_2"),
        F.col("service_score").alias("service_score"),
        F.col("combined_sog").alias("combined_sog"),
    )
    candidate_order = F.struct(
        F.col("service_score").alias("service_score"),
        F.col("distance_m").alias("distance_m"),
        F.col("time_delta_s").alias("time_delta_s"),
        (-F.col("combined_sog")).alias("negative_combined_sog"),
        F.col("collision_ts").alias("collision_ts"),
    )
    pair_order = F.struct(
        F.col("service_score").alias("service_score"),
        F.col("distance_m").alias("distance_m"),
        F.col("time_delta_s").alias("time_delta_s"),
        (-F.col("combined_sog")).alias("negative_combined_sog"),
        F.col("collision_ts").alias("collision_ts"),
    )

    per_minute_candidates = (
        pairs.groupBy("mmsi_1", "mmsi_2", "event_minute")
        .agg(F.min_by(candidate_payload, candidate_order).alias("selected"))
        .select("selected.*")
    )

    return (
        per_minute_candidates.groupBy("mmsi_1", "mmsi_2")
        .agg(
            F.min_by(
                F.struct(*[F.col(column_name) for column_name in per_minute_candidates.columns]),
                pair_order,
            ).alias("selected")
        )
        .select("selected.*")
        .select(
            "mmsi_1",
            "mmsi_2",
            coalesce_name("vessel_name_1", "callsign_1", "mmsi_1").alias("vessel_name_1"),
            coalesce_name("vessel_name_2", "callsign_2", "mmsi_2").alias("vessel_name_2"),
            "ts_1",
            "ts_2",
            "collision_ts",
            "collision_latitude",
            "collision_longitude",
            "distance_m",
            "time_delta_s",
            "sog_1",
            "sog_2",
            "status_1",
            "status_2",
            "service_score",
            "combined_sog",
        )
        .orderBy(
            F.col("service_score").asc(),
            F.col("distance_m").asc(),
            F.col("time_delta_s").asc(),
            F.col("combined_sog").desc(),
            F.col("collision_ts").asc(),
        )
        .select(
            "mmsi_1",
            "mmsi_2",
            "vessel_name_1",
            "vessel_name_2",
            "ts_1",
            "ts_2",
            "collision_ts",
            "collision_latitude",
            "collision_longitude",
            "distance_m",
            "time_delta_s",
            "sog_1",
            "sog_2",
            "status_1",
            "status_2",
        )
    )


def select_best_candidate(
    cleaned: DataFrame,
    candidates: list[CollisionCandidate],
    config: PipelineConfig,
) -> CollisionResult:
    ordered_candidates = sorted(candidates, key=candidate_sort_key)
    best_rejected: CollisionResult | None = None

    for candidate in ordered_candidates:
        trajectories = load_trajectories_for_candidate(cleaned, candidate, config)
        metrics = {
            mmsi: summarize_trajectory(mmsi, points)
            for mmsi, points in trajectories.items()
            if points
        }
        if len(metrics) < 2:
            continue

        assessment = assess_candidate_encounter(candidate, trajectories, config)
        movement_filter_passed = all(
            trajectory_is_moving(metric, config) for metric in metrics.values()
        )
        notes: str | None = None
        if assessment is not None and assessment.parallel_encounter:
            notes = (
                "Rejected as a likely same-track parallel encounter: synchronized "
                f"minimum separation {assessment.synchronized_min_distance_m:.1f} m, "
                f"close duration {assessment.close_duration_s}s, heading delta "
                f"{format_heading_delta(assessment.heading_delta_deg)}, route overlap "
                f"{assessment.route_overlap_fraction:.2f}."
            )
        elif assessment is not None and not assessment.confirmed_collision_like:
            notes = (
                "Rejected because the encounter was not sharp, crossing-like, or disruptive enough: "
                f"minimum separation {assessment.synchronized_min_distance_m:.1f} m, heading delta "
                f"{format_heading_delta(assessment.heading_delta_deg)}, route overlap "
                f"{assessment.route_overlap_fraction:.2f}, track-end anomalies "
                f"{assessment.track_end_anomaly_count}, speed-drop anomalies "
                f"{assessment.speed_drop_anomaly_count}."
            )

        result = CollisionResult(
            selected_candidate=candidate,
            top_candidates=ordered_candidates,
            trajectories=trajectories,
            metrics=metrics,
            movement_filter_passed=movement_filter_passed,
            assessment=assessment,
            notes=notes,
        )
        if best_rejected is None:
            best_rejected = result
        if (
            result.movement_filter_passed
            and assessment is not None
            and assessment.confirmed_collision_like
        ):
            return result

    if best_rejected is None:
        raise RuntimeError("Candidates were found, but no trajectory windows could be reconstructed.")

    raise RuntimeError(
        best_rejected.notes
        or "No candidate satisfied the movement and synchronized-encounter filters."
    )


def load_trajectories_for_candidate(
    cleaned: DataFrame,
    candidate: CollisionCandidate,
    config: PipelineConfig,
) -> dict[int, list[TrajectoryPoint]]:
    window_start = candidate.collision_timestamp - timedelta(
        minutes=config.trajectory_window_minutes
    )
    window_end = candidate.collision_timestamp + timedelta(
        minutes=config.trajectory_window_minutes
    )

    rows = (
        cleaned.filter(F.col("mmsi").isin(candidate.mmsi_1, candidate.mmsi_2))
        .filter(F.col("event_ts") >= F.lit(window_start))
        .filter(F.col("event_ts") <= F.lit(window_end))
        .select("mmsi", "name", "callsign", "event_ts", "latitude", "longitude", "sog")
        .orderBy(F.col("mmsi").asc(), F.col("event_ts").asc())
        .collect()
    )

    trajectories: dict[int, list[TrajectoryPoint]] = {
        candidate.mmsi_1: [],
        candidate.mmsi_2: [],
    }
    for row in rows:
        vessel_name = row["name"] or row["callsign"] or str(row["mmsi"])
        trajectories[row["mmsi"]].append(
            TrajectoryPoint(
                mmsi=row["mmsi"],
                vessel_name=vessel_name,
                timestamp=row["event_ts"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                sog=None if row["sog"] is None else float(row["sog"]),
            )
        )
    return trajectories


def summarize_trajectory(mmsi: int, points: list[TrajectoryPoint]) -> TrajectoryMetrics:
    if not points:
        return TrajectoryMetrics(mmsi=mmsi, point_count=0, average_sog=0.0, path_length_m=0.0, displacement_m=0.0)

    path_length_m = 0.0
    for left, right in zip(points, points[1:]):
        path_length_m += haversine_python(
            left.latitude,
            left.longitude,
            right.latitude,
            right.longitude,
        )

    displacement_m = 0.0
    if len(points) > 1:
        displacement_m = haversine_python(
            points[0].latitude,
            points[0].longitude,
            points[-1].latitude,
            points[-1].longitude,
        )

    sog_values = [point.sog for point in points if point.sog is not None]
    average_sog = mean(sog_values) if sog_values else 0.0
    return TrajectoryMetrics(
        mmsi=mmsi,
        point_count=len(points),
        average_sog=average_sog,
        path_length_m=path_length_m,
        displacement_m=displacement_m,
    )


def assess_candidate_encounter(
    candidate: CollisionCandidate,
    trajectories: dict[int, list[TrajectoryPoint]],
    config: PipelineConfig,
) -> EncounterAssessment | None:
    left_points = trajectories.get(candidate.mmsi_1, [])
    right_points = trajectories.get(candidate.mmsi_2, [])
    if len(left_points) < 2 or len(right_points) < 2:
        return None

    overlap_start = max(left_points[0].timestamp, right_points[0].timestamp)
    overlap_end = min(left_points[-1].timestamp, right_points[-1].timestamp)
    if overlap_end <= overlap_start:
        return None

    overlap_duration_s = int((overlap_end - overlap_start).total_seconds())
    sample_times = build_sample_times(
        overlap_start,
        overlap_end,
        config.synchronized_step_seconds,
    )
    if len(sample_times) < 3:
        return None

    left_track = interpolate_trajectory(left_points, sample_times)
    right_track = interpolate_trajectory(right_points, sample_times)
    separations = [
        haversine_python(left_lat, left_lon, right_lat, right_lon)
        for (left_lat, left_lon), (right_lat, right_lon) in zip(left_track, right_track)
    ]
    min_index = min(range(len(separations)), key=separations.__getitem__)
    synchronized_min_distance_m = separations[min_index]
    close_duration_s = sum(
        config.synchronized_step_seconds
        for separation in separations
        if separation <= config.parallel_distance_m
    )
    heading_delta_deg = angle_difference_deg(
        estimate_track_bearing_deg(left_track, min_index),
        estimate_track_bearing_deg(right_track, min_index),
    )
    route_overlap_fraction = compute_route_overlap_fraction(
        left_track,
        right_track,
        config.route_overlap_distance_m,
    )
    track_end_anomaly_count, speed_drop_anomaly_count, post_event_anomaly = evaluate_post_event_anomaly(
        candidate,
        trajectories,
        config,
    )
    parallel_encounter = (
        heading_delta_deg is not None
        and heading_delta_deg <= config.parallel_heading_delta_deg
        and (
            close_duration_s >= config.parallel_duration_s
            or route_overlap_fraction >= config.route_overlap_fraction
        )
    )
    confirmed_collision_like = (
        candidate_service_score(candidate) == 0
        and not parallel_encounter
        and post_event_anomaly
        and (
            synchronized_min_distance_m <= config.synchronized_distance_m
            or qualifies_crossing_collision(
                candidate,
                synchronized_min_distance_m,
                heading_delta_deg,
                route_overlap_fraction,
                config,
            )
        )
    )

    return EncounterAssessment(
        synchronized_min_distance_m=synchronized_min_distance_m,
        synchronized_min_timestamp=sample_times[min_index],
        overlap_duration_s=overlap_duration_s,
        close_duration_s=close_duration_s,
        heading_delta_deg=heading_delta_deg,
        route_overlap_fraction=route_overlap_fraction,
        track_end_anomaly_count=track_end_anomaly_count,
        speed_drop_anomaly_count=speed_drop_anomaly_count,
        post_event_anomaly=post_event_anomaly,
        parallel_encounter=parallel_encounter,
        confirmed_collision_like=confirmed_collision_like,
    )


def trajectory_is_moving(metrics: TrajectoryMetrics, config: PipelineConfig) -> bool:
    if metrics.point_count < config.min_trajectory_points:
        return False
    if metrics.path_length_m >= config.min_window_path_m:
        return True
    if metrics.displacement_m >= config.min_window_displacement_m:
        return True
    return metrics.average_sog >= config.moving_sog_knots


def row_to_candidate(row) -> CollisionCandidate:
    return CollisionCandidate(
        mmsi_1=int(row["mmsi_1"]),
        mmsi_2=int(row["mmsi_2"]),
        vessel_name_1=row["vessel_name_1"],
        vessel_name_2=row["vessel_name_2"],
        left_timestamp=row["ts_1"],
        right_timestamp=row["ts_2"],
        collision_timestamp=row["collision_ts"],
        collision_latitude=float(row["collision_latitude"]),
        collision_longitude=float(row["collision_longitude"]),
        distance_m=float(row["distance_m"]),
        time_delta_s=int(row["time_delta_s"]),
        sog_1=None if row["sog_1"] is None else float(row["sog_1"]),
        sog_2=None if row["sog_2"] is None else float(row["sog_2"]),
        status_1=row["status_1"],
        status_2=row["status_2"],
    )


def serialize_candidate(candidate: CollisionCandidate) -> dict[str, str | int | float | None]:
    return {
        "mmsi_1": candidate.mmsi_1,
        "mmsi_2": candidate.mmsi_2,
        "vessel_name_1": candidate.vessel_name_1,
        "vessel_name_2": candidate.vessel_name_2,
        "left_timestamp": candidate.left_timestamp.isoformat(sep=" "),
        "right_timestamp": candidate.right_timestamp.isoformat(sep=" "),
        "collision_timestamp": candidate.collision_timestamp.isoformat(sep=" "),
        "collision_latitude": round(candidate.collision_latitude, 6),
        "collision_longitude": round(candidate.collision_longitude, 6),
        "distance_m": round(candidate.distance_m, 3),
        "time_delta_s": candidate.time_delta_s,
        "sog_1": None if candidate.sog_1 is None else round(candidate.sog_1, 3),
        "sog_2": None if candidate.sog_2 is None else round(candidate.sog_2, 3),
        "status_1": candidate.status_1,
        "status_2": candidate.status_2,
    }


def serialize_assessment(
    assessment: EncounterAssessment,
) -> dict[str, str | int | float | bool | None]:
    return {
        "synchronized_min_distance_m": round(assessment.synchronized_min_distance_m, 3),
        "synchronized_min_timestamp": None
        if assessment.synchronized_min_timestamp is None
        else assessment.synchronized_min_timestamp.isoformat(sep=" "),
        "overlap_duration_s": assessment.overlap_duration_s,
        "close_duration_s": assessment.close_duration_s,
        "heading_delta_deg": None
        if assessment.heading_delta_deg is None
        else round(assessment.heading_delta_deg, 3),
        "route_overlap_fraction": round(assessment.route_overlap_fraction, 3),
        "track_end_anomaly_count": assessment.track_end_anomaly_count,
        "speed_drop_anomaly_count": assessment.speed_drop_anomaly_count,
        "post_event_anomaly": assessment.post_event_anomaly,
        "parallel_encounter": assessment.parallel_encounter,
        "confirmed_collision_like": assessment.confirmed_collision_like,
    }


def candidate_sort_key(candidate: CollisionCandidate) -> tuple[int, float, int, float, datetime]:
    return (
        candidate_service_score(candidate),
        candidate.distance_m,
        candidate.time_delta_s,
        -candidate_combined_sog(candidate),
        candidate.collision_timestamp,
    )


def result_sort_key(result: CollisionResult) -> tuple[int, float, int, float, datetime]:
    return candidate_sort_key(result.selected_candidate)


def qualifies_crossing_collision(
    candidate: CollisionCandidate,
    synchronized_min_distance_m: float,
    heading_delta_deg: float | None,
    route_overlap_fraction: float,
    config: PipelineConfig,
) -> bool:
    if candidate_service_score(candidate) > 0:
        return False
    if candidate.distance_m > config.crossing_candidate_distance_m:
        return False
    if candidate.time_delta_s < config.crossing_time_delta_min_s:
        return False
    if synchronized_min_distance_m > config.crossing_synchronized_distance_m:
        return False
    if heading_delta_deg is None or heading_delta_deg < config.crossing_heading_delta_deg:
        return False
    if route_overlap_fraction > config.crossing_route_overlap_fraction:
        return False
    return candidate_combined_sog(candidate) >= config.crossing_min_combined_sog_knots


def evaluate_post_event_anomaly(
    candidate: CollisionCandidate,
    trajectories: dict[int, list[TrajectoryPoint]],
    config: PipelineConfig,
) -> tuple[int, int, bool]:
    anomaly_window = timedelta(seconds=config.anomaly_window_seconds)
    track_end_anomaly_count = 0
    speed_drop_anomaly_count = 0

    for mmsi in (candidate.mmsi_1, candidate.mmsi_2):
        points = trajectories.get(mmsi, [])
        if not points:
            continue

        seconds_after_collision = (points[-1].timestamp - candidate.collision_timestamp).total_seconds()
        if seconds_after_collision <= config.anomaly_track_end_seconds:
            track_end_anomaly_count += 1

        pre_window = [
            point.sog
            for point in points
            if candidate.collision_timestamp - anomaly_window <= point.timestamp <= candidate.collision_timestamp
            and point.sog is not None
        ]
        post_window = [
            point.sog
            for point in points
            if candidate.collision_timestamp <= point.timestamp <= candidate.collision_timestamp + anomaly_window
            and point.sog is not None
        ]
        if not pre_window or not post_window:
            continue

        pre_average_sog = mean(pre_window)
        post_average_sog = mean(post_window)
        speed_drop_knots = pre_average_sog - post_average_sog
        strong_speed_drop = (
            pre_average_sog >= config.anomaly_min_pre_sog_knots
            and speed_drop_knots >= config.anomaly_speed_drop_knots
            and speed_drop_knots >= pre_average_sog * config.anomaly_speed_drop_fraction
        )
        if strong_speed_drop:
            speed_drop_anomaly_count += 1

    post_event_anomaly = track_end_anomaly_count >= 1 and speed_drop_anomaly_count >= 1
    return track_end_anomaly_count, speed_drop_anomaly_count, post_event_anomaly


def build_sample_times(
    start_time: datetime,
    end_time: datetime,
    step_seconds: int,
) -> list[datetime]:
    sample_times: list[datetime] = []
    current_time = start_time
    step = timedelta(seconds=step_seconds)
    while current_time <= end_time:
        sample_times.append(current_time)
        current_time += step
    if sample_times[-1] != end_time:
        sample_times.append(end_time)
    return sample_times


def interpolate_trajectory(
    points: list[TrajectoryPoint],
    sample_times: list[datetime],
) -> list[tuple[float, float]]:
    interpolated: list[tuple[float, float]] = []
    point_index = 0
    for sample_time in sample_times:
        while (
            point_index + 1 < len(points)
            and points[point_index + 1].timestamp < sample_time
        ):
            point_index += 1

        left_point = points[point_index]
        right_index = min(point_index + 1, len(points) - 1)
        right_point = points[right_index]
        if right_point.timestamp <= left_point.timestamp:
            interpolated.append((left_point.latitude, left_point.longitude))
            continue

        offset_seconds = (sample_time - left_point.timestamp).total_seconds()
        span_seconds = (right_point.timestamp - left_point.timestamp).total_seconds()
        ratio = min(max(offset_seconds / span_seconds, 0.0), 1.0)
        interpolated.append(
            (
                left_point.latitude + ratio * (right_point.latitude - left_point.latitude),
                left_point.longitude + ratio * (right_point.longitude - left_point.longitude),
            )
        )
    return interpolated


def compute_route_overlap_fraction(
    left_track: list[tuple[float, float]],
    right_track: list[tuple[float, float]],
    overlap_distance_m: float,
) -> float:
    if not left_track or not right_track:
        return 0.0
    left_fraction = mean(
        1.0 if min_distance_to_track(point, right_track) <= overlap_distance_m else 0.0
        for point in left_track
    )
    right_fraction = mean(
        1.0 if min_distance_to_track(point, left_track) <= overlap_distance_m else 0.0
        for point in right_track
    )
    return (left_fraction + right_fraction) / 2.0


def min_distance_to_track(
    point: tuple[float, float],
    track: list[tuple[float, float]],
) -> float:
    latitude, longitude = point
    return min(
        haversine_python(latitude, longitude, other_latitude, other_longitude)
        for other_latitude, other_longitude in track
    )


def estimate_track_bearing_deg(
    track: list[tuple[float, float]],
    index: int,
) -> float | None:
    if len(track) < 2:
        return None
    left_index = max(index - 1, 0)
    right_index = min(index + 1, len(track) - 1)
    if left_index == right_index:
        return None
    left_lat, left_lon = track[left_index]
    right_lat, right_lon = track[right_index]
    return bearing_deg(left_lat, left_lon, right_lat, right_lon)


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lon = radians(lon2 - lon1)
    y_axis = sin(delta_lon) * cos(lat2_rad)
    x_axis = cos(lat1_rad) * sin(lat2_rad) - sin(lat1_rad) * cos(lat2_rad) * cos(delta_lon)
    return (degrees(atan2(y_axis, x_axis)) + 360.0) % 360.0


def angle_difference_deg(left_angle: float | None, right_angle: float | None) -> float | None:
    if left_angle is None or right_angle is None:
        return None
    difference = abs(left_angle - right_angle) % 360.0
    return min(difference, 360.0 - difference)


def format_heading_delta(heading_delta_deg: float | None) -> str:
    if heading_delta_deg is None:
        return "unknown"
    return f"{heading_delta_deg:.1f} deg"


def parse_double(column_name: str):
    column = F.col(column_name)
    return F.when(F.trim(column) == "", None).otherwise(column.cast("double"))


def parse_long(column_name: str):
    column = F.col(column_name)
    return F.when(F.trim(column) == "", None).otherwise(column.cast("long"))


def normalize_string(column_name: str):
    column = F.trim(F.col(column_name))
    return F.when(column == "", None).otherwise(column)


def resolve_input_paths(input_glob: str) -> list[str]:
    matched_paths = sorted(glob.glob(input_glob))
    if not matched_paths:
        raise FileNotFoundError(f"No AIS files matched input glob: {input_glob}")
    return matched_paths


def coalesce_name(primary_name: str, secondary_name: str, mmsi_column: str):
    return F.coalesce(F.col(primary_name), F.col(secondary_name), F.col(mmsi_column).cast("string"))


def contains_service_keyword_expr(*column_names: str):
    lowered = F.lower(
        F.concat_ws(" ", *[F.coalesce(F.col(column_name), F.lit("")) for column_name in column_names])
    )
    matches = F.lit(False)
    for keyword in SERVICE_VESSEL_KEYWORDS:
        matches = matches | lowered.contains(keyword)
    return matches


def service_name_score_expr(primary_name: str, secondary_name: str):
    return F.when(contains_service_keyword_expr(primary_name, secondary_name), F.lit(1)).otherwise(
        F.lit(0)
    )


def safe_cosine(latitude_degrees: float) -> float:
    return max(cos(radians(latitude_degrees)), 0.1)


def is_service_vessel_name(*names: str | None) -> bool:
    return any(
        keyword in (name or "").lower()
        for name in names
        for keyword in SERVICE_VESSEL_KEYWORDS
    )


def candidate_service_score(candidate: CollisionCandidate) -> int:
    return int(is_service_vessel_name(candidate.vessel_name_1)) + int(
        is_service_vessel_name(candidate.vessel_name_2)
    )


def candidate_combined_sog(candidate: CollisionCandidate) -> float:
    return (candidate.sog_1 or 0.0) + (candidate.sog_2 or 0.0)


def haversine_expr(lat1, lon1, lat2, lon2):
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    arc = (
        F.pow(F.sin(delta_lat / F.lit(2.0)), F.lit(2.0))
        + F.cos(lat1_rad)
        * F.cos(lat2_rad)
        * F.pow(F.sin(delta_lon / F.lit(2.0)), F.lit(2.0))
    )
    return F.lit(EARTH_RADIUS_M) * F.lit(2.0) * F.asin(F.sqrt(F.least(F.lit(1.0), arc)))


def haversine_python(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    arc = (
        (sin_half(delta_lat) ** 2)
        + cos(lat1_rad) * cos(lat2_rad) * (sin_half(delta_lon) ** 2)
    )
    return EARTH_RADIUS_M * 2.0 * asin_sqrt(arc)


def sin_half(value: float) -> float:
    return __import__("math").sin(value / 2.0)


def asin_sqrt(value: float) -> float:
    return __import__("math").asin(sqrt(min(1.0, value)))