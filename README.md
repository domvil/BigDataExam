# AIS Collision Detector

This repository contains a PySpark solution for the December 2021 AIS assignment. The pipeline filters the Danish AIS feed to the required 50 nautical mile search area around `(55.225, 14.245)`, removes obvious GPS anomalies, searches for moving vessel pairs that reach suspiciously small separation, and then applies stricter trajectory checks so normal same-lane traffic is not mistaken for a collision.

## Project layout

- `src/ais_collision/pipeline.py`: Spark loading, filtering, candidate generation, and trajectory extraction.
- `src/ais_collision/plotting.py`: Static trajectory visualization for the selected candidate.
- `src/ais_collision/main.py`: CLI entry point that runs the pipeline and writes outputs.
- `Dockerfile` and `docker-compose.yml`: Containerized execution path required by the assignment.

## Approach summary

1. Load all December AIS CSV files with Spark's CSV reader.
2. Keep only Class A and Class B vessel messages with valid timestamps and coordinates.
3. Apply an early bounding-box filter, then a precise Haversine radius filter around the target coordinate.
4. Remove obviously stationary records by excluding anchored and moored statuses and requiring moving-speed observations for candidate generation.
5. Remove GPS jumps using per-vessel implied speed checks over successive AIS points.
6. Bucket positions by minute and a 500 m spatial grid so the self-join only compares nearby vessels.
7. Rank the closest candidate pairs for each day, then validate them using 10 minutes of trajectory context before and after the event.
8. Reject likely false positives such as service/pilot traffic, parallel same-track encounters, and close passes with no post-event disruption.
9. Keep only daily candidates that still look collision-like, then select one global month-level winner.
10. Export summary JSON, ranked candidate CSV, trajectory CSV, and a plot.

## Selection logic

Days with only weak or false-positive encounters are rejected instead of being forced into a final result.

- `top_candidates` is only a shortlist of close encounters worth checking.
- `selected_candidate` is the first candidate that passes the stricter trajectory checks.
- A candidate is rejected if it looks like parallel traffic, service/pilot activity, or a close pass with no sign of disruption after the event.
- The batch command runs one day at a time, keeps only successful daily winners, and then chooses one global winner for the month.

In slightly more detail, the final selection works like this:

1. Generate raw candidates from AIS points that are close in both space and time.
2. Rebuild a 10-minute before/after trajectory window for both vessels.
3. Interpolate both tracks onto the same timeline so separation, heading difference, and route overlap can be compared fairly.
4. Reject candidates that look like normal traffic instead of an impact.

The stricter checks are aimed at the most common false positives:

- Parallel traffic: two vessels moving in almost the same direction with long close overlap or very similar routes.
- Service or pilot traffic: helper vessels often get close to other ships without it being a collision.
- Non-disruptive close passes: the pair may get near each other, but neither vessel shows a meaningful change afterward.

The current confirmation step therefore requires both geometry and disruption:

- Geometry: either a very small synchronized separation or a crossing-style encounter.
- Disruption: at least one vessel's track ends near the event and at least one vessel shows a strong post-event speed drop.

This is why a single day can have several `top_candidates` but still no accepted collision, and why the final month-level result is usually only one pair.

## Docker run

Use Docker from the repository root. This is the recommended execution path for this project.

Prerequisites:

- Docker Desktop is installed and running.
- You run the command from the repository root so `Data/` and `output/` mount into the container correctly.

Build and run with Docker Compose:

```powershell
docker compose up --build
```

The default container command runs the month-level batch job. It reads the AIS CSV files from `Data/` and writes the final outputs to `output/full-month/` on the host.

The default Spark tuning inside the container is:

- `--driver-memory 8g`
- `--master local[4]`
- `--shuffle-partitions 200`

If you want to rerun without rebuilding the image:

```powershell
docker compose run --rm ais-collision
```

If you want to override the tuning flags for a heavier local machine, pass them explicitly:

```powershell
docker compose run --rm ais-collision \
  python -m ais_collision.batch \
  --input-glob "/workspace/Data/aisdk-2021-12-*.csv" \
  --output-dir "/workspace/output/full-month" \
  --driver-memory 10g \
  --master "local[6]" \
  --shuffle-partitions 200
```

Recommended starting points:

- 8 GB Docker memory available: `--driver-memory 4g` and `--master "local[4]"`
- 16 GB Docker memory available: `--driver-memory 8g` to `10g` and `--master "local[6]"`
- Keep Spark driver memory below the Docker memory limit so the JVM still has headroom.

If a single day returns a rejection or no accepted collision, that is an application-level result from the stricter collision checks, not a Docker failure. The batch command is the intended full-month run.

If you prefer a plain `docker run` command instead of Compose:

```powershell
docker build -t ais-collision-detector .
docker run --rm \
  -v ${PWD}/Data:/workspace/Data:ro \
  -v ${PWD}/output:/workspace/output \
  ais-collision-detector
```

On Windows PowerShell, if the bind mount form above gives you path parsing issues, use quoted absolute paths instead:

```powershell
docker build -t ais-collision-detector .
docker run --rm \
  -v "${PWD}\Data:/workspace/Data:ro" \
  -v "${PWD}\output:/workspace/output" \
  ais-collision-detector
```

If you want explicit Docker resource limits as well, use plain Docker and keep the Spark driver below that limit:

```powershell
docker build -t ais-collision-detector .
docker run --rm --memory=12g --cpus=6 \
  -v "${PWD}\Data:/workspace/Data:ro" \
  -v "${PWD}\output:/workspace/output" \
  ais-collision-detector \
  python -m ais_collision.batch \
  --input-glob "/workspace/Data/aisdk-2021-12-*.csv" \
  --output-dir "/workspace/output/full-month" \
  --driver-memory 10g \
  --master "local[6]" \
  --shuffle-partitions 200
```

## Outputs

The batch run writes the following files into `output/full-month/`:

- `collision_summary.json`: final selected candidate plus validation details.
- `top_collision_candidates.csv`: ranked shortlist of plausible encounters, not a list of confirmed collisions.
- `trajectory_<mmsi1>_<mmsi2>.csv`: exported 10-minute before/after track for the selected pair.
- `collision_trajectory.png`: static plot for the selected pair.

The summary JSON also includes the encounter assessment that explains why the selected pair survived the stricter checks.
