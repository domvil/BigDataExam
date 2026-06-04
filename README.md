# AIS Collision Detector

This repository contains a PySpark solution for the December 2021 AIS assignment. The project is designed to be run in Docker and produces the final detected collision candidate, trajectory export, ranked candidate list, and visualization.

**Written report:** See [REPORT.md](REPORT.md) for a concise discussion of the findings, the methodology used to filter AIS noise and exclude false positives, the collision-verification logic, and practical notes about the dataset and computational strategy.

## Project layout

- `src/ais_collision/pipeline.py`: Spark loading, filtering, candidate generation, and trajectory extraction.
- `src/ais_collision/plotting.py`: Static trajectory visualization for the selected candidate.
- `src/ais_collision/main.py`: CLI entry point that runs the pipeline and writes outputs.
- `src/ais_collision/batch.py`: Month-level batch runner used by the Docker workflow.
- `Dockerfile` and `docker-compose.yml`: Containerized execution path.

## Run

Run the project from the repository root:

```powershell
docker compose up --build
```

That command builds the image if needed, runs the month-level batch job in Docker, reads AIS CSV files from `Data/`, and writes the final outputs to `output/full-month/`.

To rerun without rebuilding:

```powershell
docker compose run --rm ais-collision
```

## Tuning

The default Docker command uses these Spark settings:

- `--driver-memory 8g`
- `--master local[4]`
- `--shuffle-partitions 200`

If your machine has more available RAM and CPU, you can override them:

```powershell
docker compose run --rm ais-collision \
  python -m ais_collision.batch \
  --input-glob "/workspace/Data/aisdk-2021-12-*.csv" \
  --output-dir "/workspace/output/full-month" \
  --driver-memory 10g \
  --master "local[6]" \
  --shuffle-partitions 200
```

Practical starting points:

- If Docker Desktop has about 8 GB available, use `--driver-memory 4g` to `8g` and `--master "local[4]"`.
- If Docker Desktop has about 16 GB available, use `--driver-memory 8g` to `10g` and `--master "local[6]"`.
- Keep Spark driver memory below the Docker memory limit so the JVM has headroom.

## Outputs

The batch run writes these files into `output/full-month/`:

- `collision_summary.json`: final selected candidate and assessment details.
- `top_collision_candidates.csv`: ranked shortlist of plausible encounters.
- `trajectory_<mmsi1>_<mmsi2>.csv`: exported 10-minute before/after track for the selected pair.
- `collision_trajectory.png`: static plot for the selected pair.

## Notes

- The Docker workflow is the intended way to run this repository.
- A failed single-day candidate is not necessarily a Docker problem; the collision filters can legitimately reject a day with no convincing impact.
- More detail about the methodology and findings is in `REPORT.md`.
