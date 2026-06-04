# AIS Collision Detection Report

## Objective

The task is to identify the collision, or closest collision-like encounter, inside a 50 nautical mile radius around `(55.225000, 14.245000)` during December 2021, while filtering out stationary vessels and noisy AIS behavior. The final deliverables must include the detected vessel pair, event time and coordinates, and a 10-minute before/after trajectory visualization.

## Data and constraints

The raw input consists of daily AIS CSV files for December 2021. Each record includes at least vessel identity, timestamp, latitude, longitude, speed over ground, and navigational status. The assignment imposes four constraints that strongly shape the solution:

1. The analysis must stay within the December 2021 time window.
2. Only traffic inside a 50 nautical mile radius around `(55.225000, 14.245000)` should be considered.
3. Stationary or safely parked vessels should not be treated as collision candidates.
4. The implementation must use a big-data framework and run inside Docker.

These constraints make PySpark a reasonable choice: the code can express the pipeline as distributed filtering, joins, and window operations without loading the month into a single in-memory dataframe.

## Methodology

The solution is implemented in PySpark so the raw monthly AIS data can be processed with distributed transformations instead of loading the full dataset into Pandas.

The pipeline works in five stages.


### Default thresholds used in the code

Unless a command-line flag overrides them, the report refers to the following concrete defaults from the implementation:

- Search area: center `(55.225000, 14.245000)` with radius `50 nm`.
- Input validity: only `Class A` and `Class B` AIS messages with valid timestamps and coordinates are kept.
- Stationary-status filter: `status` values `at anchor`, `moored`, and `aground` are excluded.
- Moving-speed filter: `SOG >= 1.5 kn`.
- GPS-jump filter: implied speed between consecutive messages from the same vessel must stay at or below `70 kn`.
- Candidate generation: same minute bucket, `150 m` spatial grid, raw separation `<= 100 m`, timestamp gap `<= 30 s`, and keep the best `12` ranked pairs for trajectory reconstruction.
- Trajectory window filter: `10 min` before and after the event, at least `3` points, and the track must show either `>= 1000 m` path length, `>= 250 m` displacement, or average `SOG >= 1.5 kn`.
- Synchronized validation: interpolation every `5 s` and a direct synchronized confirmation threshold of `<= 25 m`.
- Parallel-traffic rejection: heading difference `<= 12 deg` and either `>= 120 s` spent within `30 m` or route-overlap fraction `>= 0.6`.
- Crossing-style fallback confirmation: raw distance `<= 8 m`, raw timestamp gap `>= 15 s`, synchronized distance `<= 100 m`, heading difference `>= 20 deg`, route-overlap fraction `<= 0.2`, and combined `SOG >= 8 kn`.
- Post-event anomaly requirement: at least one track ends within `30 s` of the event, plus at least one speed-drop anomaly measured over `180 s` windows with pre-event average `SOG >= 4 kn`, absolute drop `>= 3 kn`, and relative drop `>= 35%`.
- Service-vessel keywords used for ranking penalties and final rejection: `rescue`, `kbv`, `kyst`, `coast`, `pilot`, `patrol`, and `sar`.

### 1. Spatial and temporal restriction

The pipeline reads the December AIS CSV files with Spark's CSV reader, parses timestamps, and drops records with invalid coordinates or missing time information. It first applies a coarse geographic filter, then a precise Haversine-distance filter centered on the assignment coordinate. This two-step restriction reduces the amount of data that reaches the more expensive pairing stage.

### 2. Data cleaning and noise reduction

Two types of noise are handled before candidate selection:

- Stationary traffic is reduced by excluding anchored and moored statuses and by requiring moving-speed observations.
- GPS anomalies are reduced by computing implied speeds between consecutive messages from the same vessel and rejecting jumps that would require unrealistic motion.

This step is important because AIS feeds often contain abrupt coordinate errors, duplicate-looking near-static traffic, and harbor support behavior that would otherwise dominate the shortest-distance ranking.

### 3. Efficient candidate generation

The core computational challenge is avoiding an all-vs-all vessel comparison. The pipeline therefore buckets positions by minute and by a spatial grid before performing the self-join. This narrows comparisons to vessels that are close in both time and space.

Candidate pairs are then ranked by:

- observed distance,
- timestamp proximity,
- combined speed,
- and penalties that push obvious service or responder traffic lower in the shortlist.

This ranking is intentionally permissive. It does not confirm a collision by itself; it only finds a manageable shortlist of encounters worth reconstructing in more detail.

### 4. Trajectory reconstruction and collision validation

For each strong candidate, the code rebuilds a 10-minute before and 10-minute after window for both vessels. Those trajectories are interpolated onto a synchronized timeline so the pair can be assessed more fairly than by raw AIS timestamps alone.

The validation logic rejects common false positives:

- service, pilot, coast guard, rescue, and similar helper-vessel interactions,
- parallel same-lane traffic with similar headings and overlapping routes,
- close passes that show no disruption after the event.

To survive the final check, an encounter must show both geometry and disruption:

- Geometry: either a very small synchronized separation or a crossing-style encounter.
- Disruption: at least one track-ending anomaly near the event and at least one strong short-window speed-drop anomaly.

This stricter validation is necessary because the monthly shortlist contains several extremely close raw encounters that are operationally normal rather than collision-like.

### 5. Month-level selection

The Docker workflow uses the batch runner, which processes the daily files, keeps only daily winners that pass the strict validation, and finally selects one global month-level result. This avoids forcing a daily answer on dates where no convincing collision-like event exists.

## Result

The final selected vessel pair is:

- MMSI `219021240` — `KARIN HOEJ`
- MMSI `232018267` — `MV SCOT CARRIER`

Detected event summary:

- Collision timestamp: `2021-12-13 05:27:43 UTC`
- Collision coordinates: `55.223079, 14.243707`
- Closest observed AIS distance: `4.076 m`

Encounter assessment from the final output:

- Synchronized minimum distance: `74.724 m`
- Heading difference: `27.937 deg`
- Route overlap fraction: `0.0`
- Track-end anomalies: `1`
- Speed-drop anomalies: `1`
- Parallel encounter: `false`
- Confirmed collision-like: `true`

The two vessels are therefore identified as the most convincing collision candidate in the month-level run.

## Findings and interpretation

The final pair is compelling for three reasons.

First, the raw event is very tight in space and time: the observed distance is only `4.076 m` with a `29 s` message gap, and both vessels are reported as under way using engine.

Second, the synchronized trajectory assessment is consistent with a crossing encounter rather than ordinary convoy or same-lane traffic. The heading difference is `27.937 deg`, the route-overlap fraction is `0.0`, and the pair is not classified as a parallel encounter.

Third, the post-event behavior is disruptive. The final assessment reports one track-end anomaly and one speed-drop anomaly. That is a much stronger collision signal than a simple close pass.

An important methodological observation is that the synchronized minimum distance (`74.724 m`) is much larger than the raw closest observed distance (`4.076 m`). This is not a contradiction. AIS messages are asynchronous, so the closest raw pair can occur between slightly offset timestamps. The model therefore uses both raw proximity for candidate discovery and synchronized interpolation plus anomaly checks for confirmation.

## Rejected alternatives

The `top_candidates` list contains several close encounters from the same day, but those are only shortlisted possibilities. The stricter trajectory checks discard many of them because they are rescue/service traffic, pilot interactions, same-lane motion, or close passes with no disruptive change after the event.

This is visible in the month summary. Some candidates have even smaller raw distances than the selected pair, but they involve names such as `KBV 302`, `RESCUE MADS JAKOBSEN`, `DANPILOT PAPA`, and `PILOT 772 SE`. Those names are exactly the kind of helper-vessel patterns that must be treated cautiously. A pure minimum-distance approach would rank them highly, but the stricter validation rejects them as likely false positives.

Another shortlisted pair, `CHARVIL` and `NYBOLIG`, reaches `3.517 m`, but without the stronger geometry-and-disruption evidence required by the final confirmation step, it should not displace the selected crossing-style encounter.

## Limitations

The result is based only on AIS messages. If updates are sparse, noisy, or slightly offset in time, the estimated encounter can shift, and the raw minimum distance can differ from the synchronized trajectory estimate.

The added anomaly checks and service-vessel filtering reduce obvious false positives, but this stricter filtering can also remove real collisions before they reach the final shortlist. The output should therefore be read as the strongest collision candidate that survives the chosen filters, not necessarily every real collision in the data.

## Visualization

The final 20-minute trajectory visualization is embedded below.

![Selected collision trajectory](docs/collision_trajectory.png)

## Reproducibility

Recommended Docker run:

```powershell
docker compose up --build
```

Docker run with explicit Spark tuning overrides:

```powershell
docker compose run --rm ais-collision \
   python -m ais_collision.batch \
   --input-glob "/workspace/Data/aisdk-2021-12-*.csv" \
   --output-dir "/workspace/output/full-month" \
   --driver-memory 10g \
   --master "local[6]" \
   --shuffle-partitions 200
```

The default Compose service already runs the batch job inside Docker and writes results to `output/full-month/`.
