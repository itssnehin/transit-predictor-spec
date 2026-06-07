# Glossary

Project-specific terms and their precise meanings. When ambiguity arises, this document wins.

| Term | Definition |
|---|---|
| **Arrival** | An event representing a vehicle reaching a scheduled stop. Derived from vehicle position sequences, not from GTFS-RT trip updates. |
| **Arrival, observed** | An arrival where we have direct evidence (a `STOPPED_AT` vehicle position) of the vehicle being at the stop. |
| **Arrival, imputed** | An arrival inferred from indirect evidence (the vehicle was later observed downstream). Less reliable. |
| **Candidate model** | A newly trained model that has not yet been compared against the champion. |
| **Champion model** | The model currently serving production traffic. Tagged in the model registry. |
| **Curated layer** | The processed data layer: arrivals events as Parquet on object store. Successor to "silver" in medallion architecture. |
| **Delay** | `observed_arrival_time - scheduled_arrival_time` in seconds. Negative = early, positive = late. |
| **Delay, predicted** | The model's output for a future stop arrival. Same units as delay. |
| **Feature cache** | In-memory or Redis store holding pre-computed and live features for fast API serving. |
| **Feature training set** | The dbt-produced table with one row per (stop, time, route) triple containing all features + target. |
| **GTFS** | General Transit Feed Specification. The static schedule format (CSVs in a ZIP). |
| **GTFS-RT** | GTFS-Realtime. Live updates in protobuf. Vehicle positions, trip updates, alerts. |
| **Ground truth** | The observed delay for a historical arrival. Used as the training target. |
| **Horizon** | How far in the future the prediction is for. Default 15 minutes. |
| **Lag feature** | A feature computed from data older than the prediction time (e.g., historical means). |
| **Live feature** | A feature computed from recent real-time data, observed before the prediction time but after the lag window. |
| **Naive baseline** | A trivial prediction strategy used as a comparison floor. Multiple variants — see `docs/ML.md`. |
| **Promotion** | The act of moving a candidate model to champion status. Atomic. |
| **Raw layer** | The unprocessed data layer: Kafka messages dumped to object store as-is. |
| **Schedule relationship** | A GTFS-RT field: SCHEDULED, ADDED, UNSCHEDULED, CANCELED, NO_DATA. We exclude CANCELED from training. |
| **Slice** | A subset of evaluation data with a specific property (e.g., "Monday peak", "rainy days"). Used for evaluation. |
| **Stop sequence** | The integer position of a stop along a trip (1, 2, 3...). From static GTFS `stop_times.txt`. |
| **Target time** | The time at which we want to predict the delay. Always in the future at prediction request. |
| **Trip** | A single run of a vehicle along a route. Has a unique trip_id in static GTFS. |
| **Vehicle position** | A GTFS-RT message containing where a specific vehicle is at a specific time. |
| **Watermark** | Spark Streaming concept: how late data can arrive before being dropped. We use 30 minutes. |

## Abbreviations

| Abbreviation | Meaning |
|---|---|
| ADR | Architectural Decision Record |
| BOM | Bureau of Meteorology (Australian weather) |
| dbt | Data Build Tool (SQL transformation framework) |
| DPC | Department of the Premier and Cabinet (Queensland, owner's current employer) |
| ECR | Elastic Container Registry (AWS) |
| EKS | Elastic Kubernetes Service (AWS) |
| GBDT | Gradient-Boosted Decision Trees |
| GTFS | General Transit Feed Specification |
| GTFS-RT | GTFS Realtime |
| IRSA | IAM Roles for Service Accounts (EKS) |
| MAE | Mean Absolute Error |
| MLOps | Machine Learning Operations |
| MSK | Managed Streaming for Kafka (AWS) |
| MWAA | Managed Workflows for Apache Airflow (AWS) |
| OIDC | OpenID Connect |
| PSI | Population Stability Index (drift metric) |
| RMSE | Root Mean Squared Error |
| SAA | Solutions Architect Associate (AWS cert) |
| SHAP | SHapley Additive exPlanations |
| SSM | Systems Manager (AWS) |
| TPE | Tree-structured Parzen Estimator (Optuna's default sampler) |

## Conventions

- **Timestamps**: stored as UTC always. Converted to Brisbane local (AEST, +10:00, no DST in QLD) only for human-readable output.
- **IDs**: use ULIDs for internal events (sortable by creation time). Use string IDs from upstream sources (GTFS) verbatim.
- **Money**: AUD unless otherwise specified.
- **Versions**: semantic versioning for code (`0.1.0`); calendar versioning for models (`2026.05.27`).
- **Branch naming**: `feat/<slug>`, `fix/<slug>`, `chore/<slug>`. Main is `main`.
- **Commit style**: Conventional Commits (`feat:`, `fix:`, `docs:`, etc.).
