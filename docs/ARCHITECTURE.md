# Architecture

Two deployment topologies: **local** (Docker Compose) and **cloud** (AWS). Components are designed so the same application code runs in both environments, with cloud-specific bindings isolated to adapter layers.

## Design Principles

1. **Adapter pattern for I/O.** Every external system (Kafka, object store, warehouse, model registry, secrets) is accessed via a thin interface. The interface has two implementations: one for local (e.g., MinIO for object store, Postgres for warehouse), one for AWS (S3, Athena, etc). Application code depends on the interface, never the concrete client.

2. **Configuration over code branching.** No `if cloud_env == "aws"` branches in business logic. Environment selection happens at the composition root via `.env` files and dependency injection.

3. **One language per layer.** Python everywhere except: SQL in dbt, HCL in Terraform, YAML in Kubernetes/Compose, Bash in scripts. No JavaScript, no Go, no Scala.

4. **Containers as the unit of deployment.** Every service ships as a Docker image. The image runs identically in Compose and in EKS. No "works in Compose but not in Kubernetes" classes of bug.

## Local Topology (Docker Compose)

Stack runs entirely on a developer laptop. Resource footprint: ~4 GB RAM, ~10 GB disk for a few days of accumulated data.

```
┌─────────────────────────────────────────────────────────────────┐
│ Docker Compose Network                                           │
│                                                                  │
│  ┌──────────────┐                                                │
│  │  ingester    │  polls TransLink GTFS-RT every 30s             │
│  │  (Python)    │  publishes protobuf-decoded JSON to Kafka      │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐         ┌────────────────────┐                 │
│  │   Kafka      │ ◀────── │  schema-registry   │                 │
│  │  (Redpanda)  │         │   (optional)       │                 │
│  └──────┬───────┘         └────────────────────┘                 │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐                                                │
│  │  stream_proc │  Spark Structured Streaming                    │
│  │  (PySpark)   │  joins to static GTFS, computes observed delay │
│  └──────┬───────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐         ┌────────────────────┐                 │
│  │   MinIO      │         │     Postgres       │                 │
│  │  (S3 stub)   │         │  (warehouse stub)  │                 │
│  └──────────────┘         └────────┬───────────┘                 │
│                                    │                             │
│         ┌──────────────────────────┘                             │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐         ┌────────────────────┐                 │
│  │     dbt      │ ──────▶ │   training table   │                 │
│  │              │         │   (Postgres)       │                 │
│  └──────────────┘         └────────────────────┘                 │
│                                                                  │
│  ┌──────────────┐         ┌────────────────────┐                 │
│  │   trainer    │ ──────▶ │     MLflow         │                 │
│  │  (XGBoost)   │         │  (model registry)  │                 │
│  └──────────────┘         └─────────┬──────────┘                 │
│                                     │                            │
│                                     ▼                            │
│                            ┌────────────────────┐                │
│                            │       api          │                │
│                            │    (FastAPI)       │   ◀── client   │
│                            └────────────────────┘                │
│                                                                  │
│  ┌──────────────┐                                                │
│  │   airflow    │  orchestrates: data backfill, daily retraining │
│  └──────────────┘                                                │
└─────────────────────────────────────────────────────────────────┘
```

### Local service substitutions

| Concept | Local choice | Why |
|---|---|---|
| Message broker | **Redpanda** | Kafka-compatible API, single binary, fast startup, no ZooKeeper |
| Object store | **MinIO** | S3-compatible API, runs in one container |
| Warehouse | **Postgres** | Athena/Redshift API isn't easy to replicate locally; Postgres is close enough for dbt SQL |
| Model registry | **MLflow** (self-hosted) | Same API as MLflow on cloud; SageMaker registry is bound to AWS |
| Orchestrator | **Airflow** (LocalExecutor) | Same code as MWAA / self-hosted on EKS |
| Secrets | `.env` files | Mapped to AWS Secrets Manager or SSM Parameter Store on cloud |

## Cloud Topology (AWS)

```
                     TransLink GTFS-RT
                            │
                            ▼
              ┌─────────────────────────────┐
              │  EKS Cluster (eu-southeast-?) │
              │                              │
              │  ┌────────────────────────┐  │
              │  │ ingester CronJob       │  │
              │  └──────────┬─────────────┘  │
              │             │                │
              │             ▼                │
              │       ┌──────────┐           │  ┌──────────────┐
              │       │   MSK    │  ◀───────────│  Kinesis*    │
              │       │ (Kafka)  │           │  │  (alternative)│
              │       └────┬─────┘           │  └──────────────┘
              │            │                 │
              │            ▼                 │
              │  ┌────────────────────────┐  │
              │  │ stream_processor Pod   │  │
              │  │ (Spark on EKS)         │  │
              │  └──────────┬─────────────┘  │
              │             │                │
              │             ▼                │
              └─────────────┼────────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  S3 (raw + processed data)  │
              │  Glue Catalog (schema)      │
              └─────────────┬───────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  Athena (query layer)       │
              │  dbt models → feature tables│
              └─────────────┬───────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  SageMaker Training Job     │
              │  XGBoost on tabular         │
              └─────────────┬───────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  SageMaker Model Registry   │
              └─────────────┬───────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  api Pod on EKS             │
              │  loads latest model on init │
              │  serves /predict endpoint   │
              └─────────────────────────────┘

Orchestration:  Step Functions (cheaper than MWAA)
Observability:  CloudWatch + Prometheus on EKS
IaC:            Terraform (everything)
CI/CD:          GitHub Actions → ECR → kubectl apply
```

\* MSK vs Kinesis: build with both options behind a feature flag, document the cost / feature trade-off as a resume bullet. Default to **Kinesis Data Streams** for the deployed version due to dramatically lower idle cost. MSK gets a write-up but is provisioned only briefly for the screenshot/demo.

### Cloud-specific decisions

| Concept | AWS choice | Rationale | Alternative considered |
|---|---|---|---|
| Container orchestration | **EKS** | Kubernetes is the portable skill; ECS would be cheaper but less marketable | ECS Fargate |
| Streaming | **Kinesis Data Streams** | Pay-per-shard; idle cost ~AU$15/month for 1 shard | MSK Serverless |
| Object store | **S3** | No alternative worth considering | — |
| Query layer | **Athena** | Serverless, pay-per-query, free at idle | Redshift Serverless |
| Model training | **SageMaker Training** | Pay-per-second; great resume keyword | Self-managed on EKS |
| Model serving | **EKS Pod (FastAPI)** | Keeps K8s skill front and centre | SageMaker Serverless Inference |
| Orchestration | **Step Functions** | ~AU$0 for daily DAG; MWAA is ~AU$550/month minimum | MWAA, self-hosted Airflow on EKS |
| Secrets | **SSM Parameter Store** | Free tier covers everything we need | Secrets Manager |
| IaC | **Terraform** | Cloud-portable, widely recognised | AWS CDK |
| CI/CD | **GitHub Actions** | Universal | CodePipeline |

### Cost guardrails

Resources that exist 24/7:

- EKS control plane: ~US$0.10/hour = ~AU$110/month. **Mitigation:** for the long-running production-style deployment, accept this. For development iterations, tear down between sessions.
- 1 × EKS node (t3.medium): ~US$0.04/hour = ~AU$45/month
- 1 × Kinesis shard: ~US$0.015/hour = ~AU$17/month
- S3 storage: negligible at our scale (<5 GB)
- Athena: pay-per-query, ~AU$5/TB scanned. Our queries scan <100 MB each.
- SageMaker: only running during training jobs (5-10 minutes daily).
- Step Functions: free tier covers our usage.

**Steady-state estimate:** AU$170-220/month if everything runs 24/7. With teardown discipline (only run during 8 hours of active dev per week), realistic spend is AU$40-80/month.

## Component Responsibilities

Each component has exactly one job. Boundaries are enforced by the interface contracts in `specs/`.

### `ingester`

- Polls TransLink GTFS-RT endpoints (vehicle positions, trip updates, alerts) on a fixed interval.
- Decodes protobuf to dictionaries.
- Publishes one message per record to Kafka with a deterministic key (vehicle_id for positions, trip_id for trip updates).
- Maintains no state. Restartable, idempotent at the message-bus level.

### `stream_processor`

- Consumes from Kafka topics.
- Maintains a broadcast-joined static GTFS schedule (refreshed daily).
- Computes the *observed delay* for each stop arrival: `actual_arrival_time - scheduled_arrival_time`.
- Writes enriched records to object store (MinIO local, S3 cloud) in Parquet, partitioned by date.
- Emits metrics to Prometheus.

### `dbt` project

- Reads from raw event tables (Postgres local, Athena cloud).
- Builds staging models, intermediate models, and feature mart models.
- Final output: `fct_arrivals` (one row per arrival event) and `feature_training_set` (one row per (stop, time) pair with features and target).
- Tests: schema tests on every model; data quality tests on key columns.

### `trainer`

- Reads the feature training set.
- Trains baselines (naive, linear regression) and an XGBoost model.
- Logs to MLflow (local) / SageMaker Experiments (cloud).
- Promotes the new model to "champion" if it beats the current champion on held-out MAE.
- Runs as a batch job. Triggered by Airflow (local) / Step Functions (cloud).

### `api`

- FastAPI service exposing `POST /predict`.
- Loads the current champion model from the registry on startup.
- Caches recent feature values per (route, stop) so predictions are fast.
- Exposes `/health`, `/metrics` for observability.

### `airflow` / Step Functions

- Daily DAG: trigger retraining, evaluate, promote/reject.
- Hourly DAG: data quality checks on the previous hour's ingested data.
- Weekly DAG: refresh static GTFS schedule.

## Data Flow Summary

1. **Ingest** (every 30s): GTFS-RT → Kafka
2. **Process** (continuous): Kafka → enriched events → object store
3. **Transform** (hourly): object store → dbt → feature tables
4. **Train** (daily): feature tables → trained model → registry
5. **Serve** (on-demand): registry → API → client

## Failure Modes (Briefly)

| Failure | Detection | Response |
|---|---|---|
| TransLink endpoint down | Ingester logs HTTP errors | Backoff + retry; alert if down >15 min |
| Kafka unavailable | Producer retries fail | Buffer to disk briefly, then alert |
| Spark job stalls | Lag metric on Kafka consumer | Restart pod |
| Model performance regression | Champion challenger test fails | New model rejected; alert |
| API model load failure | `/health` returns 503 | K8s restarts pod; alert if persistent |

These are documented for completeness. Implementation is intentionally minimal — this is not a production system.
