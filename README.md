# Transit Predictor

Real-time bus arrival delay prediction system for Brisbane (TransLink GTFS-Realtime). Predicts arrival delay 15 minutes ahead at a given stop using gradient-boosted models trained on historical observed delays plus contextual features (weather, time, day, school holidays).

Built local-first with Docker Compose, then migrated to AWS for production deployment.

## Status

Pre-development. This repository currently contains specifications only.

## For LLM Assistants Working On This Project

**Start here:** `docs/PROJECT_CONTEXT.md` — explains the why, the constraints, and the success criteria.

**Then read in order:**
1. `docs/ARCHITECTURE.md` — the system design, local and cloud
2. `docs/DATA.md` — data sources, schemas, ground truth
3. `docs/ML.md` — model choices, baselines, evaluation
4. `docs/ROADMAP.md` — phased build plan with explicit milestones
5. `specs/` — detailed component specs (only read when working on that component)

**Operating principles for this project:**

- **Local-first, cloud-later.** Everything must run on a developer laptop via `docker compose up` before any AWS code is written. Do not introduce AWS SDK dependencies into core logic — keep cloud bindings in adapter layers.
- **Skill-portable.** The owner is using this project to learn marketable skills (Kafka, Spark, Kubernetes, dbt, XGBoost, Airflow, Terraform, SageMaker). Prefer the more recognised tool when there's a tie, and document the choice.
- **Resume-grade.** Every architectural decision should produce a defensible answer to "why did you do it this way?" in an interview. Document the reasoning, not just the choice.
- **Frugal.** AWS costs are a real constraint. Aggressive `terraform destroy` workflows are mandatory. Idle infrastructure that costs more than US$5/day at rest needs explicit justification.
- **Reproducible.** Deterministic builds. Pinned dependencies. Seed all randomness. No "works on my machine".

## Quick Orientation

| | |
|---|---|
| **Domain** | Public transit (Brisbane buses, GTFS-Realtime) |
| **Problem** | Predict arrival delay (minutes) at a stop, 15 minutes in the future |
| **ML approach** | XGBoost regression on tabular features; baselines: naive (last observed delay) and linear regression |
| **Data scale** | ~5,000-15,000 vehicle position updates per hour during service hours |
| **Cloud target** | AWS (EKS, MSK or Kinesis, S3, Athena, SageMaker, Step Functions, Terraform) |
| **Local stack** | Docker Compose: Kafka, Spark, Postgres, MinIO, FastAPI, Airflow |
| **Languages** | Python 3.11 (primary), SQL (dbt), HCL (Terraform), Bash |
| **Test framework** | pytest, with separate unit/integration markers |

## Repository Layout (Planned)

```
transit-predictor/
├── README.md
├── docs/                       # High-level documentation (this folder, expanded)
├── specs/                      # Component-level specifications
├── infra/
│   ├── docker/                 # Local Docker Compose stack
│   └── terraform/              # AWS infrastructure as code
├── services/
│   ├── ingester/               # GTFS-RT polling → Kafka producer
│   ├── stream_processor/       # Spark Streaming job
│   ├── api/                    # FastAPI prediction service
│   └── trainer/                # Model training job
├── dbt/                        # SQL transformations (feature engineering)
├── airflow/                    # DAGs for orchestration
├── notebooks/                  # Exploratory analysis (not in CI)
├── tests/                      # Cross-component integration tests
├── pyproject.toml              # Python project + dependencies (uv-managed)
├── docker-compose.yml          # Local dev stack
├── Makefile                    # Common dev commands
└── .github/workflows/          # CI/CD pipelines
```
