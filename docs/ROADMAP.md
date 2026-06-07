# Roadmap

A 6-month phased build, paced for ~10 hours/week. Each phase ends with a demonstrable, screenshotable milestone.

## Phase 0: Foundation (Week 1)

**Goal:** environment ready, repo skeletoned, first ingester running.

- [ ] Initialise repository with this spec folder copied in
- [ ] Set up Python project: `uv` for dependency management, `pyproject.toml`, `ruff` for linting, `pytest` for tests
- [ ] Set up `pre-commit` hooks (ruff, type checks, secret scanning)
- [ ] Create base `docker-compose.yml` with Redpanda (Kafka), MinIO, Postgres
- [ ] Verify Compose stack comes up clean with `make up`
- [ ] Write a 30-line ingester that polls TransLink vehicle positions once and prints the count of records — no Kafka yet, just confirm the feed works
- [ ] Document any deviations from spec in `docs/DECISIONS.md` (Architecture Decision Records)

**Deliverable:** screenshot of terminal showing live TransLink data being pulled. Committed `Makefile` with `make up`, `make down`, `make test` targets.

**Time estimate:** 8-12 hours.

## Phase 1: Ingestion (Weeks 2-3)

**Goal:** continuous flow of live data from TransLink into Kafka, retained as raw Parquet on MinIO.

- [ ] Implement full ingester service per `specs/INGESTER.md`
- [ ] Containerise ingester; runs in Compose
- [ ] Kafka topics created with appropriate partitioning
- [ ] Implement a simple Kafka consumer that drains to MinIO (one consumer per topic, batched writes)
- [ ] Run the stack continuously for 24 hours; verify no crashes, no data loss
- [ ] Write integration test: ingester → kafka → consumer → object store, with a mocked TransLink endpoint
- [ ] Document the realistic data rate (records/min) and storage rate (MB/day)

**Deliverable:** 24 hours of ingested data sitting in MinIO. A query (Python script) that loads it and prints summary stats: number of vehicles seen, distinct routes, time coverage.

**Time estimate:** 16-20 hours.

## Phase 2: Stream Processing & Ground Truth (Weeks 4-6)

**Goal:** raw events transformed into the `arrivals` table with observed delays.

- [ ] Download static GTFS, load into Postgres
- [ ] Implement Spark Structured Streaming job per `specs/STREAM_PROCESSOR.md`
- [ ] Ground truth computation: derive observed arrival times from vehicle position sequences (this is the hardest part of the project — budget extra time)
- [ ] Edge case handling per `docs/DATA.md`
- [ ] Write to curated Parquet in MinIO, partitioned by date
- [ ] Unit tests for ground truth derivation against hand-crafted fixtures
- [ ] Sanity check: plot scheduled vs observed arrivals for one route on one day; eyeball for sanity

**Deliverable:** `arrivals` Parquet files in MinIO for the data accumulated so far. A Jupyter notebook in `notebooks/` showing a histogram of `observed_delay_s` — does it look like a transit delay distribution? (Long right tail, mode slightly positive, some negatives.)

**Time estimate:** 24-32 hours. This is the longest phase. Don't rush.

## Phase 3: Features & dbt (Weeks 7-8)

**Goal:** feature engineering pipeline producing training-ready tables.

- [ ] Set up `dbt` project pointing at Postgres
- [ ] Implement staging models (one per raw source)
- [ ] Implement intermediate models (joins, enrichment)
- [ ] Implement feature mart per `specs/ML_FEATURES.md`
- [ ] dbt tests on every model
- [ ] dbt docs generated and committed
- [ ] Verify feature freshness: the `feature_training_set` should have rows for every (stop, hour) pair in the last 7 days

**Deliverable:** screenshot of dbt docs UI. A query against `feature_training_set` showing 10 random rows. Confirmed row count is in the expected ballpark.

**Time estimate:** 12-16 hours.

## Phase 4: Baselines & First Model (Weeks 9-10)

**Goal:** XGBoost beats baselines on held-out data.

- [ ] Implement baselines per `docs/ML.md` (naive, last-observed, schedule-only, linear regression)
- [ ] Set up MLflow tracking server in Compose
- [ ] Implement trainer service per `specs/TRAINER.md`
- [ ] Train baselines, log to MLflow
- [ ] Train initial XGBoost with reasonable defaults, log
- [ ] Compare on held-out test week
- [ ] Iterate: feature additions, hyperparameter tuning via Optuna
- [ ] Document findings in `notebooks/MODEL_DEVELOPMENT.ipynb`

**Deliverable:** MLflow UI screenshot showing XGBoost beating all baselines on MAE. Feature importance plot. Predicted-vs-actual scatter plot. Brief writeup of what worked.

**Time estimate:** 16-24 hours.

## Phase 5: Serving & Local Demo (Weeks 11-12)

**Goal:** end-to-end working system on the local stack.

- [ ] Implement FastAPI service per `specs/API.md`
- [ ] Model loading from MLflow registry on startup
- [ ] Feature cache for online prediction (Redis or in-memory, depending on requirements)
- [ ] `/predict`, `/health`, `/metrics` endpoints
- [ ] Prometheus + Grafana in Compose for observability
- [ ] End-to-end smoke test: hit the API, get a prediction, log it
- [ ] Polish: README updated with screenshots, architecture diagram (mermaid or excalidraw), demo gif

**Deliverable:** `make demo` command that brings up the whole stack, ingests data, trains a model on whatever's accumulated, starts the API, and runs a few example predictions against it. Linked in the GitHub README.

**Time estimate:** 16-20 hours.

**This is the MVP.** At this point the project is portfolio-ready. Everything after is cloud migration and polish.

## Phase 6: AWS Foundations (Weeks 13-16)

**Goal:** AWS Solutions Architect Associate cert + first AWS deployment.

- [ ] Study for and sit AWS SAA — budget 60-80 hours of study, spread over 4-6 weeks
- [ ] In parallel: set up AWS account, configure billing alerts, set up IAM properly (no root key in your laptop)
- [ ] Terraform skeleton: VPC, EKS cluster (Autopilot or small node group), S3 bucket, ECR repo
- [ ] First deployment: push a hello-world FastAPI to ECR, deploy to EKS, hit it from your laptop
- [ ] Document the Terraform module structure
- [ ] **Cost discipline:** `terraform destroy` after every session. Verify with cost explorer at end of week.

**Deliverable:** AWS SAA pass. Working EKS cluster (provisioned via Terraform) serving a hello-world endpoint. AWS bill for the month < AU$80.

**Time estimate:** 80-100 hours total (mostly cert study).

## Phase 7: AWS Migration (Weeks 17-22)

**Goal:** the full local stack, running on AWS.

- [ ] Migrate ingester to EKS CronJob
- [ ] Stand up Kinesis Data Stream (1 shard) — feature-flag to switch between local Kafka and Kinesis in the ingester
- [ ] Stand up MSK temporarily for the "I have used MSK" claim — write a short Spark Streaming job against it, screenshot, tear down
- [ ] Migrate stream processor to EKS-deployed Spark
- [ ] Migrate object store to S3 (already abstracted via interfaces, should be config-only)
- [ ] Set up Glue Catalog + Athena
- [ ] Port dbt models from Postgres to Athena syntax (mostly the same, minor function differences)
- [ ] Move model training to SageMaker Training Job (containerised, pulls feature set from Athena)
- [ ] Step Functions orchestration for daily retraining
- [ ] Deploy API to EKS, model loaded from SageMaker Model Registry
- [ ] CloudWatch dashboards for service metrics
- [ ] End-to-end test in cloud

**Deliverable:** project running in AWS, generating predictions for live Brisbane data. Architecture diagram updated to show AWS topology. README updated with cloud deployment instructions.

**Time estimate:** 60-80 hours.

## Phase 8: Polish & Differentiation (Weeks 23-26)

**Goal:** make this look like a project a hiring manager would notice.

- [ ] Public-facing dashboard: simple Streamlit or React app showing live predictions on a Brisbane map. Deploy to Vercel or as another EKS service. Linked from README.
- [ ] Blog post: "Lessons from building a real-time transit delay predictor on AWS". Cover architecture decisions, what went wrong, model performance, surprising findings. Publish on Medium or personal site.
- [ ] LinkedIn post linking blog + repo + demo
- [ ] AWS Data Engineer Associate cert (60 hours of study + exam)
- [ ] Update CV with refined bullets, hard numbers from the running system
- [ ] Rehearse the project pitch: 30-second version, 2-minute version, 10-minute deep dive

**Deliverable:** the project is now a portfolio centrepiece. Time to apply for jobs.

**Time estimate:** 40-60 hours.

## Total Time Budget

| Phase | Hours |
|---|---|
| 0: Foundation | 10 |
| 1: Ingestion | 18 |
| 2: Stream processing | 28 |
| 3: Features & dbt | 14 |
| 4: Baselines & model | 20 |
| 5: Serving & demo | 18 |
| 6: AWS foundations + SAA | 90 |
| 7: AWS migration | 70 |
| 8: Polish + DEA cert | 50 |
| **Total** | **~320 hours** |

At 10 hours/week, that's ~32 weeks. At 12 hours/week, ~27 weeks. At 8 hours/week, ~40 weeks.

The 6-month estimate at the top assumes 12 hours/week and some efficiency from LLM-assisted development. Adjust to your actual capacity.

## Off-Ramps (If Things Take Longer Than Expected)

If at month 4 you're only at the end of Phase 4, that's fine. The MVP (end of Phase 5) is genuinely portfolio-worthy on its own. The cloud migration is the differentiator, but you can apply for jobs with just the local version while the cloud migration is in progress.

**Minimum viable resume bullet** is achievable by end of Phase 5: streaming, K8s, classical ML, MLOps, Docker — all of those are present without AWS. The AWS migration upgrades the resume bullet from good to excellent.

## Risk Register

| Risk | Likelihood | Mitigation |
|---|---|---|
| TransLink API changes or restricts access | Medium | Use abstracted ingestion interface; could swap to NSW or VIC transit data if needed |
| Ground truth derivation harder than expected | High | Budgeted extra time in Phase 2; can simplify by trusting GTFS-RT `arrival.delay` field as a fallback |
| AWS cost overruns | Medium | Budget alerts, aggressive teardown, well-rehearsed Terraform destroy |
| Burnout from 32 weeks of side-project work on top of day job | Medium | Take breaks. Phases are designed to be self-contained — a 2-week pause won't derail the project |
| Model doesn't beat baselines | Low | Genuinely informative if it happens; document and discuss as an interview talking point |
| New AWS service launches mid-build that changes the optimal architecture | Low | Don't chase; ship what you've built. Document the alternative in a "future work" section |
