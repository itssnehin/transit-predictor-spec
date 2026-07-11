# Decisions Log

Architectural Decision Records (ADR-lite). Each significant decision gets an entry. Append, never edit history.

Format:

```
## NNNN: <Title>

**Date:** YYYY-MM-DD
**Status:** Proposed | Accepted | Superseded by NNNN
**Context:** What's the situation that demands a decision?
**Decision:** What was decided?
**Consequences:** What are the trade-offs? What did we give up?
**Alternatives considered:** What else did we look at?
```

---

## 0001: Build local-first, migrate to cloud later

**Date:** 2026-05-27
**Status:** Accepted
**Context:** Owner has limited AWS budget on a graduate salary. Going cloud-first risks burning money learning AWS-specific quirks while still figuring out the core ML and data engineering. Going local-only forgoes the AWS resume bullet which is the main career goal.
**Decision:** Build the entire stack with Docker Compose first. Use cloud-portable abstractions (Kafka, S3-compatible object store, dbt, MLflow, Kubernetes). Migrate to AWS after Phase 5.
**Consequences:** Slower path to the AWS resume bullet (~3 months longer). Higher initial complexity from building abstractions. But: financial safety, faster iteration, better learning of fundamentals.
**Alternatives considered:**
- Cloud-first on AWS: rejected due to cost risk
- Cloud-first on GCP: rejected because the AWS market is the target
- Local-only forever: rejected because the AWS bullet matters

## 0002: AWS over GCP and Azure

**Date:** 2026-05-27
**Status:** Accepted
**Context:** Owner is on a 485 visa optimising for sponsorship-track employers in Australia. Need to choose a single cloud to commit to.
**Decision:** AWS.
**Consequences:** Owner's existing GCP knowledge becomes secondary; learning curve is steeper; idle infra is more expensive. But: largest sponsorship-willing employer pool runs AWS; market signal is strongest.
**Alternatives considered:**
- GCP: better DX, cheaper, but smaller AU job market
- Azure: strong in govt/enterprise but weaker in tech startups; weaker ML tooling

## 0003: Kinesis (not MSK) for production streaming

**Date:** 2026-05-27
**Status:** Accepted
**Context:** Kafka is the marketable skill. Kinesis is cheaper to run idle. MSK provides a true Kafka API but charges ~AU$130/month minimum.
**Decision:** Production-deployed version uses Kinesis Data Streams (1 shard, ~AU$17/month). Build MSK once briefly for the "I have used MSK" resume claim and screenshot, then tear down.
**Consequences:** Two ingester code paths to maintain (Kafka and Kinesis). Slightly less clean architecturally. But: significant ongoing cost savings, and the MSK story is fine for interviews ("I evaluated MSK vs Kinesis and chose Kinesis for cost; here's the comparison...").
**Alternatives considered:**
- MSK only: too expensive for sustained idle
- Kinesis only: misses the "Kafka" keyword on resume
- MSK Serverless: still expensive for our volume; not as cost-predictable

## 0004: XGBoost over alternatives

**Date:** 2026-05-27
**Status:** Accepted
**Context:** The model needs to be defensible in interviews ("why XGBoost?"). Need something that demonstrates classical ML competence specifically — that's the resume gap being filled.
**Decision:** XGBoost regression as the primary model. Baselines include linear regression.
**Consequences:** Owner doesn't get a neural network on this resume (already has one from the generative imaging project). But: matches what 80% of industry tabular ML actually uses.
**Alternatives considered:**
- LightGBM: equivalent; chose XGBoost for slightly more name recognition
- CatBoost: slightly better for high-cardinality categoricals but less widely recognised
- Neural net (TabNet, etc.): over-engineered for this dataset size; doesn't fill the "classical ML" gap

## 0005: Postgres as warehouse stand-in locally (not DuckDB)

**Date:** 2026-05-27
**Status:** Accepted
**Context:** Local dbt needs a database. Options: Postgres, DuckDB, sqlite. Final destination is Athena.
**Decision:** Postgres locally. Athena in cloud. dbt project has two profile targets.
**Consequences:** Slight SQL dialect differences between local and cloud (handled by dbt's macros where possible, manually otherwise). Postgres is heavier than DuckDB.
**Alternatives considered:**
- DuckDB: lighter, faster, supports Athena-like SQL well. Rejected because Postgres also doubles as Airflow's metadata DB (one fewer service).
- sqlite: too limited for dbt's needs

## 0006: Phase 0 foundation tooling choices

**Date:** 2026-05-30
**Status:** Accepted
**Context:** Phase 0 (ROADMAP.md) requires standing up the Python project, pre-commit hooks, and a base Docker Compose stack. Several details were left open by the specs and needed concrete choices.
**Decision:**
- **Secret scanning = gitleaks** (not detect-secrets). Wired into `.pre-commit-config.yaml`. Chosen for stronger industry/resume recognition. Trade-off: requires the gitleaks binary (or Go) available to pre-commit, an extra install step on Windows vs. a pure-Python tool.
- **Image tags pinned, not `:latest`.** INFRA.md shows `:latest`; we pin (`redpanda:v24.2.7`, `minio:RELEASE.2024-10-13T13-34-11Z`, `postgres:16-alpine`) for reproducibility per CLAUDE.md. Tags were chosen without a registry pull (compose was validated config-only this session); **verify/refresh on first `make up`.**
- **Python pinned to `>=3.11,<3.12`** in `pyproject.toml` (CLAUDE.md mandates 3.11, not 3.12+).
- **Phase 0 compose = base three services only** (redpanda, minio, postgres). The full INFRA.md stack (MLflow, Prometheus, Grafana, Airflow, app services) lands in later phases. Redpanda Console was deliberately omitted to honour the roadmap's "base" scope.
- **mypy strict excludes `**/tests/**`.** Tests exercise untyped third-party protobuf APIs (`gtfs-realtime-bindings` ships no stubs); strict typing there forces noisy `type: ignore`. Application code remains `mypy --strict` clean. ruff + pytest cover tests.
- **Generated `main.py` stub removed** and the package renamed `transit-predictor-spec` → `transit-predictor` in `pyproject.toml`; real entrypoints live under `services/`.
**Consequences:** Reproducible, lint/type-clean foundation. gitleaks adds a binary dependency for contributors.
**Alternatives considered:**
- detect-secrets (pure-Python, no binary) — rejected for weaker name recognition.
- `:latest` image tags — rejected; violates the reproducibility commitment.
- mypy-strict on tests via stub packages or casts — rejected as noise for a portfolio project.

**Follow-up (2026-06-07):** First `make up` confirmed. All three pinned image tags (`redpanda:v24.2.7`, `minio:RELEASE.2024-10-13T13-34-11Z`, `postgres:16-alpine`) pulled and ran healthy. MinIO curl healthcheck confirmed working (curl 8.10.1 present in image). All containers healthy within 34 seconds.

## 0007: TransLink GTFS-RT vehicle positions endpoint verified

**Date:** 2026-05-30
**Status:** Accepted
**Context:** DATA.md flags that feed URLs change and the first ingester task is to confirm the current endpoint.
**Decision:** Confirmed `https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions` is live and serves valid GTFS-RT protobuf. A single Phase-0 poll on 2026-05-30 returned **780 vehicle position records** (feed header timestamp present, decoded cleanly via `gtfs-realtime-bindings`). No API key required.
**Consequences:** The vehicle-positions endpoint in `.env.example` and `services/ingester/poll_once.py` is trustworthy as of this date. Trip Updates and Alerts endpoints are recorded in `.env.example` but **not yet verified** — confirm before relying on them in Phase 1.
**Alternatives considered:** n/a (verification task, not a fork).

## 0008: NDJSON for the raw layer (not Parquet)

**Date:** 2026-06-08
**Status:** Accepted
**Context:** The sink consumer (Phase 1) needs to persist raw Kafka messages to MinIO. Two options: write each batch directly as Parquet, or write line-delimited JSON (NDJSON). The raw layer is a verbatim backup — every byte that came off the wire.
**Decision:** NDJSON for Phase 1. Parquet conversion is deferred to Phase 2 (Spark Structured Streaming job).
**Consequences:**
- *Good:* No schema commitment at ingest time. The vehicle_positions payload schema is observed-not-specified until Phase 2 reads live data. NDJSON is trivially writable from pure Python with no Spark dependency in the sink service. Files are human-readable for debugging.
- *Trade-off:* Raw storage is 3–5× larger than Parquet for the same data. Queries via Athena against NDJSON are slower and costlier than Parquet + partition pruning. This is acceptable because the raw layer is append-only (never queried directly in production) and Phase 2 will normalise it into the curated Parquet layer that Athena/dbt actually reads.
**Alternatives considered:**
- Parquet at ingest: rejected. Requires schema-on-write, a Spark or PyArrow dependency in the sink, and a schema that is not yet finalised from live data.
- Avro with Confluent Schema Registry: appropriate for Phase 3+; over-engineered for Phase 1 where the goal is to prove the pipeline plumbing works.

## 0009: Static GTFS loader — bus-only subset into Postgres via COPY

**Date:** 2026-06-08
**Status:** Accepted
**Context:** Phase 2 needs the scheduled timetable in Postgres so the stream processor can compute `scheduled_arrival_time` per (trip, stop). The `SEQ_GTFS.zip` archive (verified live, 36.9 MB compressed) expands to a 221 MB `stop_times.txt` plus a 36 MB `shapes.txt`. There is no dedicated spec file for the loader; design is derived from `docs/DATA.md` (table list + `route_type = 3` rule) and `specs/STREAM_PROCESSOR.md` (`STATIC_GTFS_PATH` = Postgres locally).
**Decision:** A one-shot batch loader (`services/gtfs_loader`) that:
1. **Filters to buses** (`route_type = 3`, configurable via `GTFS_ROUTE_TYPES`) and cascades the filter: kept routes → kept trips → kept stop_times. Cuts the largest table down before it touches Postgres.
2. **Skips `shapes.txt`** — route geometry is for map drawing, irrelevant to ground-truth derivation.
3. Stores `arrival_time` / `departure_time` as **`TEXT`, not `TIME`** — GTFS permits values past `24:00:00` (e.g. `25:30:00` = 01:30 next calendar day, same *service* day), which a `TIME` column rejects.
4. **DROP + CREATE on every run** inside a single transaction — the static feed is refreshed ~weekly; a full atomic replace is simpler and safer than upserts, and the bus subset is small.
5. Bulk loads with Postgres **`COPY`** (streamed, memory-bounded) rather than `INSERT`/`executemany`.
**Consequences:**
- *Good:* `stop_times` shrinks dramatically once filtered to buses; load is fast and runs in one transaction. No FK constraints (analytical load) keeps COPY fast and load-order simple.
- *Trade-off:* No referential integrity enforced in-DB; a malformed feed could load orphan `stop_times`. Acceptable — dbt tests (Phase 3) are the data-quality gate, per DATA.md.
- A future phase wanting ferries/rail flips `GTFS_ROUTE_TYPES` with no code change.
**Alternatives considered:**
- `pandas.to_sql`: rejected — pulls a heavy dependency and is far slower than COPY for the `stop_times` subset.
- Load the full multi-modal feed: rejected — wastes storage and contradicts the `route_type = 3` scope in DATA.md.
- `TIME` columns with normalisation at load: rejected — loses the service-day semantics the stream processor needs; defer parsing to where the service date is known.

## 0010: Spark runs in local[*] mode in a container (no cluster locally)

**Date:** 2026-06-08
**Status:** Accepted
**Context:** `specs/STREAM_PROCESSOR.md` commits to Spark 3.5.x / PySpark, deployed to EKS in the cloud (Phase 7). For the local stack we must choose *how* Spark runs: a dedicated master+worker cluster in Compose, embedded local mode, or on the Windows host directly. The spec itself notes "we're using Spark because the skill is portable, not because we need its scale" — Brisbane peaks at ~3,000 records/min, trivially handled by one executor.
**Decision:** Run PySpark in **`local[*]` mode inside a single container**. The PySpark/Structured-Streaming code is identical to a clustered deployment; only the master URL and submit mechanism differ. Migrating to EKS later changes configuration, not application code.
**Consequences:**
- *Good:* One container, no master/worker orchestration, fast iteration, easy debugging. Same DataFrame + Structured Streaming APIs as production. Avoids Spark-on-Windows `winutils.exe` friction by never running Spark on the host.
- *Design implication:* The hard ground-truth derivation is written as **pure Python** (unit-tested with plain pytest, no Spark/JVM), with Spark as a thin shell that reads Kafka, groups by `trip_id`, applies the pure function, and writes Parquet. Keeps the complex logic testable on any machine and keeps Spark-dependent tests (chispa) as in-container integration tests.
- *Trade-off:* Loses the literal "ran a multi-node Spark cluster locally" talking point — recovered in Phase 7 (EKS Spark) and the temporary MSK exercise.
**Alternatives considered:**
- Dedicated Spark cluster in Compose (master + worker, `spark-submit`): closest to prod topology but ~1.5 GB JVM images, more moving parts, slower iteration — scale we provably don't need locally.
- PySpark on the Windows host via uv: fastest edit-run loop but requires a `winutils.exe` Hadoop shim that is fiddly and non-reproducible across machines; rejected for the shared dev path.

## Template for new entries

```
## NNNN: <Title>

**Date:** YYYY-MM-DD
**Status:** Proposed | Accepted | Superseded by NNNN
**Context:**
**Decision:**
**Consequences:**
**Alternatives considered:**
```
