# Infrastructure Spec

## Local Stack (Docker Compose)

Single `docker-compose.yml` at repo root. All services on a single user-defined bridge network. Persistent volumes for data that should survive `docker compose down`.

### Services

| Service | Image | Port(s) | Notes |
|---|---|---|---|
| `redpanda` | `docker.redpanda.com/redpandadata/redpanda:latest` | 9092 (Kafka), 9644 (admin) | Kafka-compatible single-binary broker |
| `redpanda-console` | `docker.redpanda.com/redpandadata/console:latest` | 8080 | Web UI for inspecting Kafka |
| `minio` | `minio/minio:latest` | 9000 (S3 API), 9001 (console) | S3-compatible object store |
| `postgres` | `postgres:16-alpine` | 5432 | Warehouse stand-in + Airflow metadata |
| `mlflow` | `ghcr.io/mlflow/mlflow:latest` | 5000 | Experiment tracking + model registry |
| `prometheus` | `prom/prometheus:latest` | 9090 | Metrics scraping |
| `grafana` | `grafana/grafana:latest` | 3000 | Dashboards |
| `airflow-webserver` | custom (from `apache/airflow:2.10`) | 8081 | DAG UI |
| `airflow-scheduler` | same as above | — | DAG execution |
| `ingester` | `${REPO}/ingester:dev` | 9100 (metrics) | Built from `services/ingester/` |
| `stream-processor` | `${REPO}/stream-processor:dev` | 4040 (Spark UI), 9101 (metrics) | Built from `services/stream_processor/` |
| `trainer` | `${REPO}/trainer:dev` | — | Run on-demand via `docker compose run trainer` |
| `api` | `${REPO}/api:dev` | 8000 | FastAPI service |

### Volumes

| Volume | Mounted by | Purpose |
|---|---|---|
| `redpanda-data` | redpanda | Kafka log retention |
| `minio-data` | minio | Object store data |
| `postgres-data` | postgres | DB persistence |
| `mlflow-artefacts` | mlflow | Model artefacts (MinIO-backed in production-style; local file mount for dev) |

### Bootstrap

A `make bootstrap` target runs after `make up`:

1. Wait for services to be healthy (`docker compose ps` until all `(healthy)`)
2. Create MinIO buckets: `transit-raw`, `transit-curated`, `transit-mlflow`
3. Create Kafka topics: `gtfsrt.vehicle_positions`, `gtfsrt.trip_updates`, `gtfsrt.alerts`, `arrivals`
4. Initialise Postgres schemas: `raw`, `staging`, `marts`, `mlflow`, `airflow`
5. Load static GTFS to Postgres (downloads if missing)
6. Initialise Airflow DB
7. Print URLs for each UI

### Resource Limits

Set CPU and memory limits on every container. Total budget for the stack on a 16GB laptop: ~6GB RAM, ~4 CPU cores. Leaves room for IDE, browser, etc.

## Cloud Stack (AWS, Terraform)

### Terraform Layout

```
infra/terraform/
├── envs/
│   ├── dev/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── terraform.tfvars
│   └── prod/             # not used initially; structure for future
├── modules/
│   ├── network/          # VPC, subnets, security groups
│   ├── eks/              # EKS cluster + node group
│   ├── kinesis/          # Kinesis Data Stream
│   ├── msk/              # Optional, feature-flagged
│   ├── storage/          # S3 buckets, Glue Catalog
│   ├── athena/           # Workgroup
│   ├── sagemaker/        # Model registry + training role
│   ├── ecr/              # Container registries
│   ├── observability/    # CloudWatch dashboards, alarms
│   └── iam/              # Roles, policies (least-privilege)
└── README.md
```

### State Management

- Backend: S3 + DynamoDB locking
- The S3 bucket and DynamoDB table for state are bootstrapped manually once (chicken-and-egg) and documented in `infra/terraform/bootstrap.md`

### Naming Convention

- Resources: `transit-predictor-{env}-{component}` (e.g., `transit-predictor-dev-ingester-cluster`)
- Tags: every resource gets `Project=transit-predictor`, `Env=dev`, `ManagedBy=terraform`, `Owner=snehin`
- These tags drive cost reports and the `terraform destroy` safety net

### Networking

Single VPC per environment.

- 3 public subnets (1 per AZ), for load balancers
- 3 private subnets (1 per AZ), for EKS nodes
- 1 NAT Gateway (cost-optimised; production would use 1 per AZ for HA)
- VPC endpoints for S3 and ECR (saves NAT bandwidth costs)
- Security groups: principle of least privilege, but for a portfolio project don't agonise — restrict at the SG, not the IAM, level

NAT Gateway is the single biggest cost-of-idle item in this architecture (~US$45/month). It exists because EKS nodes in private subnets need to pull from public ECR for OCI images. Alternatives we don't take:

- Put EKS nodes in public subnets (cheaper but worse practice)
- Mirror all images into ECR (more work; arguable benefit)

For active development sessions, accept the cost. For long pauses, `terraform destroy` the whole VPC.

### EKS

- Mode: Managed node group (NOT Fargate, NOT Autopilot — those don't exist on EKS) OR EKS Auto Mode if available
- 1 node group, t3.medium x 1-2 nodes
- Cluster version: latest stable (verify at build time)
- Add-ons: VPC CNI, kube-proxy, CoreDNS, EBS CSI driver, AWS Load Balancer Controller (installed via Helm)
- Authentication: IAM-based, with `aws-auth` ConfigMap mapping the owner's IAM user to `system:masters`

### Kinesis

- 1 Data Stream named `gtfsrt-events`
- 1 shard (~AU$17/month) — sufficient for our volume
- Retention: 24 hours (default)
- Note: Kinesis has different semantics from Kafka (no consumer groups in the same way); the ingester and stream processor have separate code paths for the two backends

### S3 Buckets

| Bucket | Purpose | Lifecycle |
|---|---|---|
| `transit-predictor-{env}-raw` | Raw Kafka dumps | Expire after 30 days |
| `transit-predictor-{env}-curated` | Processed arrivals | Expire after 90 days |
| `transit-predictor-{env}-mlflow` | Model artefacts | Keep indefinitely |
| `transit-predictor-{env}-tf-state` | Terraform state | Versioned, encryption, MFA delete (optional) |

All buckets: versioning on (small cost, big safety), public access blocked, SSE-S3 encryption.

### IAM

Three principal categories:

1. **Owner human user**: console + CLI access via IAM Identity Center / SSO. NO long-lived access keys on the laptop.
2. **GitHub Actions OIDC role**: federated from GitHub Actions for CI/CD. Permissions: ECR push, EKS describe/update.
3. **Service roles**: one per EKS service account (via IRSA). Each has only the permissions that service needs.

### CI/CD

GitHub Actions workflow on push to main:

```
1. Checkout
2. Set up Python, install uv, restore cached deps
3. Lint (ruff)
4. Type check (mypy)
5. Test (pytest, with coverage)
6. Build Docker images (one job per service, parallel)
7. Tag with git SHA
8. (If on main) Authenticate to AWS via OIDC
9. Push images to ECR
10. Update Kubernetes manifests with new image tags (sed or kustomize)
11. kubectl apply
12. Wait for rollout to complete
```

Separate workflow on PR: only runs steps 1-6 (build + test, no deploy).

### Observability

- CloudWatch Logs: all EKS container logs ship here via Fluent Bit
- CloudWatch Metrics: AWS resource metrics (Kinesis, EKS, etc.) native
- Prometheus on EKS: scrapes service metrics (ingester, api). Optional CloudWatch metric streams from Prometheus.
- Grafana on EKS or Amazon Managed Grafana (the latter for the resume bullet, the former for cost)
- CloudWatch Alarms:
  - Kinesis IteratorAge > 5 minutes
  - API 5xx rate > 1% over 5 minutes
  - EKS node CPU > 80% sustained
  - Daily AWS spend > AU$5

### Cost Controls (Mandatory)

1. **AWS Budget** set at AU$50/month with alerts at 50%, 80%, 100%, 120%
2. **Cost allocation tags** enabled on all resources
3. **`terraform destroy` runbook** in `infra/terraform/README.md` — must teardown cleanly with zero leftover billable resources
4. **Weekly cost review**: every Sunday, check Cost Explorer, document anomalies in a `cost-log.md` file
5. **No "I'll clean it up later"**: every spin-up has a planned tear-down time

### Disaster Recovery

For a portfolio project, DR is minimal:

- Terraform state versioned in S3 (recoverable from accidental destroy)
- Trained models stored in S3 with versioning (recoverable)
- Raw data... if lost, we re-ingest from TransLink. No long-term data loss matters.

No multi-region. No backup strategy beyond S3 versioning. Documented as a known limitation.

## Migration Order (Local → Cloud)

When transitioning to AWS, the order matters:

1. **Storage first**: stand up S3 buckets, migrate object store binding from MinIO → S3 via config. Test that ingester writes to both with a feature flag.
2. **Streaming**: stand up Kinesis (or MSK), migrate ingester output. Stream processor adapter updates.
3. **Compute**: EKS cluster up, deploy ingester pod. Then stream processor. Then API.
4. **Warehouse**: Athena workgroup, port dbt models. Verify same outputs as Postgres.
5. **Training**: SageMaker Training Job runs the same trainer container. Migrate model registry usage.
6. **Orchestration**: Step Functions replaces Airflow.
7. **Observability**: CloudWatch dashboards and alarms.

Each step is independently testable. Don't try to migrate everything at once.
