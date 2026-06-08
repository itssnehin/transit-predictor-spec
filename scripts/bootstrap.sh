#!/usr/bin/env bash
# scripts/bootstrap.sh
#
# One-time initialisation after `make up`:
#   1. Create the three Kafka topics in Redpanda (3 partitions each)
#   2. Create the two MinIO buckets (transit-raw, transit-curated)
#
# Safe to re-run — both operations are create-if-not-exists.
#
# Usage:
#   make bootstrap          # preferred
#   ./scripts/bootstrap.sh  # direct

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ---------------------------------------------------------------------------
# Load .env so the Python bucket-creation step can read MinIO credentials.
# The Docker stack overrides these with its own internal values, but for the
# MinIO API call we connect from the host (localhost:9000).
# ---------------------------------------------------------------------------
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a                        # export all variables that follow
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Fall back to the same defaults as .env.example
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

echo "==> Creating Kafka topics (via rpk inside the redpanda container)..."

# rpk is Redpanda's built-in CLI tool. --replicas 1 because we run a
# single-node cluster in development.
TOPICS=(
    "gtfsrt.vehicle_positions"
    "gtfsrt.trip_updates"
    "gtfsrt.alerts"
)

for topic in "${TOPICS[@]}"; do
    echo "    topic: $topic  (partitions=3, replicas=1)"
    docker compose exec -T redpanda \
        rpk topic create "$topic" \
        --partitions 3 \
        --replicas 1 \
        2>&1 | grep -v "^$" || true   # swallow "already exists" exit code
done

echo ""
echo "==> Creating MinIO buckets (via boto3 from the host)..."

cd "$PROJECT_ROOT"
MINIO_ENDPOINT="$MINIO_ENDPOINT" \
MINIO_ROOT_USER="$MINIO_ROOT_USER" \
MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
uv run python - <<'PYEOF'
import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint    = os.environ["MINIO_ENDPOINT"]
access_key  = os.environ["MINIO_ROOT_USER"]
secret_key  = os.environ["MINIO_ROOT_PASSWORD"]
raw_bucket  = os.environ.get("RAW_BUCKET", "transit-raw")
cur_bucket  = os.environ.get("CURATED_BUCKET", "transit-curated")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
)

for bucket in [raw_bucket, cur_bucket]:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"    bucket: {bucket}  — created")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"    bucket: {bucket}  — already exists (ok)")
        else:
            print(f"    bucket: {bucket}  — ERROR: {exc}", file=sys.stderr)
            sys.exit(1)
PYEOF

echo ""
echo "Bootstrap complete."
echo ""
echo "  Kafka topics  : gtfsrt.vehicle_positions"
echo "                  gtfsrt.trip_updates"
echo "                  gtfsrt.alerts"
echo "  MinIO buckets : transit-raw"
echo "                  transit-curated"
echo ""
echo "Next: check ingester logs with \`make logs\`"
echo "      then run \`uv run python scripts/query_stats.py\` after data accumulates."
