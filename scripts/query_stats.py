#!/usr/bin/env python3
"""Print summary statistics from the raw vehicle_positions NDJSON files.

This is the Phase 1 deliverable query described in docs/ROADMAP.md:

    "A query (Python script) that loads [the raw data] and prints summary
    stats: number of vehicles seen, distinct routes, time coverage."

The script reads every NDJSON file under::

    s3://<RAW_BUCKET>/raw/vehicle_positions/

and prints a summary table to stdout.  Each line in those files is a
KafkaMessage JSON object (see services/ingester/models.py) produced by the
ingester service.

Usage::

    uv run python scripts/query_stats.py

Environment variables (all optional — defaults match .env.example):

    MINIO_ENDPOINT       http://localhost:9000
    MINIO_ROOT_USER      minioadmin
    MINIO_ROOT_PASSWORD  minioadmin
    RAW_BUCKET           transit-raw
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _make_s3_client() -> Any:  # noqa: ANN401 — boto3 ships no PEP-561 stubs
    """Build a boto3 S3 client from environment variables."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        config=Config(signature_version="s3v4"),
    )


def _iter_records(
    s3: Any,  # noqa: ANN401 — boto3 ships no PEP-561 stubs
    bucket: str,
    prefix: str,
) -> Generator[dict[str, Any], None, None]:
    """Yield every parsed JSON record from all .ndjson files under *prefix*.

    Uses the list_objects_v2 paginator so it handles buckets with more than
    1 000 objects (the default AWS page size).
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            if not key.endswith(".ndjson"):
                continue
            response = s3.get_object(Bucket=bucket, Key=key)
            body: str = response["Body"].read().decode("utf-8")
            for raw_line in body.splitlines():
                line = raw_line.strip()
                if line:
                    yield json.loads(line)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Entry point.  Returns an exit code (0 = success, 1 = error)."""
    bucket = os.environ.get("RAW_BUCKET", "transit-raw")
    prefix = "raw/vehicle_positions/"

    s3 = _make_s3_client()

    # Guard: bucket must exist before we try to list it.
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            print(
                f"Bucket {bucket!r} does not exist. "
                "Run `make bootstrap` first, then let the ingester run for a few minutes.",
                file=sys.stderr,
            )
            return 1
        raise

    # -----------------------------------------------------------------
    # Collect stats from every NDJSON record
    # -----------------------------------------------------------------
    vehicle_ids: set[str] = set()
    route_ids: set[str] = set()
    timestamps: list[int] = []
    record_count = 0

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    print(f"Reading from {endpoint}  s3://{bucket}/{prefix}")
    print("(this may take a moment if many files have accumulated)")
    print()

    for record in _iter_records(s3, bucket, prefix):
        record_count += 1

        envelope: dict[str, Any] = record.get("envelope", {})
        payload: dict[str, Any] = record.get("payload", {})

        # Vehicle ID — nested inside the VehiclePosition.vehicle sub-message
        vid: str = payload.get("vehicle", {}).get("id", "")
        if vid:
            vehicle_ids.add(vid)

        # Route ID — from the TripDescriptor embedded in VehiclePosition
        rid: str = payload.get("trip", {}).get("route_id", "")
        if rid:
            route_ids.add(rid)

        # Timestamp — prefer the per-vehicle GPS timestamp over the feed header
        # timestamp because it is closer to the actual observation time.
        raw_ts: int | None = payload.get("timestamp")
        if raw_ts:
            timestamps.append(int(raw_ts))
        else:
            feed_ts: int | None = envelope.get("feed_timestamp")
            if feed_ts:
                timestamps.append(int(feed_ts))

    # -----------------------------------------------------------------
    # Format and print the summary table
    # -----------------------------------------------------------------
    if record_count == 0:
        print(
            "No records found under the vehicle_positions prefix.\n"
            "Start the stack (`make up && make bootstrap`) and wait a few minutes\n"
            "for the ingester to poll TransLink and the sink to flush to MinIO.",
        )
        return 0

    # Time coverage string
    if timestamps:
        ts_min = datetime.fromtimestamp(min(timestamps), tz=UTC)
        ts_max = datetime.fromtimestamp(max(timestamps), tz=UTC)
        duration_secs = max(timestamps) - min(timestamps)
        duration_str = _format_duration(duration_secs)
        time_coverage = (
            f"{ts_min.strftime('%Y-%m-%d %H:%M:%S')} UTC  ->  "
            f"{ts_max.strftime('%Y-%m-%d %H:%M:%S')} UTC  "
            f"({duration_str})"
        )
    else:
        time_coverage = "N/A (no vehicle timestamps in payload)"

    sep = "=" * 64
    print(sep)
    print("  Transit Predictor - Phase 1 | Vehicle Positions Summary")
    print(sep)
    print(f"  Records loaded     : {record_count:>10,}")
    print(f"  Distinct vehicles  : {len(vehicle_ids):>10,}")
    print(f"  Distinct routes    : {len(route_ids):>10,}")
    print(f"  Time coverage      : {time_coverage}")
    print(sep)

    return 0


def _format_duration(seconds: int) -> str:
    """Return a human-readable duration string, e.g. '2 h 15 min' or '45 min'."""
    if seconds < 60:
        return f"{seconds} sec"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours} h {mins} min"


if __name__ == "__main__":
    raise SystemExit(main())
