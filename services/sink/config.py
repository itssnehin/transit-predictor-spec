"""Sink service configuration, loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

_VALID_FEEDS = frozenset({"vehicle_positions", "trip_updates", "alerts"})

_TOPIC_FOR_FEED = {
    "vehicle_positions": "gtfsrt.vehicle_positions",
    "trip_updates": "gtfsrt.trip_updates",
    "alerts": "gtfsrt.alerts",
}


class SinkConfigError(ValueError):
    """Raised when required sink configuration is missing or invalid."""


@dataclass(frozen=True)
class SinkConfig:
    """Immutable configuration for the sink consumer."""

    kafka_bootstrap_servers: str
    topics: tuple[str, ...]
    consumer_group_id: str
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    raw_bucket: str
    # Flush a batch when it exceeds this many bytes (default 4 MB)
    batch_size_bytes: int
    # Flush a batch if no messages for this many seconds (default 60)
    flush_interval_seconds: int

    @classmethod
    def from_env(cls) -> SinkConfig:
        """Build config from environment variables.

        Raises:
            SinkConfigError: If any required variable is absent or invalid.
        """

        def require(key: str) -> str:
            """Return env var or raise."""
            val = os.environ.get(key, "").strip()
            if not val:
                raise SinkConfigError(f"Required environment variable {key!r} is not set")
            return val

        kafka = require("KAFKA_BOOTSTRAP_SERVERS")
        group_id = os.environ.get("SINK_CONSUMER_GROUP", "sink-raw").strip()

        raw_feeds = os.environ.get("ENABLED_FEEDS", "vehicle_positions,trip_updates")
        feeds = frozenset(f.strip() for f in raw_feeds.split(",") if f.strip())
        unknown = feeds - _VALID_FEEDS
        if unknown:
            raise SinkConfigError(f"Unknown feeds in ENABLED_FEEDS: {sorted(unknown)}")
        topics = tuple(_TOPIC_FOR_FEED[f] for f in sorted(feeds))

        s3_endpoint = require("MINIO_ENDPOINT")
        s3_key = require("MINIO_ROOT_USER")
        s3_secret = require("MINIO_ROOT_PASSWORD")
        bucket = os.environ.get("RAW_BUCKET", "transit-raw")

        batch_bytes = int(os.environ.get("SINK_BATCH_SIZE_BYTES", str(4 * 1024 * 1024)))
        flush_interval = int(os.environ.get("SINK_FLUSH_INTERVAL_SECONDS", "60"))

        return cls(
            kafka_bootstrap_servers=kafka,
            topics=topics,
            consumer_group_id=group_id,
            s3_endpoint_url=s3_endpoint,
            s3_access_key=s3_key,
            s3_secret_key=s3_secret,
            raw_bucket=bucket,
            batch_size_bytes=batch_bytes,
            flush_interval_seconds=flush_interval,
        )
