"""Stream processor configuration, loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

_VALID_OFFSETS = frozenset({"earliest", "latest"})


class StreamConfigError(ValueError):
    """Raised when required stream-processor configuration is missing or invalid."""


@dataclass(frozen=True)
class StreamConfig:
    """Immutable configuration for the Spark stream processor."""

    kafka_bootstrap_servers: str
    vehicle_positions_topic: str
    trip_updates_topic: str
    # "latest" for live tailing, "earliest" to replay the whole topic (backfill).
    starting_offsets: str
    # Structured Streaming micro-batch cadence, e.g. "60 seconds".
    trigger_interval: str
    # Late-arrival tolerance for the streaming watermark.
    watermark_minutes: int
    # Where Spark stores streaming checkpoints (offsets + state) for exactly-once
    # recovery. Local path for dev; an s3a:// path in later phases.
    checkpoint_location: str
    # Spark log verbosity (Spark is chatty at INFO).
    log_level: str
    # Postgres (static GTFS schedule, loaded by services/gtfs_loader).
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str

    @property
    def pg_conninfo(self) -> str:
        """Return a libpq connection string for the schedule repository."""
        return (
            f"host={self.pg_host} port={self.pg_port} "
            f"user={self.pg_user} password={self.pg_password} "
            f"dbname={self.pg_database}"
        )

    @classmethod
    def from_env(cls) -> StreamConfig:
        """Build config from environment variables.

        Raises:
            StreamConfigError: If any required variable is absent or invalid.
        """

        def require(key: str) -> str:
            val = os.environ.get(key, "").strip()
            if not val:
                raise StreamConfigError(f"Required environment variable {key!r} is not set")
            return val

        kafka = require("KAFKA_BOOTSTRAP_SERVERS")

        offsets = os.environ.get("STREAM_STARTING_OFFSETS", "latest").strip()
        if offsets not in _VALID_OFFSETS:
            raise StreamConfigError(
                f"STREAM_STARTING_OFFSETS must be one of {sorted(_VALID_OFFSETS)}, got {offsets!r}"
            )

        try:
            watermark = int(os.environ.get("STREAM_WATERMARK_MINUTES", "30"))
        except ValueError as exc:
            raise StreamConfigError("STREAM_WATERMARK_MINUTES must be an integer") from exc
        if watermark < 0:
            raise StreamConfigError("STREAM_WATERMARK_MINUTES must be non-negative")

        try:
            pg_port = int(os.environ.get("POSTGRES_PORT", "5432"))
        except ValueError as exc:
            raise StreamConfigError("POSTGRES_PORT must be an integer") from exc

        return cls(
            kafka_bootstrap_servers=kafka,
            vehicle_positions_topic=os.environ.get(
                "STREAM_VEHICLE_POSITIONS_TOPIC", "gtfsrt.vehicle_positions"
            ).strip(),
            trip_updates_topic=os.environ.get(
                "STREAM_TRIP_UPDATES_TOPIC", "gtfsrt.trip_updates"
            ).strip(),
            starting_offsets=offsets,
            trigger_interval=os.environ.get("STREAM_TRIGGER_INTERVAL", "60 seconds").strip(),
            watermark_minutes=watermark,
            checkpoint_location=os.environ.get(
                "STREAM_CHECKPOINT_LOCATION", "/tmp/spark-checkpoints"
            ).strip(),
            log_level=os.environ.get("STREAM_LOG_LEVEL", "WARN").strip(),
            pg_host=os.environ.get("POSTGRES_HOST", "localhost").strip(),
            pg_port=pg_port,
            pg_user=require("POSTGRES_USER"),
            pg_password=require("POSTGRES_PASSWORD"),
            pg_database=require("POSTGRES_DB"),
        )
