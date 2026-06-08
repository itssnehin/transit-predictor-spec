"""Ingester configuration, loaded once at startup from environment variables.

All required variables are validated at load time; the process exits immediately
if anything is missing or malformed.  No defaults hide misconfiguration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

INGESTER_VERSION = "0.1.0"

_VALID_FEEDS = frozenset({"vehicle_positions", "trip_updates", "alerts"})
_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR"})


class ConfigError(ValueError):
    """Raised when a required environment variable is missing or invalid."""


@dataclass(frozen=True)
class IngesterConfig:
    """Immutable configuration snapshot for the ingester service."""

    kafka_bootstrap_servers: str
    vehicle_positions_url: str
    trip_updates_url: str
    alerts_url: str | None
    poll_interval_seconds: int
    log_level: str
    metrics_port: int
    enabled_feeds: frozenset[str]
    version: str = INGESTER_VERSION

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> IngesterConfig:
        """Build config from environment variables.

        Raises:
            ConfigError: If any required variable is absent or invalid.
        """

        def require(key: str) -> str:
            """Return env var or raise ConfigError."""
            val = os.environ.get(key, "").strip()
            if not val:
                raise ConfigError(f"Required environment variable {key!r} is not set")
            return val

        kafka = require("KAFKA_BOOTSTRAP_SERVERS")
        vp_url = require("TRANSLINK_VEHICLE_POSITIONS_URL")
        tu_url = require("TRANSLINK_TRIP_UPDATES_URL")
        alerts_url: str | None = os.environ.get("TRANSLINK_ALERTS_URL", "").strip() or None

        interval = _parse_int("POLL_INTERVAL_SECONDS", default=30, min_val=1)
        metrics_port = _parse_int("METRICS_PORT", default=9100, min_val=1, max_val=65535)

        log_level = os.environ.get("LOG_LEVEL", "INFO").upper().strip()
        if log_level not in _VALID_LOG_LEVELS:
            raise ConfigError(
                f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}, got {log_level!r}"
            )

        raw_feeds = os.environ.get("ENABLED_FEEDS", "vehicle_positions,trip_updates")
        enabled_feeds: frozenset[str] = frozenset(
            f.strip() for f in raw_feeds.split(",") if f.strip()
        )
        unknown = enabled_feeds - _VALID_FEEDS
        if unknown:
            raise ConfigError(f"Unknown feeds in ENABLED_FEEDS: {sorted(unknown)}")
        if not enabled_feeds:
            raise ConfigError("ENABLED_FEEDS must contain at least one valid feed name")

        return cls(
            kafka_bootstrap_servers=kafka,
            vehicle_positions_url=vp_url,
            trip_updates_url=tu_url,
            alerts_url=alerts_url,
            poll_interval_seconds=interval,
            log_level=log_level,
            metrics_port=metrics_port,
            enabled_feeds=enabled_feeds,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def url_for_feed(self, feed_name: str) -> str | None:
        """Return the configured URL for *feed_name*, or ``None`` if unset."""
        urls = {
            "vehicle_positions": self.vehicle_positions_url,
            "trip_updates": self.trip_updates_url,
            "alerts": self.alerts_url,
        }
        return urls.get(feed_name)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_int(
    key: str,
    *,
    default: int,
    min_val: int | None = None,
    max_val: int | None = None,
) -> int:
    """Parse an integer environment variable with bounds checking."""
    raw = os.environ.get(key, str(default)).strip()
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{key} must be an integer, got {raw!r}") from None
    if min_val is not None and value < min_val:
        raise ConfigError(f"{key} must be >= {min_val}, got {value}")
    if max_val is not None and value > max_val:
        raise ConfigError(f"{key} must be <= {max_val}, got {value}")
    return value
