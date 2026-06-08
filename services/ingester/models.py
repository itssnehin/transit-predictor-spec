"""Shared type definitions for Kafka message envelopes and payloads.

Every message published to Kafka has this two-level shape::

    {
        "envelope": { ingested_at, source_feed, ingester_version, feed_timestamp },
        "payload":  { ... protobuf-decoded fields ... }
    }

The envelope is appended by the ingester; the payload is the verbatim
protobuf-decoded GTFS-RT entity using the field names from the proto spec
(snake_case, matching the official GTFS-RT naming).
"""

from __future__ import annotations

from typing import Any, TypedDict


class Envelope(TypedDict):
    """Metadata wrapper added to every Kafka message by the ingester."""

    ingested_at: str        # UTC ISO-8601, e.g. "2026-05-30T08:00:30.123Z"
    source_feed: str        # "vehicle_positions" | "trip_updates" | "alerts"
    ingester_version: str   # semver, e.g. "0.1.0"
    feed_timestamp: int     # FeedHeader.timestamp (POSIX seconds)


class KafkaMessage(TypedDict):
    """Full Kafka message: ingester envelope + protobuf-decoded GTFS-RT payload."""

    envelope: Envelope
    payload: dict[str, Any]
