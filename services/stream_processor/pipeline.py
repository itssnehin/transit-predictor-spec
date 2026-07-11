"""Driver-side arrival pipeline: raw Kafka messages in, arrival events out.

Owns the cross-micro-batch trip state (ADR 0011): a dict of
:class:`TripTracker` keyed by ``trip_id``, evicted once a trip's scheduled
end + watermark has passed. Spark calls :meth:`ArrivalPipeline.process_records`
from ``foreachBatch``; everything in here is plain Python and unit-testable
without a JVM.

Input records are the ingester's Kafka messages, already JSON-decoded::

    {"envelope": {"source_feed": "vehicle_positions", ...}, "payload": {...}}

Payload quirks inherited from protobuf's JSON mapping (MessageToDict):

- uint64 fields arrive as **strings** — ``"timestamp": "1780883225"``
- unset optional fields are omitted; per the GTFS-RT spec, a missing
  ``current_status`` means ``IN_TRANSIT_TO``
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from services.stream_processor.ground_truth import (
    IN_TRANSIT_TO,
    ArrivalEvent,
    Observation,
    TripTracker,
)
from services.stream_processor.schedule_repo import ScheduleRepository

logger = logging.getLogger(__name__)

_CANCELED = "CANCELED"


def _epoch_to_utc(value: str | int | float) -> datetime:
    """Convert a POSIX-seconds value (possibly a protobuf string) to UTC."""
    return datetime.fromtimestamp(int(value), tz=UTC)


@dataclass
class _TrackedTrip:
    """A live trip's tracker plus bookkeeping for eviction."""

    tracker: TripTracker
    scheduled_end: datetime
    last_seen: datetime


class ArrivalPipeline:
    """Fold raw GTFS-RT messages into arrival events, holding trip state."""

    def __init__(self, repo: ScheduleRepository, *, watermark: timedelta) -> None:
        """Wire the pipeline to a schedule repository.

        Args:
            repo: Open schedule repository (Postgres-backed).
            watermark: Late-arrival tolerance; trip state is dropped once
                ``now > scheduled_end + watermark`` (spec: 30 minutes).
        """
        self._repo = repo
        self._watermark = watermark
        self._trips: dict[str, _TrackedTrip] = {}

    @property
    def active_trips(self) -> int:
        """Number of trips currently held in state (exposed for logging)."""
        return len(self._trips)

    def process_records(
        self, records: list[dict[str, Any]], *, now: datetime | None = None
    ) -> list[ArrivalEvent]:
        """Process one micro-batch of decoded Kafka messages.

        Args:
            records: Ingester messages (vehicle positions and trip updates,
                possibly mixed) in arbitrary order.
            now: Injectable clock for tests; defaults to the current instant.

        Returns:
            All arrival events derived from this batch.
        """
        now = now or datetime.now(tz=UTC)
        events: list[ArrivalEvent] = []

        for record in records:
            envelope = record.get("envelope") or {}
            payload = record.get("payload") or {}
            feed = envelope.get("source_feed")
            if feed == "vehicle_positions":
                events.extend(self._handle_position(payload, envelope))
            elif feed == "trip_updates":
                self._handle_trip_update(payload, envelope)
            # alerts and unknown feeds are not part of ground truth: ignore.

        self._evict_expired(now)
        return events

    # ------------------------------------------------------------------
    # Per-feed handlers
    # ------------------------------------------------------------------

    def _handle_position(
        self, payload: dict[str, Any], envelope: dict[str, Any]
    ) -> list[ArrivalEvent]:
        trip_id = (payload.get("trip") or {}).get("trip_id")
        if not trip_id:
            return []  # deadheading / unassigned vehicle

        timestamp = payload.get("timestamp") or envelope.get("feed_timestamp")
        if not timestamp:
            return []
        observed_at = _epoch_to_utc(timestamp)

        tracked = self._tracked_for(trip_id, observed_at)
        if tracked is None:
            return []
        tracked.last_seen = observed_at

        status = payload.get("current_status")
        sequence = payload.get("current_stop_sequence")
        if status is None and sequence is not None:
            # GTFS-RT: "If current_status is missing IN_TRANSIT_TO should be
            # assumed" (proto2 optional field omitted by MessageToDict).
            status = IN_TRANSIT_TO

        ingested_at = _parse_iso(envelope.get("ingested_at")) or observed_at
        observation = Observation(
            current_stop_sequence=int(sequence) if sequence is not None else None,
            current_status=status,
            timestamp=observed_at,
            ingested_at=ingested_at,
        )
        return tracked.tracker.observe(observation)

    def _handle_trip_update(
        self, payload: dict[str, Any], envelope: dict[str, Any]
    ) -> None:
        """Trip updates feed ground truth only via cancellations (spec)."""
        trip = payload.get("trip") or {}
        if trip.get("schedule_relationship") != _CANCELED:
            return
        trip_id = trip.get("trip_id")
        if not trip_id:
            return

        timestamp = payload.get("timestamp") or envelope.get("feed_timestamp")
        observed_at = _epoch_to_utc(timestamp) if timestamp else datetime.now(tz=UTC)
        # Create-and-cancel: even if no position has arrived yet, a tracker is
        # registered so later positions for the cancelled trip emit nothing.
        tracked = self._tracked_for(trip_id, observed_at)
        if tracked is not None:
            tracked.tracker.cancel()
            logger.info("Trip %s cancelled via trip_update", trip_id)

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    def _tracked_for(self, trip_id: str, observed_at: datetime) -> _TrackedTrip | None:
        tracked = self._trips.get(trip_id)
        if tracked is not None:
            return tracked

        schedule = self._repo.get(trip_id, observed_at)
        if schedule is None or not schedule.stops:
            return None  # non-bus trip or no usable scheduled times

        tracked = _TrackedTrip(
            tracker=TripTracker(schedule=schedule),
            scheduled_end=max(s.scheduled_arrival_ts for s in schedule.stops),
            last_seen=observed_at,
        )
        self._trips[trip_id] = tracked
        return tracked

    def _evict_expired(self, now: datetime) -> None:
        """Drop trips past scheduled end + watermark (spec state TTL)."""
        expired = [
            trip_id
            for trip_id, tracked in self._trips.items()
            if now > tracked.scheduled_end + self._watermark
        ]
        for trip_id in expired:
            # finalize() is a documented no-op (did_not_observe stops emit
            # nothing) but is called for contract completeness.
            self._trips[trip_id].tracker.finalize()
            del self._trips[trip_id]
        if expired:
            logger.info(
                "Evicted %d finished trips (state size now %d)", len(expired), len(self._trips)
            )


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 string to an aware datetime, or None."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Row serialisation for the curated Parquet sink (docs/DATA.md `arrivals`)
# ---------------------------------------------------------------------------

# Column order for the arrivals table. `dt` (UTC date of observed_arrival_ts,
# per spec) is last: it is the partition column.
ARRIVAL_COLUMNS: tuple[str, ...] = (
    "event_id",
    "trip_id",
    "route_id",
    "stop_id",
    "stop_sequence",
    "scheduled_arrival_ts",
    "observed_arrival_ts",
    "observed_delay_s",
    "observed_delay_imputed",
    "day_of_week",
    "hour_of_day",
    "is_school_day",
    "is_public_holiday",
    "ingested_at",
    "processed_at",
    "dt",
)


def event_to_row(event: ArrivalEvent) -> tuple[Any, ...]:
    """Flatten an ArrivalEvent into a tuple matching ARRIVAL_COLUMNS.

    Kept here (not in app.py) so it has no pyspark import and stays testable
    on the host, where the spark extra is not installed.
    """
    return (
        event.event_id,
        event.trip_id,
        event.route_id,
        event.stop_id,
        event.stop_sequence,
        event.scheduled_arrival_ts,
        event.observed_arrival_ts,
        event.observed_delay_s,
        event.observed_delay_imputed,
        event.day_of_week,
        event.hour_of_day,
        event.is_school_day,
        event.is_public_holiday,
        event.ingested_at,
        event.processed_at,
        event.observed_arrival_ts.astimezone(UTC).date().isoformat(),
    )
