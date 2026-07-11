"""Ground-truth derivation: observed arrivals from vehicle position sequences.

This is the algorithmic core of the project (specs/STREAM_PROCESSOR.md,
"Ground Truth Algorithm"). It is deliberately pure Python with **no Spark
imports**: the logic is unit-testable with plain pytest, and Spark acts only
as a thin distribution shell that groups observations by trip and folds them
through a :class:`TripTracker`.

Rules implemented (see also docs/DATA.md "Ground Truth Definition"):

1. ``STOPPED_AT`` stop N (not yet recorded) -> real arrival at N,
   ``observed_arrival_ts`` = the record's timestamp. On a long dwell (multiple
   ``STOPPED_AT`` records) the first one wins.
2. Any advance to sequence N (``STOPPED_AT`` / ``IN_TRANSIT_TO`` /
   ``INCOMING_AT``) implies every unrecorded scheduled stop *before* N was
   passed -> impute arrivals for them with ``observed_delay_imputed = True``.
3. Backtracking (reported sequence decreases) is logged as a warning; the
   recorded-stop set makes re-processing naturally idempotent.
4. A cancelled trip (``schedule_relationship = CANCELED`` on the TripUpdate)
   emits no further events.
5. Stops the vehicle never gave evidence of passing are *not* emitted
   (``did_not_observe`` — excluded from training per docs/DATA.md).

Event ids are deterministic (UUID5 of trip + service date + stop sequence):
Kafka delivery is at-least-once, so replays must produce identical ids for
downstream deduplication rather than fresh random ones.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# TransLink operates on Brisbane time; Queensland has no DST, but zoneinfo
# handles that for us either way. Scheduled GTFS times are wall-clock times
# on the *service date* in this zone.
BRISBANE_TZ = ZoneInfo("Australia/Brisbane")

# GTFS-RT VehiclePosition.current_status values as rendered by
# protobuf MessageToDict (enum name strings).
STOPPED_AT = "STOPPED_AT"
IN_TRANSIT_TO = "IN_TRANSIT_TO"
INCOMING_AT = "INCOMING_AT"

_KNOWN_STATUSES = frozenset({STOPPED_AT, IN_TRANSIT_TO, INCOMING_AT})

# Namespace for deterministic arrival event ids.
_EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "transit-predictor/arrivals")


def parse_gtfs_time(service_date: date, gtfs_time: str) -> datetime:
    """Convert a GTFS ``HH:MM:SS`` string to a UTC timestamp.

    GTFS times are measured from *noon minus 12 hours* on the service date and
    may exceed ``24:00:00`` (e.g. ``25:30:00`` is 01:30 the next calendar day,
    still part of the same service day). This is why gtfs_stop_times stores
    them as TEXT (ADR 0009). Anchoring at midnight Brisbane time and adding a
    timedelta handles the overflow naturally.

    Args:
        service_date: The service date the trip runs on (Brisbane local).
        gtfs_time: ``HH:MM:SS``, where HH may be >= 24.

    Returns:
        The scheduled moment as a timezone-aware UTC datetime.
    """
    hours, minutes, seconds = (int(part) for part in gtfs_time.split(":"))
    midnight = datetime(
        service_date.year, service_date.month, service_date.day, tzinfo=BRISBANE_TZ
    )
    return (midnight + timedelta(hours=hours, minutes=minutes, seconds=seconds)).astimezone(UTC)


@dataclass(frozen=True)
class ScheduledStop:
    """One scheduled stop on a trip (a row of gtfs_stop_times, resolved)."""

    stop_id: str
    stop_sequence: int
    scheduled_arrival_ts: datetime


@dataclass(frozen=True)
class TripSchedule:
    """The static-GTFS context for one trip on one service date."""

    trip_id: str
    route_id: str
    service_date: date
    stops: tuple[ScheduledStop, ...]

    def stop_at(self, sequence: int) -> ScheduledStop | None:
        """Return the scheduled stop with this sequence, or None."""
        for stop in self.stops:
            if stop.stop_sequence == sequence:
                return stop
        return None


@dataclass(frozen=True)
class Observation:
    """One GTFS-RT vehicle position record, already parsed and UTC-normalised."""

    current_stop_sequence: int | None
    current_status: str | None
    timestamp: datetime
    ingested_at: datetime


@dataclass(frozen=True)
class ArrivalEvent:
    """A derived arrival — one row of the curated ``arrivals`` table.

    ``is_school_day`` / ``is_public_holiday`` are None until the in-repo
    Queensland calendar CSVs exist (flagged spec gap; see docs/DATA.md
    "Tertiary Sources"). dbt models treat None as unknown, not False.
    """

    event_id: str
    trip_id: str
    route_id: str
    stop_id: str
    stop_sequence: int
    scheduled_arrival_ts: datetime
    observed_arrival_ts: datetime
    observed_delay_s: int
    observed_delay_imputed: bool
    day_of_week: int  # 0=Mon, from scheduled arrival in Brisbane local time
    hour_of_day: int  # from scheduled arrival in Brisbane local time
    is_school_day: bool | None
    is_public_holiday: bool | None
    ingested_at: datetime
    processed_at: datetime


@dataclass
class TripTracker:
    """Stateful per-trip fold: feed observations, collect arrival events.

    One tracker instance corresponds to one (trip_id, service_date). The
    mutable state is deliberately tiny — a set of already-recorded stop
    sequences, the last seen sequence, and a cancelled flag — so it can be
    serialised into Spark's state store later.
    """

    schedule: TripSchedule
    recorded: set[int] = field(default_factory=set)
    last_sequence: int | None = None
    cancelled: bool = False

    def observe(self, obs: Observation) -> list[ArrivalEvent]:
        """Fold one vehicle position into the trip state.

        Returns:
            Newly derived arrival events (possibly empty), in stop order.
        """
        if self.cancelled:
            return []
        if obs.current_stop_sequence is None or obs.current_status is None:
            return []
        if obs.current_status not in _KNOWN_STATUSES:
            logger.warning(
                "Unknown current_status %r on trip %s — ignoring record",
                obs.current_status,
                self.schedule.trip_id,
            )
            return []

        sequence = obs.current_stop_sequence
        if self.last_sequence is not None and sequence < self.last_sequence:
            # Edge case per spec: backtracking. Rare; latest record is the
            # source of truth, and `recorded` keeps us idempotent.
            logger.warning(
                "Backtrack on trip %s: sequence %d after %d",
                self.schedule.trip_id,
                sequence,
                self.last_sequence,
            )
        self.last_sequence = sequence

        events: list[ArrivalEvent] = []

        # Rule 2: reaching sequence N is evidence every earlier scheduled stop
        # was passed. Impute arrivals for the unrecorded ones.
        for stop in self.schedule.stops:
            if stop.stop_sequence < sequence and stop.stop_sequence not in self.recorded:
                events.append(self._emit(stop, obs, imputed=True))

        # Rule 1: a real, directly observed arrival.
        if obs.current_status == STOPPED_AT and sequence not in self.recorded:
            stop_here = self.schedule.stop_at(sequence)
            if stop_here is None:
                logger.warning(
                    "Trip %s reported STOPPED_AT unscheduled sequence %d — skipping",
                    self.schedule.trip_id,
                    sequence,
                )
            else:
                events.append(self._emit(stop_here, obs, imputed=False))

        return events

    def cancel(self) -> None:
        """Mark the trip cancelled: no further events will be emitted."""
        self.cancelled = True

    def finalize(self) -> list[ArrivalEvent]:
        """Close out the trip (watermark expiry / new trip_id on the vehicle).

        Rules 1–2 already emitted everything the vehicle gave evidence for, so
        stops still unrecorded here were never confirmably passed: they are
        ``did_not_observe`` and deliberately produce no rows (docs/DATA.md —
        such stops are excluded from training rather than given fake labels).
        """
        return []

    def _emit(self, stop: ScheduledStop, obs: Observation, *, imputed: bool) -> ArrivalEvent:
        """Build the arrival event for *stop* and mark it recorded."""
        self.recorded.add(stop.stop_sequence)
        scheduled_local = stop.scheduled_arrival_ts.astimezone(BRISBANE_TZ)
        delay = obs.timestamp - stop.scheduled_arrival_ts
        event_id = uuid.uuid5(
            _EVENT_NAMESPACE,
            f"{self.schedule.trip_id}/{self.schedule.service_date.isoformat()}"
            f"/{stop.stop_sequence}",
        )
        return ArrivalEvent(
            event_id=str(event_id),
            trip_id=self.schedule.trip_id,
            route_id=self.schedule.route_id,
            stop_id=stop.stop_id,
            stop_sequence=stop.stop_sequence,
            scheduled_arrival_ts=stop.scheduled_arrival_ts,
            observed_arrival_ts=obs.timestamp,
            observed_delay_s=int(delay.total_seconds()),
            observed_delay_imputed=imputed,
            day_of_week=scheduled_local.weekday(),
            hour_of_day=scheduled_local.hour,
            is_school_day=None,
            is_public_holiday=None,
            ingested_at=obs.ingested_at,
            processed_at=datetime.now(tz=UTC),
        )


def derive_arrivals(
    schedule: TripSchedule, observations: list[Observation]
) -> list[ArrivalEvent]:
    """Batch convenience: fold a full observation sequence for one trip.

    Observations are processed in timestamp order regardless of input order —
    Kafka partitions preserve per-key order, but micro-batch boundaries do not.

    Args:
        schedule: The trip's static-GTFS schedule context.
        observations: Vehicle positions for this trip (any order).

    Returns:
        All derived arrival events, in emission order.
    """
    tracker = TripTracker(schedule=schedule)
    events: list[ArrivalEvent] = []
    for obs in sorted(observations, key=lambda o: o.timestamp):
        events.extend(tracker.observe(obs))
    events.extend(tracker.finalize())
    return events
