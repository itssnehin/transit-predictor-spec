"""Schedule lookups: resolve a live observation to a TripSchedule.

Bridges the static GTFS tables in Postgres (loaded by services/gtfs_loader)
and the pure ground-truth algorithm. The interesting problem here is
**service-date resolution**: a position observed at 01:30 may belong to a
trip on *yesterday's* service date (scheduled past ``24:00:00``) or today's.

Resolution is full-calendar (owner decision, 2026-06-22):

1. Candidate dates are the observation's Brisbane date and the day before.
   (GTFS encodes after-midnight trips as ``>24h`` times on the *previous*
   service date, so "tomorrow" is never a candidate.)
2. A candidate is *active* if the trip's ``service_id`` runs that day per
   ``gtfs_calendar`` (weekday bits + date range), overridden by
   ``gtfs_calendar_dates`` exceptions (1 = added, 2 = removed).
3. Among active candidates, pick the one whose scheduled time window
   (first to last stop) is closest to the observation instant.
4. If the calendar says *neither* candidate is active but a vehicle is
   demonstrably running the trip, reality wins: fall back to the
   closest-window candidate and log a warning.

The calendar tables are tiny (hundreds of rows) and are loaded whole into
memory at ``open()``. Per-trip stop times are fetched lazily and cached,
including **negative caching**: the SEQ realtime feed carries trains and
ferries whose trip_ids are not in our bus-only tables, and they keep
reporting forever — without the negative cache they would hit Postgres on
every poll.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import psycopg

from services.stream_processor.ground_truth import (
    BRISBANE_TZ,
    ScheduledStop,
    TripSchedule,
    parse_gtfs_time,
)

logger = logging.getLogger(__name__)


def parse_gtfs_date(value: str) -> date:
    """Parse a GTFS ``YYYYMMDD`` date string."""
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


@dataclass(frozen=True)
class CalendarEntry:
    """One gtfs_calendar row: which weekdays a service runs, within a range."""

    weekdays: tuple[bool, bool, bool, bool, bool, bool, bool]  # Monday-first
    start_date: date
    end_date: date


@dataclass(frozen=True)
class ServiceDateCandidate:
    """A possible service date for an observation, with calendar verdict."""

    service_date: date
    active: bool
    # Scheduled first/last arrival for the trip on this date, as UTC instants.
    window: tuple[datetime, datetime]


def is_service_active(
    service_date: date,
    calendar: CalendarEntry | None,
    exceptions: Mapping[date, int],
) -> bool:
    """Return whether a service runs on *service_date* per the GTFS calendar.

    ``gtfs_calendar_dates`` exceptions override the weekly pattern:
    exception_type 1 adds service on that date, 2 removes it.
    """
    exception = exceptions.get(service_date)
    if exception == 1:
        return True
    if exception == 2:
        return False
    if calendar is None:
        return False
    if not (calendar.start_date <= service_date <= calendar.end_date):
        return False
    return calendar.weekdays[service_date.weekday()]


def choose_service_date(
    observed_at: datetime,
    candidates: Sequence[ServiceDateCandidate],
) -> ServiceDateCandidate | None:
    """Pick the best service-date candidate for an observation.

    Active candidates are preferred; if the calendar rejects every candidate
    (yet a vehicle is running the trip), all candidates stay in play — reality
    beats the calendar. The winner is the candidate whose scheduled window is
    closest to the observation (distance 0 if inside the window). Ties go to
    the earlier-listed candidate, so callers should list today first.
    """
    if not candidates:
        return None
    pool = [c for c in candidates if c.active] or list(candidates)

    def distance(candidate: ServiceDateCandidate) -> float:
        start, end = candidate.window
        if start <= observed_at <= end:
            return 0.0
        return min(
            abs((observed_at - start).total_seconds()),
            abs((observed_at - end).total_seconds()),
        )

    return min(pool, key=distance)


class ScheduleRepository:
    """Cached lookups from live trip_ids to schedule context.

    Usage::

        repo = ScheduleRepository(conninfo)
        repo.open()                      # connect + load calendar into memory
        schedule = repo.get(trip_id, observed_at)   # None for non-bus trips
    """

    def __init__(
        self,
        conninfo: str,
        *,
        connect: Callable[[str], Any] = psycopg.connect,
    ) -> None:
        """Store connection settings; ``connect`` is injectable for tests."""
        self._conninfo = conninfo
        self._connect = connect
        self._conn: Any = None
        self._calendar: dict[str, CalendarEntry] = {}
        self._exceptions: dict[str, dict[date, int]] = {}
        # trip_id -> (route_id, service_id), or None for known non-bus trips.
        self._trip_meta: dict[str, tuple[str, str] | None] = {}
        # trip_id -> ordered (stop_sequence, stop_id, arrival_time) rows.
        self._trip_times: dict[str, tuple[tuple[int, str, str | None], ...]] = {}
        self._schedules: dict[tuple[str, date], TripSchedule] = {}

    def open(self) -> None:
        """Connect to Postgres and load the (small) calendar tables whole."""
        self._conn = self._connect(self._conninfo)
        for service_id, *days, start, end in self._query(
            "SELECT service_id, monday, tuesday, wednesday, thursday, friday,"
            " saturday, sunday, start_date, end_date FROM gtfs_calendar"
        ):
            self._calendar[service_id] = CalendarEntry(
                weekdays=tuple(bool(d) for d in days),  # type: ignore[arg-type]
                start_date=parse_gtfs_date(start),
                end_date=parse_gtfs_date(end),
            )
        for service_id, date_str, exception_type in self._query(
            "SELECT service_id, date, exception_type FROM gtfs_calendar_dates"
        ):
            self._exceptions.setdefault(service_id, {})[parse_gtfs_date(date_str)] = (
                exception_type
            )
        logger.info(
            "Calendar loaded: %d services, %d exception dates",
            len(self._calendar),
            sum(len(e) for e in self._exceptions.values()),
        )

    def close(self) -> None:
        """Close the Postgres connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def get(self, trip_id: str, observed_at: datetime) -> TripSchedule | None:
        """Resolve a trip observation to its schedule, or None if not a bus trip.

        Args:
            trip_id: The GTFS-RT trip identifier.
            observed_at: Server timestamp of the observation (UTC).

        Returns:
            The trip's schedule anchored to the resolved service date, or None
            when the trip is unknown (non-bus modes) or has no usable times.
        """
        meta = self._meta_for(trip_id)
        if meta is None:
            return None
        route_id, service_id = meta

        times = self._times_for(trip_id)
        if not times:
            return None

        local_date = observed_at.astimezone(BRISBANE_TZ).date()
        candidates = []
        for candidate_date in (local_date, local_date - timedelta(days=1)):
            window = _window_for(times, candidate_date)
            if window is None:
                continue
            candidates.append(
                ServiceDateCandidate(
                    service_date=candidate_date,
                    active=is_service_active(
                        candidate_date,
                        self._calendar.get(service_id),
                        self._exceptions.get(service_id, {}),
                    ),
                    window=window,
                )
            )

        chosen = choose_service_date(observed_at, candidates)
        if chosen is None:
            logger.warning("Trip %s has no schedulable candidate dates", trip_id)
            return None
        if not chosen.active:
            logger.warning(
                "Trip %s observed but calendar says service %s inactive on %s — "
                "using closest window anyway",
                trip_id,
                service_id,
                chosen.service_date,
            )

        key = (trip_id, chosen.service_date)
        if key not in self._schedules:
            self._schedules[key] = TripSchedule(
                trip_id=trip_id,
                route_id=route_id,
                service_date=chosen.service_date,
                stops=tuple(
                    ScheduledStop(
                        stop_id=stop_id,
                        stop_sequence=seq,
                        scheduled_arrival_ts=parse_gtfs_time(chosen.service_date, arrival),
                    )
                    for seq, stop_id, arrival in times
                    if arrival is not None
                ),
            )
        return self._schedules[key]

    # ------------------------------------------------------------------
    # Internal: cached queries
    # ------------------------------------------------------------------

    def _query(self, sql: str, params: tuple[object, ...] = ()) -> list[tuple[Any, ...]]:
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            rows: list[tuple[Any, ...]] = cur.fetchall()
            return rows

    def _meta_for(self, trip_id: str) -> tuple[str, str] | None:
        if trip_id not in self._trip_meta:
            rows = self._query(
                "SELECT route_id, service_id FROM gtfs_trips WHERE trip_id = %s",
                (trip_id,),
            )
            # None is cached too: non-bus trip_ids report continuously and
            # must not re-query Postgres every poll.
            self._trip_meta[trip_id] = (rows[0][0], rows[0][1]) if rows else None
        return self._trip_meta[trip_id]

    def _times_for(self, trip_id: str) -> tuple[tuple[int, str, str | None], ...]:
        if trip_id not in self._trip_times:
            rows = self._query(
                "SELECT stop_sequence, stop_id, arrival_time FROM gtfs_stop_times"
                " WHERE trip_id = %s ORDER BY stop_sequence",
                (trip_id,),
            )
            self._trip_times[trip_id] = tuple((r[0], r[1], r[2]) for r in rows)
        return self._trip_times[trip_id]


def _window_for(
    times: Sequence[tuple[int, str, str | None]], service_date: date
) -> tuple[datetime, datetime] | None:
    """Scheduled [first, last] arrival for a trip on a date, as UTC instants."""
    known = [arrival for _, _, arrival in times if arrival is not None]
    if not known:
        return None
    return (
        parse_gtfs_time(service_date, known[0]),
        parse_gtfs_time(service_date, known[-1]),
    )
