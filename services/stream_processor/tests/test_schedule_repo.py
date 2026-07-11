"""Unit tests for schedule lookups and service-date resolution.

The calendar/resolution logic is tested as pure functions; ScheduleRepository
is tested against a fake psycopg connection with canned rows — no Postgres.

Fixture calendar: 2026-06-01 is a Monday. "SVC-WKDY" runs Mon-Fri through
2026. Times use Brisbane local (UTC+10, no DST).
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

from services.stream_processor.ground_truth import BRISBANE_TZ, parse_gtfs_time
from services.stream_processor.schedule_repo import (
    CalendarEntry,
    ScheduleRepository,
    ServiceDateCandidate,
    choose_service_date,
    is_service_active,
    parse_gtfs_date,
)

WEEKDAYS = (True, True, True, True, True, False, False)
CAL_2026 = CalendarEntry(
    weekdays=WEEKDAYS, start_date=date(2026, 1, 1), end_date=date(2026, 12, 31)
)


def _brisbane(y: int, mo: int, d: int, h: int, mi: int) -> datetime:
    return datetime(y, mo, d, h, mi, tzinfo=BRISBANE_TZ).astimezone(UTC)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def test_parse_gtfs_date() -> None:
    assert parse_gtfs_date("20260601") == date(2026, 6, 1)


def test_is_service_active_weekday_pattern() -> None:
    assert is_service_active(date(2026, 6, 1), CAL_2026, {}) is True  # Monday
    assert is_service_active(date(2026, 6, 6), CAL_2026, {}) is False  # Saturday


def test_is_service_active_outside_date_range() -> None:
    assert is_service_active(date(2027, 6, 1), CAL_2026, {}) is False


def test_is_service_active_exception_overrides_pattern() -> None:
    monday = date(2026, 6, 1)
    saturday = date(2026, 6, 6)
    # Type 2 removes a normally-running day (e.g. public holiday Monday).
    assert is_service_active(monday, CAL_2026, {monday: 2}) is False
    # Type 1 adds service on a normally-off day.
    assert is_service_active(saturday, CAL_2026, {saturday: 1}) is True


def test_is_service_active_without_calendar_row() -> None:
    d = date(2026, 6, 1)
    assert is_service_active(d, None, {}) is False
    assert is_service_active(d, None, {d: 1}) is True  # exception-only services


def test_choose_service_date_prefers_containing_window() -> None:
    # Night bus: scheduled 25:00-25:30 on Monday = 01:00-01:30 Tuesday wall
    # clock. Observed Tuesday 01:15, both Monday and Tuesday are active.
    observed = _brisbane(2026, 6, 2, 1, 15)
    monday = ServiceDateCandidate(
        service_date=date(2026, 6, 1),
        active=True,
        window=(
            parse_gtfs_time(date(2026, 6, 1), "25:00:00"),
            parse_gtfs_time(date(2026, 6, 1), "25:30:00"),
        ),
    )
    tuesday = ServiceDateCandidate(
        service_date=date(2026, 6, 2),
        active=True,
        window=(
            parse_gtfs_time(date(2026, 6, 2), "25:00:00"),
            parse_gtfs_time(date(2026, 6, 2), "25:30:00"),
        ),
    )

    chosen = choose_service_date(observed, [tuesday, monday])

    assert chosen is not None
    assert chosen.service_date == date(2026, 6, 1)  # yesterday's service day


def test_choose_service_date_falls_back_when_calendar_denies_all() -> None:
    observed = _brisbane(2026, 6, 7, 8, 3)  # Sunday, weekday-only service
    sunday = ServiceDateCandidate(
        service_date=date(2026, 6, 7),
        active=False,
        window=(
            parse_gtfs_time(date(2026, 6, 7), "08:00:00"),
            parse_gtfs_time(date(2026, 6, 7), "08:15:00"),
        ),
    )
    saturday = ServiceDateCandidate(
        service_date=date(2026, 6, 6),
        active=False,
        window=(
            parse_gtfs_time(date(2026, 6, 6), "08:00:00"),
            parse_gtfs_time(date(2026, 6, 6), "08:15:00"),
        ),
    )

    chosen = choose_service_date(observed, [sunday, saturday])

    assert chosen is not None
    assert chosen.service_date == date(2026, 6, 7)  # reality beats the calendar


def test_choose_service_date_empty_candidates() -> None:
    assert choose_service_date(_brisbane(2026, 6, 1, 8, 0), []) is None


# ---------------------------------------------------------------------------
# ScheduleRepository against a fake connection
# ---------------------------------------------------------------------------

_CALENDAR_ROWS = [
    ("SVC-WKDY", 1, 1, 1, 1, 1, 0, 0, "20260101", "20261231"),
]
_EXCEPTION_ROWS = [
    ("SVC-WKDY", "20260608", 2),  # service removed on Monday 2026-06-08
]
_TRIP_ROWS = {
    "T-BUS": [("route-199", "SVC-WKDY")],
}
_STOP_TIME_ROWS = {
    "T-BUS": [
        (1, "S1", "08:00:00"),
        (2, "S2", "08:05:00"),
        (3, "S3", None),  # non-timepoint stop: no scheduled time
        (4, "S4", "08:15:00"),
    ],
}


class _FakeCursor:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn
        self._rows: list[tuple] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple = ()) -> None:
        conn = self._conn
        if "FROM gtfs_calendar_dates" in sql:
            self._rows = list(_EXCEPTION_ROWS)
        elif "FROM gtfs_calendar" in sql:
            self._rows = list(_CALENDAR_ROWS)
        elif "FROM gtfs_trips" in sql:
            conn.trip_queries += 1
            self._rows = list(_TRIP_ROWS.get(params[0], []))
        elif "FROM gtfs_stop_times" in sql:
            self._rows = list(_STOP_TIME_ROWS.get(params[0], []))
        else:  # pragma: no cover - unexpected query
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    def __init__(self) -> None:
        self.trip_queries = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


def _open_repo() -> tuple[ScheduleRepository, _FakeConn]:
    fake = _FakeConn()
    repo = ScheduleRepository("conninfo-unused", connect=lambda _: fake)
    repo.open()
    return repo, fake


def test_get_resolves_bus_trip_to_schedule() -> None:
    repo, _ = _open_repo()
    observed = _brisbane(2026, 6, 1, 8, 3)  # Monday, mid-trip

    schedule = repo.get("T-BUS", observed)

    assert schedule is not None
    assert schedule.route_id == "route-199"
    assert schedule.service_date == date(2026, 6, 1)
    # The None-arrival stop (S3) is excluded; times anchor to the service date.
    assert [s.stop_id for s in schedule.stops] == ["S1", "S2", "S4"]
    assert schedule.stops[0].scheduled_arrival_ts == parse_gtfs_time(
        date(2026, 6, 1), "08:00:00"
    )


def test_get_unknown_trip_returns_none_and_is_negative_cached() -> None:
    repo, fake = _open_repo()
    observed = _brisbane(2026, 6, 1, 8, 3)

    assert repo.get("T-TRAIN", observed) is None
    assert repo.get("T-TRAIN", observed) is None

    assert fake.trip_queries == 1  # second call served from the negative cache


def test_get_reuses_cached_schedule_object() -> None:
    repo, _ = _open_repo()
    observed = _brisbane(2026, 6, 1, 8, 3)

    first = repo.get("T-BUS", observed)
    second = repo.get("T-BUS", observed)

    assert first is second


def test_get_warns_when_calendar_denies_but_vehicle_running(caplog) -> None:
    repo, _ = _open_repo()
    # 2026-06-08 has an exception_type=2: calendar says no service, but a
    # vehicle is reporting the trip anyway.
    observed = _brisbane(2026, 6, 8, 8, 3)

    with caplog.at_level(logging.WARNING):
        schedule = repo.get("T-BUS", observed)

    assert schedule is not None
    assert schedule.service_date == date(2026, 6, 8)
    assert any("calendar says service" in r.message for r in caplog.records)


def test_close_closes_connection() -> None:
    repo, fake = _open_repo()
    repo.close()
    assert fake.closed is True
