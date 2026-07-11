"""Unit tests for the GTFS filter cascade and type coercion helpers.

These exercise the real loading logic against an in-memory ZIP, using a fake
cursor that captures what would have been COPY'd. No Postgres required.
"""

from __future__ import annotations

import io
import zipfile

from services.gtfs_loader import loader
from services.gtfs_loader.loader import _float, _int, _txt


def _make_zip(files: dict[str, str]) -> zipfile.ZipFile:
    """Build an in-memory ZIP from {filename: csv_text}."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    buf.seek(0)
    return zipfile.ZipFile(buf)


class _FakeCopy:
    def __init__(self, sink: list[tuple[object, ...]]) -> None:
        self._sink = sink

    def __enter__(self) -> _FakeCopy:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def write_row(self, row: tuple[object, ...]) -> None:
        self._sink.append(row)


class _FakeCursor:
    """Captures rows passed to COPY, keyed by destination table."""

    def __init__(self) -> None:
        self.copied: dict[str, list[tuple[object, ...]]] = {}

    def copy(self, sql: str) -> _FakeCopy:
        table = sql.split()[1]  # "COPY <table> (cols) FROM STDIN"
        return _FakeCopy(self.copied.setdefault(table, []))


# --------------------------------------------------------------------------
# Type coercion helpers
# --------------------------------------------------------------------------


def test_txt_empty_becomes_none() -> None:
    assert _txt("  ") is None
    assert _txt("hello") == "hello"


def test_int_empty_becomes_none() -> None:
    assert _int("") is None
    assert _int(" 7 ") == 7


def test_float_empty_becomes_none() -> None:
    assert _float("") is None
    assert _float("-27.47") == -27.47


# --------------------------------------------------------------------------
# Filter cascade
# --------------------------------------------------------------------------


def test_routes_filtered_to_bus_type() -> None:
    zf = _make_zip(
        {
            "routes.txt": (
                "route_id,route_short_name,route_long_name,route_type\n"
                "R_BUS,100,City to Suburb,3\n"
                "R_TRAIN,Ferny Grove,Rail line,2\n"
                "R_FERRY,CityCat,River ferry,4\n"
            )
        }
    )
    cur = _FakeCursor()
    kept = loader._load_routes(cur, zf, frozenset({3}))  # type: ignore[arg-type]

    assert kept == {"R_BUS"}
    assert len(cur.copied["gtfs_routes"]) == 1
    assert cur.copied["gtfs_routes"][0] == ("R_BUS", "100", "City to Suburb", 3)


def test_trips_filtered_to_kept_routes() -> None:
    zf = _make_zip(
        {
            "trips.txt": (
                "route_id,service_id,trip_id,trip_headsign,direction_id\n"
                "R_BUS,S1,T1,Downtown,0\n"
                "R_BUS,S1,T2,Uptown,1\n"
                "R_TRAIN,S1,T3,Ferny Grove,0\n"  # different route — must be dropped
            )
        }
    )
    cur = _FakeCursor()
    kept = loader._load_trips(cur, zf, {"R_BUS"})  # type: ignore[arg-type]

    assert kept == {"T1", "T2"}
    assert len(cur.copied["gtfs_trips"]) == 2


def test_stop_times_filtered_to_kept_trips() -> None:
    zf = _make_zip(
        {
            "stop_times.txt": (
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                "T1,08:00:00,08:00:30,S100,1\n"
                "T1,25:30:00,25:31:00,S200,2\n"  # >24h time must survive as text
                "T9,09:00:00,09:00:00,S300,1\n"  # unknown trip — dropped
            )
        }
    )
    cur = _FakeCursor()
    n = loader._load_stop_times(cur, zf, {"T1"})  # type: ignore[arg-type]

    assert n == 2
    rows = cur.copied["gtfs_stop_times"]
    # (trip_id, stop_sequence, stop_id, arrival_time, departure_time)
    assert rows[0] == ("T1", 1, "S100", "08:00:00", "08:00:30")
    # The after-midnight time is preserved verbatim, not coerced/rejected.
    assert rows[1] == ("T1", 2, "S200", "25:30:00", "25:31:00")
