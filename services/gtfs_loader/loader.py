"""Download the static GTFS feed and load the bus subset into Postgres.

Load order matters because we filter downstream tables by what survived
upstream:

    routes (route_type in {3})            -> set of bus route_ids
        trips (route_id in bus routes)    -> set of bus trip_ids
            stop_times (trip_id in trips)  -- the 221 MB file, streamed + filtered
    stops, calendar, calendar_dates       -- loaded whole (small, shared across modes)

Bulk load uses Postgres COPY rather than INSERT: for the stop_times subset
(hundreds of thousands of rows) COPY is one to two orders of magnitude faster.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import psycopg

from services.gtfs_loader.config import GtfsLoaderConfig

if TYPE_CHECKING:
    from psycopg import Cursor

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# GTFS time fields may exceed 24:00:00, so they are loaded as text. csv reads
# everything as str; these helpers map "" (GTFS's empty value) to SQL NULL and
# cast numeric columns.


def _txt(value: str) -> str | None:
    """Return the string, or None if it is empty/whitespace."""
    value = value.strip()
    return value or None


def _int(value: str) -> int | None:
    """Parse an int, or None if the field is empty."""
    value = value.strip()
    return int(value) if value else None


def _float(value: str) -> float | None:
    """Parse a float, or None if the field is empty."""
    value = value.strip()
    return float(value) if value else None


def download_gtfs(config: GtfsLoaderConfig, *, force: bool = False) -> Path:
    """Download the GTFS ZIP to the configured path, skipping if already cached.

    Args:
        config: Loader configuration.
        force: Re-download even if the file already exists.

    Returns:
        Path to the downloaded ZIP.
    """
    path = Path(config.download_path)
    if path.exists() and not force:
        logger.info("Using cached GTFS archive at %s (%.1f MB)", path, path.stat().st_size / 1e6)
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading GTFS archive from %s", config.gtfs_url)
    with httpx.stream("GET", config.gtfs_url, timeout=120.0, follow_redirects=True) as resp:
        resp.raise_for_status()
        with path.open("wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=1 << 20):
                fh.write(chunk)
    logger.info("Downloaded %.1f MB to %s", path.stat().st_size / 1e6, path)
    return path


def _open_csv(zf: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    """Yield rows of a GTFS CSV file as dicts keyed by column name.

    Uses utf-8-sig so a leading byte-order-mark on the header row is stripped.
    """
    with zf.open(name) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        yield from csv.DictReader(text)


def _copy_into(
    cur: Cursor[object],
    table: str,
    columns: list[str],
    rows: Iterable[tuple[object, ...]],
) -> int:
    """COPY an iterable of row tuples into *table*; return the row count."""
    col_list = ", ".join(columns)
    count = 0
    with cur.copy(f"COPY {table} ({col_list}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
            count += 1
    return count


def _load_routes(cur: Cursor[object], zf: zipfile.ZipFile, keep_types: frozenset[int]) -> set[str]:
    """Load routes whose route_type is in keep_types; return kept route_ids."""
    kept: set[str] = set()

    def rows() -> Iterator[tuple[object, ...]]:
        for r in _open_csv(zf, "routes.txt"):
            rtype = _int(r.get("route_type", ""))
            if rtype is None or rtype not in keep_types:
                continue
            route_id = r["route_id"].strip()
            kept.add(route_id)
            yield (
                route_id,
                _txt(r.get("route_short_name", "")),
                _txt(r.get("route_long_name", "")),
                rtype,
            )

    n = _copy_into(
        cur,
        "gtfs_routes",
        ["route_id", "route_short_name", "route_long_name", "route_type"],
        rows(),
    )
    logger.info("Loaded gtfs_routes rows=%d (kept route_types=%s)", n, sorted(keep_types))
    return kept


def _load_trips(cur: Cursor[object], zf: zipfile.ZipFile, keep_routes: set[str]) -> set[str]:
    """Load trips on a kept route; return the set of kept trip_ids."""
    kept: set[str] = set()

    def rows() -> Iterator[tuple[object, ...]]:
        for r in _open_csv(zf, "trips.txt"):
            route_id = r.get("route_id", "").strip()
            if route_id not in keep_routes:
                continue
            trip_id = r["trip_id"].strip()
            kept.add(trip_id)
            yield (
                trip_id,
                route_id,
                r.get("service_id", "").strip(),
                _txt(r.get("trip_headsign", "")),
                _int(r.get("direction_id", "")),
            )

    n = _copy_into(
        cur,
        "gtfs_trips",
        ["trip_id", "route_id", "service_id", "trip_headsign", "direction_id"],
        rows(),
    )
    logger.info("Loaded gtfs_trips rows=%d", n)
    return kept


def _load_stop_times(cur: Cursor[object], zf: zipfile.ZipFile, keep_trips: set[str]) -> int:
    """Stream the large stop_times file, keeping only rows for kept trips."""

    def rows() -> Iterator[tuple[object, ...]]:
        for r in _open_csv(zf, "stop_times.txt"):
            trip_id = r.get("trip_id", "").strip()
            if trip_id not in keep_trips:
                continue
            yield (
                trip_id,
                _int(r.get("stop_sequence", "")),
                r.get("stop_id", "").strip(),
                _txt(r.get("arrival_time", "")),
                _txt(r.get("departure_time", "")),
            )

    n = _copy_into(
        cur,
        "gtfs_stop_times",
        ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"],
        rows(),
    )
    logger.info("Loaded gtfs_stop_times rows=%d", n)
    return n


def _load_stops(cur: Cursor[object], zf: zipfile.ZipFile) -> int:
    """Load all stops (small; shared across transport modes)."""

    def rows() -> Iterator[tuple[object, ...]]:
        for r in _open_csv(zf, "stops.txt"):
            yield (
                r["stop_id"].strip(),
                _txt(r.get("stop_name", "")),
                _float(r.get("stop_lat", "")),
                _float(r.get("stop_lon", "")),
            )

    n = _copy_into(cur, "gtfs_stops", ["stop_id", "stop_name", "stop_lat", "stop_lon"], rows())
    logger.info("Loaded gtfs_stops rows=%d", n)
    return n


def _load_calendar(cur: Cursor[object], zf: zipfile.ZipFile) -> int:
    """Load the weekly service calendar."""
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    def rows() -> Iterator[tuple[object, ...]]:
        for r in _open_csv(zf, "calendar.txt"):
            yield (
                r["service_id"].strip(),
                *[_int(r.get(d, "")) for d in days],
                _txt(r.get("start_date", "")),
                _txt(r.get("end_date", "")),
            )

    n = _copy_into(
        cur, "gtfs_calendar", ["service_id", *days, "start_date", "end_date"], rows()
    )
    logger.info("Loaded gtfs_calendar rows=%d", n)
    return n


def _load_calendar_dates(cur: Cursor[object], zf: zipfile.ZipFile) -> int:
    """Load service exceptions (added/removed service on specific dates)."""

    def rows() -> Iterator[tuple[object, ...]]:
        for r in _open_csv(zf, "calendar_dates.txt"):
            yield (
                r["service_id"].strip(),
                r["date"].strip(),
                _int(r.get("exception_type", "")),
            )

    n = _copy_into(
        cur, "gtfs_calendar_dates", ["service_id", "date", "exception_type"], rows()
    )
    logger.info("Loaded gtfs_calendar_dates rows=%d", n)
    return n


def load(config: GtfsLoaderConfig, *, force_download: bool = False) -> dict[str, int]:
    """Download the GTFS feed and load the bus subset into Postgres.

    The whole load runs in a single transaction: either the schedule is fully
    replaced or nothing changes. Returns a {table: row_count} summary.

    Args:
        config: Loader configuration.
        force_download: Re-download the ZIP even if a cached copy exists.

    Returns:
        Mapping of table name to number of rows loaded.
    """
    zip_path = download_gtfs(config, force=force_download)
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")

    counts: dict[str, int] = {}
    with (
        zipfile.ZipFile(zip_path) as zf,
        psycopg.connect(config.conninfo) as conn,
        conn.cursor() as cur,
    ):
        logger.info("Applying schema (drop + create)")
        cur.execute(schema_sql)

        kept_routes = _load_routes(cur, zf, config.route_types)
        kept_trips = _load_trips(cur, zf, kept_routes)
        counts["gtfs_routes"] = len(kept_routes)
        counts["gtfs_trips"] = len(kept_trips)
        counts["gtfs_stop_times"] = _load_stop_times(cur, zf, kept_trips)
        counts["gtfs_stops"] = _load_stops(cur, zf)
        counts["gtfs_calendar"] = _load_calendar(cur, zf)
        counts["gtfs_calendar_dates"] = _load_calendar_dates(cur, zf)

        conn.commit()

    logger.info("GTFS load complete: %s", counts)
    return counts
