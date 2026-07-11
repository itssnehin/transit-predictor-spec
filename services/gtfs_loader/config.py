"""Static GTFS loader configuration, loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

# GTFS route_type values we keep. 3 = Bus (per the GTFS spec and docs/DATA.md).
# Kept configurable so a future phase could add ferries (4) or rail (2) without
# touching code.
_DEFAULT_ROUTE_TYPES = "3"


class GtfsLoaderConfigError(ValueError):
    """Raised when required loader configuration is missing or invalid."""


@dataclass(frozen=True)
class GtfsLoaderConfig:
    """Immutable configuration for the static GTFS loader."""

    gtfs_url: str
    download_path: str
    # Postgres connection
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str
    # route_type values to keep (e.g. {3} for buses only)
    route_types: frozenset[int]

    @property
    def conninfo(self) -> str:
        """Return a libpq connection string for psycopg.connect()."""
        return (
            f"host={self.pg_host} port={self.pg_port} "
            f"user={self.pg_user} password={self.pg_password} "
            f"dbname={self.pg_database}"
        )

    @classmethod
    def from_env(cls) -> GtfsLoaderConfig:
        """Build config from environment variables.

        Raises:
            GtfsLoaderConfigError: If any required variable is absent or invalid.
        """

        def require(key: str) -> str:
            val = os.environ.get(key, "").strip()
            if not val:
                raise GtfsLoaderConfigError(f"Required environment variable {key!r} is not set")
            return val

        url = os.environ.get(
            "STATIC_GTFS_URL",
            "https://gtfsrt.api.translink.com.au/GTFS/SEQ_GTFS.zip",
        ).strip()
        download_path = os.environ.get("STATIC_GTFS_PATH", "data/SEQ_GTFS.zip").strip()

        raw_types = os.environ.get("GTFS_ROUTE_TYPES", _DEFAULT_ROUTE_TYPES)
        try:
            route_types = frozenset(int(t.strip()) for t in raw_types.split(",") if t.strip())
        except ValueError as exc:
            raise GtfsLoaderConfigError(
                f"GTFS_ROUTE_TYPES must be comma-separated integers, got {raw_types!r}"
            ) from exc
        if not route_types:
            raise GtfsLoaderConfigError("GTFS_ROUTE_TYPES resolved to an empty set")

        try:
            pg_port = int(os.environ.get("POSTGRES_PORT", "5432"))
        except ValueError as exc:
            raise GtfsLoaderConfigError("POSTGRES_PORT must be an integer") from exc

        return cls(
            gtfs_url=url,
            download_path=download_path,
            pg_host=os.environ.get("POSTGRES_HOST", "localhost").strip(),
            pg_port=pg_port,
            pg_user=require("POSTGRES_USER"),
            pg_password=require("POSTGRES_PASSWORD"),
            pg_database=require("POSTGRES_DB"),
            route_types=route_types,
        )
