"""Unit tests for GtfsLoaderConfig.from_env()."""

from __future__ import annotations

import pytest

from services.gtfs_loader.config import GtfsLoaderConfig, GtfsLoaderConfigError

_REQUIRED = {
    "POSTGRES_USER": "transit",
    "POSTGRES_PASSWORD": "transit",
    "POSTGRES_DB": "transit",
}


def _load(monkeypatch: pytest.MonkeyPatch, extra: dict[str, str] | None = None) -> GtfsLoaderConfig:
    extra = extra or {}
    # Clear every var the loader reads, then set the requested ones.
    for key in (
        *_REQUIRED,
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "STATIC_GTFS_URL",
        "STATIC_GTFS_PATH",
        "GTFS_ROUTE_TYPES",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, val in {**_REQUIRED, **extra}.items():
        monkeypatch.setenv(key, val)
    return GtfsLoaderConfig.from_env()


def test_defaults_are_sane(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch)
    assert cfg.pg_host == "localhost"
    assert cfg.pg_port == 5432
    assert cfg.route_types == frozenset({3})  # buses only
    assert cfg.gtfs_url.endswith("SEQ_GTFS.zip")
    assert cfg.download_path == "data/SEQ_GTFS.zip"


def test_conninfo_contains_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch, {"POSTGRES_HOST": "db", "POSTGRES_PORT": "6000"})
    assert "host=db" in cfg.conninfo
    assert "port=6000" in cfg.conninfo
    assert "dbname=transit" in cfg.conninfo


def test_multiple_route_types_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch, {"GTFS_ROUTE_TYPES": "2,3,4"})
    assert cfg.route_types == frozenset({2, 3, 4})


def test_missing_required_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.setenv("POSTGRES_PASSWORD", "x")
    monkeypatch.setenv("POSTGRES_DB", "x")
    with pytest.raises(GtfsLoaderConfigError, match="POSTGRES_USER"):
        GtfsLoaderConfig.from_env()


def test_invalid_route_types_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(GtfsLoaderConfigError, match="comma-separated integers"):
        _load(monkeypatch, {"GTFS_ROUTE_TYPES": "bus,3"})


def test_empty_route_types_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(GtfsLoaderConfigError, match="empty set"):
        _load(monkeypatch, {"GTFS_ROUTE_TYPES": " , "})


def test_invalid_port_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(GtfsLoaderConfigError, match="POSTGRES_PORT"):
        _load(monkeypatch, {"POSTGRES_PORT": "abc"})
