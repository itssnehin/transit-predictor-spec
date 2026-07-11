"""Unit tests for StreamConfig.from_env().

These are pure-Python (no Spark import), so they run on the host without a JVM.
Spark-dependent tests live in test_session/test_app and run in-container.
"""

from __future__ import annotations

import pytest

from services.stream_processor.config import StreamConfig, StreamConfigError

_REQUIRED = {
    "KAFKA_BOOTSTRAP_SERVERS": "redpanda:9092",
    "POSTGRES_USER": "transit",
    "POSTGRES_PASSWORD": "transit",
    "POSTGRES_DB": "transit",
}


def _load(monkeypatch: pytest.MonkeyPatch, extra: dict[str, str] | None = None) -> StreamConfig:
    extra = extra or {}
    for key in (
        *_REQUIRED,
        "STREAM_STARTING_OFFSETS",
        "STREAM_WATERMARK_MINUTES",
        "STREAM_TRIGGER_INTERVAL",
        "STREAM_CHECKPOINT_LOCATION",
        "STREAM_VEHICLE_POSITIONS_TOPIC",
        "STREAM_LOG_LEVEL",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, val in {**_REQUIRED, **extra}.items():
        monkeypatch.setenv(key, val)
    return StreamConfig.from_env()


def test_defaults_are_sane(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch)
    assert cfg.kafka_bootstrap_servers == "redpanda:9092"
    assert cfg.vehicle_positions_topic == "gtfsrt.vehicle_positions"
    assert cfg.starting_offsets == "latest"
    assert cfg.trigger_interval == "60 seconds"
    assert cfg.watermark_minutes == 30


def test_missing_kafka_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, val in _REQUIRED.items():
        monkeypatch.setenv(key, val)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(StreamConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        StreamConfig.from_env()


def test_missing_postgres_credentials_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, val in _REQUIRED.items():
        monkeypatch.setenv(key, val)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    with pytest.raises(StreamConfigError, match="POSTGRES_USER"):
        StreamConfig.from_env()


def test_pg_conninfo_contains_connection_details(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch, {"POSTGRES_HOST": "postgres", "POSTGRES_PORT": "5433"})
    assert "host=postgres" in cfg.pg_conninfo
    assert "port=5433" in cfg.pg_conninfo
    assert "dbname=transit" in cfg.pg_conninfo


def test_invalid_offsets_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(StreamConfigError, match="STREAM_STARTING_OFFSETS"):
        _load(monkeypatch, {"STREAM_STARTING_OFFSETS": "newest"})


def test_earliest_offsets_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch, {"STREAM_STARTING_OFFSETS": "earliest"})
    assert cfg.starting_offsets == "earliest"


def test_non_integer_watermark_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(StreamConfigError, match="STREAM_WATERMARK_MINUTES"):
        _load(monkeypatch, {"STREAM_WATERMARK_MINUTES": "half-hour"})


def test_negative_watermark_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(StreamConfigError, match="non-negative"):
        _load(monkeypatch, {"STREAM_WATERMARK_MINUTES": "-5"})
