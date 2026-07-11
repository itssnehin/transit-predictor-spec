"""Unit tests for StreamConfig.from_env().

These are pure-Python (no Spark import), so they run on the host without a JVM.
Spark-dependent tests live in test_session/test_app and run in-container.
"""

from __future__ import annotations

import pytest

from services.stream_processor.config import StreamConfig, StreamConfigError


def _load(monkeypatch: pytest.MonkeyPatch, extra: dict[str, str] | None = None) -> StreamConfig:
    extra = extra or {}
    for key in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "STREAM_STARTING_OFFSETS",
        "STREAM_WATERMARK_MINUTES",
        "STREAM_TRIGGER_INTERVAL",
        "STREAM_CHECKPOINT_LOCATION",
        "STREAM_VEHICLE_POSITIONS_TOPIC",
        "STREAM_LOG_LEVEL",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    for key, val in extra.items():
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
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(StreamConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        StreamConfig.from_env()


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
