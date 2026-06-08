"""Unit tests for IngesterConfig.from_env()."""

import pytest

from services.ingester.config import ConfigError, IngesterConfig

_BASE_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "TRANSLINK_VEHICLE_POSITIONS_URL": "http://example.com/vp",
    "TRANSLINK_TRIP_UPDATES_URL": "http://example.com/tu",
}


def _load(extra: dict | None = None, *, monkeypatch: pytest.MonkeyPatch) -> IngesterConfig:
    """Load config with *_BASE_ENV* plus any *extra* overrides."""
    extra = extra or {}
    env = {**_BASE_ENV, **extra}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    # Clear optional vars that might bleed in from the real environment,
    # but only if they were NOT explicitly provided in *extra*.
    optional_keys = ("TRANSLINK_ALERTS_URL", "POLL_INTERVAL_SECONDS",
                     "LOG_LEVEL", "METRICS_PORT", "ENABLED_FEEDS")
    for key in optional_keys:
        if key not in extra:
            monkeypatch.delenv(key, raising=False)
    return IngesterConfig.from_env()


def test_defaults_are_sane(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch=monkeypatch)
    assert cfg.poll_interval_seconds == 30
    assert cfg.log_level == "INFO"
    assert cfg.metrics_port == 9100
    assert cfg.enabled_feeds == frozenset({"vehicle_positions", "trip_updates"})
    assert cfg.alerts_url is None


def test_custom_values_are_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(
        {
            "POLL_INTERVAL_SECONDS": "60",
            "LOG_LEVEL": "DEBUG",
            "METRICS_PORT": "9200",
            "ENABLED_FEEDS": "vehicle_positions,alerts",
            "TRANSLINK_ALERTS_URL": "http://example.com/alerts",
        },
        monkeypatch=monkeypatch,
    )
    assert cfg.poll_interval_seconds == 60
    assert cfg.log_level == "DEBUG"
    assert cfg.metrics_port == 9200
    assert cfg.enabled_feeds == frozenset({"vehicle_positions", "alerts"})
    assert cfg.alerts_url == "http://example.com/alerts"


def test_missing_required_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRANSLINK_VEHICLE_POSITIONS_URL", "http://x.com")
    monkeypatch.setenv("TRANSLINK_TRIP_UPDATES_URL", "http://x.com")
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(ConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        IngesterConfig.from_env()


def test_invalid_poll_interval_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigError, match="POLL_INTERVAL_SECONDS"):
        _load({"POLL_INTERVAL_SECONDS": "notanint"}, monkeypatch=monkeypatch)


def test_poll_interval_below_minimum_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigError, match="POLL_INTERVAL_SECONDS"):
        _load({"POLL_INTERVAL_SECONDS": "0"}, monkeypatch=monkeypatch)


def test_invalid_log_level_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        _load({"LOG_LEVEL": "VERBOSE"}, monkeypatch=monkeypatch)


def test_unknown_feed_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigError, match="Unknown feeds"):
        _load({"ENABLED_FEEDS": "vehicle_positions,banana"}, monkeypatch=monkeypatch)


def test_empty_enabled_feeds_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigError, match="ENABLED_FEEDS"):
        _load({"ENABLED_FEEDS": ","}, monkeypatch=monkeypatch)


def test_url_for_feed(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _load(monkeypatch=monkeypatch)
    assert cfg.url_for_feed("vehicle_positions") == "http://example.com/vp"
    assert cfg.url_for_feed("trip_updates") == "http://example.com/tu"
    assert cfg.url_for_feed("alerts") is None
    assert cfg.url_for_feed("unknown") is None
