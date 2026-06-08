"""Unit tests for FeedPoller — HTTP and Kafka calls are mocked."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import respx
from google.transit import gtfs_realtime_pb2

from services.ingester.config import IngesterConfig
from services.ingester.feed import FeedPoller
from services.ingester.producer import KafkaProducer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_config() -> IngesterConfig:
    return IngesterConfig(
        kafka_bootstrap_servers="localhost:9092",
        vehicle_positions_url="http://fake.translink/vp",
        trip_updates_url="http://fake.translink/tu",
        alerts_url=None,
        poll_interval_seconds=30,
        log_level="INFO",
        metrics_port=9100,
        enabled_feeds=frozenset({"vehicle_positions", "trip_updates"}),
    )


def _make_vp_feed(num_vehicles: int) -> bytes:
    """Build a serialised FeedMessage with *num_vehicles* VehiclePosition entities."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1748332830
    for i in range(num_vehicles):
        entity = feed.entity.add()
        entity.id = f"e{i}"
        entity.vehicle.vehicle.id = f"BCC_{i}"
        entity.vehicle.trip.trip_id = f"trip_{i}"
        entity.vehicle.trip.route_id = f"route_{i % 5}"
        entity.vehicle.position.latitude = -27.4698
        entity.vehicle.position.longitude = 153.0251
        entity.vehicle.timestamp = 1748332828
    return feed.SerializeToString()


# ---------------------------------------------------------------------------
# poll_once tests
# ---------------------------------------------------------------------------

@respx.mock
def test_poll_once_publishes_correct_count() -> None:
    respx.get("http://fake.translink/vp").mock(
        return_value=httpx.Response(200, content=_make_vp_feed(5))
    )
    mock_producer = MagicMock(spec=KafkaProducer)
    poller = FeedPoller(
        "vehicle_positions", "http://fake.translink/vp", _make_config(), mock_producer
    )

    count = poller._poll_once()

    assert count == 5
    assert mock_producer.publish.call_count == 5


@respx.mock
def test_poll_once_uses_correct_kafka_topic() -> None:
    respx.get("http://fake.translink/vp").mock(
        return_value=httpx.Response(200, content=_make_vp_feed(1))
    )
    mock_producer = MagicMock(spec=KafkaProducer)
    poller = FeedPoller(
        "vehicle_positions", "http://fake.translink/vp", _make_config(), mock_producer
    )

    poller._poll_once()

    topic = mock_producer.publish.call_args[0][0]
    assert topic == "gtfsrt.vehicle_positions"


@respx.mock
def test_poll_once_uses_vehicle_id_as_key() -> None:
    respx.get("http://fake.translink/vp").mock(
        return_value=httpx.Response(200, content=_make_vp_feed(1))
    )
    mock_producer = MagicMock(spec=KafkaProducer)
    poller = FeedPoller(
        "vehicle_positions", "http://fake.translink/vp", _make_config(), mock_producer
    )

    poller._poll_once()

    key = mock_producer.publish.call_args[0][1]
    assert key == "BCC_0"


@respx.mock
def test_poll_once_envelope_has_expected_fields() -> None:
    respx.get("http://fake.translink/vp").mock(
        return_value=httpx.Response(200, content=_make_vp_feed(1))
    )
    mock_producer = MagicMock(spec=KafkaProducer)
    poller = FeedPoller(
        "vehicle_positions", "http://fake.translink/vp", _make_config(), mock_producer
    )

    poller._poll_once()

    message = mock_producer.publish.call_args[0][2]
    env = message["envelope"]
    assert env["source_feed"] == "vehicle_positions"
    assert env["ingester_version"] == "0.1.0"
    assert isinstance(env["feed_timestamp"], int)
    assert "ingested_at" in env


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------

@respx.mock
def test_poll_with_retry_returns_none_on_4xx() -> None:
    respx.get("http://fake.translink/vp").mock(return_value=httpx.Response(404))
    mock_producer = MagicMock(spec=KafkaProducer)
    poller = FeedPoller(
        "vehicle_positions", "http://fake.translink/vp", _make_config(), mock_producer
    )

    result = poller._poll_with_retry()

    assert result is None
    mock_producer.publish.assert_not_called()


@respx.mock
def test_poll_with_retry_returns_none_after_exhausted_5xx_retries() -> None:
    # Always return 500 — should exhaust all retries
    respx.get("http://fake.translink/vp").mock(return_value=httpx.Response(500))
    mock_producer = MagicMock(spec=KafkaProducer)
    poller = FeedPoller(
        "vehicle_positions", "http://fake.translink/vp", _make_config(), mock_producer
    )

    with patch("services.ingester.feed.time.sleep"):  # skip actual backoff sleeps
        result = poller._poll_with_retry()

    assert result is None


# ---------------------------------------------------------------------------
# Readiness probe test
# ---------------------------------------------------------------------------

def test_is_ready_false_before_first_poll() -> None:
    mock_producer = MagicMock(spec=KafkaProducer)
    poller = FeedPoller(
        "vehicle_positions", "http://fake.translink/vp", _make_config(), mock_producer
    )
    assert poller.is_ready() is False
