"""Integration test: ingester poll cycle → Kafka → verify records.

Requires Docker (testcontainers spins up a real Redpanda container).
Run with:  uv run pytest -m integration -v

What this tests:
  1. A Redpanda container starts and Kafka topics are created.
  2. The FeedPoller is pointed at a mock HTTP endpoint (respx) that returns
     a fixed GTFS-RT protobuf payload.
  3. One poll cycle runs.
  4. A Kafka consumer drains the topic and asserts the correct number of
     records are present with the expected envelope shape.
"""

from __future__ import annotations

import json
import time

import httpx
import pytest
import respx
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from google.transit import gtfs_realtime_pb2
from testcontainers.kafka import RedpandaContainer

from services.ingester.config import IngesterConfig
from services.ingester.feed import FeedPoller
from services.ingester.producer import KafkaProducer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vp_feed(num_vehicles: int) -> bytes:
    """Return a serialised FeedMessage with *num_vehicles* VehiclePosition entities."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1748332830
    for i in range(num_vehicles):
        entity = feed.entity.add()
        entity.id = f"e{i}"
        entity.vehicle.vehicle.id = f"BCC_{i}"
        entity.vehicle.trip.trip_id = f"trip_{i}"
        entity.vehicle.trip.route_id = f"route_{i % 3}"
        entity.vehicle.position.latitude = -27.4698
        entity.vehicle.position.longitude = 153.0251
        entity.vehicle.timestamp = 1748332828
    return feed.SerializeToString()


def _create_topic(bootstrap: str, topic: str) -> None:
    """Create *topic* in the Kafka cluster at *bootstrap*."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for _t, f in fs.items():
        try:
            f.result()
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def _drain_topic(bootstrap: str, topic: str, expected: int, timeout: float = 15.0) -> list[dict]:
    """Consume messages from *topic* until *expected* are collected or *timeout* elapses."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "test-drain",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages = []
    deadline = time.monotonic() + timeout
    while len(messages) < expected and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            messages.append(json.loads(msg.value().decode("utf-8")))
    consumer.close()
    return messages


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_poll_cycle_produces_records_to_kafka() -> None:
    """One FeedPoller poll cycle publishes the correct records to a real Kafka broker."""
    with RedpandaContainer() as redpanda:
        bootstrap = redpanda.get_bootstrap_server()
        topic = "gtfsrt.vehicle_positions"
        _create_topic(bootstrap, topic)

        producer = KafkaProducer(bootstrap)
        config = IngesterConfig(
            kafka_bootstrap_servers=bootstrap,
            vehicle_positions_url="http://fake.translink/vp",
            trip_updates_url="http://fake.translink/tu",
            alerts_url=None,
            poll_interval_seconds=30,
            log_level="INFO",
            metrics_port=9100,
            enabled_feeds=frozenset({"vehicle_positions"}),
        )
        poller = FeedPoller("vehicle_positions", "http://fake.translink/vp", config, producer)

        num_vehicles = 3
        with respx.mock:
            respx.get("http://fake.translink/vp").mock(
                return_value=httpx.Response(200, content=_make_vp_feed(num_vehicles))
            )
            count = poller._poll_once()

        producer.flush(timeout=10.0)

        assert count == num_vehicles, f"Expected {num_vehicles} records published, got {count}"

        messages = _drain_topic(bootstrap, topic, expected=num_vehicles)
        assert len(messages) == num_vehicles, (
            f"Expected {num_vehicles} messages in Kafka, got {len(messages)}"
        )

        # Verify envelope shape on each message
        for msg in messages:
            env = msg["envelope"]
            assert env["source_feed"] == "vehicle_positions"
            assert env["ingester_version"] == "0.1.0"
            assert isinstance(env["feed_timestamp"], int)
            assert "ingested_at" in env
            # Verify payload has vehicle data
            assert "vehicle" in msg["payload"] or "trip" in msg["payload"]
