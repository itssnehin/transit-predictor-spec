"""Unit tests for the Phase 0 ingester smoke test. No network access."""

from google.transit import gtfs_realtime_pb2

from services.ingester.poll_once import count_vehicle_entities, parse_feed


def _make_feed(num_vehicles: int) -> bytes:
    """Build a serialized FeedMessage with ``num_vehicles`` vehicle entities."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1748332830
    for i in range(num_vehicles):
        entity = feed.entity.add()
        entity.id = f"v{i}"
        entity.vehicle.vehicle.id = f"BCC_{i}"
        entity.vehicle.position.latitude = -27.4698
        entity.vehicle.position.longitude = 153.0251
    return feed.SerializeToString()


def test_parse_feed_roundtrips_serialized_message() -> None:
    feed = parse_feed(_make_feed(3))
    assert feed.header.gtfs_realtime_version == "2.0"
    assert feed.header.timestamp == 1748332830
    assert len(feed.entity) == 3


def test_count_vehicle_entities_ignores_non_vehicle_entities() -> None:
    feed = parse_feed(_make_feed(5))
    # A service-alert entity carries no vehicle position and must not be counted.
    alert_entity = feed.entity.add()
    alert_entity.id = "alert-1"
    alert_entity.alert.header_text.translation.add().text = "Detour in effect"

    assert count_vehicle_entities(feed) == 5


def test_count_vehicle_entities_empty_feed() -> None:
    assert count_vehicle_entities(parse_feed(_make_feed(0))) == 0
