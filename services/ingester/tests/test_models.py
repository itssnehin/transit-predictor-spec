"""Unit tests for Envelope and KafkaMessage type shapes."""

from services.ingester.models import Envelope, KafkaMessage


def test_envelope_has_required_keys() -> None:
    envelope: Envelope = {
        "ingested_at": "2026-05-30T08:00:00.000+00:00",
        "source_feed": "vehicle_positions",
        "ingester_version": "0.1.0",
        "feed_timestamp": 1748332830,
    }
    assert envelope["source_feed"] == "vehicle_positions"
    assert envelope["ingester_version"] == "0.1.0"
    assert isinstance(envelope["feed_timestamp"], int)


def test_kafka_message_wraps_envelope_and_payload() -> None:
    msg: KafkaMessage = {
        "envelope": {
            "ingested_at": "2026-05-30T08:00:00.000+00:00",
            "source_feed": "vehicle_positions",
            "ingester_version": "0.1.0",
            "feed_timestamp": 1748332830,
        },
        "payload": {"vehicle": {"id": "BCC_1234"}},
    }
    assert msg["envelope"]["source_feed"] == "vehicle_positions"
    assert msg["payload"]["vehicle"]["id"] == "BCC_1234"
