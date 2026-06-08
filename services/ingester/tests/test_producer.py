"""Unit tests for KafkaProducer — Kafka calls are mocked."""

import json
from unittest.mock import MagicMock, patch

from services.ingester.producer import KafkaProducer


def _make_producer(mock_confluent: MagicMock) -> KafkaProducer:
    """Instantiate KafkaProducer with a mocked underlying confluent Producer."""
    return KafkaProducer("localhost:9092")


def test_publish_calls_produce_with_correct_topic_and_key() -> None:
    with patch("services.ingester.producer.Producer") as MockProducer:
        mock_inner = MockProducer.return_value
        producer = KafkaProducer("localhost:9092")
        producer.publish("gtfsrt.vehicle_positions", "BCC_1234", {"hello": "world"})

        assert mock_inner.produce.call_count == 1
        # confluent-kafka Producer.produce takes topic as the first positional arg
        positional_args = mock_inner.produce.call_args[0]
        keyword_args = mock_inner.produce.call_args[1]
        topic_arg = positional_args[0] if positional_args else keyword_args.get("topic")
        assert topic_arg == "gtfsrt.vehicle_positions"


def test_publish_serialises_message_as_json() -> None:
    with patch("services.ingester.producer.Producer") as MockProducer:
        mock_inner = MockProducer.return_value
        producer = KafkaProducer("localhost:9092")
        payload = {"envelope": {"source_feed": "vehicle_positions"}, "payload": {"id": "1"}}
        producer.publish("topic", "key", payload)

        call_kwargs = mock_inner.produce.call_args[1]
        value_bytes: bytes = call_kwargs["value"]
        decoded = json.loads(value_bytes.decode("utf-8"))
        assert decoded["envelope"]["source_feed"] == "vehicle_positions"


def test_publish_encodes_key_as_bytes() -> None:
    with patch("services.ingester.producer.Producer") as MockProducer:
        mock_inner = MockProducer.return_value
        producer = KafkaProducer("localhost:9092")
        producer.publish("topic", "BCC_999", {})

        call_kwargs = mock_inner.produce.call_args[1]
        assert call_kwargs["key"] == b"BCC_999"


def test_flush_delegates_to_confluent_flush() -> None:
    with patch("services.ingester.producer.Producer") as MockProducer:
        mock_inner = MockProducer.return_value
        mock_inner.flush.return_value = 0
        producer = KafkaProducer("localhost:9092")
        producer.flush(timeout=5.0)

        mock_inner.flush.assert_called_once_with(5.0)
