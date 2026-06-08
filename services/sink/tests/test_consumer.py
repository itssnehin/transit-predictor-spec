"""Unit tests for SinkConsumer — Kafka and S3 calls are mocked."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from services.sink.config import SinkConfig
from services.sink.consumer import SinkConsumer


def _make_config() -> SinkConfig:
    return SinkConfig(
        kafka_bootstrap_servers="localhost:9092",
        topics=("gtfsrt.vehicle_positions",),
        consumer_group_id="test-sink",
        s3_endpoint_url="http://localhost:9000",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        raw_bucket="transit-raw",
        batch_size_bytes=4 * 1024 * 1024,
        flush_interval_seconds=60,
    )


def _make_sink() -> tuple[SinkConsumer, MagicMock, MagicMock]:
    """Return (consumer, mock_confluent_consumer, mock_s3_client)."""
    with (
        patch("services.sink.consumer.Consumer") as MockConsumer,
        patch("services.sink.consumer.boto3") as mock_boto3,
    ):
        mock_inner_consumer = MockConsumer.return_value
        mock_s3 = mock_boto3.client.return_value
        sink = SinkConsumer(_make_config())
    return sink, mock_inner_consumer, mock_s3


def test_flush_topic_writes_ndjson_to_s3() -> None:
    sink, _, mock_s3 = _make_sink()
    record = json.dumps({"envelope": {"source_feed": "vehicle_positions"}, "payload": {}}).encode()

    sink._buffers["gtfsrt.vehicle_positions"].append(record + b"\n")
    sink._buffer_bytes["gtfsrt.vehicle_positions"] = len(record) + 1

    sink._flush_topic("gtfsrt.vehicle_positions")

    assert mock_s3.put_object.call_count == 1
    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "transit-raw"
    assert call_kwargs["Key"].startswith("raw/vehicle_positions/dt=")
    assert call_kwargs["Key"].endswith(".ndjson")
    assert call_kwargs["ContentType"] == "application/x-ndjson"


def test_flush_topic_clears_buffer_after_success() -> None:
    sink, _, mock_s3 = _make_sink()
    record = b'{"test": "data"}\n'
    sink._buffers["gtfsrt.vehicle_positions"].append(record)
    sink._buffer_bytes["gtfsrt.vehicle_positions"] = len(record)

    sink._flush_topic("gtfsrt.vehicle_positions")

    assert sink._buffers["gtfsrt.vehicle_positions"] == []
    assert sink._buffer_bytes["gtfsrt.vehicle_positions"] == 0


def test_flush_topic_retains_buffer_on_s3_error() -> None:
    sink, _, mock_s3 = _make_sink()
    mock_s3.put_object.side_effect = Exception("S3 down")
    record = b'{"test": "data"}\n'
    sink._buffers["gtfsrt.vehicle_positions"].append(record)
    sink._buffer_bytes["gtfsrt.vehicle_positions"] = len(record)

    sink._flush_topic("gtfsrt.vehicle_positions")

    # Buffer must NOT be cleared so the data survives and retries next cycle
    assert len(sink._buffers["gtfsrt.vehicle_positions"]) == 1


def test_flush_topic_noop_on_empty_buffer() -> None:
    sink, _, mock_s3 = _make_sink()

    sink._flush_topic("gtfsrt.vehicle_positions")

    mock_s3.put_object.assert_not_called()


def test_s3_key_includes_feed_name_and_date_partition() -> None:
    sink, _, mock_s3 = _make_sink()
    sink._buffers["gtfsrt.vehicle_positions"].append(b'{"x":1}\n')
    sink._buffer_bytes["gtfsrt.vehicle_positions"] = 8

    sink._flush_topic("gtfsrt.vehicle_positions")

    key: str = mock_s3.put_object.call_args[1]["Key"]
    # raw/vehicle_positions/dt=YYYY-MM-DD/hour=HH/<uuid>.ndjson
    parts = key.split("/")
    assert parts[0] == "raw"
    assert parts[1] == "vehicle_positions"
    assert parts[2].startswith("dt=")
    assert parts[3].startswith("hour=")
