"""Sink consumer: drains Kafka topics to MinIO as NDJSON files.

Each feed (vehicle_positions, trip_updates, alerts) is written to::

    s3://<raw_bucket>/raw/<feed_name>/dt=YYYY-MM-DD/hour=HH/<uuid>.ndjson

A new file is flushed when either:
  - The accumulated per-topic buffer exceeds SINK_BATCH_SIZE_BYTES, or
  - SINK_FLUSH_INTERVAL_SECONDS have elapsed since the last flush.

**Why NDJSON instead of Parquet?**
The raw layer is a verbatim backup of Kafka messages.  Converting to Parquet
requires knowing the full schema up-front; in Phase 1 that schema is still
being validated against live data.  Phase 2 (Spark Structured Streaming) will
read this NDJSON and write the curated Parquet layer.  See docs/DECISIONS.md
ADR 0008.
"""

from __future__ import annotations

import io
import logging
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, KafkaError, KafkaException

from services.sink.config import SinkConfig

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SinkConsumer:
    """Drains Kafka topics to MinIO/S3 as partitioned NDJSON files."""

    def __init__(self, config: SinkConfig) -> None:
        """Initialise consumer and S3 client; do not start polling yet."""
        self._config = config
        self._running = True

        self._consumer: Consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "group.id": config.consumer_group_id,
                "auto.offset.reset": "earliest",
                # Manual commit after flush so we don't lose messages on crash
                "enable.auto.commit": False,
            }
        )

        self._s3 = boto3.client(
            "s3",
            endpoint_url=config.s3_endpoint_url,
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
            config=Config(signature_version="s3v4"),
        )

        # Accumulate raw JSON bytes per topic
        self._buffers: dict[str, list[bytes]] = defaultdict(list)
        self._buffer_bytes: dict[str, int] = defaultdict(int)
        self._last_flush_at: float = time.monotonic()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Block and consume until a shutdown signal is received."""
        self._consumer.subscribe(list(self._config.topics))
        logger.info(
            "Sink consumer started topics=%s bucket=%s",
            self._config.topics,
            self._config.raw_bucket,
        )

        while self._running:
            msg = self._consumer.poll(timeout=1.0)

            if msg is None:
                self._flush_if_interval_elapsed()
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            raw_topic = msg.topic()
            topic: str = raw_topic if raw_topic is not None else ""
            # Keep raw bytes as-is (already valid JSON from the ingester)
            value: bytes = msg.value() or b""
            self._buffers[topic].append(value + b"\n")
            self._buffer_bytes[topic] += len(value) + 1

            if self._buffer_bytes[topic] >= self._config.batch_size_bytes:
                self._flush_topic(topic)
                self._safe_commit()

        self._flush_all()
        self._consumer.close()
        logger.info("Sink consumer stopped")

    def stop(self) -> None:
        """Signal the run loop to stop after the current poll cycle."""
        self._running = False

    # ------------------------------------------------------------------
    # Flush helpers
    # ------------------------------------------------------------------

    def _flush_if_interval_elapsed(self) -> None:
        """Flush all buffered topics if the idle interval has elapsed."""
        elapsed = time.monotonic() - self._last_flush_at
        if elapsed >= self._config.flush_interval_seconds:
            self._flush_all()
            self._safe_commit()

    def _safe_commit(self) -> None:
        """Commit offsets, ignoring the benign _NO_OFFSET error.

        confluent-kafka raises KafkaException(_NO_OFFSET) when commit() is
        called but there are no stored offsets yet — this happens on the very
        first flush cycle before the consumer has polled from every assigned
        partition.  It is not a data-loss risk; we just skip the commit for
        that cycle and it will succeed on the next one.
        """
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as exc:
            err: KafkaError = exc.args[0]
            if err.code() == KafkaError._NO_OFFSET:  # noqa: SLF001
                logger.debug("commit skipped — no offsets stored yet (first cycle)")
            else:
                raise

    def _flush_all(self) -> None:
        """Flush every topic that has buffered data."""
        for topic in list(self._buffers):
            if self._buffers[topic]:
                self._flush_topic(topic)
        self._last_flush_at = time.monotonic()

    def _flush_topic(self, topic: str) -> None:
        """Write buffered records for *topic* to MinIO and clear the buffer."""
        records = self._buffers[topic]
        if not records:
            return

        now = datetime.now(tz=UTC)
        feed_name = topic.split(".")[-1]  # "gtfsrt.vehicle_positions" → "vehicle_positions"
        key = (
            f"raw/{feed_name}/"
            f"dt={now.strftime('%Y-%m-%d')}/"
            f"hour={now.strftime('%H')}/"
            f"{uuid.uuid4()}.ndjson"
        )
        body = b"".join(records)

        try:
            self._s3.put_object(
                Bucket=self._config.raw_bucket,
                Key=key,
                Body=io.BytesIO(body),
                ContentType="application/x-ndjson",
            )
            logger.info(
                "Flushed topic=%s records=%d bytes=%d key=%s",
                topic,
                len(records),
                len(body),
                key,
            )
        except Exception:
            logger.exception("S3 upload failed for topic=%s key=%s", topic, key)
            # Don't clear the buffer so the data isn't lost; it will retry next cycle
            return

        self._buffers[topic].clear()
        self._buffer_bytes[topic] = 0


def main() -> int:
    """Run the sink consumer. Returns process exit code."""
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    from services.sink.config import SinkConfigError

    try:
        config = SinkConfig.from_env()
    except SinkConfigError as exc:
        print(f"FATAL sink configuration error: {exc}", file=sys.stderr)
        return 1

    consumer = SinkConsumer(config)

    def _shutdown(sig: int, _frame: object) -> None:
        logger.info("Signal %d received — stopping sink consumer", sig)
        consumer.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    consumer.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
