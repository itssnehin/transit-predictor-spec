"""Thin wrapper around confluent_kafka.Producer.

Handles JSON serialisation, delivery callbacks, and error metric updates so
that callers (FeedPoller) only deal with plain Python dicts.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from services.ingester import metrics as m

logger = logging.getLogger(__name__)


class KafkaProducer:
    """JSON-serialising Kafka producer with async delivery tracking."""

    def __init__(self, bootstrap_servers: str) -> None:
        """Connect to the Kafka cluster at *bootstrap_servers*."""
        self._producer: Producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                # acks=1: leader acknowledges; balances durability and throughput
                "acks": 1,
                # Batch for up to 100 ms to amortise per-message overhead
                "linger.ms": 100,
                # 64 KB batch size per partition
                "batch.size": 65_536,
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, topic: str, key: str, message: dict[str, Any]) -> None:
        """Serialise *message* as UTF-8 JSON and produce to *topic*.

        The produce call is non-blocking; delivery is confirmed asynchronously
        via the callback and failures are counted in Prometheus.

        Args:
            topic: Target Kafka topic name.
            key: Partition key (e.g. vehicle_id, trip_id).
            message: Payload dict to serialise as JSON.
        """
        value = json.dumps(message, separators=(",", ":")).encode("utf-8")

        def _on_delivery(err: KafkaError | None, _msg: Message) -> None:
            if err:
                logger.warning(
                    "Kafka delivery failed topic=%s key=%s err=%s", topic, key, err
                )
                m.kafka_publish_errors_total.labels(feed=topic).inc()

        try:
            self._producer.produce(
                topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=_on_delivery,
            )
            # poll(0): trigger any pending delivery callbacks without blocking
            self._producer.poll(0)
        except KafkaException as exc:
            logger.error("Kafka produce error topic=%s key=%s: %s", topic, key, exc)
            m.kafka_publish_errors_total.labels(feed=topic).inc()

    def flush(self, timeout: float = 30.0) -> None:
        """Block until all outstanding messages are delivered or *timeout* elapses.

        Called during graceful shutdown (SIGTERM handler).
        """
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.warning("Kafka flush timed out; %d messages undelivered", remaining)
