"""Per-feed poller: HTTP fetch → protobuf decode → Kafka publish.

Each enabled feed gets one FeedPoller instance running in its own thread
(see main.py).  Polls are independent so a slow feed does not block others.

Retry policy (per spec INGESTER.md):
  - HTTP 4xx: log, increment metric, skip this cycle (permanent issue).
  - HTTP 5xx / timeout: exponential backoff (1 s, 2 s, 4 s, 8 s, max 60 s),
    then give up and wait for the next scheduled poll.
  - Success: update last_successful_poll_at and reset backoff.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2

from services.ingester import metrics as m
from services.ingester.config import IngesterConfig
from services.ingester.models import Envelope, KafkaMessage
from services.ingester.producer import KafkaProducer

logger = logging.getLogger(__name__)

_USER_AGENT = "transit-predictor/0.1 (snehin.kukreja@gmail.com)"
_REQUEST_TIMEOUT_S = 15.0

# Kafka topic for each feed name (spec: INGESTER.md §Interfaces)
_TOPIC: dict[str, str] = {
    "vehicle_positions": "gtfsrt.vehicle_positions",
    "trip_updates": "gtfsrt.trip_updates",
    "alerts": "gtfsrt.alerts",
}

# Exponential backoff delays in seconds (spec: 1s, 2s, 4s, 8s, cap 60s)
_BACKOFF_DELAYS = [1, 2, 4, 8]


class FeedPoller:
    """Polls one GTFS-RT feed endpoint and publishes decoded records to Kafka."""

    def __init__(
        self,
        feed_name: str,
        url: str,
        config: IngesterConfig,
        producer: KafkaProducer,
    ) -> None:
        """Initialise a poller for *feed_name* at *url*."""
        self._feed_name = feed_name
        self._url = url
        self._config = config
        self._producer = producer
        self._topic = _TOPIC[feed_name]
        self._last_successful_poll_at: float | None = None

    # ------------------------------------------------------------------
    # Properties used by the health server
    # ------------------------------------------------------------------

    @property
    def feed_name(self) -> str:
        """The feed name this poller handles."""
        return self._feed_name

    def is_ready(self) -> bool:
        """Return True if there was a successful poll in the last 5 minutes."""
        if self._last_successful_poll_at is None:
            return False
        return (time.time() - self._last_successful_poll_at) < 300

    # ------------------------------------------------------------------
    # Polling loop (runs in a dedicated thread)
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Blocking poll loop. Runs until the process is terminated."""
        logger.info("FeedPoller started feed=%s url=%s", self._feed_name, self._url)
        while True:
            # Schedule next poll with ±5 s jitter (spec requirement)
            jitter = random.uniform(-5.0, 5.0)
            next_poll_at = (
                time.monotonic() + self._config.poll_interval_seconds + jitter
            )

            count = self._poll_with_retry()
            if count is not None:
                self._last_successful_poll_at = time.time()
                m.seconds_since_last_successful_poll.labels(
                    feed=self._feed_name
                ).set(0)
                logger.info(
                    "Polled feed=%s records=%d", self._feed_name, count
                )
            else:
                if self._last_successful_poll_at is not None:
                    age = time.time() - self._last_successful_poll_at
                    m.seconds_since_last_successful_poll.labels(
                        feed=self._feed_name
                    ).set(age)

            sleep_s = max(0.0, next_poll_at - time.monotonic())
            time.sleep(sleep_s)

    # ------------------------------------------------------------------
    # Single poll cycle with retry
    # ------------------------------------------------------------------

    def _poll_with_retry(self) -> int | None:
        """Fetch, decode, publish. Returns record count or None on failure."""
        for attempt, delay in enumerate([0] + _BACKOFF_DELAYS):
            if delay > 0:
                jitter = random.uniform(0, delay * 0.2)
                time.sleep(delay + jitter)

            try:
                count = self._poll_once()
                return count
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                m.poll_total.labels(feed=self._feed_name, status=str(status)).inc()
                if 400 <= status < 500:
                    logger.warning(
                        "4xx from feed=%s status=%d — skipping cycle",
                        self._feed_name,
                        status,
                    )
                    return None  # permanent client error, don't retry
                # 5xx — retry with backoff
                if attempt < len(_BACKOFF_DELAYS):
                    logger.warning(
                        "5xx from feed=%s status=%d attempt=%d — retrying in %ds",
                        self._feed_name,
                        status,
                        attempt + 1,
                        _BACKOFF_DELAYS[attempt],
                    )
                else:
                    logger.error(
                        "5xx from feed=%s all retries exhausted — skipping cycle",
                        self._feed_name,
                    )
                    return None
            except httpx.TimeoutException:
                m.poll_total.labels(feed=self._feed_name, status="timeout").inc()
                if attempt < len(_BACKOFF_DELAYS):
                    logger.warning(
                        "Timeout feed=%s attempt=%d — retrying in %ds",
                        self._feed_name,
                        attempt + 1,
                        _BACKOFF_DELAYS[attempt],
                    )
                else:
                    logger.error(
                        "Timeout feed=%s all retries exhausted — skipping cycle",
                        self._feed_name,
                    )
                    return None
        return None  # unreachable, satisfies type checker

    def _poll_once(self) -> int:
        """One HTTP fetch + decode + publish.  Returns the number of records published."""
        with m.poll_duration_seconds.labels(feed=self._feed_name).time():
            response = httpx.get(
                self._url,
                headers={"User-Agent": _USER_AGENT},
                timeout=_REQUEST_TIMEOUT_S,
            )
            response.raise_for_status()

        m.poll_total.labels(feed=self._feed_name, status="200").inc()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        feed_timestamp = feed.header.timestamp
        ingested_at = datetime.now(tz=UTC).isoformat(timespec="milliseconds")

        count = 0
        for entity in feed.entity:
            payload = self._entity_to_payload(entity)
            if payload is None:
                continue

            envelope: Envelope = {
                "ingested_at": ingested_at,
                "source_feed": self._feed_name,
                "ingester_version": self._config.version,
                "feed_timestamp": feed_timestamp,
            }
            message: KafkaMessage = {"envelope": envelope, "payload": payload}
            key = self._extract_key(entity)
            self._producer.publish(self._topic, key, message)  # type: ignore[arg-type]
            m.records_published_total.labels(feed=self._feed_name).inc()
            count += 1

        return count

    # ------------------------------------------------------------------
    # Protobuf helpers
    # ------------------------------------------------------------------

    def _entity_to_payload(
        self, entity: Any  # noqa: ANN401 — protobuf FeedEntity has no stubs
    ) -> dict[str, Any] | None:
        """Convert the relevant sub-message of a FeedEntity to a plain dict.

        Returns None if the entity does not contain a message for this feed.
        Uses official protobuf JSON format with proto field names preserved
        (snake_case, matching GTFS-RT spec naming).
        """
        sub: Any
        match self._feed_name:
            case "vehicle_positions":
                if not entity.HasField("vehicle"):
                    return None
                sub = entity.vehicle
            case "trip_updates":
                if not entity.HasField("trip_update"):
                    return None
                sub = entity.trip_update
            case "alerts":
                if not entity.HasField("alert"):
                    return None
                sub = entity.alert
            case _:
                return None

        return json_format.MessageToDict(  # type: ignore[no-any-return]
            sub,
            preserving_proto_field_name=True,
        )

    def _extract_key(self, entity: Any) -> str:  # noqa: ANN401
        """Return the Kafka partition key for a FeedEntity.

        Falls back to entity.id if the primary key field is empty.
        """
        match self._feed_name:
            case "vehicle_positions":
                key = entity.vehicle.vehicle.id
            case "trip_updates":
                key = entity.trip_update.trip.trip_id
            case _:  # alerts
                key = entity.id
        return str(key) if key else str(entity.id)
