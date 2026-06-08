"""Ingester service entrypoint.

Wires together configuration, Kafka producer, per-feed pollers, and the
health/metrics HTTP server.  Run with::

    python -m services.ingester.main

or via Docker (see services/ingester/Dockerfile).

Lifecycle:
  1. Load and validate config from env vars (fail fast on bad config).
  2. Initialise Kafka producer.
  3. Spawn one daemon thread per enabled feed.
  4. Serve /health, /ready, /metrics on METRICS_PORT (main thread, blocking).
  5. On SIGTERM/SIGINT: flush producer, shut down HTTP server, exit cleanly.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import types

from services.ingester.config import ConfigError, IngesterConfig
from services.ingester.feed import FeedPoller
from services.ingester.health import make_server
from services.ingester.producer import KafkaProducer

logger = logging.getLogger(__name__)


def _setup_logging(level: str) -> None:
    """Configure root logger to emit structured-ish lines to stdout."""
    logging.basicConfig(
        stream=sys.stdout,
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def main() -> int:
    """Run the ingester.  Returns process exit code."""
    # 1. Config ----------------------------------------------------------
    try:
        config = IngesterConfig.from_env()
    except ConfigError as exc:
        # logging not yet configured; write directly to stderr
        print(f"FATAL configuration error: {exc}", file=sys.stderr)
        return 1

    _setup_logging(config.log_level)
    logger.info(
        "Starting ingester version=%s feeds=%s interval=%ds",
        config.version,
        sorted(config.enabled_feeds),
        config.poll_interval_seconds,
    )

    # 2. Kafka producer --------------------------------------------------
    producer = KafkaProducer(config.kafka_bootstrap_servers)

    # 3. Per-feed poller threads -----------------------------------------
    pollers: list[FeedPoller] = []
    for feed_name in sorted(config.enabled_feeds):
        url = config.url_for_feed(feed_name)
        if url is None:
            logger.warning("Feed %r is enabled but has no URL configured; skipping", feed_name)
            continue
        poller = FeedPoller(feed_name, url, config, producer)
        pollers.append(poller)
        thread = threading.Thread(
            target=poller.run,
            name=f"poller-{feed_name}",
            daemon=True,  # dies with the main thread
        )
        thread.start()
        logger.info("Started poller thread for feed=%s", feed_name)

    if not pollers:
        logger.error("No pollers started — check ENABLED_FEEDS and URL config")
        return 1

    # 4. Health / metrics HTTP server (blocks main thread) ---------------
    server = make_server(config.metrics_port, pollers)
    logger.info("Health/metrics server listening on port %d", config.metrics_port)

    # 5. Graceful shutdown -----------------------------------------------
    def _shutdown(sig: int, _frame: types.FrameType | None) -> None:
        logger.info("Received signal %d — shutting down gracefully", sig)
        # Flush ensures in-flight Kafka messages are delivered before exit
        producer.flush(timeout=30.0)
        server.shutdown()  # unblocks serve_forever()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    server.serve_forever()

    logger.info("Ingester stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
