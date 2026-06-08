"""Prometheus metrics for the ingester service.

All metrics are module-level singletons.  Import this module once; the
prometheus_client registry is global so metrics are shared across threads.

Exposed at ``/metrics`` on ``METRICS_PORT`` (default 9100).
"""

from prometheus_client import Counter, Gauge, Histogram

poll_total = Counter(
    "ingester_poll_total",
    "GTFS-RT feed polls, labelled by feed name and HTTP outcome",
    ["feed", "status"],
)

poll_duration_seconds = Histogram(
    "ingester_poll_duration_seconds",
    "HTTP poll round-trip latency in seconds",
    ["feed"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 30.0],
)

records_published_total = Counter(
    "ingester_records_published_total",
    "Records successfully enqueued for Kafka publish",
    ["feed"],
)

kafka_publish_errors_total = Counter(
    "ingester_kafka_publish_errors_total",
    "Kafka delivery failures (from async delivery callback)",
    ["feed"],
)

seconds_since_last_successful_poll = Gauge(
    "ingester_seconds_since_last_successful_poll",
    "Seconds elapsed since the last successful HTTP poll (staleness signal)",
    ["feed"],
)
