# Ingester Service Spec

## Purpose

Poll TransLink GTFS-Realtime endpoints on a fixed schedule, decode the protobuf, and publish individual records to Kafka topics.

## Behaviour

### Scheduling

- Polls each enabled feed every `POLL_INTERVAL_SECONDS` (default: 30)
- Polls run independently per feed; a slow vehicle_positions poll does not block trip_updates
- Implements jitter (±5s) to avoid hitting the upstream on exact-second boundaries

### Polling

For each feed:

1. HTTP GET the feed URL with a sensible User-Agent (`transit-predictor/0.1 (snehin.kukreja@gmail.com)`)
2. Timeout: 15 seconds
3. On HTTP 200: decode protobuf, publish records
4. On HTTP 4xx: log warning, increment metric, do not retry (likely a permanent issue)
5. On HTTP 5xx or timeout: exponential backoff with jitter (1s, 2s, 4s, 8s, max 60s), then continue with next scheduled poll
6. Track `last_successful_poll_at` per feed for health checks

### Publishing

For each record in the decoded protobuf:

- Convert to dictionary (use `protobuf_to_dict` or equivalent)
- Add envelope: `ingested_at` (UTC ISO timestamp), `source_feed` (string), `ingester_version` (semver)
- Serialise as JSON
- Publish to Kafka topic:
  - `gtfsrt.vehicle_positions` (key: `vehicle.id`)
  - `gtfsrt.trip_updates` (key: `trip.trip_id`)
  - `gtfsrt.alerts` (key: `id`)
- Use producer acks=1 for durability/throughput balance
- Batch publishes: linger_ms=100, batch_size=64KB

### State

The ingester is **stateless**. Multiple instances can run safely (though we deploy one).

## Configuration

All via environment variables. Read once at startup, fail fast if invalid.

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | — | `localhost:9092` locally, MSK broker list in cloud |
| `TRANSLINK_VEHICLE_POSITIONS_URL` | yes | — | Full URL |
| `TRANSLINK_TRIP_UPDATES_URL` | yes | — | Full URL |
| `TRANSLINK_ALERTS_URL` | no | — | Full URL; if unset, alerts feed is disabled |
| `POLL_INTERVAL_SECONDS` | no | 30 | Per-feed poll cadence |
| `LOG_LEVEL` | no | INFO | DEBUG/INFO/WARN/ERROR |
| `METRICS_PORT` | no | 9100 | Prometheus scrape port |
| `ENABLED_FEEDS` | no | vehicle_positions,trip_updates | Comma-separated list |

## Interfaces

### Outbound: Kafka

Topic: `gtfsrt.vehicle_positions`

Partitions: 3 (local) / 6 (cloud). Keyed by `vehicle.id` for ordering per vehicle.

Message schema (JSON):

```json
{
  "envelope": {
    "ingested_at": "2026-05-27T08:00:30.123Z",
    "source_feed": "vehicle_positions",
    "ingester_version": "0.1.0",
    "feed_timestamp": 1748332830
  },
  "payload": {
    "vehicle": {
      "id": "BCC_2341",
      "label": "2341"
    },
    "trip": {
      "trip_id": "1234.567.123",
      "route_id": "66",
      "schedule_relationship": "SCHEDULED"
    },
    "position": {
      "latitude": -27.4698,
      "longitude": 153.0251,
      "bearing": 91.5,
      "speed": 8.3
    },
    "current_stop_sequence": 14,
    "current_status": "IN_TRANSIT_TO",
    "timestamp": 1748332828
  }
}
```

Similar shapes for trip_updates and alerts — match the GTFS-RT protobuf field names directly. Do not invent your own naming convention.

### Outbound: Prometheus metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `ingester_poll_total` | counter | `feed`, `status` | Polls, by feed and HTTP status |
| `ingester_poll_duration_seconds` | histogram | `feed` | Poll round-trip time |
| `ingester_records_published_total` | counter | `feed` | Records published to Kafka |
| `ingester_kafka_publish_errors_total` | counter | `feed` | Failed Kafka publishes |
| `ingester_seconds_since_last_successful_poll` | gauge | `feed` | For staleness alerts |

Exposed on `/metrics` (Prometheus text format) at `METRICS_PORT`.

## Implementation Notes

- Use the official `gtfs-realtime-bindings` Python package for protobuf decoding
- Use `confluent-kafka-python` for the Kafka producer (faster than `kafka-python`, official upstream support)
- Use `httpx` for HTTP (supports async; better than `requests` for this workload even if we stay sync)
- Single-process, threaded model is fine. No need for async or multi-process at our scale.

## Testing

### Unit tests

- Protobuf decoding: parse a captured live response, assert correct field extraction
- Envelope construction: given a fake payload, assert envelope has correct shape
- Kafka producer mocking: assert `produce()` called with correct topic/key/value

### Integration tests

- Stand up Kafka in a test fixture (`testcontainers-python`)
- Mock the TransLink endpoint with a fixed protobuf response
- Run the ingester for 1 polling cycle
- Drain Kafka topic, assert records present with correct schema

### Manual smoke test

- `docker compose up`
- After 60 seconds, run `kcat -b localhost:9092 -t gtfsrt.vehicle_positions -C -e` and verify records flow

## Operational Behaviour

- Liveness probe: process is alive. Always returns 200 if the HTTP server is up.
- Readiness probe: at least one successful poll in the last 5 minutes. Returns 503 otherwise.
- Graceful shutdown on SIGTERM: stop polling, flush Kafka producer, exit within 30s.

## Out Of Scope

- Schema evolution (we don't change the message format; if GTFS-RT changes we update the code)
- Multi-region (single Brisbane region)
- Authentication (public feed)
- Compression on Kafka (default is fine at our scale)
