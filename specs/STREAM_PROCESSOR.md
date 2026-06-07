# Stream Processor Spec

## Purpose

Consume raw GTFS-RT events from Kafka, derive ground truth (observed arrival delays) by correlating vehicle positions with the static schedule, and write enriched events to the object store as Parquet.

## Behaviour Overview

The stream processor is a long-running Spark Structured Streaming job. It maintains state about in-progress trips and emits an `arrival` event each time a vehicle is determined to have arrived at a scheduled stop.

This is the most algorithmically complex part of the system. The ground truth derivation is non-trivial.

## Inputs

- Kafka: `gtfsrt.vehicle_positions`, `gtfsrt.trip_updates`
- Static GTFS data: loaded at startup, refreshed daily. Broadcast to all executors.

## Outputs

- Object store: `s3://bucket/curated/arrivals/dt=YYYY-MM-DD/part-*.parquet`
- Schema: see `docs/DATA.md` `arrivals` schema
- Partitioned by date (UTC) of `observed_arrival_ts`

## Ground Truth Algorithm

Per trip:

1. Maintain a stateful aggregation keyed by `trip_id`
2. State holds: the trip's stop_sequence list (from static GTFS), and for each stop, the observed arrival event if one has been derived
3. For each incoming vehicle_position record on this trip:
   a. If `current_status == STOPPED_AT` and the corresponding stop in the state has no observed arrival yet:
      - Record `observed_arrival_ts = record.timestamp`
      - Emit an `arrival` event
   b. If `current_status == IN_TRANSIT_TO` and `current_stop_sequence == N`, then we have *implicitly* arrived at stop N-1 (and possibly skipped intermediate stops). For each unrecorded earlier stop:
      - Record `observed_arrival_ts = record.timestamp` (with `observed_delay_imputed = true`)
      - Emit an `arrival` event
4. When the trip terminates (vehicle stops sending updates for 10+ minutes after the scheduled end, or sends a new trip_id), emit `arrival` events with `observed_delay_imputed = true` for any remaining unrecorded stops where we have evidence the vehicle passed (otherwise mark as `did_not_observe`).

Watermark: 30 minutes. Trip state is dropped after the trip's scheduled end time + 30 min.

### Edge Cases (Match the catalogue in DATA.md)

- **Skipped stop:** detected by `IN_TRANSIT_TO` to a downstream stop with no prior `STOPPED_AT`. Emit with `imputed = true`.
- **Backtracking** (vehicle's reported stop_sequence decreases): rare; log warning and use the latest sequence as source of truth.
- **Trip cancelled** (`schedule_relationship = CANCELED` on TripUpdate): mark all unrecorded stops as `cancelled` and emit no arrival events for them.
- **No vehicle ever seen** for a scheduled trip: at end-of-day, emit `did_not_observe` events for those scheduled stops (out of scope for v1 — defer).

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | — | Same as ingester |
| `OBJECT_STORE_ENDPOINT` | yes | — | MinIO endpoint locally, S3 in cloud |
| `OBJECT_STORE_BUCKET` | yes | — | Bucket name |
| `OBJECT_STORE_ACCESS_KEY` | yes (local) | — | Cloud uses IAM role |
| `OBJECT_STORE_SECRET_KEY` | yes (local) | — | Cloud uses IAM role |
| `STATIC_GTFS_PATH` | yes | — | Path to extracted GTFS files (Postgres locally, S3 cloud) |
| `CHECKPOINT_PATH` | yes | — | Spark checkpoint location for exactly-once delivery |
| `WATERMARK_MINUTES` | no | 30 | Late-arrival tolerance |

## State Management

- Use Spark's `mapGroupsWithState` keyed on `trip_id`
- Checkpoint to durable storage (MinIO/S3) for failure recovery
- State TTL: trip's scheduled end + watermark, then dropped

## Output Schema

See `docs/DATA.md` `arrivals` schema.

Partitioning: by `dt` (UTC date of `observed_arrival_ts`). One Parquet file per micro-batch per partition (Spark default).

## Implementation Notes

- Spark version: 3.5.x (stable, well-supported)
- Use PySpark, not Scala. Owner is more productive in Python.
- Kafka source uses the `kafka` connector built into Spark Streaming
- Trigger: `processingTime("60 seconds")` — micro-batches every minute. Tuneable.
- Output mode: `Append` (only finalised arrivals are emitted)
- Coalesce to 1 file per partition per batch to avoid small-file problem (at our scale, this is fine)

## Testing

### Unit tests

- Ground truth algorithm against synthetic trip sequences. Hand-craft 10+ test cases covering happy path, skip, cancel, late-arriving vehicle.
- Use `chispa` or similar for DataFrame assertions.

### Integration tests

- Stand up Kafka + MinIO via testcontainers
- Publish a captured day of real data
- Run the stream processor for a few minutes
- Assert: arrivals Parquet exists, schema correct, row counts within tolerance

### Backtest

- Once we have 7 days of accumulated data, manually verify a few cherry-picked trips:
  - Pick 5 trips at random
  - Open Google Maps / TransLink app, see if the times look right
  - Document any discrepancies as a known limitation

## Operational Behaviour

- Liveness: Spark job is running. K8s probe via the Spark UI (port 4040) `/`.
- Readiness: Kafka consumer lag below threshold (e.g., 60 seconds of data).
- Restart strategy: K8s Job restart on failure. Spark checkpoint resumes from last micro-batch.

## Performance Expectations

At Brisbane's scale (~3,000 records/min peak):

- Latency: end-to-end (Kafka publish → curated Parquet) under 2 minutes
- Throughput: trivially handled by a single Spark executor with 2GB heap
- This is NOT a high-throughput problem. We're using Spark because the *skill* is portable, not because we need its scale.

## Out Of Scope

- Real-time exactly-once semantics (at-least-once is fine; deduplication happens in dbt)
- Sub-minute latency
- Cross-trip enrichment (we keep trip state local; cross-trip features come from dbt)
