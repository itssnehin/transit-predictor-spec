# Data

## Primary Source: TransLink GTFS-Realtime

TransLink (Queensland's transit authority) publishes real-time data in the GTFS-Realtime standard (protobuf format). Three feeds:

| Feed | URL | Update frequency | Records per poll (typical) |
|---|---|---|---|
| Vehicle Positions | `https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions` | ~15-30s | 500-2000 |
| Trip Updates | `https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates` | ~15-30s | 800-3000 |
| Service Alerts | `https://gtfsrt.api.translink.com.au/api/realtime/SEQ/Alerts` | event-driven | 0-50 |

**Verify before building.** URLs change. As of the spec date these were correct; the first ingester task is to confirm and document the current endpoints. Check `https://gtfsrt.api.translink.com.au` or the TransLink developer portal for current paths.

No API key required for the public realtime feeds. Be a good citizen: poll at the published cadence, not more often.

### GTFS-Realtime Schema

The protobuf schema is the GTFS-Realtime spec maintained by Google. Use the official Python bindings: `gtfs-realtime-bindings`. Don't write your own parser.

Key fields we use (`VehiclePosition`):

```
vehicle.id           — unique vehicle ID
trip.trip_id         — references trip in static GTFS
trip.route_id        — references route in static GTFS
position.latitude    — current lat
position.longitude   — current lng
position.bearing     — heading
position.speed       — m/s (sometimes null)
current_stop_sequence — index into stop_times for this trip
current_status       — INCOMING_AT, STOPPED_AT, IN_TRANSIT_TO
timestamp            — POSIX seconds, when this position was recorded
```

Key fields we use (`TripUpdate.StopTimeUpdate`):

```
stop_sequence
stop_id
arrival.time         — predicted arrival time (POSIX seconds)
arrival.delay        — delay in seconds vs schedule (positive = late)
departure.time
departure.delay
schedule_relationship — SCHEDULED, SKIPPED, NO_DATA
```

The `arrival.delay` field is the operator's best current estimate — useful as a feature but **not** the ground truth.

## Secondary Source: TransLink Static GTFS

The static GTFS feed (a ZIP of CSVs) provides the schedule. Refreshed by TransLink approximately weekly.

URL: published on TransLink's open data page. Typically something like `https://translink.com.au/sites/default/files/assets/resources/about-translink/reporting-and-publications/open-data/gtfs/SEQ_GTFS.zip` — **verify before building.**

Key files:

| File | Purpose |
|---|---|
| `agency.txt` | Operator metadata |
| `routes.txt` | route_id → route_short_name, route_type |
| `trips.txt` | trip_id → route_id, service_id, headsign |
| `stops.txt` | stop_id → name, lat, lng, stop_type |
| `stop_times.txt` | trip_id, stop_id, stop_sequence, arrival_time, departure_time |
| `calendar.txt` | service_id → days of operation |
| `calendar_dates.txt` | service_id exceptions (e.g., public holidays) |

Filter to bus routes only at ingestion time: `route_type = 3`.

## Tertiary Sources

### Bureau of Meteorology

Weather observations for Brisbane. We use the free observation feed.

URL pattern: `http://www.bom.gov.au/fwo/IDQ60901/IDQ60901.94576.json` — Brisbane Aero. Verify before building; BOM URL paths are notoriously stable but occasionally move.

Polled hourly. Fields used: temperature, precipitation (last hour), wind speed, humidity.

### Queensland Public Holiday Calendar

Static CSV maintained in-repo. Source: `data.qld.gov.au`. Update annually.

### Queensland School Term Calendar

Static CSV maintained in-repo. Affects passenger load patterns. Source: Queensland Department of Education.

## Ground Truth Definition

This is the most important section. The model is trained against *observed delay*, not predicted delay. The ground truth derivation:

1. For each trip, we collect all `VehiclePosition` updates over the trip's life.
2. For each stop on the trip's schedule, we identify when the vehicle was *actually at* the stop. Definition: the first `VehiclePosition` with `current_status = STOPPED_AT` and `current_stop_sequence = N`.
3. `observed_arrival_time = timestamp` of that position record.
4. `scheduled_arrival_time = stop_times.arrival_time` for that (trip_id, stop_sequence).
5. `observed_delay = observed_arrival_time - scheduled_arrival_time` (seconds).

**Edge cases (document handling):**

- Vehicle skips a stop: no `STOPPED_AT` record for that sequence. Use the first `IN_TRANSIT_TO` record where `current_stop_sequence > N` as a proxy, with a flag indicating imputed.
- Multiple `STOPPED_AT` records (long dwell): use the first.
- Trip cancelled (`schedule_relationship = CANCELED`): exclude from training.
- Vehicle not seen at all: trip excluded from training.
- Clock skew between vehicle and server: trust the server timestamp on the GTFS-RT message, not the vehicle clock.

## Data Volume Estimates

- ~2,000 buses operating during peak hours
- ~1.5 position updates per bus per minute
- ~3,000 records/minute peak, ~500/minute off-peak
- ~3 million records/day raw, ~1 million after deduplication and filtering
- ~50-100 MB/day compressed Parquet on object store
- After 30 days of accumulation: ~3 GB on object store

These are estimates. Measure actuals after the first week of ingestion and update this document.

## Storage Schema (Object Store)

Raw layer: `s3://bucket/raw/vehicle_positions/dt=YYYY-MM-DD/hour=HH/*.parquet`

Curated layer (after stream processing): `s3://bucket/curated/arrivals/dt=YYYY-MM-DD/*.parquet`

Schema for `arrivals`:

```
event_id              string (uuid)
trip_id               string
route_id              string
stop_id               string
stop_sequence         int
scheduled_arrival_ts  timestamp
observed_arrival_ts   timestamp
observed_delay_s      int            -- negative if early, positive if late
observed_delay_imputed bool
day_of_week           int            -- 0=Mon
hour_of_day           int
is_school_day         bool
is_public_holiday     bool
ingested_at           timestamp
processed_at          timestamp
```

## Feature Engineering (dbt models)

See `specs/ML_FEATURES.md` for the full list. Brief summary:

**Stop-level features** (broadcast joined):
- stop_id, stop_lat, stop_lng, stop_type
- route_id, route_short_name
- direction_id (0/1)

**Temporal features** (computed per prediction request):
- hour_of_day, day_of_week, is_weekend
- minutes_since_midnight
- is_school_day, is_public_holiday
- time_of_day_bucket (early-morning / am-peak / midday / pm-peak / evening / night)

**Lag features** (windowed from curated layer):
- mean_delay_this_stop_last_30d_same_hour
- mean_delay_this_route_last_7d_same_hour
- mean_delay_upstream_stop_last_60min
- p50, p90 delay this stop last 30 days
- variance of delay this stop last 30 days

**Live features** (computed from recent vehicle positions):
- current_delay_observed_upstream  (delay of this trip at the previous stop, if available)
- current_average_route_delay_last_15min
- count_of_active_vehicles_on_route

**Weather features** (joined on hour):
- temp_c, precipitation_mm_last_hour, wind_speed_kmh
- is_raining (precipitation > 0.5mm)

**Target:**
- `delay_at_stop_s` — observed delay in seconds at the target stop for a future arrival 15 minutes out

## Data Quality Tests (dbt)

Mandatory tests on every model:

- `not_null` on all keys and the target
- `unique` on event_id, on (trip_id, stop_sequence) for arrivals
- `accepted_values` on route_type, day_of_week, hour_of_day
- Custom test: `observed_delay_s` between -1800 (30 min early) and 7200 (2 hours late); flag anything outside as anomaly

If more than 5% of rows fail any test, the dbt run fails and Airflow alerts. This is the primary data quality gate.

## Backfill Strategy

There is no historical archive of TransLink GTFS-RT publicly available. The model can only train on data we have collected since deployment.

**Implication:** the first 2-4 weeks of the project produce no useful predictions. The trainer should still run daily, but expect terrible metrics until the dataset is large enough (target: 7+ days of clean data).

This is a known limitation. Document it prominently in the project README — it's a good interview talking point about real-world ML pragmatism.

**Cold start mitigation:**

- Initial baseline model uses *only* schedule-derived features (no lag features) — it learns "buses are usually 2 minutes late at 8am at this stop" from a few days of data.
- Lag features come online after 30 days of accumulation.
- Live features come online from day 1.
