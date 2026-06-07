# ML Features Catalogue

Definitive list of features used by the model. Any feature not in this document is not in the model. Any addition requires updating this document first, then implementing.

For each feature: name, source, computation, leakage risk assessment, and serving-time availability.

## Target

| Field | Type | Description |
|---|---|---|
| `delay_at_stop_s` | int | Observed delay in seconds at (stop_id, route_id) at the first arrival ≥ target_time |

Computation: from the `arrivals` curated table, `observed_delay_s`. Negative = early, positive = late.

## Identifier Features (not used by model directly; for joins)

| Field | Type |
|---|---|
| `prediction_id` | string (ULID) |
| `route_id` | string |
| `stop_id` | string |
| `target_time` | timestamp (UTC) |

## Static Stop & Route Features

Sourced from static GTFS. Refreshed weekly.

| Feature | Type | Computation | Notes |
|---|---|---|---|
| `stop_lat` | float | from `stops.txt` | |
| `stop_lng` | float | from `stops.txt` | |
| `stop_zone_id` | category | from `stops.txt`, fare zone | |
| `route_short_name` | category | from `routes.txt` | |
| `route_type` | category | from `routes.txt` | Will be constant (3=bus) since we filter |
| `direction_id` | int | from `trips.txt` (0 or 1) | Inbound vs outbound |
| `stop_sequence_normalised` | float | `stop_sequence / max(stop_sequence)` per trip | Position along route (0 to 1) |

Leakage: none, static.
Serving availability: always (loaded at startup).

## Temporal Features

Computed from `target_time` directly.

| Feature | Type | Computation |
|---|---|---|
| `hour_of_day` | int (0-23) | UTC offset to Brisbane local time first |
| `day_of_week` | int (0-6) | 0=Monday |
| `is_weekend` | bool | Sat or Sun |
| `minutes_since_midnight` | int | 0-1439 |
| `is_school_day` | bool | Joined from QLD school calendar |
| `is_public_holiday` | bool | Joined from QLD public holiday list |
| `time_of_day_bucket` | category | early-morning (4-6) / am-peak (6-9) / midday (9-15) / pm-peak (15-19) / evening (19-22) / night (22-4) |

Leakage: none.
Serving availability: always.

## Weather Features

Sourced from BOM observations for Brisbane Aero (BOM station 94576). Joined on hour.

| Feature | Type | Computation |
|---|---|---|
| `temp_c` | float | Temperature at top of `target_time` hour |
| `precip_mm_last_hour` | float | Rainfall in the hour up to `target_time` |
| `wind_speed_kmh` | float | Sustained wind speed |
| `humidity_pct` | float | Relative humidity |
| `is_raining` | bool | `precip_mm_last_hour > 0.5` |

Leakage: at training time we have actual observations. At serving time for a 15-min-ahead prediction, we use the latest available observation as a proxy for the target hour. This is a small training/serving skew — document it.
Serving availability: usually yes (BOM hourly; we cache last 24h). Fallback: use seasonal averages if cache is stale.

## Historical (Lag) Features

Computed by dbt nightly. Aggregations over the last 30 days of accumulated data.

| Feature | Type | Window | Group by |
|---|---|---|---|
| `mean_delay_this_stop_30d` | float | last 30 days | stop_id |
| `mean_delay_this_stop_same_hour_30d` | float | last 30 days, same hour | (stop_id, hour_of_day) |
| `mean_delay_this_stop_same_dow_hour_30d` | float | last 30 days, same dow + hour | (stop_id, day_of_week, hour_of_day) |
| `mean_delay_this_route_7d` | float | last 7 days | route_id |
| `mean_delay_this_route_same_hour_7d` | float | last 7 days, same hour | (route_id, hour_of_day) |
| `p50_delay_this_stop_30d` | float | last 30 days | stop_id |
| `p90_delay_this_stop_30d` | float | last 30 days | stop_id |
| `stddev_delay_this_stop_30d` | float | last 30 days | stop_id |
| `n_observations_this_stop_30d` | int | last 30 days | stop_id |

Leakage: window excludes the current day. Pre-computed daily means training/serving consistency.
Serving availability: from a cached lookup table refreshed nightly. Missing values handled by XGBoost (NaN).

## Live (Online) Features

Computed at serving time from rolling windows of recent `arrivals` data. These are the trickiest features — they require real-time state.

| Feature | Type | Computation |
|---|---|---|
| `upstream_delay_observed_s` | float | If a vehicle is already running this trip and has passed stop N-1, the delay it had at that stop. NaN otherwise. |
| `upstream_stops_observed_count` | int | Number of stops on the current trip already observed |
| `route_mean_delay_last_15min` | float | Mean observed delay on this route in the last 15 minutes (any stop) |
| `route_active_vehicles_count` | int | Number of vehicles currently active on this route |
| `route_p90_delay_last_60min` | float | 90th percentile delay on this route in the last hour |

Leakage: these use only data from before `target_time`. Verify in training that the time filter is strict.
Serving availability: requires the feature cache (see `specs/API.md`). On startup, the cache backfills from the last 2 hours of `arrivals` data, then keeps current via Kafka subscription.

## Interaction Features

Created from base features. Not strictly necessary (XGBoost will find interactions), but a few are cheap and helpful:

| Feature | Type | Computation |
|---|---|---|
| `is_peak_school_day` | bool | `time_of_day_bucket in (am-peak, pm-peak) AND is_school_day` |
| `wet_peak` | bool | `is_raining AND time_of_day_bucket in (am-peak, pm-peak)` |
| `route_x_hour` | category | target-encoded combination |

## Excluded Features (For Now)

Documenting features we deliberately don't use, with reasons:

- **`trip_id`**: too high cardinality, low generalisation value
- **`vehicle_id`**: same; vehicles move between routes
- **Real-time traffic data**: would require Google Maps or similar paid API
- **Special event calendars** (sports, concerts at Brisbane Live, Suncorp): future enhancement
- **Trip headsign**: encoded by route_id + direction_id already
- **Operator (Brisbane Transport vs Transdev)**: encoded by route_id

## Feature Importance Expectations (Hypotheses)

Before training, document hypotheses. After training, compare to SHAP output. This makes the analysis interpretable.

Expected top-5:
1. `upstream_delay_observed_s` (strongest signal when available)
2. `route_mean_delay_last_15min`
3. `mean_delay_this_stop_same_dow_hour_30d`
4. `time_of_day_bucket` (peak vs off-peak)
5. `is_raining` or `precip_mm_last_hour`

Expected weak features (will be candidates for removal):
- `humidity_pct`
- `stop_zone_id`
- `route_type` (constant within bus filter)

After training: revisit, document actual top-10 with surprises.
