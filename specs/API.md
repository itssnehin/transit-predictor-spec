# API Service Spec

## Purpose

Serve XGBoost-based delay predictions via a FastAPI HTTP service. Loads the current champion model from the registry on startup and provides synchronous predictions.

## Endpoints

### `POST /predict`

Request:

```json
{
  "route_id": "66",
  "stop_id": "318363",
  "target_time": "2026-05-27T08:15:00+10:00"
}
```

- `route_id`: required, string, must exist in static GTFS
- `stop_id`: required, string, must exist in static GTFS
- `target_time`: required, ISO 8601 with timezone. Must be in the future. Must be within 60 minutes of now (longer horizons are out of scope).

Response (200):

```json
{
  "predicted_delay_seconds": 132,
  "predicted_arrival_time": "2026-05-27T08:17:12+10:00",
  "horizon_minutes": 15,
  "model_version": "2026.05.26",
  "request_id": "01HV...",
  "features_used": {
    "live_features_available": true,
    "upstream_observed_delay_seconds": 95
  }
}
```

Errors:

- 400: invalid input (validation errors from Pydantic)
- 404: route_id or stop_id not in current GTFS
- 422: target_time in the past or > 60 min in future
- 503: model not loaded (startup race condition)
- 500: unexpected error (log full trace, return safe message)

### `GET /health`

Liveness probe. Always returns 200 OK if process is up.

```json
{"status": "ok"}
```

### `GET /ready`

Readiness probe. Returns 200 OK if model is loaded and feature cache is initialised; 503 otherwise.

```json
{
  "status": "ready",
  "model_version": "2026.05.26",
  "model_loaded_at": "2026-05-27T03:15:42Z",
  "feature_cache_records": 12834
}
```

### `GET /metrics`

Prometheus metrics endpoint. See metrics section below.

## Model Lifecycle

### Loading

On startup:

1. Connect to model registry (MLflow local, SageMaker Model Registry cloud)
2. Fetch the model tagged `champion`
3. Download to local filesystem
4. Load into memory via `xgboost.XGBRegressor.load_model()`
5. Load feature schema (column list, dtypes) alongside model
6. Mark service as ready

### Refresh

The API does not auto-refresh during runtime. To pick up a new champion, the K8s deployment is rolled (`kubectl rollout restart deployment/api`). This is triggered by the retraining DAG after a successful champion promotion.

Why this approach: avoids the complexity of hot-reloading inside a running service; the cost (a few seconds of downtime during rollout) is acceptable for a portfolio project. Rolling deployment with `maxUnavailable=0` keeps the service responsive throughout.

## Feature Computation at Inference Time

The hard part. The model needs all features that were present in training, computed *at the time of the prediction request*.

Three categories:

1. **Static features** (stop_lat, route_short_name, etc.): looked up from an in-memory dict loaded from GTFS at startup. ~10ms.

2. **Temporal features** (hour_of_day, is_school_day, etc.): computed from `target_time` directly. <1ms.

3. **Lag features** (mean_delay_last_30d_same_hour, etc.): pre-computed nightly by dbt and loaded into an in-memory cache (or Redis) on startup. Refreshed when the deployment rolls.

4. **Live features** (upstream_observed_delay, current_route_avg_delay_last_15min, etc.): computed from a rolling window of recent arrivals. Two implementation options:

   **Option A (simpler):** the API queries an in-memory cache populated by a background consumer reading from the `arrivals` Kafka topic / S3. Pros: simple. Cons: stateful service.

   **Option B (proper):** a separate "feature server" service (Feast or a custom one) handles online features. Pros: clean separation. Cons: more components.

   **Choice: Option A** for v1. Build a `FeatureCache` class that subscribes to the `arrivals` topic and maintains rolling windows. If we want to flex Feast as a resume bullet, do it in Phase 8 polish.

## Performance Targets

- p50 latency: <50ms
- p95 latency: <150ms
- p99 latency: <300ms
- Throughput: 50 req/s on a single pod (we don't need more)

These are easy targets given XGBoost inference is <10ms.

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `MODEL_REGISTRY_URI` | yes | — | MLflow tracking URI / SageMaker registry name |
| `MODEL_NAME` | yes | `transit-delay-predictor` | Registered model name |
| `MODEL_STAGE` | no | `champion` | Stage tag to load |
| `KAFKA_BOOTSTRAP_SERVERS` | yes | — | For live feature consumer |
| `FEATURE_CACHE_REDIS_URL` | no | — | If set, use Redis instead of in-memory |
| `LOG_LEVEL` | no | INFO | |
| `METRICS_PORT` | no | 9100 | |
| `API_PORT` | no | 8000 | |

## Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `api_requests_total` | counter | `endpoint`, `status` | HTTP requests by status code |
| `api_request_duration_seconds` | histogram | `endpoint` | Latency |
| `api_predictions_total` | counter | `route_id` (only top 20) | Predictions served |
| `api_prediction_delay_seconds` | histogram | — | Distribution of predicted delays |
| `api_model_load_age_seconds` | gauge | — | How long ago the current model was loaded |
| `api_feature_cache_records` | gauge | — | Records in feature cache |
| `api_feature_cache_lag_seconds` | gauge | — | How fresh is the cache |

## Logging

Structured JSON. One log line per request with:

- `request_id` (ULID, returned in response too)
- `route_id`, `stop_id`, `target_time`
- `predicted_delay_seconds`
- `latency_ms`
- `model_version`
- `live_features_available` (bool)

These logs are the input to drift monitoring. Ship them to CloudWatch (cloud) or stdout (local; fluentbit picks up).

## Testing

### Unit tests

- Request validation: invalid inputs return correct 4xx codes
- Feature computation: given a fake request, assert features have correct values
- Model loading: mock the registry, assert model is loaded correctly
- Error handling: assert exceptions are caught and translated to 500s with safe messages

### Integration tests

- Stand up MLflow + Kafka in fixtures
- Train and register a tiny model
- Start the API
- Hit `/predict` and assert valid response
- Assert metrics are exposed

### Load test

- `locust` or `vegeta`, 50 req/s for 5 minutes
- Assert p95 < 150ms, no errors

## Security

- No authentication (portfolio project; out of scope)
- CORS open in development; restricted to a known dashboard origin in production
- Rate limit: 10 req/s per IP via FastAPI middleware
- Input sanitation handled by Pydantic models

## Out Of Scope

- Batch prediction endpoint
- WebSocket / streaming predictions
- A/B testing across model versions
- Multi-region deployment
- User accounts
