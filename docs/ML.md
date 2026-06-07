# Machine Learning

## Problem Statement

**Given:** a target (route_id, stop_id, target_time) where `target_time` is 15 minutes in the future, and the current state of the transit network.

**Predict:** the delay in seconds at which a bus on that route will arrive at that stop at or after `target_time`. Positive = late, negative = early.

**Why 15 minutes:** short enough that current network state is highly informative; long enough that the prediction is useful (a 1-minute horizon is uninteresting — you can just look out the window).

## Model Choices

### Baselines (must be implemented before XGBoost)

1. **Naive (constant):** predict 0. Bus is on time. This is the dumbest possible baseline and exists to sanity-check evaluation code.

2. **Naive (last observed):** predict the delay observed at this stop in the previous hour. Captures "if it's bad now, it'll be bad in 15 min".

3. **Linear regression:** standard sklearn pipeline. Same features as XGBoost. Provides the "is XGBoost worth it" comparison.

4. **Schedule-only naive:** predict `mean_historical_delay_at_stop_at_hour`. Uses only the schedule and lag features, no live state. Captures "Tuesday 8:15am at this stop is usually 90s late".

XGBoost must beat **all four** baselines on the held-out test set. If it doesn't, that's an interesting result to document, not a failure to hide.

### Primary Model: XGBoost Regression

`xgboost.XGBRegressor` with:

- Objective: `reg:squarederror` (RMSE) or `reg:absoluteerror` (MAE) — try both, pick what generalises better
- `tree_method=hist` for speed
- `early_stopping_rounds=50` on a validation fold
- `eval_metric=mae`

Hyperparameter search: **Optuna** with TPE sampler, 50-100 trials. Track in MLflow.

Search space:

| Parameter | Range |
|---|---|
| `n_estimators` | 100 – 2000 |
| `max_depth` | 3 – 10 |
| `learning_rate` | 0.01 – 0.3 (log scale) |
| `subsample` | 0.6 – 1.0 |
| `colsample_bytree` | 0.6 – 1.0 |
| `min_child_weight` | 1 – 10 |
| `reg_alpha` | 1e-8 – 10 (log scale) |
| `reg_lambda` | 1e-8 – 10 (log scale) |

### What We Are NOT Doing And Why

- **Neural networks.** Tabular features, small data, GBDT is the right tool. A neural net here is over-engineering. (We have a separate generative imaging project for deep learning.)
- **Sequence models (LSTM, Transformer).** The feature engineering already encodes temporal context as lag features. A sequence model adds complexity without clear win on this problem size.
- **Multi-task learning.** Single target. Don't get cute.
- **Per-route models.** One global model with `route_id` as a categorical feature. Per-route models would be a future enhancement, not v1.

## Feature Engineering

See `docs/DATA.md` for the catalogue. Key principles:

1. **Categorical encoding:** target encoding for `stop_id`, `route_id` (high cardinality); one-hot for `day_of_week`, `time_of_day_bucket`. Use `category_encoders` library.

2. **Missing data:** XGBoost handles `NaN` natively. Do not impute live features that are legitimately missing (e.g., no upstream observation yet — that's information).

3. **Leakage prevention:** all features must be computable using only data available *at the time of prediction*. This is the single biggest risk in time-series ML. Audit features systematically; document the "data available at prediction time" for each feature.

4. **Feature pipeline:** the same feature computation code runs in training (batch, against historical data) and in serving (online, against a feature cache). Implement once, use both places. This is non-negotiable to avoid training/serving skew.

## Evaluation

### Splitting

**Temporal split, no shuffle.** Splits are made on calendar dates:

- Train: weeks 1 to N-2
- Validation: week N-1 (for early stopping, hyperparameter selection)
- Test: week N (frozen, only touched at the end)

Where N is the current week. The test week rolls forward as new data accumulates.

**No k-fold cross-validation.** Time-series data; standard k-fold causes leakage. Use rolling-origin evaluation if you want cross-validation: train on weeks 1-4, test on 5; train on 1-5, test on 6; etc.

### Metrics

Primary: **Mean Absolute Error (MAE)** in seconds. Intuitive: "the model is off by 90 seconds on average".

Secondary:

- **RMSE** — penalises large errors more, useful for catching tail behaviour
- **Median Absolute Error** — robust to outliers
- **MAPE** — interpretable but explodes near zero, use with care
- **Within-1-minute accuracy** — fraction of predictions within ±60s of truth. Business-relevant.
- **Within-3-minute accuracy** — same, at ±180s

### Evaluation Slices

Don't just report aggregate metrics. Slice by:

- Time of day (peak vs off-peak — peak is harder, slower buses + tighter schedule)
- Day of week (Monday vs Sunday have different patterns)
- Route (some routes have wildly worse delay distributions)
- Weather (rainy days are harder)
- Horizon (5-min vs 15-min vs 30-min predictions — train one model, evaluate at multiple horizons)

A model with great average MAE but terrible peak-hour MAE is a worse model than the metrics suggest. Always look at slices.

### Champion / Challenger

Each retraining produces a candidate model. To be promoted to champion, it must:

1. Beat the current champion's MAE on the held-out test week by ≥1% (relative)
2. Not regress on any major slice by more than 5%
3. Pass a sanity check: predictions for a fixed set of (route, stop, time) inputs should look reasonable (no NaN, no extreme values)

If any check fails, the candidate is rejected and the champion stays.

## MLOps

### Experiment Tracking

**Local:** MLflow tracking server in Docker Compose. Logs:
- Hyperparameters
- Metrics (all of the above, on all slices)
- Trained model artefact
- Feature importance plot
- Predicted-vs-actual scatter plot
- A small slice of predictions for spot-checking

**Cloud:** SageMaker Experiments + Model Registry. Same fields. The MLflow client can also write to SageMaker if we want unified tooling.

### Model Registry

Each model is registered with:

- Version (semver based on training date: e.g., 2026.06.15)
- Stage tag: `candidate`, `champion`, `archived`
- Training metadata: data window, code commit hash, hyperparameters
- Evaluation metrics on the test week

API service loads the model tagged `champion`. Promotion is atomic: update the tag, the next API restart picks up the new model.

### Retraining Schedule

Daily, at a low-traffic hour (3am local time):

1. Pull yesterday's curated data
2. Refresh feature tables (dbt run)
3. Train candidate model on rolling window (last N weeks)
4. Evaluate on held-out test week
5. Run champion/challenger check
6. If candidate wins: tag as champion, alert
7. If candidate loses: archive, log reason
8. Garbage collect models older than 30 days, except the current champion

### Drift Monitoring

The API logs every prediction and (eventually) its observed truth. Daily job computes:

- Rolling 24-hour MAE on live predictions
- Comparison to test-week MAE
- Alert if live MAE exceeds test MAE by more than 30%

Feature drift: weekly job computes population stability index (PSI) on key features comparing the last 7 days against the training window. Alert if PSI > 0.2 on any major feature.

These are simple checks. They're enough for a portfolio project. Production-grade drift monitoring is out of scope.

## Reproducibility

- All randomness seeded. `random.seed`, `numpy.random.seed`, `xgboost.set_config(verbosity=0)`, etc.
- Data versions captured: each model logs the SHA256 hash of the training dataset.
- Code versions captured: each model logs the git commit it was trained from.
- Dependencies pinned: `uv.lock` or `requirements.txt` with versions.

Given the same code, same data, same seed, the model should reproduce exactly. Test this once and document the result.

## Interpretability

Not a primary goal, but worth a section in the final write-up:

- SHAP values on the test set, aggregated to identify top features
- Partial dependence plots for the top 5 features
- A few cherry-picked example predictions with feature contributions

This gives interview material: "what features matter most?" → "according to SHAP, the dominant signal is the upstream-stop observed delay, followed by hour-of-day and weather".

## Open Questions

These are things to investigate during the build, not decisions to make upfront:

1. Should we use `route_type` (bus/train/ferry) as a feature even though we're filtering to buses only? (No — but document the decision.)
2. Should we predict delay or arrival time? (Delay is mean-zero-ish; better numerical properties. Stick with delay.)
3. Quantile regression instead of point estimate? (Maybe v2 — useful for confidence intervals.)
4. Should we model "will this bus skip this stop entirely" as a separate classifier? (Maybe — but the dataset is sparse for skipped stops. Defer.)
