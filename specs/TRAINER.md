# Trainer Service Spec

## Purpose

Train the XGBoost regression model on the latest feature dataset, evaluate against baselines and the current champion, and promote the new model if it wins.

Runs as a batch job — not a long-running service.

## Triggering

- **Local:** Airflow DAG runs daily at 3am AEST (LocalExecutor, single worker)
- **Cloud:** Step Functions state machine triggered by EventBridge schedule, kicks off a SageMaker Training Job

Both invoke the same container with the same entrypoint and arguments.

## Inputs

- Feature training set: from dbt-produced `feature_training_set` table (Postgres local / Athena cloud)
- Current champion model: from MLflow / SageMaker Model Registry
- Configuration: hyperparameter search budget, time window for training data

## Outputs

- A new candidate model registered in the registry (always, even if it loses)
- Updated `champion` tag if the candidate wins
- MLflow run with all metrics, plots, artefacts
- A summary report logged to CloudWatch / stdout

## Workflow

```
1. Pull training data: last N weeks (default 8 weeks) ending at midnight UTC yesterday
2. Verify dataset size: must have at least 100,000 rows; abort if not
3. Split into train / val / test by week (see docs/ML.md)
4. Compute baselines on test set, log all metrics
5. Run Optuna study on train + val: minimise MAE on val (default 50 trials)
6. Refit best hyperparameters on train + val combined
7. Evaluate on test set: compute MAE, RMSE, slice metrics, plots
8. Compare candidate vs current champion on the same test set
9. Apply promotion criteria (see docs/ML.md):
   - Beat champion MAE by ≥1% relative
   - No slice regression worse than -5%
   - Sanity checks pass
10. If win: tag candidate as champion, archive old champion
11. If lose: tag candidate as archived, log reason
12. Emit summary metrics to monitoring
```

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `FEATURE_TABLE_URI` | yes | — | SQL connection string / Athena workgroup |
| `MODEL_REGISTRY_URI` | yes | — | MLflow / SageMaker registry |
| `TRAINING_WINDOW_WEEKS` | no | 8 | Rolling training window |
| `OPTUNA_TRIALS` | no | 50 | Hyperparameter search budget |
| `OPTUNA_TIMEOUT_MINUTES` | no | 90 | Wall-clock budget for search |
| `MIN_TRAINING_ROWS` | no | 100000 | Abort if dataset too small |
| `PROMOTION_THRESHOLD_PCT` | no | 1.0 | Required MAE improvement to promote |
| `MAX_SLICE_REGRESSION_PCT` | no | 5.0 | Reject if any slice regresses more than this |

## Implementation Notes

### Code structure

```
services/trainer/
├── pyproject.toml
├── Dockerfile
├── src/
│   └── trainer/
│       ├── __init__.py
│       ├── __main__.py        # CLI entrypoint
│       ├── data.py             # data loading from feature store
│       ├── baselines.py        # naive, last-observed, schedule-only, linear regression
│       ├── xgboost_model.py    # XGBoost training + Optuna
│       ├── evaluation.py       # metrics + slice analysis
│       ├── promotion.py        # champion/challenger logic
│       └── registry.py         # MLflow/SageMaker adapter
└── tests/
```

### Determinism

- Set seeds for `random`, `numpy`, `xgboost`, `optuna`
- Pass `n_jobs=1` where reproducibility matters; otherwise allow parallelism
- Log seed + commit hash to MLflow run

### Resource expectations

- Dataset size: ~1M rows after 8 weeks of accumulation
- Memory: <4GB for the XGBoost training; can run on a single small instance
- Training time: 5-30 minutes for the full Optuna study, depending on `OPTUNA_TRIALS`

### SageMaker integration (cloud)

The same container runs as a SageMaker Training Job. Differences:

- Input data: SageMaker mounts S3 paths to `/opt/ml/input/data/`. We read CSV/Parquet from there.
- Output artefacts: SageMaker expects `/opt/ml/model/` to contain the trained model. Symlink to MLflow's output dir.
- Environment: SageMaker injects `SM_*` env vars; ignore unless we need them.

This dual-mode design (run locally OR in SageMaker, same code) is the key value of the trainer service.

## Evaluation Output

Every training run produces (and logs to MLflow):

- Hyperparameters of best trial
- Aggregate metrics: MAE, RMSE, median_AE, within_1min, within_3min
- Slice metrics: same metrics broken down by hour_of_day_bucket, day_of_week, top-10 routes, weather conditions
- Plots: predicted vs actual scatter (test set), residual histogram, feature importance bar chart, SHAP summary plot for top features
- Comparison table: candidate vs champion across all metrics
- Promotion decision: WIN / LOSE / REJECTED, with reason

## Testing

### Unit tests

- Baselines produce sane predictions on synthetic data
- Promotion logic: given fake metrics, returns correct decision
- Data loader: handles empty results, missing columns, schema mismatches

### Integration test

- End-to-end training on a tiny fixture dataset (1000 rows)
- Asserts: model registered, metrics logged, no exceptions

### Acceptance test (manual)

- Run trainer against the real accumulated dataset weekly during early development
- Sanity-check the promotion decisions: do they match what you'd expect from looking at the metrics?

## Failure Modes

| Failure | Detection | Response |
|---|---|---|
| Dataset too small | row count < threshold | Exit non-zero; Airflow alerts |
| Optuna trials all fail | best trial returned None | Exit non-zero; alert |
| Cannot reach model registry | connection error | Retry 3x with backoff; then exit |
| Candidate is much worse than champion | MAE >50% worse | Still register (for debugging) but reject promotion; log loud warning |
| Slice metrics produce NaN (empty slice) | metric == nan | Exclude that slice from regression check |

## Out Of Scope

- Distributed training (XGBoost on this dataset fits in memory)
- AutoML beyond Optuna (no PyCaret, no autosklearn)
- Online learning / incremental updates
- Multi-target models
- Quantile regression for confidence intervals (deferred to v2)
