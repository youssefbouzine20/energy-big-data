# P5 — ML Models

Training, evaluation, and serving the three machine-learning models required by
the professor's spec Section D. Two ML tasks: anomaly classification AND
consumption regression (both explicit in Section F: "courbes de prédiction
versus consommation réelle").

## Purpose in the pipeline

```
MongoDB (P3) ──► ML training (P5 — this folder) ──► ml/models/*.pkl ──► ml_predictions collection ──► Dashboard (P4)
```

## Two ML tasks (BOTH required by spec)

| Task | Type | Target | Output collection | Used by dashboard |
|---|---|---|---|---|
| **Anomaly detection** | Binary classification | `is_anomaly` | `ml_predictions.anomaly_proba` | Predictive alerts |
| **Consumption forecast** | Regression | Next-window `consumption_kwh` | `ml_predictions.consumption_forecast` | "Prediction vs actual" chart (Section F) |

## Three algorithms to compare (Section D — REQUIRED)

Professor mandates: **"étude comparative de trois approches algorithmiques distinctes"**.

| Model | Strength | Why this one |
|---|---|---|
| **Logistic Regression** | Linear baseline, fully interpretable | Mandatory baseline; if it beats nothing, no point in complex models |
| **Random Forest** | Non-linear, robust to outliers, no feature scaling needed | Strong baseline for tabular data, handles class imbalance via `class_weight` |
| **Gradient Boosting** | Best performance on imbalanced data, captures non-linear interactions | Typically the strongest contender for tabular ML tasks |

(Alternatives considered but not chosen: SVM — slow on large data; XGBoost — extra dependency for marginal gain over GB; neural networks — overkill for tabular data.)

## Evaluation metrics (REQUIRED — Section D)

Professor mandates: **"Précision, Rappel, F1-Score, temps d'inférence"** (4 metrics).

| Metric | What it measures | Why we need it |
|---|---|---|
| **Precision** | TP / (TP+FP) | When alarm fires, is it real? Avoid alert fatigue. |
| **Recall** | TP / (TP+FN) | Do we catch real anomalies? Missing them is dangerous. |
| **F1-Score** | Harmonic mean of P & R | Single number for class-imbalanced comparison |
| **Inference time (ms)** ⚠️ | Avg ms per prediction | Required by spec — explicit "temps d'inférence" |

Bonus: ROC-AUC (overall discrimination), training time, model size on disk.

### Required final comparison table (goes into the Rapport)

```
| Model            | Precision | Recall | F1   | ROC-AUC | Inference (ms) | Model size |
|------------------|-----------|--------|------|---------|----------------|------------|
| LogisticReg      |    0.82   |  0.71  | 0.76 |  0.89   |     0.3        |   2 KB     |
| RandomForest     |    0.91   |  0.78  | 0.84 |  0.94   |     1.8        |   24 MB    |
| GradientBoosting |    0.93   |  0.81  | 0.87 |  0.96   |     0.9        |   1.5 MB   |
```

Use `time.perf_counter()` averaged over 1000 test predictions for inference time.

## Features available

From `meters_raw`:
- `consumption_kwh`, `voltage_v`, `frequency_hz`, `power_factor`
- `hour_of_day`, `day_of_week`, `is_peak_hour`
- `zone` (one-hot encoded)

After join with `weather` (nearest-timestamp):
- `temperature_c`, `feels_like_c`, `humidity_pct`, `solar_irradiance_wm2`, `wind_speed_ms`
- `weather_severity` (one-hot encoded)

After join with `incidents` (zone + active-window):
- `active_incident_count`, `latest_severity_ord` (LOW=0, MEDIUM=1, HIGH=2, CRITICAL=3)

After join with `market-prices` (if/when P1 adds the topic):
- `price_eur_mwh` (energy market price at the time of measurement)

## Class imbalance handling

About 7% of readings are anomalies (forced injection every 15 cycles). Strategies, in order of preference:

1. **`class_weight='balanced'`** — sklearn built-in, no extra deps, just works
2. **Stratified train/test split** — `train_test_split(..., stratify=y)` (always do this)
3. **SMOTE oversampling** — only if 1+2 are insufficient (requires adding `imbalanced-learn` to requirements)

## Data Quality validation (Section E — required before training)

Before each training run, P5 must verify training data quality. Output to `ml_data_quality`:

| Check | Threshold | Action if violated |
|---|---|---|
| Feature completeness | ≥ 99% non-null | Impute or drop row |
| Target leakage | None | Investigate feature engineering |
| Class balance | 3% < anomaly rate < 15% | Re-weight or alert producer team |
| Temporal coverage | ≥ 80% of training window | Wait or retrain later |

## Required env vars

`MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DB_NAME`

## What to build

1. **`ml/load_data.py`** — read training data from MongoDB into Pandas. Apply **temporal split** (last 20% by timestamp for test — never random shuffle for time series).
2. **`ml/features.py`** — feature engineering: one-hot encode `zone` and `weather_severity`, scale numerics for LR, derive lagged consumption (t-1, t-2, t-3 windows).
3. **`ml/train.py`** — train all 3 models, measure inference time, save to `ml/models/{name}.pkl`.
4. **`ml/evaluate.py`** — generate the final comparison table for the Rapport.
5. **`ml/predict.py`** — load best model, predict next-window for all zones, write to `ml_predictions`. Runs every 15 min via cron or scheduler.

## Starter snippet (classification)

```python
import time
import joblib
import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score

from storage.mongo_client import get_db

db = get_db()
df = pd.DataFrame(list(db.meters_raw.find({}, {"_id": 0})))
df = df.sort_values("timestamp").reset_index(drop=True)

FEATURES = ["voltage_v", "frequency_hz", "power_factor",
            "hour_of_day", "day_of_week", "is_peak_hour", "consumption_kwh"]
X, y = df[FEATURES], df["is_anomaly"].astype(int)

# TEMPORAL split (last 20%), not random
split = int(len(df) * 0.8)
X_train, X_test, y_train, y_test = X[:split], X[split:], y[:split], y[split:]

models = {
    "logreg": Pipeline([("scaler", StandardScaler()),
                        ("clf", LogisticRegression(class_weight="balanced", max_iter=1000))]),
    "rf":     RandomForestClassifier(class_weight="balanced", n_estimators=200, random_state=42),
    "gb":     GradientBoostingClassifier(random_state=42),
}

Path("ml/models").mkdir(exist_ok=True)
results = []
for name, model in models.items():
    t0 = time.perf_counter()
    model.fit(X_train, y_train)
    train_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(1000):
        model.predict(X_test.iloc[[0]])
    infer_ms = (time.perf_counter() - t0) / 1000 * 1000  # ms per prediction

    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]
    joblib.dump(model, f"ml/models/{name}.pkl")

    results.append({
        "model": name, "train_s": train_time, "infer_ms": infer_ms,
        "report": classification_report(y_test, preds, output_dict=True),
        "roc_auc": roc_auc_score(y_test, proba),
    })

# Persist results table for the Rapport
db.ml_evaluation.insert_one({"timestamp": pd.Timestamp.utcnow(), "results": results})
```

## Required justifications for the defense

1. **Why these 3 algorithms?** (not SVM, not neural networks) — use the table above
2. **Why temporal split, not random shuffle?** (Time-series data: future leakage destroys validity)
3. **How do you handle class imbalance?** (`class_weight='balanced'` first, SMOTE as fallback)
4. **What is your operating threshold?** (For binary classification: not always 0.5 — pick based on precision-recall tradeoff)
5. **What features matter most?** (Use `feature_importances_` from RF for the Rapport)
6. **What is the expected drift?** (Weather seasonality → retrain every 30 days; document in Rapport)

## Run

```bash
.venv/bin/python -m ml.train       # train all 3, save pkls, fill ml_evaluation
.venv/bin/python -m ml.evaluate    # print final comparison table
.venv/bin/python -m ml.predict     # generate predictions for dashboard (run every 15 min)
```

## Dependencies (already in `requirements.txt`)

- `pandas==2.2.2`, `scikit-learn==1.5.0`, `numpy==1.26.4`, `pymongo==4.7.3`

## Notes

- `ml/models/*.pkl` is gitignored ([.gitignore](../.gitignore) line 230) — never commit binaries
- Retrain weekly on fresh MongoDB data; never train once and freeze
- For the defense: print the comparison table on a slide. Highlight the winner and explain why (per metric).
