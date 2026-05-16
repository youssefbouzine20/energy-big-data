# P5 — ML Models  (FULL WORKFLOW + WHAT TO BUILD)

> Owner: P5 teammate.
> Reads from: MongoDB (`meters_raw`, `weather`, `incidents`, `market_prices`).
> Writes to:  MongoDB (`ml_predictions`, `ml_evaluation`, `ml_data_quality`).
> Used by:    P4 dashboard reads `ml_predictions` for the prediction-vs-actual chart and alerts.

---

## 0. The pipeline in one picture

```
                     P3 MongoDB                                  P4 Dashboard
                    ─────────────                              ──────────────
                     meters_raw      ──┐
                     weather          ──┤  pymongo.find        ml_predictions ──► prediction-vs-actual chart
                     incidents       ──┼─────────► [training] ─►ml_evaluation ──► defense Rapport table
                     market_prices   ──┘                       ml_data_quality
                                            │
                                            ▼
                                    ┌──────────────────┐
                                    │  YOU ARE HERE    │
                                    │  (P5 ML)         │
                                    │                  │
                                    │ 1. load + join   │
                                    │ 2. feature eng.  │
                                    │ 3. temporal split│
                                    │ 4. train 3 models│
                                    │ 5. evaluate      │
                                    │ 6. save best.pkl │
                                    │ 7. predict every │
                                    │    15 min        │
                                    └──────────────────┘
```

**Your job in one sentence:** train and compare **3 ML algorithms** on **2
ML tasks** (anomaly classification + consumption regression), measure them
on **4 metrics** (Precision, Recall, F1, inference time), pick a winner per
task, and run a 15-minute scheduled prediction job that feeds the dashboard.

---

## 1. The 2 ML tasks (BOTH required by spec)

| Task | Type | Target | Output collection field | Used by dashboard |
|---|---|---|---|---|
| **Anomaly detection** | Binary classification | `is_anomaly` (bool) | `ml_predictions.anomaly_proba` (0..1) | Predictive alerts threshold |
| **Consumption forecast** | Regression | next-window avg `consumption_kwh` | `ml_predictions.consumption_forecast` (float) | Prediction-vs-actual line chart (Section F) |

Both tasks are explicit:
- Anomaly detection — Section D mentions "détection d'anomalies"
- Consumption forecast — Section F demands "courbes de prédiction versus consommation réelle"

---

## 2. The 3 algorithms — REQUIRED defense answer

The professor's Section D mandates: "**étude comparative de trois approches algorithmiques distinctes**".

| Model | Strength | Why we picked it |
|---|---|---|
| **Logistic Regression** (classification) / **Linear Regression** (regression) | Linear baseline, fully interpretable | Mandatory baseline — if it beats nothing, no point in complex models |
| **Random Forest** | Non-linear, robust to outliers, no feature scaling needed | Strong tabular baseline; handles class imbalance via `class_weight` |
| **Gradient Boosting** | Best raw performance on imbalanced tabular data, captures non-linear interactions | Typically the strongest contender for this kind of problem |

**Alternatives considered and rejected:**

| Rejected | Why |
|---|---|
| SVM | Slow on > 10k rows; no native probability output (Platt scaling adds latency) |
| XGBoost | Marginal gain over sklearn GB; extra dependency we don't need |
| Neural networks (MLP/LSTM) | Overkill for tabular data of this size; harder to defend the architecture choice in 6 minutes of defense |
| Isolation Forest | Could use for unsupervised anomaly, but we have labels — supervised is stronger |

---

## 3. The 4 evaluation metrics — REQUIRED (Section D)

The professor's Section D mandates: "**Précision, Rappel, F1-Score, temps d'inférence**".

| Metric | Formula | Why |
|---|---|---|
| **Precision** | TP / (TP + FP) | When alarm fires, is it real? Avoid alert fatigue. |
| **Recall** | TP / (TP + FN) | Do we catch real anomalies? Missing is dangerous. |
| **F1-Score** | 2·P·R / (P + R) | Single number for class-imbalanced comparison |
| **Inference time** ⚠️ | ms per single prediction | EXPLICITLY required — measure with `time.perf_counter()` averaged over 1000 calls |

**Bonus** (not required but strengthens the Rapport):
- ROC-AUC: overall discrimination quality (model-agnostic threshold)
- Training time
- Model size on disk
- For regression: MAE, RMSE, R²

### The required final comparison table (paste into the Rapport)

```
| Model            | Precision | Recall | F1   | ROC-AUC | Inference (ms) | Model size |
|------------------|-----------|--------|------|---------|----------------|------------|
| LogisticReg      |   0.82    |  0.71  | 0.76 |  0.89   |     0.3        |    2 KB    |
| RandomForest     |   0.91    |  0.78  | 0.84 |  0.94   |     1.8        |   24 MB    |
| GradientBoosting |   0.93    |  0.81  | 0.87 |  0.96   |     0.9        |   1.5 MB   |
```

(Numbers above are illustrative — your actual results will differ.)

---

## 4. Features — what's available from MongoDB

### From `meters_raw` (one row = one meter reading)

| Feature | Type | Notes |
|---|---|---|
| `consumption_kwh` | float | also the regression target |
| `voltage_v` | float | strongest anomaly signal |
| `frequency_hz` | float | grid stability |
| `power_factor` | float | efficiency |
| `hour_of_day` | int 0..23 (UTC) | temporal — circular encoding recommended |
| `day_of_week` | int 0..6 | temporal — also circular |
| `is_peak_hour` | bool | derived (7-9 or 17-21 UTC) |
| `zone` | str A/B/C/D | one-hot encode |
| `is_anomaly` | bool | classification target |
| `anomaly_reason` | str/null | NOT a feature — leak of target |

### After joining `weather` (nearest-timestamp lookup, ±2 min)

| Feature | Type |
|---|---|
| `temperature_c`, `feels_like_c` | float |
| `humidity_pct`, `solar_irradiance_wm2`, `wind_speed_ms` | float |
| `weather_severity` | str — one-hot encode (5 categories) |

### After joining `incidents` (active-incident-in-zone-at-time)

| Feature | Type |
|---|---|
| `active_incident_count` | int (0 if none) |
| `latest_severity_ord` | int (LOW=0..CRITICAL=3) |
| `incident_recency_min` | float (minutes since last incident in zone) |

### After joining `market_prices` (latest hourly value before timestamp)

| Feature | Type |
|---|---|
| `price_eur_mwh` | float |
| `renewable_share_pct` | float |
| `trend` | str — one-hot encode (3 values) |

### Engineered features (you create)

| Feature | How |
|---|---|
| `consumption_lag_1` | Same meter's consumption 15 min ago |
| `consumption_lag_2` | 30 min ago |
| `consumption_lag_3` | 45 min ago |
| `consumption_rolling_mean_1h` | Avg over last 4 readings |
| `voltage_rolling_std_1h` | Std-dev over last 4 readings (instability indicator) |
| `hour_sin`, `hour_cos` | Circular encoding of `hour_of_day` |
| `dow_sin`, `dow_cos` | Circular encoding of `day_of_week` |

---

## 5. Class imbalance handling

About 7% of readings are anomalies (forced injection every 15 cycles by P1).
Strategies, in order of preference:

1. **`class_weight='balanced'`** — sklearn built-in, no extra deps, just works
2. **Stratified train/test split** — `train_test_split(..., stratify=y)` ALWAYS do this
3. **SMOTE oversampling** — only if 1+2 are insufficient (requires `imbalanced-learn` dependency)
4. **Threshold tuning** — pick operating threshold from the precision-recall curve, not 0.5

### Defense answer

> "We use `class_weight='balanced'` because it gives the optimizer the right
> per-class loss weighting without modifying the data distribution, which would
> bias the calibration of `predict_proba`. Threshold tuning is done on the
> validation set against the precision-recall tradeoff — for energy outage
> alerting, we prefer high recall (don't miss outages) at the cost of some
> precision (some false alarms are tolerable)."

---

## 6. Temporal split — NOT random shuffle

Time-series data MUST be split chronologically — not randomly.
Random shuffle would let the model train on data from the future of the test
period, leaking information and inflating scores.

```python
df = df.sort_values("timestamp").reset_index(drop=True)
split = int(len(df) * 0.8)             # last 20% for test
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]
```

For cross-validation, use `sklearn.model_selection.TimeSeriesSplit` not `KFold`.

---

## 7. Data quality validation — Section E (REQUIRED)

Before each training run, P5 must verify training data quality and write to
`ml_data_quality`. If any check fails, log it and either skip retraining or
adjust class weights.

| Check | Threshold | Action if violated |
|---|---|---|
| Feature completeness | ≥ 99% non-null | Impute median or drop row |
| No target leakage | `anomaly_reason` not in features | Investigate feature engineering |
| Class balance | 3% ≤ anomaly rate ≤ 15% | Re-weight or alert producer team |
| Temporal coverage | ≥ 80% of training window has data | Wait or use shorter window |
| Feature drift (vs prev. month) | Each feature's mean within 2σ | Trigger retraining alert |

---

## 8. What you must build — explicit task list

| # | File | Purpose | Done when |
|---|---|---|---|
| 1 | `ml/__init__.py` | Empty marker | File exists |
| 2 | `ml/load_data.py` | Read training data from Mongo, join weather + incidents + prices | Returns clean Pandas DataFrame |
| 3 | `ml/features.py` | One-hot encode, scale numerics, derive lagged features, circular encode time | Returns (X, y) for both tasks |
| 4 | `ml/quality.py` | Run the 5 data-quality checks; write to `ml_data_quality` | Skips training and warns if any check fails |
| 5 | `ml/train_classifier.py` | Train LogReg + RF + GB for `is_anomaly`; save `models/anomaly_*.pkl` | 3 .pkl files exist; metrics in `ml_evaluation` |
| 6 | `ml/train_regressor.py` | Train Linear + RF + GB for `consumption_kwh`; save `models/forecast_*.pkl` | 3 .pkl files exist; metrics in `ml_evaluation` |
| 7 | `ml/evaluate.py` | Print final comparison tables; produce the markdown table for Rapport | Tables print to stdout, are well-formatted |
| 8 | `ml/predict.py` | Load best models, predict next 15-min window for all zones, write to `ml_predictions` | Run every 15 min via cron |
| 9 | `ml/scheduler.py` | Optional: APScheduler that runs `predict.py` every 15 min | One process, no cron needed |
| 10 | `ml/models/.gitkeep` | Folder placeholder; `*.pkl` is gitignored | — |
| 11 | `ml/README.md` (this file) | Already written | — |

---

## 9. Starter snippet — classification training

```python
"""ml/train_classifier.py — train all 3 anomaly-detection models."""
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from storage.mongo_client import get_db
from ml.features import build_features

# ── 1. Load + join + feature engineering ────────────────────────────────────
db = get_db()
print("[INFO] Loading meters_raw + joins from Mongo ...")
df_raw = pd.DataFrame(list(
    db.meters_raw.find({}, {"_id": 0}).sort("timestamp", 1).limit(100_000)
))
print(f"[INFO] {len(df_raw)} rows loaded.")

X, y = build_features(df_raw, db, target="is_anomaly")   # joins weather/incidents/prices
print(f"[INFO] Features: {X.shape}, target balance: {y.mean()*100:.1f}% positive")

# ── 2. Temporal split (last 20% for test) ───────────────────────────────────
split = int(len(X) * 0.8)
X_train, X_test = X.iloc[:split], X.iloc[split:]
y_train, y_test = y.iloc[:split], y.iloc[split:]

# ── 3. Define the 3 models ──────────────────────────────────────────────────
models = {
    "logreg": Pipeline([
        ("scaler", StandardScaler()),
        ("clf",    LogisticRegression(class_weight="balanced", max_iter=1000, random_state=42)),
    ]),
    "rf":  RandomForestClassifier(class_weight="balanced", n_estimators=200,
                                  max_depth=15, n_jobs=-1, random_state=42),
    "gb":  GradientBoostingClassifier(n_estimators=200, max_depth=4, random_state=42),
}

# ── 4. Train + evaluate ─────────────────────────────────────────────────────
Path("ml/models").mkdir(parents=True, exist_ok=True)
results = []
for name, model in models.items():
    print(f"\n[INFO] Training {name} ...")
    t0 = time.perf_counter()
    model.fit(X_train, y_train)
    train_s = time.perf_counter() - t0

    # Inference time over 1000 single-row predictions
    sample = X_test.iloc[[0]]
    t0 = time.perf_counter()
    for _ in range(1000):
        model.predict(sample)
    infer_ms = (time.perf_counter() - t0) / 1000 * 1000

    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        "model":     name,
        "train_s":   round(train_s, 2),
        "infer_ms":  round(infer_ms, 3),
        "precision": round(precision_score(y_test, preds), 4),
        "recall":    round(recall_score(y_test, preds), 4),
        "f1":        round(f1_score(y_test, preds), 4),
        "roc_auc":   round(roc_auc_score(y_test, proba), 4),
    }
    results.append(metrics)

    # Save model
    path = f"ml/models/anomaly_{name}.pkl"
    joblib.dump(model, path)
    metrics["model_path"] = path
    metrics["model_size_kb"] = round(Path(path).stat().st_size / 1024, 1)
    print(f"  → {metrics}")

# ── 5. Persist results for the Rapport ──────────────────────────────────────
db.ml_evaluation.insert_one({
    "task":          "anomaly_classification",
    "trained_at":    pd.Timestamp.utcnow(),
    "n_train":       len(X_train),
    "n_test":        len(X_test),
    "results":       results,
    "winner":        max(results, key=lambda r: r["f1"])["model"],
})

# ── 6. Print the comparison table ───────────────────────────────────────────
df_results = pd.DataFrame(results)
print("\n=== ANOMALY CLASSIFICATION COMPARISON ===")
print(df_results.to_markdown(index=False))
```

### Regression starter (same shape)

```python
"""ml/train_regressor.py — train all 3 consumption-forecast models."""
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
# ... same scaffolding as above, but:
# - target = next-window consumption (shift)
# - models use Regressor variants
# - metrics: MAE, RMSE, R², plus inference time (still required by spec)
```

---

## 10. Starter snippet — predict.py (runs every 15 min)

```python
"""ml/predict.py — load best models, forecast next window per zone, write to ml_predictions."""
from datetime import datetime, timedelta, timezone

import joblib
import pandas as pd
from storage.mongo_client import get_db
from ml.features import build_features

db    = get_db()
now   = datetime.now(timezone.utc)
next_window = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0) + timedelta(minutes=15)

# ── Load best models (winner from ml_evaluation) ────────────────────────────
clf = joblib.load("ml/models/anomaly_rf.pkl")              # adjust to your winner
reg = joblib.load("ml/models/forecast_gb.pkl")             # adjust to your winner

# ── For each zone, build feature row and predict ────────────────────────────
ZONES = ["A", "B", "C", "D"]
historical_peaks = {
    z: db.meters_aggregated_15min.aggregate([
        {"$match": {"zone": z, "window_start": {"$gte": now - timedelta(days=30)}}},
        {"$group": {"_id": None, "peak": {"$max": "$avg_consumption"}}}
    ]).next().get("peak", 0.05) for z in ZONES
}

for zone in ZONES:
    # Build the same feature row build_features produces, for the latest reading per zone
    latest = list(db.meters_raw.find({"zone": zone}).sort("timestamp", -1).limit(1))
    if not latest: continue
    X_one, _ = build_features(pd.DataFrame(latest), db, target=None, predict_mode=True)

    forecast      = float(reg.predict(X_one)[0])
    anomaly_proba = float(clf.predict_proba(X_one)[0, 1])

    ratio = forecast / max(historical_peaks[zone], 1e-6)
    if   ratio >= 0.95: alert = "CRITICAL"
    elif ratio >= 0.85: alert = "WARNING"
    elif ratio >= 0.70: alert = "INFO"
    else:               alert = "NORMAL"

    db.ml_predictions.update_one(
        {"zone": zone, "forecast_for": next_window},
        {"$set": {
            "zone":             zone,
            "model_name":       "RandomForest+GradientBoosting",
            "prediction_time":  now,
            "forecast_for":     next_window,
            "consumption_forecast": round(forecast, 4),
            "anomaly_proba":    round(anomaly_proba, 4),
            "alert_level":      alert,
            "ratio_to_peak":    round(ratio, 3),
        }},
        upsert=True
    )
    print(f"[{zone}] forecast={forecast:.4f} kWh  proba={anomaly_proba:.2f}  alert={alert}")
```

---

## 11. Required justifications for the defense

1. **Why these 3 algorithms?** (not SVM, neural networks) — use the table in §2
2. **Why temporal split, not random shuffle?** — Time-series data; future leakage destroys validity
3. **How do you handle class imbalance?** — `class_weight='balanced'` first, threshold tuning second, SMOTE only if needed
4. **What is your operating threshold for alerts?** — Picked from precision-recall curve to favor recall (we'd rather false-alarm than miss an outage)
5. **What features matter most?** — Print `feature_importances_` from RF for the Rapport
6. **What is the expected data drift?** — Weather seasonality → retrain every 30 days; document in Rapport
7. **Why this winner per task?** — Highest F1 (or RMSE for regression) WITHIN the inference-time budget (< 5 ms)
8. **What is the inference time budget and why?** — Dashboard refreshes every 30s and predicts 4 zones, so per-prediction must be << 1s. We aim < 5 ms to leave headroom.
9. **Are predictions explainable?** — Yes for LogReg (coefficients), partial for RF (`feature_importances_`), GB has SHAP available — reference but don't promise SHAP if it's not built

---

## 12. Run + verify

### One-time training

```bash
.venv/bin/python -m ml.quality              # validate training data first
.venv/bin/python -m ml.train_classifier     # trains 3, saves .pkl, fills ml_evaluation
.venv/bin/python -m ml.train_regressor      # trains 3, saves .pkl, fills ml_evaluation
.venv/bin/python -m ml.evaluate             # prints final comparison tables
```

### Run prediction job (every 15 min)

```bash
# Manual one-off
.venv/bin/python -m ml.predict

# OR: run the scheduler (long-lived process)
.venv/bin/python -m ml.scheduler
```

### Cron alternative (Linux/WSL)

```cron
*/15 * * * * cd /path/to/energy-big-data && .venv/bin/python -m ml.predict >> logs/ml_predict.log 2>&1
```

### Verify

- `ls ml/models/` shows 6 .pkl files (3 anomaly + 3 forecast)
- `db.ml_evaluation.find().sort({trained_at:-1}).limit(2)` shows your two latest training runs
- `db.ml_predictions.find().sort({prediction_time:-1}).limit(8)` shows 4 zones × 2 latest windows
- Defense slide: print the comparison table from `ml/evaluate.py`

---

## 13. Common pitfalls

1. **Random shuffle on time-series** → inflated scores, worthless on real data. Use temporal split.
2. **Including `anomaly_reason` as a feature** → target leak; model gets ~100% F1 but useless in production
3. **Training on tiny data** → wait for ≥ 24h of producer history (~80k smart-meter rows)
4. **Class-weight=balanced + SMOTE together** → over-corrects, hurts precision. Pick one.
5. **Regression target shift** → predict next-window consumption, not current. Use `df["consumption_kwh"].shift(-1)` (within zone group).
6. **`predict_proba` not calibrated** → for LogReg it is; for RF and GB it isn't perfectly. Use `CalibratedClassifierCV` if you need true probabilities.
7. **Inference time measured wrong** → measuring batch predict on 10k rows ≠ single-row latency. Always loop 1000× single-row.
8. **Models grow over time** → set `n_estimators` cap; warn if `.pkl` > 100 MB.

---

## 14. Section H Ethics — your responsibilities

The professor's Section H requires ethical ML. Your contribution:

- **Transparency:** every prediction includes the `model_name` field so the dashboard can show "predicted by RandomForest"
- **Auditability:** training runs are logged to `ml_evaluation` with full metrics + sample size; never overwrite, always insert new docs
- **Bias detection:** verify per-zone F1 in evaluation — if zone A is 0.95 but zone D is 0.60, document the disparity in the Rapport
- **No PII in features:** never use `meter_id` or any identifying info as a model input — only physical measurements + derived temporal features
- **Right to explanation (GDPR Art. 22):** be ready to answer "why did the model flag this reading as anomalous?" using `feature_importances_` or per-prediction SHAP

---

## 15. Dependencies (already in root `requirements.txt`)

`pandas==2.2.2`, `scikit-learn==1.5.0`, `numpy==1.26.4`, `pymongo==4.7.3`. Optional: `apscheduler` for `scheduler.py`, `imbalanced-learn` for SMOTE if needed.

---

## 16. Notes

- `ml/models/*.pkl` is gitignored — never commit binaries
- Retrain weekly on fresh MongoDB data; never train once and freeze
- For the defense: print the comparison table on a slide. Highlight the
  winner per task and explain why per metric.
- Save the trained model's `feature_names_in_` in a sidecar `.json` so
  `predict.py` can validate the feature order matches at inference time.
