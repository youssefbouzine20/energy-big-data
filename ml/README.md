# P5 — ML Models

This folder is for training, evaluating, and serving the three machine-learning
models required by the project specification.

## Purpose in the pipeline

```
MongoDB (P3) ──► ML training (P5 — this folder) ──► ml/models/*.pkl ──► Dashboard (P4)
```

## Models to compare

The professor requires **three** models compared by Precision, Recall, F1-Score:

| Model | Suggested lib | Strength |
|---|---|---|
| Logistic Regression | `sklearn.linear_model.LogisticRegression` | Baseline, interpretable |
| Random Forest | `sklearn.ensemble.RandomForestClassifier` | Robust to outliers, no scaling needed |
| Gradient Boosting | `sklearn.ensemble.GradientBoostingClassifier` | Often best F1 on imbalanced data |

(XGBoost is an alternative to the third model — slightly better performance, but adds a dependency.)

## Targets

| Target variable | Type | Source field |
|---|---|---|
| `is_anomaly` | Binary classification | `meters_raw.is_anomaly` |
| `consumption_kwh` (next interval) | Regression | `meters_aggregated_15min.consumption_kwh` |

Start with the **classification task** (anomaly detection) — it's better aligned with the professor's eval criteria (Precision/Recall/F1).

## Features available

From `meters_raw`: `consumption_kwh`, `voltage_v`, `frequency_hz`, `power_factor`, `hour_of_day`, `day_of_week`, `is_peak_hour`, `zone`

After joining with `weather` (by nearest timestamp): `temperature_c`, `feels_like_c`, `humidity_pct`, `solar_irradiance_wm2`, `wind_speed_ms`, `weather_severity`

After joining with `incidents` (by zone + time window): `active_incident_count`, `latest_severity`

## Class imbalance

About 7% of meter readings are anomalies (forced injection every 15 cycles in the producer). For binary classification this is moderately imbalanced — strategies:

- `class_weight='balanced'` in sklearn (simplest, recommended first try)
- SMOTE oversampling (`imblearn` — not yet in `requirements.txt`)
- Stratified train/test split (`train_test_split(..., stratify=y)`)

## Required env vars

- `MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DB_NAME`

## What to build

1. **`ml/load_data.py`** — read training data from MongoDB into a Pandas DataFrame. Apply temporal split (last 20% for test, no shuffling).
2. **`ml/features.py`** — feature engineering: one-hot encode `zone`, scale numeric features, derive lagged consumption.
3. **`ml/train.py`** — train all 3 models in a loop, save each as `ml/models/{name}.pkl`.
4. **`ml/evaluate.py`** — evaluate each model on test set, output Precision/Recall/F1/ROC-AUC comparison table.

## Starter snippet

```python
# ml/train.py
import pandas as pd
import joblib
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score

from storage.mongo_client import get_db

db = get_db()
docs = list(db.meters_raw.find({}, {"_id": 0}))
df = pd.DataFrame(docs)

FEATURES = ["voltage_v", "frequency_hz", "power_factor",
            "hour_of_day", "day_of_week", "is_peak_hour", "consumption_kwh"]
X = df[FEATURES]
y = df["is_anomaly"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

models = {
    "logreg": Pipeline([("scaler", StandardScaler()),
                        ("clf", LogisticRegression(class_weight="balanced"))]),
    "rf":     RandomForestClassifier(class_weight="balanced", n_estimators=200, random_state=42),
    "gb":     GradientBoostingClassifier(random_state=42),
}

Path("ml/models").mkdir(exist_ok=True)
for name, model in models.items():
    model.fit(X_train, y_train)
    joblib.dump(model, f"ml/models/{name}.pkl")
    preds = model.predict(X_test)
    print(f"\n=== {name} ===")
    print(classification_report(y_test, preds))
    print(f"ROC-AUC: {roc_auc_score(y_test, model.predict_proba(X_test)[:,1]):.3f}")
```

## Dependencies

All in `requirements.txt`:
- `pandas==2.2.2`
- `scikit-learn==1.5.0`
- `numpy==1.26.4`
- `pymongo==4.7.3`

## Run

```bash
.venv/bin/python -m ml.train
.venv/bin/python -m ml.evaluate
```

## Output for the defense

Generate a single comparison table the professor can grade:

```
| Model            | Precision | Recall | F1    | ROC-AUC |
|------------------|-----------|--------|-------|---------|
| LogisticReg      |  0.82     | 0.71   | 0.76  | 0.89    |
| RandomForest     |  0.91     | 0.78   | 0.84  | 0.94    |
| GradientBoosting |  0.93     | 0.81   | 0.87  | 0.96    |
```

## Notes

- Models saved to `ml/models/*.pkl` are gitignored (per `.gitignore` line 230) — do not commit them.
- Re-run training weekly on fresh MongoDB data; do not train on a frozen dataset.
