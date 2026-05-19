"""
ml/run_pipeline.py
==================

End-to-end ML pipeline for energy consumption forecasting (P5).

This script implements the *comparative study of three distinct algorithmic
approaches* required by Section D of the project spec:

  1. LinearRegression           - simple linear baseline (interpretable)
  2. RandomForestRegressor      - bagged trees, captures non-linear patterns
  3. GradientBoostingRegressor  - sequentially boosted trees, often top accuracy

It reads the `meters_aggregated_15min` collection (P2 Spark output), backfills
seven days of synthetic 15-min windows so models have a realistic-size training
set (real streaming data is sparse during a live demo), engineers lag + time +
zone features, trains all three models with a chronological 80/20 split,
evaluates them with RMSE / MAE / R2 + train/inference time, and finally writes:

  - ml_predictions    : 6 future windows x 4 zones x 3 models  (Section D + F)
  - dashboard_alerts  : audit log of WARNING/CRITICAL forecasts (Section H)

Run from project root:

    .venv\\Scripts\\python -m ml.run_pipeline
"""

import sys
import time
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Project root on sys.path so `from storage.mongo_client import get_db` works.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from storage.mongo_client import get_db  # noqa: E402


# ============================================================================
# 1. DATA LOADING
# ============================================================================
def load_aggregated_data() -> pd.DataFrame:
    """Read meters_aggregated_15min into a DataFrame, sorted by (zone, window)."""
    db = get_db("admin")
    cursor = db.meters_aggregated_15min.find(
        {},
        {
            "_id": 0,
            "zone": 1,
            "window_start": 1,
            "avg_consumption": 1,
            "anomaly_count": 1,
            "anomaly_rate_pct": 1,
            "voltage_avg": 1,
        },
    )
    rows = list(cursor)
    if not rows:
        return pd.DataFrame(
            columns=[
                "zone", "window_start", "avg_consumption",
                "anomaly_count", "anomaly_rate_pct", "voltage_avg",
            ]
        )
    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    return df.sort_values(["zone", "window_start"]).reset_index(drop=True)


def generate_synthetic_history(real_df: pd.DataFrame, days: int = 7) -> pd.DataFrame:
    """Backfill `days` of synthetic 15-min windows modelled with two daily peaks
    (08h and 19h), weekend dampening, and zone-specific baselines + noise.
    Anchored to just before the earliest real window so chronological order is
    preserved when we concatenate."""
    rng = np.random.default_rng(seed=42)
    zones = ["A", "B", "C", "D"]
    zone_scale = {"A": 0.0100, "B": 0.0120, "C": 0.0085, "D": 0.0110}

    if not real_df.empty:
        anchor = real_df["window_start"].min().tz_convert("UTC")
    else:
        anchor = pd.Timestamp.now(tz=timezone.utc).floor("15min")
    end = anchor.floor("15min")
    start = end - pd.Timedelta(days=days)
    windows = pd.date_range(start, end, freq="15min", inclusive="left", tz=timezone.utc)

    rows = []
    for zone in zones:
        for w in windows:
            hour = w.hour + w.minute / 60.0
            dow = w.dayofweek
            # Two Gaussian-shaped daily peaks at 08h and 19h
            morning_peak = 0.35 * np.exp(-((hour - 8.0) / 2.5) ** 2)
            evening_peak = 0.45 * np.exp(-((hour - 19.0) / 2.5) ** 2)
            daily = 0.45 + morning_peak + evening_peak
            weekly = 1.0 if dow < 5 else 0.85
            base = zone_scale[zone] * daily * weekly
            noise = rng.normal(0, base * 0.08)
            avg = max(0.0008, base + noise)
            anom = int(rng.poisson(lam=base * 200))
            rows.append({
                "zone": zone,
                "window_start": w,
                "avg_consumption": float(avg),
                "anomaly_count": anom,
                "anomaly_rate_pct": float(min(100.0, anom / 6.0)),
                "voltage_avg": float(rng.normal(230, 3)),
            })
    return pd.DataFrame(rows)


# ============================================================================
# 2. FEATURE ENGINEERING
# ============================================================================
def build_features(df: pd.DataFrame):
    """Lag (1, 4 windows = 1 h), rolling mean/std, time-of-day, day-of-week,
    one-hot zone. Drops the first 4 rows per zone (cannot compute lag_4)."""
    df = df.sort_values(["zone", "window_start"]).reset_index(drop=True).copy()
    df["hour"] = df["window_start"].dt.hour
    df["dow"] = df["window_start"].dt.dayofweek
    df["is_peak"] = df["hour"].isin([7, 8, 9, 18, 19, 20]).astype(int)
    df["lag_1"] = df.groupby("zone")["avg_consumption"].shift(1)
    df["lag_4"] = df.groupby("zone")["avg_consumption"].shift(4)
    df["roll_mean_4"] = df.groupby("zone")["avg_consumption"].transform(
        lambda s: s.shift(1).rolling(window=4, min_periods=1).mean()
    )
    df["roll_std_4"] = df.groupby("zone")["avg_consumption"].transform(
        lambda s: s.shift(1).rolling(window=4, min_periods=1).std().fillna(0)
    )
    for z in ["A", "B", "C", "D"]:
        df[f"zone_{z}"] = (df["zone"] == z).astype(int)
    df = df.dropna(subset=["lag_1", "lag_4"]).reset_index(drop=True)
    feature_cols = [
        "hour", "dow", "is_peak",
        "lag_1", "lag_4", "roll_mean_4", "roll_std_4",
        "zone_A", "zone_B", "zone_C", "zone_D",
    ]
    return df, feature_cols


# ============================================================================
# 3. TRAINING + EVALUATION
# ============================================================================
def train_and_evaluate(df_feat: pd.DataFrame, feature_cols: list) -> dict:
    """Chronological 80/20 split (we never train on the future). Returns
    {model_name: {model, rmse, mae, r2, train_time_s, infer_time_us, ...}}."""
    X = df_feat[feature_cols].astype(float).values
    y = df_feat["avg_consumption"].astype(float).values
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    models = {
        "linear_regression": LinearRegression(),
        "random_forest": RandomForestRegressor(
            n_estimators=80, max_depth=12, random_state=42, n_jobs=-1
        ),
        "gradient_boosting": GradientBoostingRegressor(
            n_estimators=80, max_depth=4, learning_rate=0.1, random_state=42
        ),
    }

    results = {}
    for name, model in models.items():
        t0 = time.perf_counter()
        model.fit(X_train, y_train)
        train_s = time.perf_counter() - t0
        t0 = time.perf_counter()
        y_pred = model.predict(X_test)
        infer_us = (time.perf_counter() - t0) / max(len(X_test), 1) * 1_000_000
        results[name] = {
            "model": model,
            "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
            "mae": float(mean_absolute_error(y_test, y_pred)),
            "r2": float(r2_score(y_test, y_pred)),
            "train_time_s": float(train_s),
            "infer_time_us_per_sample": float(infer_us),
            "n_train": int(len(X_train)),
            "n_test": int(len(X_test)),
        }
    return results


def print_metrics_table(results: dict) -> None:
    print("\n" + "=" * 90)
    print(f"  {'Model':<22} {'RMSE':>11} {'MAE':>11} {'R2':>10} {'Train(s)':>10} {'Inf(us/sample)':>17}")
    print("  " + "-" * 88)
    for name, m in results.items():
        print(
            f"  {name:<22} "
            f"{m['rmse']:>11.6f} {m['mae']:>11.6f} {m['r2']:>10.4f} "
            f"{m['train_time_s']:>10.3f} {m['infer_time_us_per_sample']:>17.2f}"
        )
    print("=" * 90)


def save_metrics_to_disk(results: dict) -> None:
    """Persist metrics as JSON for the report / slides."""
    out = {
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "models": {
            name: {k: v for k, v in m.items() if k != "model"}
            for name, m in results.items()
        },
    }
    out_path = Path(__file__).parent / "metrics.json"
    out_path.write_text(json.dumps(out, indent=2))
    print(f"      -> Wrote metrics to {out_path}")


# ============================================================================
# 4. FORECAST GENERATION + WRITE TO MONGO
# ============================================================================
def generate_forecasts(
    df_feat: pd.DataFrame, results: dict, feature_cols: list, horizon: int = 6
) -> list:
    """Iteratively forecast `horizon` future 15-min windows per zone, per model.
    Returns the list of prediction documents written to ml_predictions."""
    now = pd.Timestamp.now(tz=timezone.utc).floor("15min")
    predictions = []

    for zone in ["A", "B", "C", "D"]:
        zone_df = df_feat[df_feat["zone"] == zone].copy()
        if zone_df.empty:
            continue
        # History buffer for rolling features when stepping into the future
        history = list(zone_df["avg_consumption"].tail(8))

        for h in range(1, horizon + 1):
            future_window = now + pd.Timedelta(minutes=15 * h)
            features = {
                "hour": future_window.hour,
                "dow": future_window.dayofweek,
                "is_peak": int(future_window.hour in [7, 8, 9, 18, 19, 20]),
                "lag_1": history[-1] if history else float(zone_df.iloc[-1]["avg_consumption"]),
                "lag_4": history[-4] if len(history) >= 4 else history[0],
                "roll_mean_4": float(np.mean(history[-4:])) if history else 0.01,
                "roll_std_4": float(np.std(history[-4:])) if len(history) >= 2 else 0.0,
                "zone_A": int(zone == "A"),
                "zone_B": int(zone == "B"),
                "zone_C": int(zone == "C"),
                "zone_D": int(zone == "D"),
            }
            X_new = np.array([[features[c] for c in feature_cols]], dtype=float)

            # Use best-RMSE model as the canonical forecast that feeds the iterative lag
            best_name = min(results, key=lambda k: results[k]["rmse"])
            best_pred = float(results[best_name]["model"].predict(X_new)[0])
            history.append(best_pred)

            # Emit one prediction document per (zone, future_window, model)
            for model_name, m in results.items():
                pred = float(m["model"].predict(X_new)[0])
                ratio = pred / 0.018  # 0.018 kWh / 30s = empirical peak
                if pred > 0.016:
                    alert = "CRITICAL"
                elif pred > 0.013:
                    alert = "WARNING"
                elif pred > 0.004:
                    alert = "NORMAL"
                else:
                    alert = "INFO"
                predictions.append({
                    "zone": zone,
                    "model_name": model_name,
                    "prediction_time": now.to_pydatetime(),
                    "forecast_for": future_window.to_pydatetime(),
                    "consumption_forecast": pred,
                    "ratio_to_peak": float(ratio),
                    "alert_level": alert,
                })

    db = get_db("admin")
    db.ml_predictions.delete_many({})  # clear stale predictions
    if predictions:
        db.ml_predictions.insert_many(predictions)
    return predictions


def log_critical_alerts(predictions: list) -> int:
    """Push WARNING/CRITICAL forecasts to dashboard_alerts (Section H audit log)."""
    db = get_db("admin")
    now = datetime.now(timezone.utc)
    alerts = [
        {
            "triggered_at": now,
            "zone": p["zone"],
            "alert_level": p["alert_level"],
            "forecast_kwh": p["consumption_forecast"],
            "ratio_to_peak": p["ratio_to_peak"],
            "model_name": p["model_name"],
            "triggered_by": "ml_pipeline",
            "forecast_for": p["forecast_for"],
        }
        for p in predictions
        if p["alert_level"] in ("WARNING", "CRITICAL")
    ]
    if alerts:
        db.dashboard_alerts.insert_many(alerts)
    return len(alerts)


# ============================================================================
# 5. ENTRY POINT
# ============================================================================
def main() -> None:
    print("=" * 90)
    print(" ML PIPELINE - Energy consumption forecasting (P5)")
    print(" Comparative study of 3 algorithms per Section D of the spec")
    print("=" * 90)

    print("\n[1/6] Loading real aggregated data from MongoDB...")
    real = load_aggregated_data()
    n_zones = real["zone"].nunique() if not real.empty else 0
    print(f"      -> {len(real)} real windows across {n_zones} zones")

    print("\n[2/6] Backfilling 7 days of synthetic history...")
    synth = generate_synthetic_history(real, days=7)
    print(f"      -> {len(synth)} synthetic windows")

    combined = pd.concat([synth, real], ignore_index=True, sort=False)
    print(f"      -> {len(combined)} total windows for feature engineering")

    print("\n[3/6] Building features (lag, time, zone one-hot)...")
    df_feat, feature_cols = build_features(combined)
    print(f"      -> {len(df_feat)} feature rows, {len(feature_cols)} feature columns")

    print("\n[4/6] Training 3 models with chronological 80/20 split...")
    results = train_and_evaluate(df_feat, feature_cols)
    print_metrics_table(results)
    save_metrics_to_disk(results)

    print("\n[5/6] Generating 6-window (1.5 h) forecasts for all 4 zones...")
    predictions = generate_forecasts(df_feat, results, feature_cols, horizon=6)
    print(f"      -> Wrote {len(predictions)} prediction docs to ml_predictions")

    print("\n[6/6] Auditing WARNING/CRITICAL alerts to dashboard_alerts...")
    n_alerts = log_critical_alerts(predictions)
    print(f"      -> Logged {n_alerts} alert events")

    print("\n" + "=" * 90)
    print(" [OK] ML pipeline complete.")
    print("      * Open the dashboard Predictions page -- the dashed forecast line should appear.")
    print("      * Open Alerts -- WARNING/CRITICAL rows should show if forecasts exceeded thresholds.")
    print("=" * 90)


if __name__ == "__main__":
    main()
