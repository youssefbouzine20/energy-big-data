# MongoDB — Guide for P4 (Dashboard) and P5 (ML)

> **Who this is for:** P4 and P5 teammates. P3 has already set up everything in MongoDB — this guide tells you what exists, how to connect, and exactly what to call.
>
> **One rule:** Never create your own `MongoClient`. Always use `get_db()` from `storage.mongo_client`. Everything else follows from that.

---

## Contents

1. [What's in the database right now](#1-whats-in-the-database-right-now)
2. [How to connect](#2-how-to-connect)
3. [For P4 — Dashboard queries](#3-for-p4--dashboard-queries)
4. [For P5 — ML training and predictions](#4-for-p5--ml-training-and-predictions)
5. [Field name reference — what P2 actually writes](#5-field-name-reference--what-p2-actually-writes)
6. [If something looks wrong](#6-if-something-looks-wrong)

---

## 1. What's in the database right now

| Collection | Written by | What it holds | TTL |
|---|---|---|---|
| `meters_raw` | P2 Spark | Individual smart-meter readings: consumption, voltage, frequency, anomaly flags | 90 days |
| `meters_aggregated_15min` | P2 Spark | 15-minute aggregates per zone: avg/max/min consumption, anomaly rate, weather join, active incidents | 1 year |
| `weather` | P2 Spark | Temperature, humidity, severity per timestamp | 1 year |
| `incidents` | P2 Spark | Raw incident reports | 2 years |
| `incidents_enriched` | P2 Spark | Incidents with NLP keyword extraction (`nlp_keywords` field) | 2 years |
| `feedback_nlp` | P2 Spark | User feedback with sentiment labels (`sentiment_predicted` field) | 1 year |
| `rss_feeds` | P2 Spark | News feeds with category and relevance score | 1 year |
| `market_prices` | P2 Spark | Energy market prices per timestamp | 1 year |
| `data_quality_metrics` | P2 Spark | Per-window quality metrics: coverage, completeness, noise, anomaly rate | 1 year |
| `ml_predictions` | **P5 (you)** | Model forecasts — starts empty, P5 fills this | 6 months |
| `dashboard_alerts` | **P4 (you)** | Triggered alert log — starts empty, P4 fills this | 1 year |

**Current snapshot** (with P1 + P2 running):

```
meters_raw:              ~7 000 docs
weather:                   ~350 docs
incidents:                 ~180 docs
feedback_nlp:              ~230 docs
rss_feeds:                  ~90 docs
market_prices:               ~3 docs   ← sparse, P2 passthrough
data_quality_metrics:       ~12 docs
meters_aggregated_15min:     0 docs    ← P2 watermark fix pending
ml_predictions:              0 docs    ← P5 hasn't run yet
dashboard_alerts:            0 docs    ← P4 hasn't triggered any yet
```

> `ml_predictions` and `dashboard_alerts` being empty is expected at this stage. `meters_aggregated_15min` at 0 is a known P2 issue with the watermark — a fix is in progress.

---

## 2. How to connect

### Install dependencies (once, from the project root)

```bash
pip install pymongo==4.7.3 python-dotenv==1.2.2
```

### Connect in Python

```python
from storage.mongo_client import get_db

db = get_db()   # returns the energy_db database, role="reader"
```

That's it. The connection pool (max 50 connections), `.env` loading, and reconnection are all handled. You never need to touch `MongoClient` directly.

### Environment variables

The `.env` at the project root already has the right values for local dev. The two things to be aware of:

```
MONGO_HOST=localhost          # use this when running on the host machine
# MONGO_HOST=mongodb          # use this when running INSIDE a Docker container
MONGO_PORT=27017
MONGO_DB_NAME=energy_db
MONGO_USERNAME=energy_admin
MONGO_PASSWORD=change-me-before-deploy
```

> P5 note: If you write predictions from a script running on the host (not inside Docker), you also need MONGO_SPARK_USER and MONGO_SPARK_PASS in your .env. Add these two lines if they're not already there:
MONGO_SPARK_USER=spark_writer
MONGO_SPARK_PASS=<value>
To find <value>: open storage/init/init-mongo.js and look for the line where spark_writer is created — the password there is what the volume was initialized with. To verify it works, run a small test insert with role="spark_writer" after connecting. If you get Authentication failed, the .env value doesn't match the volume — this happens if .env was changed after the first docker compose up. In that case the only fix is down -v && up -d (destroys all data), so coordinate with P3 before doing that.

### Verify your connection works

```bash
python -m storage.mongo_client
```

This prints the collection list and document counts. If it fails, check your `.env` values.

---

## 3. For P4 — Dashboard queries

All functions are in `storage/query_patterns.py`. Import and call — no raw MongoDB queries needed.

### KPI cards and heatmap

```python
from storage.query_patterns import get_latest_zone_aggregates

results = get_latest_zone_aggregates(zones=["A", "B", "C", "D"], window_count=1)

# Each result looks like:
# {
#   "zone": "B",
#   "latest_windows": [{
#       "window_start": datetime(...),
#       "avg_consumption": 0.045,
#       "anomaly_rate_pct": 2.3,
#       "active_incidents": 1,
#       "weather_temperature_c": 28.4,
#       "weather_severity": "HOT",
#       "meter_count": 12,
#       "zone_lat": 35.575,
#       "zone_lon": -5.370
#   }]
# }
```

### Consumption timeseries (line chart)

```python
from storage.query_patterns import get_zone_timeseries
from datetime import datetime, timedelta

end   = datetime.utcnow()
start = end - timedelta(hours=24)

points = get_zone_timeseries(zone="A", start=start, end=end, metric="avg_consumption")

# Returns: [{"window_start": datetime(...), "avg_consumption": 0.043}, ...]
# Other valid metrics: "anomaly_rate_pct", "active_incidents", "weather_temperature_c"
```

### Peak consumption alerts

```python
from storage.query_patterns import get_peak_consumption_alert

alerts = get_peak_consumption_alert(
    zones=["A", "B", "C", "D"],
    threshold_multiplier=1.5,   # alert if 50% above recent baseline
    lookback_windows=4          # compare against last 4 × 15min windows
)

# Returns: [{"zone": "B", "current_value": 0.089, "baseline": 0.051, "ratio": 1.74}, ...]
# Empty list = no zones in alert state
```

### Incident word cloud

```python
from storage.query_patterns import get_nlp_keyword_frequency
from datetime import datetime, timedelta

keywords = get_nlp_keyword_frequency(
    zones=["A", "B", "C", "D"],   # or omit for all zones
    start=datetime.utcnow() - timedelta(days=7),
    end=datetime.utcnow(),
    top_k=20
)

# Returns: [{"keyword": "transformer", "count": 14, "zones": ["A", "C"]}, ...]
```

### Sentiment distribution chart

```python
from storage.query_patterns import get_sentiment_distribution

# Overall counts
dist = get_sentiment_distribution(group_by="overall")

# Per zone
dist = get_sentiment_distribution(group_by="zone")

# Per channel (WEB / MOBILE / CALL_CENTER / EMAIL)
dist = get_sentiment_distribution(group_by="channel")

# Returns: [{"_id": "NEGATIVE", "count": 87, "pct": 37.8}, ...]
```

### Data quality badge

```python
from storage.query_patterns import get_latest_quality_metrics

quality = get_latest_quality_metrics(topics=["smart-meters"])

# Returns per topic:
# {
#   "topic": "smart-meters",
#   "metrics": [{
#       "window_start": datetime(...),
#       "completeness_pct": 97.2,
#       "noise_rate_pct": 1.1,
#       "anomaly_rate_pct": 2.3,
#       "temporal_coverage_pct": 99.0,
#       "alert": "OK"        # or "DEGRADED" / "CRITICAL"
#   }]
# }
```

### Prediction vs actual chart

```python
from storage.query_patterns import get_prediction_vs_actual

# ⚠️  Returns empty until P5 has written at least one prediction.
# This is expected — the function won't error, it just returns [].

comparison = get_prediction_vs_actual(zone="A", windows=10)

# Returns: [{
#   "window": datetime(...),
#   "model": "xgb-v2.1",
#   "predicted": 0.052,
#   "actual": 0.049,           # None if meters_aggregated_15min is still at 0 docs
#   "error_pct": 6.12
# }, ...]
```

### Incident drill-down (detail page)

```python
from storage.query_patterns import get_incident_meter_correlation

detail = get_incident_meter_correlation("INC-20260518-001")

# Returns the incident + correlated anomaly counts calculated live from meters_raw:
# {
#   "incident": {
#       "incident_id": "INC-20260518-001",
#       "zone": "B",
#       "severity": "HIGH",
#       "correlated_anomalies": 7,       # computed on-the-fly
#       "correlated_voltage_drops": 3    # computed on-the-fly
#   },
#   "affected_meters": ["SM-007", "SM-012"],
#   "reading_count": 48,
#   "readings": [...]   # capped at 100 docs
# }
```

### Writing dashboard alerts

```python
from storage.mongo_client import get_dashboard_alerts_collection
from datetime import datetime, timezone

alerts = get_dashboard_alerts_collection()
alerts.insert_one({
    "triggered_at": datetime.now(timezone.utc),
    "zone": "B",
    "alert_type": "HIGH_CONSUMPTION",   # or PEAK_ALERT, ANOMALY_RATE, etc.
    "threshold": 0.95,
    "observed_value": 1.02,
    "message": "Zone B consumption exceeded threshold by 7%"
})
```

> TTL on `dashboard_alerts` is 365 days — no manual cleanup needed.

---

## 4. For P5 — ML training and predictions

### Reading training data

`meters_aggregated_15min` is the recommended training source — it's already aggregated and includes weather and incident context per 15-minute window. Use `meters_raw` only if you need individual meter readings.

```python
from storage.query_patterns import get_ml_training_features
from datetime import datetime, timedelta

end   = datetime.utcnow()
start = end - timedelta(days=60)

features = get_ml_training_features(
    zone="A",
    start=start,
    end=end,
    include_weather=True,    # enriches with humidity, solar, wind from weather collection
    include_market=True      # enriches with market_price_mad, renewable_share_pct
)

import pandas as pd
df = pd.DataFrame(features)

# Columns available (all from meters_aggregated_15min + optional joins):
# window_start, avg_consumption, max_consumption, anomaly_rate_pct,
# active_incidents, weather_temperature_c, weather_severity, meter_count,
# weather_humidity, weather_solar, weather_wind,      ← if include_weather=True
# market_price_mad, market_renewable_pct, market_trend  ← if include_market=True
```

> **If `meters_aggregated_15min` is at 0 docs:** This is a known P2 watermark issue. Use `meters_raw` as a fallback while P2 deploys the fix. See below.

### Reading raw meter data (fallback or granular features)

```python
from storage.query_patterns import get_raw_meter_window
from datetime import datetime, timedelta

start = datetime(2026, 5, 18, 12, 0)
end   = start + timedelta(minutes=15)

readings = get_raw_meter_window(zone="B", window_start=start, window_end=end)

# Returns per reading:
# {
#   "meter_id": "SM-007",
#   "timestamp": datetime(...),
#   "zone": "B",
#   "consumption_kwh": 0.0234,
#   "voltage_v": 230.5,
#   "frequency_hz": 50.01,
#   "is_anomaly": False,
#   "anomaly_reason": None,
#   "hour_of_day": 12,
#   "is_peak_hour": True
# }
```

### Writing predictions

Use `role="spark_writer"` so the write uses the correct credentials:

```python
from storage.mongo_client import get_ml_predictions_collection
from datetime import datetime, timezone

preds = get_ml_predictions_collection(role="spark_writer")

preds.insert_many([
    {
        "zone": "B",
        "model_name": "xgb-v2.1",                               # required
        "prediction_time": datetime.now(timezone.utc),           # required — when the model ran
        "forecast_for": datetime(2026, 5, 18, 12, 15,            # required — what window it predicts
                                 tzinfo=timezone.utc),
        "consumption_forecast": 0.052,
        "anomaly_proba": 0.18,
        "alert_level": "INFO",     # INFO | WARNING | CRITICAL | NORMAL
        "ratio_to_peak": 0.62
    },
    # ... more predictions
])
```

**Required fields** (validator will warn if missing): `zone`, `model_name`, `prediction_time`, `forecast_for`.

**Important:** `forecast_for` must match the `window_start` values in `meters_aggregated_15min` for `get_prediction_vs_actual()` to join correctly. Use the same 15-minute floor rounding:

```python
from datetime import datetime

def floor_15min(dt: datetime) -> datetime:
    return dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)

forecast_for = floor_15min(target_datetime)
```

### Reading your own predictions back

```python
from storage.query_patterns import get_prediction_vs_actual

comparison = get_prediction_vs_actual(zone="A", model_name="xgb-v2.1", windows=20)

# Returns [{window, model, predicted, actual, error_pct}, ...]
# Use this to evaluate model performance or to feed the dashboard widget.
```

### Quality trend for feature validation

```python
from storage.query_patterns import get_quality_trend

trend = get_quality_trend(topic="smart-meters", metric="completeness_pct", hours=48)
# Returns: [{"window_start": datetime(...), "completeness_pct": 97.2}, ...]
# Useful to know whether your training data has gaps before fitting.
```

---

## 5. Field name reference — what P2 actually writes

The README describes some fields that P2 doesn't yet produce. The pre-built functions in `query_patterns.py` already account for all of these. This table is here so you don't get confused if you write your own raw queries.

| Collection | Field in README | What P2 actually writes | Impact |
|---|---|---|---|
| `incidents_enriched` | `computed_at` | **`ingested_at`** | TTL runs on `ingested_at`. Sort/filter on `timestamp` for the event time. |
| `incidents_enriched` | `correlated_anomalies`, `correlated_voltage_drops` | **Not written by P2** | `get_incident_meter_correlation()` calculates these live from `meters_raw`. |
| `incidents_enriched` | `nlp_sentiment` | **Not written by P2** | Absent. Use `nlp_keywords` for word cloud instead. |
| `data_quality_metrics` | `zone_balance_ratio` | **Not written by P2** | Absent. Don't include in queries or dataframes. |
| `feedback_nlp` | `sentiment` | **`sentiment_input`** (ground truth) + **`sentiment_predicted`** (NLP output) | Use `sentiment_predicted` for the dashboard chart. |
| `weather` | `temp_celsius` (README) | **`temperature_c`** | Use `temperature_c` in any raw query or join. |

> **Rule of thumb before writing a custom query:** run `db.<collection>.findOne()` in mongosh or Compass to see the real field names. The validators in P3 accept the actual P2 fields, not the README fields.

---

## 6. If something looks wrong

### Empty results from a collection you expect to have data

```bash
# Check counts directly in the container
docker exec <mongo> mongosh \
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin \
  --quiet energy_db \
  --eval "db.getCollectionNames().forEach(c => print(c.padEnd(30) + db[c].estimatedDocumentCount()))"
```

If a collection shows 0 but should have data, the issue is in P1/P2, not MongoDB.

### Inspect a real document before building a query

```bash
docker exec <mongo> mongosh \
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin \
  --quiet energy_db \
  --eval "printjson(db.<collection_name>.findOne())"
```

This is the fastest way to check actual field names and types.

### A query is slow or seems to scan everything

Run `.explain()` on it and check the `stage` field. It must say `IXSCAN`, not `COLLSCAN`:

```python
explain = db.meters_aggregated_15min.find(
    {"zone": "A", "window_start": {"$gte": start}}
).explain()

print(explain["queryPlanner"]["winningPlan"]["stage"])
# Should print: FETCH or IXSCAN — not COLLSCAN
```

If you see `COLLSCAN`, you're either querying on a field without an index, or the field order in your filter doesn't match the compound index. Ask P3.

### `get_prediction_vs_actual()` returns rows where `actual` is `None`

This means `forecast_for` in your prediction doesn't match any `window_start` in `meters_aggregated_15min`. Check that you're using the 15-minute floor rounding shown in Section 4.

### Connection errors

```bash
# Test connection from Python
python -m storage.mongo_client

# If that fails, check which container is running
docker ps --filter "name=mongodb" --format "table {{.Names}}\t{{.Status}}"
```

If the container is `mongodb-local`/`mongodb-pseudo`/`mongodb-dist` and is running, but Python can't connect, verify `MONGO_HOST=localhost` in your `.env` (not `mongodb`, which only works from inside Docker).

---

## Quick reference — timestamp fields by collection

| Collection | Sort/filter by | TTL field | Write role |
|---|---|---|---|
| `meters_raw` | `timestamp` | `ingested_at` | P2 (read-only for P4/P5) |
| `meters_aggregated_15min` | `window_start` | `window_start` | P2 (read-only for P4/P5) |
| `weather` | `timestamp` | `ingested_at` | P2 (read-only for P4/P5) |
| `incidents` | `timestamp` | `timestamp` | P2 (read-only for P4/P5) |
| `incidents_enriched` | `timestamp` | `ingested_at` ⚠️ | P2 (read-only for P4/P5) |
| `feedback_nlp` | `timestamp` | `computed_at` | P2 (read-only for P4/P5) |
| `data_quality_metrics` | `window_start` | `window_start` | P2 (read-only for P4/P5) |
| `ml_predictions` | `prediction_time` | `prediction_time` | **P5 writes here** |
| `dashboard_alerts` | `triggered_at` | `triggered_at` | **P4 writes here** |

⚠️ `incidents_enriched` TTL field is `ingested_at`, not `computed_at` (README is wrong here — P2 doesn't write `computed_at` to this collection).
