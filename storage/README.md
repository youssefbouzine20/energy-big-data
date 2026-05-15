# P3 — MongoDB Storage

This folder is for MongoDB collection design, indexes, retention policies, and
the Python helper module other parts of the project import.

## Purpose in the pipeline

```
Spark (P2) ──► MongoDB (P3 — this folder) ──► Streamlit (P4) + ML (P5)
```

## Collections to create

| Collection | Source | Purpose | TTL |
|---|---|---|---|
| `meters_raw` | Spark `smart-meters` passthrough | Raw meter readings for audit | 90 days |
| `meters_aggregated_15min` | Spark windowed output | Dashboard + ML training | 1 year |
| `weather` | Spark `weather` passthrough | Weather correlation features | 1 year |
| `incidents` | Spark `incident-reports` passthrough | Incident history | 2 years |
| `incidents_enriched` | Spark NLP output | Keywords + sentiment | 2 years |
| `ml_predictions` | P5 model output | Prediction audit trail | 6 months |

Retention durations match `ethics/GDPR.md` Section 4. **Always** use MongoDB TTL indexes to enforce them automatically (do not delete manually).

## Indexes required

```python
# meters_raw and meters_aggregated_15min
db.meters_raw.create_index([("meter_id", 1), ("timestamp", -1)])
db.meters_raw.create_index([("zone", 1), ("timestamp", -1)])
db.meters_raw.create_index("timestamp", expireAfterSeconds=90*86400)  # TTL

# weather
db.weather.create_index("timestamp", expireAfterSeconds=365*86400)

# incidents
db.incidents.create_index([("zone", 1), ("timestamp", -1)])
db.incidents.create_index([("severity", 1), ("resolved", 1)])
db.incidents.create_index("timestamp", expireAfterSeconds=2*365*86400)
```

P4 (dashboard) will query by `(zone, timestamp)` for time-series and by `(severity, resolved)` for active-incident counts. Without these indexes, dashboard latency will be 100x worse.

## Required env vars

Read from project-root `.env`:
- `MONGO_HOST`, `MONGO_PORT`
- `MONGO_USERNAME`, `MONGO_PASSWORD`
- `MONGO_DB_NAME`

## What to build

1. **`storage/init_db.py`** — idempotent script that creates all collections, indexes, and TTL policies. Run once after `docker compose up`.
2. **`storage/mongo_client.py`** — shared module that other parts (P4, P5) import to get an authenticated `MongoClient` instance.

## Starter snippet

```python
# storage/mongo_client.py
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

def get_client() -> MongoClient:
    return MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", 27017)),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        authSource="admin",
    )

def get_db():
    return get_client()[os.getenv("MONGO_DB_NAME", "energy_db")]
```

```python
# storage/init_db.py — run once
from storage.mongo_client import get_db

db = get_db()
db.meters_raw.create_index([("meter_id", 1), ("timestamp", -1)])
db.meters_raw.create_index("timestamp", expireAfterSeconds=90*86400)
# ... (all indexes from the table above)
print("MongoDB initialized.")
```

## Document schemas

Mirror the Kafka schemas in `ingestion/schemas/` 1:1 — same field names, same types. Do not flatten or rename. This keeps Spark → MongoDB writes trivial and the data dictionary consistent.

## Run

```bash
.venv/bin/python -m storage.init_db
```

## Verification

```bash
# List collections
docker exec mongodb-local mongosh -u admin -p $MONGO_PASSWORD --eval "use energy_db; db.getCollectionNames()"

# Confirm TTL indexes
docker exec mongodb-local mongosh -u admin -p $MONGO_PASSWORD --eval "use energy_db; db.meters_raw.getIndexes()"
```
