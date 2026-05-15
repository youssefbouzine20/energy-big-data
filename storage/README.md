# P3 — MongoDB Storage

MongoDB collection design, indices, retention policies, and the shared Python
helper module that P2 / P4 / P5 import.

## Purpose in the pipeline

```
Spark (P2) ──► MongoDB (P3 — this folder) ──► Streamlit (P4) + ML (P5)
```

## Why MongoDB? (REQUIRED justification — professor's Section C)

The professor explicitly grades on "pourquoi telle base NoSQL?". Be ready to defend the choice:

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **MongoDB** | Native JSON (matches Kafka message shape 1:1), mature Spark connector, flexible schema for evolving features, HA via replica sets, TTL indexes for GDPR retention | No native SQL joins, eventual consistency under partition | ✅ **Chosen** |
| HBase | Massive scale, strong consistency, Hadoop ecosystem | Heavy ops overhead, column-family model awkward for nested JSON, no native time-series | ❌ Overkill for project scale |
| Cassandra | Linear write scalability, multi-datacenter | Restrictive query model (must know key in advance), eventual consistency | ❌ Wrong query pattern for dashboard |
| Neo4j | Graph queries (zone connectivity, incident propagation) | Not for high-volume time-series ingestion | ❌ Wrong workload |
| ElasticSearch | Full-text search on incident descriptions | Not a primary store, expensive at scale, no transactions | ➜ Could complement MongoDB for NLP search |

## Velocity & Variety (professor's framing in Section C)

- **Velocity:** producers emit ~20 msg/min on `smart-meters` + 2 msg/min weather/incidents = ~30k msg/day. MongoDB handles 10k+ inserts/sec on single node. Headroom is large.
- **Variety:** 4 different document schemas (meters_raw, weather, incidents, ml_predictions). Flexible-schema NoSQL is the natural fit.

## High Availability (professor's Section C: "haute disponibilité")

Production setup uses **replica set** (3 members: 1 primary + 2 secondaries):
- Automatic failover via Raft-like protocol when primary becomes unreachable
- Read scaling: P4 dashboard can read from secondaries with `readPreference=secondaryPreferred`
- Oplog-based replication enables point-in-time recovery
- Aligns with project's distributed mode (Section G — 3 physical nodes for Kafka + 3 for Mongo)

For the demo, single-node MongoDB is acceptable (matches local + pseudo modes) — note this in the Rapport with the deployment plan for production.

## Collections

| Collection | Source | Purpose | Retention (TTL) |
|---|---|---|---|
| `meters_raw` | Spark passthrough of `smart-meters` | Audit + ML training | 90 days |
| `meters_aggregated_15min` | Spark windowed | Dashboard + ML features | 1 year |
| `weather` | Spark passthrough of `weather` | Weather correlation | 1 year |
| `incidents` | Spark passthrough of `incident-reports` | History | 2 years |
| `incidents_enriched` | Spark NLP output | Word cloud + correlation | 2 years |
| `feedback_nlp` | Spark NLP on user feedback | Sentiment, dashboard | 1 year |
| `data_quality_metrics` | Spark quality job | Defense Rapport | 1 year |
| `ml_predictions` | P5 model output | Dashboard prediction-vs-actual curves | 6 months |
| `dashboard_alerts` | P4 saturation alerts log | Audit + Rapport | 1 year |

Retention durations match [ethics/GDPR.md](../ethics/GDPR.md) Section 4. **Always** use MongoDB TTL indexes — never delete manually.

## Indices required

```python
# meters_raw
db.meters_raw.create_index([("meter_id", 1), ("timestamp", -1)])
db.meters_raw.create_index([("zone", 1), ("timestamp", -1)])
db.meters_raw.create_index([("is_anomaly", 1), ("timestamp", -1)])  # for ML queries
db.meters_raw.create_index("timestamp", expireAfterSeconds=90*86400)  # TTL

# aggregated
db.meters_aggregated_15min.create_index([("zone", 1), ("window_start", -1)])
db.meters_aggregated_15min.create_index("window_start", expireAfterSeconds=365*86400)

# incidents
db.incidents.create_index([("zone", 1), ("timestamp", -1)])
db.incidents.create_index([("severity", 1), ("resolved", 1)])
db.incidents.create_index("timestamp", expireAfterSeconds=2*365*86400)

# predictions (used by dashboard for prediction-vs-actual chart)
db.ml_predictions.create_index([("zone", 1), ("forecast_for", 1)], unique=True)
db.ml_predictions.create_index("forecast_for", expireAfterSeconds=180*86400)
```

P4 dashboard will repeatedly query `(zone, timestamp)` for time-series and `(severity, resolved)` for active incidents. Without these indexes, dashboard latency is 100× worse.

## Required env vars

`MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DB_NAME`

## What to build

1. **`storage/init_db.py`** — idempotent script: create all collections, indexes, TTL policies. Run once after `docker compose up`.
2. **`storage/mongo_client.py`** — shared module that P4 / P5 import for an authenticated `MongoClient`.
3. **`storage/replica_setup.md`** (optional, for Rapport) — explains how to convert single-node to 3-member replica set for HA.

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

## Required justifications for the defense

1. Why MongoDB over HBase/Cassandra/Neo4j/ElasticSearch? (use the table above)
2. Why these specific indexes? (Compound key order matters: `(zone, timestamp)` not `(timestamp, zone)` because dashboard always filters by zone first)
3. Why TTL indexes for retention vs application-level deletion? (Atomicity, no race with backups, GDPR auditability)
4. How does the system maintain HA under network partition? (Replica set quorum, Raft consensus, eventual consistency window)
5. Document schemas mirror Kafka schemas 1:1 — why no normalization? (Read-heavy workload + Spark joins handle relations + simpler GDPR erasure)

## Data Quality contribution (Section E)

P3 doesn't compute quality metrics (that's P2) but enforces them at write time:
- Reject malformed documents at the Mongo schema validation layer (`db.createCollection(..., validator={...})`)
- Log rejections to `data_quality_metrics` collection for the Rapport

## Run

```bash
.venv/bin/python -m storage.init_db
```

## Verification

```bash
docker exec mongodb-local mongosh -u admin -p $MONGO_PASSWORD --eval \
  "use energy_db; db.getCollectionNames(); db.meters_raw.getIndexes()"
```
