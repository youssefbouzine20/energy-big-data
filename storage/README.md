# P3 — MongoDB Storage  (FULL WORKFLOW + WHAT TO BUILD)

> Owner: P3 teammate.
> Reads from: nothing (you are the persistence layer).
> Writes to:  nothing (Spark writes; you provide the schema + access patterns).
> Used by:    P2 (writes aggregates/raw/NLP), P4 dashboard (reads aggregates),
>             P5 ML (reads `meters_raw` for training, writes `ml_predictions`).

---

## 0. The pipeline in one picture

```
   P2 Spark                     P3 MongoDB                    P4 Dashboard
  ──────────                  ─────────────                  ──────────────
   writeStream  ──── upsert ──► meters_aggregated_15min ─── pymongo.find ──► heatmap, KPIs
   writeStream  ──── append ──► meters_raw               ─── pymongo.find ──► P5 ML training
   writeStream  ──── append ──► incidents_enriched       ─── pymongo.find ──► word cloud
   writeStream  ──── append ──► feedback_nlp             ─── pymongo.find ──► sentiment chart
   writeStream  ──── append ──► data_quality_metrics     ─── pymongo.find ──► quality badge
                                       ▲
                                       │
                            P5 ML  ─── insert ─── ml_predictions ──► prediction-vs-actual chart
```

**Your job in one sentence:** define the MongoDB collections, indexes, TTL
retention policies, service accounts, and the shared Python helper module
that P4 and P5 use to read/write — then explain in the defense WHY MongoDB
and HOW it satisfies high availability + GDPR.

---

## 1. What is already done by P1 (don't redo it)

P1 created `storage/init/init-mongo.js` which is **already mounted** into the
mongodb container in all 3 compose files at
`/docker-entrypoint-initdb.d/init-mongo.js`. On the very first MongoDB
startup (when `/data/db` is empty), Mongo runs every `*.js` in that
directory exactly once. The script creates:

- **5 collections:** `meters_raw`, `meters_aggregated_15min`, `weather`, `incidents`, `ml_predictions`
- **1 service account:** `spark_writer` (readWrite on `energy_db`) — Spark uses this
- **Query indexes:** `(meter_id, timestamp)`, `(zone, timestamp)`, etc.
- **TTL indexes:** 90 d raw / 1 y aggregated / 1 y weather / 2 y incidents / 6 mo predictions

To re-run the init script (after editing it):

```bash
docker compose -f docker/docker-compose.local.yml down -v   # destroys the volume
docker compose -f docker/docker-compose.local.yml up -d     # init script runs again
```

**You will extend this** with extra collections (`incidents_enriched`,
`feedback_nlp`, `data_quality_metrics`, `dashboard_alerts`) once P2 starts
writing them. See §4.

---

## 2. Why MongoDB? — REQUIRED defense answer (Section C of the spec)

The professor explicitly grades on "**pourquoi telle base NoSQL ?**". Memorize this table.

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **MongoDB** | Native JSON (matches Kafka message shape 1:1), mature Spark connector, flexible schema for evolving features, replica sets for HA, TTL indexes for GDPR retention, secondary indexes for dashboard queries | No native SQL joins, eventual consistency under partition | ✅ **Chosen** |
| HBase | Massive scale, strong consistency, Hadoop ecosystem | Heavy ops overhead, column-family model awkward for nested JSON, no native time-series | ❌ Overkill for project scale; HDFS dependency adds complexity |
| Cassandra | Linear write scalability, multi-datacenter, no SPOF | Restrictive query model (must know partition key in advance), no ad-hoc dashboard queries | ❌ Wrong access pattern — dashboard needs flexible queries |
| Neo4j | Graph queries (zone connectivity, incident propagation) | Not for high-volume time-series ingestion | ❌ Wrong workload |
| ElasticSearch | Full-text search on incident descriptions | Not a primary store, expensive at scale, no transactions | ➜ Could complement MongoDB for NLP search; not chosen for primary |

### Why MongoDB matches the 5 V's of Big Data here

| V | Project value | How MongoDB handles it |
|---|---|---|
| **Volume** | ~30 k msg/day, growing | Sharding-ready; single node handles 10k+ inserts/s already |
| **Velocity** | Real-time streams from Spark | mongo-spark-connector batches inserts efficiently; ack=majority for durability |
| **Variety** | 6 different topic schemas + derived collections | Schema-flexible documents; per-collection validators if needed |
| **Veracity** | Data quality varies (sensor noise) | MongoDB schema validation (`db.createCollection(.., validator)`) catches obvious junk |
| **Value** | Long-term ML training + dashboard insights | Compound indexes power dashboard queries in <50ms |

---

## 3. High Availability — REQUIRED defense answer (Section C)

For the **Rapport** and the defense, explain how the production deployment
would achieve "**haute disponibilité**":

### 3-member replica set (production)

```
                 ┌─────────────┐
                 │  PRIMARY    │ ◄── all writes go here
                 │  mongo-1    │
                 └──────┬──────┘
                  oplog │ replication
                ┌───────┴───────┐
                ▼               ▼
        ┌─────────────┐ ┌─────────────┐
        │  SECONDARY  │ │  SECONDARY  │ ◄── readPreference=secondaryPreferred for dashboard
        │  mongo-2    │ │  mongo-3    │
        └─────────────┘ └─────────────┘
```

- **Failover:** if PRIMARY becomes unreachable, the two SECONDARIES hold a
  Raft-like election and one of them becomes the new PRIMARY. Typical
  failover time: 10-12 seconds.
- **Read scaling:** the P4 dashboard can read from secondaries with
  `readPreference=secondaryPreferred` to avoid loading the primary.
- **Durability:** Spark writes with `w=majority` so a write is ack'd only
  after at least 2 of 3 members have it on disk.
- **Point-in-time recovery:** the oplog (operations log) is a replayable
  history of every write — restore to any moment in the last N hours.

For our **demo we run single-node MongoDB** (matches the 3 compose modes).
Document this in the Rapport with the production deployment plan above.

---

## 4. Collection design (the data contract for the whole team)

The init script already creates collections 1-5. **You add the rest** as
P2/P4/P5 start writing (extend `init-mongo.js`, then `down -v && up -d` once).

| # | Collection | Source | Purpose | TTL | Already in init script? |
|---|---|---|---|---|---|
| 1 | `meters_raw` | Spark passthrough of `smart-meters` | Audit + ML training | 90 d | ✅ |
| 2 | `meters_aggregated_15min` | Spark windowed aggregate | Dashboard + ML features | 365 d | ✅ |
| 3 | `weather` | Spark passthrough of `weather` | Weather correlation | 365 d | ✅ |
| 4 | `incidents` | Spark passthrough of `incident-reports` | History | 730 d | ✅ |
| 5 | `ml_predictions` | P5 ML model output | Dashboard prediction-vs-actual | 180 d | ✅ |
| 6 | `incidents_enriched` | Spark NLP output | Word cloud + correlation | 730 d | ⏳ Add when P2 starts NLP |
| 7 | `feedback_nlp` | Spark NLP on user-feedback | Sentiment analysis chart | 365 d | ⏳ Add when P2 starts feedback NLP |
| 8 | `rss_feeds` | Spark passthrough of `rss-feeds` | News-to-load correlation | 365 d | ⏳ Add when needed |
| 9 | `market_prices` | Spark passthrough of `market-prices` | Price ML feature | 365 d | ⏳ Add when needed |
| 10 | `data_quality_metrics` | Spark per-window quality | Section E reporting | 365 d | ⏳ Add when P2 ships |
| 11 | `dashboard_alerts` | P4 alert-trigger log | Section H ethics audit | 365 d | ⏳ Add when P4 ships |

Retention durations match `ethics/GDPR.md` Section 4. **Always** use MongoDB
TTL indexes — never write a cron job that deletes manually. TTL is atomic,
auditable, and survives backups.

---

## 5. Document schemas — exact contract with P2

Each section is what Spark writes. P3's job is to verify these shapes match
the index strategy.

### 5.1 `meters_raw`  (Spark append)

```json
{
  "_id":          "<auto>",
  "meter_id":     "SM-007",
  "timestamp":    ISODate("2026-05-16T18:00:30Z"),
  "zone":         "B",
  "consumption_kwh": 0.0234, "voltage_v": 230.5, "frequency_hz": 50.01,
  "power_factor": 0.95, "is_anomaly": false, "anomaly_reason": null,
  "hour_of_day":  18, "day_of_week": 5, "is_peak_hour": true,
  "zone_lat":     35.575, "zone_lon": -5.370,
  "ingested_at":  ISODate("2026-05-16T18:00:31.123Z")
}
```

### 5.2 `meters_aggregated_15min`  (Spark upsert on `(zone, window_start)`)

```json
{
  "_id":          "<auto>",
  "zone":         "B",
  "window_start": ISODate("2026-05-16T18:00:00Z"),
  "window_end":   ISODate("2026-05-16T18:15:00Z"),
  "avg_consumption":   0.0231,
  "max_consumption":   0.045,
  "min_consumption":   0.012,
  "total_consumption_kwh": 25.4,
  "anomaly_count":     3,
  "anomaly_rate_pct":  7.5,
  "voltage_min":       214.5, "voltage_max": 245.2, "voltage_avg": 230.1,
  "frequency_stddev":  0.04,
  "meter_count":       5,
  "weather_temperature_c": 27.5,
  "weather_severity":      "NORMAL",
  "active_incidents":      1,
  "zone_lat":     35.575, "zone_lon": -5.370,
  "computed_at":  ISODate("2026-05-16T18:15:02.456Z")
}
```

### 5.3 `incidents_enriched`  (Spark append)

```json
{
  "_id":             "<auto>",
  "incident_id":     "INC-20260516-001",
  "zone":            "B", "timestamp": ISODate("..."),
  "severity":        "HIGH", "type": "POWER_OUTAGE",
  "description":     "...",
  "nlp_keywords":    ["voltage","outage","transformer","crew"],
  "correlated_anomalies":      12,
  "correlated_voltage_drops":  8,
  "computed_at":     ISODate("...")
}
```

### 5.4 `ml_predictions`  (P5 insert/upsert on `(zone, forecast_for)`)

```json
{
  "_id":             "<auto>",
  "zone":            "B",
  "model_name":      "RandomForest",
  "prediction_time": ISODate("2026-05-16T18:00:00Z"),    // when forecast was made
  "forecast_for":    ISODate("2026-05-16T18:15:00Z"),    // window the forecast targets
  "consumption_forecast": 0.038,                          // kWh per meter avg
  "anomaly_proba":   0.23,                                // probability of anomaly
  "alert_level":     "INFO",                              // INFO | WARNING | CRITICAL | NORMAL
  "ratio_to_peak":   0.72                                 // forecast / historical zone peak
}
```

### 5.5 `data_quality_metrics`  (Spark insert per topic per window)

See P2 README §2.7 for the exact shape.

---

## 6. Indexes — what the init script creates (and why)

Compound key order matters: index is most useful when the leftmost field is
the one you filter exactly on.

```javascript
// Already in init-mongo.js:
db.meters_raw.createIndex({ meter_id: 1, timestamp: -1 });
db.meters_raw.createIndex({ zone:     1, timestamp: -1 });

db.meters_aggregated_15min.createIndex({ zone:        1, window_start: -1 });
db.meters_aggregated_15min.createIndex({ window_start: 1 });

db.weather.createIndex({ timestamp: -1 });
db.weather.createIndex({ weather_severity: 1, timestamp: -1 });

db.incidents.createIndex({ zone: 1,     timestamp: -1 });
db.incidents.createIndex({ severity: 1, timestamp: -1 });
db.incidents.createIndex({ resolved: 1 });

db.ml_predictions.createIndex({ model_name: 1, prediction_time: -1 });
db.ml_predictions.createIndex({ zone: 1, prediction_time: -1 });
```

You will add (when those collections exist):

```javascript
db.incidents_enriched.createIndex({ zone: 1, timestamp: -1 });
db.incidents_enriched.createIndex({ "nlp_keywords": 1 });        // for word-cloud term lookups

db.feedback_nlp.createIndex({ zone: 1, timestamp: -1 });
db.feedback_nlp.createIndex({ sentiment_predicted: 1, timestamp: -1 });

db.data_quality_metrics.createIndex({ topic: 1, window_start: -1 });

db.dashboard_alerts.createIndex({ alert_level: 1, triggered_at: -1 });
```

### Why these specific indexes?

- **Dashboard always filters by zone first**, then time range → `(zone, timestamp)` order
- **Word cloud queries by keyword** → `nlp_keywords` index makes a 2-second query into 50ms
- **Active-incidents banner** filters `resolved: false` → `resolved` index
- **Quality badge** picks the latest per topic → `(topic, window_start)` order

---

## 7. What you must build — explicit task list

| # | File | Purpose | Done when |
|---|---|---|---|
| 1 | `storage/__init__.py` | Empty marker | File exists |
| 2 | `storage/mongo_client.py` | Shared `get_client()` / `get_db()` for P4 & P5 | `from storage.mongo_client import get_db` works in any module |
| 3 | `storage/extend_init.py` | Idempotent script to add collections 6-11 + their indexes | Re-running is a no-op; collections appear in mongosh |
| 4 | `storage/replica_setup.md` | Markdown explaining how to convert single-node → 3-member replica set | Includes commands + diagram for the Rapport |
| 5 | `storage/healthcheck.py` | Optional: Python script to print collection sizes, index stats, TTL countdown | `python -m storage.healthcheck` prints a clean table |
| 6 | `storage/README.md` (this file) | Already written | — |

---

## 8. Starter snippet — `mongo_client.py`

```python
"""Shared MongoDB client used by P4 dashboard and P5 ML modules."""
import os
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def get_client(role: str = "reader") -> MongoClient:
    """
    role:
      "reader"      -> uses root admin creds for read-heavy P4/P5 work (local dev)
      "spark_writer" -> uses the limited writer service account (Spark only)
    """
    if role == "spark_writer":
        user = os.getenv("MONGO_SPARK_USER", "spark_writer")
        pwd  = os.getenv("MONGO_SPARK_PASS", "change-me-before-deploy")
    else:
        user = os.getenv("MONGO_USERNAME", "energy_admin")
        pwd  = os.getenv("MONGO_PASSWORD", "change-me-before-deploy")

    return MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", 27017)),
        username=user,
        password=pwd,
        authSource="admin",
        # readPreference="secondaryPreferred",   # enable when running a replica set
    )


def get_db(role: str = "reader"):
    return get_client(role)[os.getenv("MONGO_DB_NAME", "energy_db")]


if __name__ == "__main__":
    db = get_db()
    print("Collections:", db.list_collection_names())
    print("meters_raw count:", db.meters_raw.estimated_document_count())
```

---

## 9. Required justifications for the defense

1. **Why MongoDB over HBase / Cassandra / Neo4j / ElasticSearch?** — use the table in §2
2. **Why these specific indexes?** — compound key order matters; dashboard always filters by zone first
3. **Why TTL indexes for retention vs application-level deletion?** — atomicity, no race with backups, GDPR auditability
4. **How does the system maintain HA under network partition?** — replica set quorum, Raft consensus, eventual consistency window of ~5-10s
5. **Why no normalization (denormalized documents)?** — Read-heavy workload + Spark joins handle relations + simpler GDPR erasure (delete one document = delete one user's record)
6. **Sharding plan for production scale?** — shard key `(zone, timestamp)` would distribute load while keeping zone-local queries on a single shard

---

## 10. Run + verify

### 🔖 The MongoDB container name depends on the mode you're running

Use this table to pick the right container name before any `docker exec` command:

| Mode | Compose file | MongoDB container | How to switch |
|---|---|---|---|
| **Local** | `docker-compose.local.yml` | `mongodb-local` | `docker compose -f docker/docker-compose.local.yml --env-file .env up -d` |
| **Pseudo-distributed** | `docker-compose.pseudo.yml` | `mongodb-pseudo` | `docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d` |
| **Fully distributed** | `docker-compose.distributed.yml` | `mongodb-dist` | `docker compose -f docker/docker-compose.distributed.yml --env-file .env up -d` |

Check what's running right now (any mode):

```powershell
docker ps --filter "name=mongodb" --format "table {{.Names}}\t{{.Status}}"
```

You should see exactly **one** of `mongodb-local` / `mongodb-pseudo` / `mongodb-dist`.
Throughout the rest of this section, substitute the matching name for `<mongodb-container>`.

### One-time: extend the init script (after P2 ships NLP)

```powershell
# After editing storage/init/init-mongo.js to add new collections + indexes.
# Pick the compose file matching your current mode:
docker compose -f docker/docker-compose.local.yml down -v
docker compose -f docker/docker-compose.local.yml --env-file ..\.env up -d
```

> ⚠️ `down -v` **destroys** Mongo data (and Kafka logs) in pseudo + distributed
> modes. Only do this if you actually need to re-run the init script. Local
> mode has no volume so `down -v` and `down` are equivalent there.

### Sanity-check the database from the host

```powershell
# Replace <mongodb-container> with mongodb-local / mongodb-pseudo / mongodb-dist
docker exec <mongodb-container> mongosh `
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin `
  --eval "db.getSiblingDB('energy_db').runCommand({listCollections: 1, nameOnly: true})"

# Show indexes for a specific collection
docker exec <mongodb-container> mongosh `
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin `
  --eval "db.getSiblingDB('energy_db').meters_aggregated_15min.getIndexes()"
```

Linux / WSL / Mac users replace the PowerShell backticks with bash backslashes.

### Live collection counts (every mode — same command pattern)

```powershell
docker exec <mongodb-container> mongosh `
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin `
  --quiet energy_db `
  --eval "db.getCollectionNames().forEach(c => print(c.padEnd(28) + ': ' + db[c].countDocuments() + ' docs'))"
```

Expected (with all producers + Spark running for >15 min): 11 collections.

### From Python (host-side, after `pip install -r requirements.txt`)

```powershell
.venv\Scripts\python -m storage.mongo_client
# Should print: Collections: [...], meters_raw count: N
```

`storage/mongo_client.py` connects via `localhost:27017` (read from `.env`).
This works regardless of which mode is running because all 3 modes publish
Mongo on the same host port (`MONGO_PORT=27017`).

### From a GUI

**MongoDB Compass** (free GUI, recommended for P3):
1. Download from https://www.mongodb.com/products/compass
2. Connect using URI: `mongodb://energy_admin:change-me-before-deploy@localhost:27017/?authSource=admin`
3. Browse `energy_db` → all 11 collections + their indexes
4. The URI is the same for all 3 modes (host port is always `27017`).

> Kafka UI (port 8090) doesn't browse Mongo — it's only for Kafka topics + schemas.

---

## 11. Common pitfalls

1. **`Authentication failed`** → verify `MONGO_USERNAME` and `MONGO_PASSWORD` in `.env` match what was set when the volume was first created. If you change credentials in `.env` after the volume exists, mongo keeps the old creds. Fix: `down -v` and `up -d`.
2. **Init script didn't run** → only runs when `/data/db` is empty. After any edit, `down -v` first.
3. **TTL not deleting documents** → TTL background thread runs ~every 60 s; document deletion can lag by minutes. Confirm with `db.serverStatus().metrics.ttl`.
4. **Index not used by query** → `db.collection.find({...}).explain()` and check `winningPlan.stage`. If it says `COLLSCAN`, the index isn't being used.
5. **Replica set won't start** → KAFKA_CLUSTER_ID equivalent for Mongo is the replica set name; never change it after first init.
6. **Compass can connect locally but Spark can't** → Spark runs in Docker network and must use `mongodb:27017`, not `localhost:27017`.

---

## 12. Data quality contribution (Section E)

P3 doesn't compute quality metrics (P2 does), but P3 **enforces** them at write
time using MongoDB schema validators:

```javascript
// Add to init-mongo.js to reject malformed docs
db.runCommand({
    collMod: "meters_raw",
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["meter_id", "timestamp", "zone", "consumption_kwh"],
            properties: {
                consumption_kwh: { bsonType: "double", minimum: 0, maximum: 1.0 },
                voltage_v:       { bsonType: "double", minimum: 200, maximum: 260 },
                zone:            { enum: ["A","B","C","D"] }
            }
        }
    },
    validationLevel: "strict",
    validationAction: "warn"   // log to mongod.log; don't reject (let Spark handle retries)
});
```

This catches "impossible" data that slips through (e.g., a Spark UDF bug),
without breaking the streaming pipeline.

---

## 13. Dependencies (already in root `requirements.txt`)

`pymongo==4.7.3`, `python-dotenv==1.2.2`. No new dependencies needed.
