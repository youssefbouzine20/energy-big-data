# energy-big-data — Real-Time Predictive Energy Load Analysis

> University capstone project — **M126 Big Data**, ENSA Tétouan, Pr. Imad Sassi.
> Team of 5. Defense **2026-05-21**.

A complete real-time data pipeline that simulates a city electrical grid
(Tétouan), ingests 6 streams into Kafka, processes them with Spark, stores
results in MongoDB, and exposes a Streamlit dashboard with ML-based
predictive alerts.

---

## 📐 Architecture (full pipeline)

```
   P1 — INGESTION                       P2 — PROCESSING            P3 — STORAGE
  ────────────────                     ─────────────────          ──────────────
   smart-meters     ─┐                  Spark Structured            MongoDB 7.0
   weather          ─┤                  Streaming                  ──────────────
   incident-reports ─┼─► Kafka  ───►   • 15-min windows  ───►       meters_raw
   rss-feeds        ─┤   (KRaft)        • zone aggregation          meters_aggregated_15min
   market-prices    ─┤   6 topics       • broadcast-join weather    incidents_enriched
   user-feedback    ─┘                  • NLP on incidents +        feedback_nlp
                                          user feedback             ml_predictions
                          ▲              • data-quality metrics     data_quality_metrics
                          │                                                │
                ┌─────────┴────────────┐                                   │
                │ Schema Registry      │                                   │
                │ Kafka UI (Provectus) │                       ┌───────────┴──────────┐
                └──────────────────────┘                       ▼                      ▼
                                                       P4 — DASHBOARD          P5 — ML
                                                      ──────────────         ──────────
                                                       Streamlit             3 models
                                                       • heatmap             • LogisticReg
                                                       • word cloud          • RandomForest
                                                       • prediction          • GradientBoosting
                                                         vs actual           Tasks:
                                                       • predictive          • anomaly classif.
                                                         alerts              • consumption forec.
```

Detailed workflow diagrams, schemas, and code starters live in each
teammate's folder README — see [§3 Team & Navigation](#3-team--navigation).

---

## 1. 🧰 Stack

| Component | Version | Purpose |
|---|---|---|
| Python | 3.10.11 | Producers, consumers, Spark jobs, dashboard, ML |
| Confluent Kafka | 7.5.0 | Message broker, KRaft mode (no ZooKeeper) |
| Apache Spark | 3.5.0 | Structured Streaming (15-min windows, NLP) |
| MongoDB | 7.0 | NoSQL sink, TTL retention enforced by indexes |
| Streamlit | 1.35.0 | Real-time dashboard (port 8501) |
| Confluent Schema Registry | 7.5.0 | Central JSON-schema catalog (port 8081) |
| Kafka UI (Provectus) | latest | Web UI for topics + schemas (port 8090) |
| Docker | Desktop | All services containerised |

All Python deps are pinned in `requirements.txt`. No global installs needed beyond Docker + Python 3.10.

---

## 2. ✅ Prerequisites (do once per machine)

| Requirement | Check |
|---|---|
| Docker Desktop running | `docker info` — must succeed |
| Python 3.10.11 (3.10.x is fine) | `python --version` |
| Git | `git --version` |
| 4 GB free RAM (8 GB for distributed mode) | — |
| 10 GB free disk (Docker images + Mongo data) | — |
| Internet on first run | for Docker image pulls + Spark connector JARs |

> **Editor:** anything that supports Python; we recommend VS Code with the
> Python extension. After opening the repo, **set the Python interpreter to
> `.venv` you create below** so imports resolve correctly.

---

## 3. 👥 Team & Navigation

Every teammate has a **dedicated folder** with a deeply detailed README that
covers: data contracts (inputs/outputs), workflow steps, what to build,
starter code, defense answers, and common pitfalls. Read your folder's
README first; come back here only for cross-cutting setup.

| Role | Owner | Folder | Detailed README | What this part does |
|---|---|---|---|---|
| **P1** | Infrastructure & Ingestion | `ingestion/` + `docker/` | (this README + `ingestion/schemas/*.json`) | Kafka cluster, 6 producers, JSON schemas, Schema Registry, verification consumer |
| **P2** | Spark Structured Streaming | `processing/` | [`processing/README.md`](processing/README.md) | Read 6 Kafka topics, 15-min windowed aggregation, NLP, write to Mongo |
| **P3** | MongoDB Storage | `storage/` | [`storage/README.md`](storage/README.md) | Collection design, indexes, TTL/GDPR, replica-set HA plan |
| **P4** | Streamlit Dashboard | `dashboard/` | [`dashboard/README.md`](dashboard/README.md) | All 4 sections required by spec F: heatmap, prediction-vs-actual, word cloud, predictive alerts |
| **P5** | ML Models | `ml/` | [`ml/README.md`](ml/README.md) | Compare 3 algorithms on 2 tasks (anomaly + forecast) using 4 required metrics |

Cross-cutting docs:
- [`ethics/GDPR.md`](ethics/GDPR.md) — GDPR compliance (legal basis, retention, rights)
- [`docs/`](docs/) — defense slides, methodology Rapport draft (when ready)

---

## 4. 🚀 First 5 minutes — clone-to-running pipeline

The fastest way to see the whole thing alive. Use **local mode** (no
persistence — perfect for dev). All commands assume you are in the project
root.

### Step 1 — Clone and create the venv

**Linux / WSL (recommended — matches the professor's environment):**
```bash
git clone https://github.com/youssefbouzine20/energy-big-data.git
cd energy-big-data
python3.10 -m venv .venv
source .venv/bin/activate
pip install --default-timeout=100 -r requirements.txt
```

**Windows (PowerShell):**
```powershell
git clone https://github.com/youssefbouzine20/energy-big-data.git
cd energy-big-data
python -m venv .venv
.venv\Scripts\Activate.ps1
.venv\Scripts\pip install --default-timeout=100 -r requirements.txt
```

### Step 2 — Configure `.env` (one-time per clone)

```bash
cp .env.example .env                                              # Linux/WSL
copy .env.example .env                                            # Windows

# Generate a Kafka cluster UUID and paste it into .env → KAFKA_CLUSTER_ID
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
```

Paste the UUID after `KAFKA_CLUSTER_ID=` in `.env`. **Don't** change it later
— Kafka refuses to start if you do (it would corrupt the volume).

> ⚠️ **Security:** the `.env.example` ships `MONGO_PASSWORD=change-me-before-deploy`.
> For local dev that's fine, but **rotate it** before any non-local
> deployment. The MongoDB image starts WITHOUT authentication if these
> values are blank.

### Step 3 — Start the infrastructure (local mode)

```bash
docker compose -f docker/docker-compose.local.yml --env-file .env up -d
```

This brings up: Kafka + Schema Registry + Kafka UI + MongoDB + Spark master + Spark worker.
First run pulls ~2 GB of images.

Wait ~30 seconds for all services to be healthy:
```bash
docker compose -f docker/docker-compose.local.yml ps
# All entries should show "Up (healthy)" or "Up"
```

### Step 4 — Register schemas with Schema Registry (one-time per cluster)

```bash
# Linux/WSL
python -m ingestion.schemas.register_schemas

# Windows
.venv\Scripts\python -m ingestion.schemas.register_schemas
```

Expected output:
```
[OK]    smart-meters-value             id=1
[OK]    weather-value                  id=2
[OK]    incident-reports-value         id=3
[OK]    rss-feeds-value                id=4
[OK]    market-prices-value            id=5
[OK]    user-feedback-value            id=6
```

### Step 5 — Start the 6 producers (6 terminals)

Open **6 terminals** in the project root. In each, activate the venv and
run one producer.

**Linux / WSL:**
```bash
# In each terminal, first run:
source .venv/bin/activate

# Then in each terminal pick one:
python -m ingestion.producers.weather_producer        # terminal 1 — start FIRST
python -m ingestion.producers.smart_meter_producer    # terminal 2
python -m ingestion.producers.incident_producer       # terminal 3
python -m ingestion.producers.rss_producer            # terminal 4
python -m ingestion.producers.market_price_producer   # terminal 5
python -m ingestion.producers.user_feedback_producer  # terminal 6
```

**Windows (PowerShell):**
```powershell
.venv\Scripts\python -m ingestion.producers.weather_producer        # terminal 1
.venv\Scripts\python -m ingestion.producers.smart_meter_producer    # terminal 2
.venv\Scripts\python -m ingestion.producers.incident_producer       # terminal 3
.venv\Scripts\python -m ingestion.producers.rss_producer            # terminal 4
.venv\Scripts\python -m ingestion.producers.market_price_producer   # terminal 5
.venv\Scripts\python -m ingestion.producers.user_feedback_producer  # terminal 6
```

> **Why start `weather_producer` first?** It writes the shared-state file
> that `smart_meter_producer` and `incident_producer` read for
> weather-coupled simulation. They will use the default `NORMAL` weather if
> the file is missing, which is harmless but less realistic.

You should see lines like:
```
[INFO] Smart meter producer — 20 meters, interval=30s
  → SM-001 | 0.0234 kWh | freq=50.01Hz | anomaly=None
  → SM-002 | 0.0185 kWh | freq=50.01Hz | anomaly=None
  ...
[METRICS] sent=200 errors=0 rate=120.0/min uptime=1.7m
```

### Step 6 — Verify the pipeline end-to-end

In a **7th terminal**, run the verification consumer:

```bash
python -m ingestion.consumers.verify_topics --max-seconds 60
```

It subscribes to all 6 topics, validates every message against its JSON
schema, runs sanity checks, and after 60 s prints a summary:
```
=== SUMMARY ===
topic                     valid   schema_invalid   sanity_failed
smart-meters                 60                0               0
weather                       2                0               0
incident-reports              1                0               0
rss-feeds                     0                0               0   ← may be 0 in 60 s window
market-prices                 0                0               0   ← hourly, may stay 0
user-feedback                 1                0               0
[PASS] All subscribed topics delivered schema-valid messages.
```

Exit code is `0` on success, `1` on schema/sanity violation, `2` if a topic
was silent. Use `--no-require-each` if you don't want exit code 2 for slow
topics.

### Step 7 — Open the web UIs

| URL | What you see |
|---|---|
| http://localhost:8090 | **Kafka UI** — browse topics, messages live, partitions, consumer groups, schemas |
| http://localhost:8081/subjects | **Schema Registry** REST — all 6 schema subjects listed |
| http://localhost:8080 | **Spark Master UI** — empty until P2's job runs |
| http://localhost:8501 | **Streamlit** — empty until P4's app runs |

**You're done with P1's setup.** P2 can now write `processing/` jobs that
consume from `kafka:29092` (inside Docker) or `localhost:9092` (from the host).

---

## 5. 🏗️ The 3 deployment modes — when to use which

| Mode | File | Brokers | RF | Persists data? | Use it for |
|---|---|---|---|---|---|
| **Local** | `docker-compose.local.yml` | 1 | 1 | ❌ No (no volume) | Daily development; restarts wipe the slate |
| **Pseudo-distributed** | `docker-compose.pseudo.yml` | 1 | 1 | ✅ Yes (named volumes) | Multi-day work; data survives `docker compose down` |
| **Fully distributed** | `docker-compose.distributed.yml` | 3 | 3 (MIN_ISR=2) | ✅ Yes | Defense demo to show RF=3 + broker failover |

### Switching modes

```bash
# 1. Stop current mode
docker compose -f docker/docker-compose.<current>.yml down

# 2. Start new mode
docker compose -f docker/docker-compose.<new>.yml --env-file .env up -d
```

> ⚠️ **Never** run two compose files at the same time — they all bind to
> the same host ports (9092, 8090, 8081, 8080, 27017).

### Distributed mode extra step

After switching to distributed, edit `.env`:
```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096
KAFKA_REPLICATION_FACTOR=3
```

Or if your producers run **inside** the Docker network:
```
KAFKA_BOOTSTRAP_SERVERS=kafka1:29092,kafka2:29092,kafka3:29092
```

> **Honest disclaimer:** distributed mode runs 3 brokers as separate
> containers on **one host**, demonstrating RF=3 and quorum behaviour. For
> a true multi-machine deployment, each broker would run on its own
> physical machine with `KAFKA_ADVERTISED_LISTENERS` pointing to the host's
> reachable IP. We document this in the Rapport.

### ⚠️ Stop commands — read before you type

| Command | Effect |
|---|---|
| `docker compose -f <file> down` | Stops + removes containers. **Volumes preserved.** |
| `docker compose -f <file> down -v` | **DESTROYS volumes** — all Kafka logs + Mongo data lost forever. |
| `docker compose -f <file> stop` | Pause containers; restart with `start`. Fastest way to "free up the laptop." |

---

## 6. 📊 The 6 Kafka topics (P1's data contract for the team)

| Topic | Partitions | Interval | Retention | Schema file | Used by |
|---|---|---|---|---|---|
| `smart-meters` | 3 | 30 s | 7 d | [`smart_meter.schema.json`](ingestion/schemas/smart_meter.schema.json) | P2 (windowed agg), P5 (ML training) |
| `weather` | 1 | 30 s | 30 d | [`weather.schema.json`](ingestion/schemas/weather.schema.json) | P2 (broadcast join), P5 (feature) |
| `incident-reports` | 1 | 60 s | 90 d | [`incident_report.schema.json`](ingestion/schemas/incident_report.schema.json) | P2 (NLP), P4 (word cloud) |
| `rss-feeds` | 1 | 120 s | 30 d | [`rss_feed.schema.json`](ingestion/schemas/rss_feed.schema.json) | P2 passthrough, P5 (feature) |
| `market-prices` | 1 | 1 h | 90 d | [`market_price.schema.json`](ingestion/schemas/market_price.schema.json) | P5 (price feature), P4 (price-vs-load chart) |
| `user-feedback` | 3 | 45 s | 30 d | [`user_feedback.schema.json`](ingestion/schemas/user_feedback.schema.json) | P2 (sentiment NLP), P4 (sentiment chart) |

All topics use `cleanup.policy=delete`, `compression.type=snappy`. Distributed mode adds `min.insync.replicas=2`.

### Smart-meter message fields (the most important schema)

| Field | Type | Purpose |
|---|---|---|
| `consumption_kwh` | float [0.001, 0.12] | **ML regression target** (P5) |
| `voltage_v` | float [210, 250] | Anomaly detection feature (P5) |
| `frequency_hz` | float [48, 52] | Grid stability feature |
| `power_factor` | float [0, 1] | Efficiency feature |
| `is_anomaly` | bool | **ML classification target** (P5) |
| `anomaly_reason` | enum / null | 4-class anomaly type — DON'T use as feature (target leak) |
| `hour_of_day`, `day_of_week`, `is_peak_hour` | int / int / bool | Temporal features (UTC) |
| `zone` | A / B / C / D | Aggregation key (P2) + one-hot feature (P5) |
| `zone_lat`, `zone_lon` | float | Geographic heatmap (P4) |

Full schemas in [`ingestion/schemas/`](ingestion/schemas/) — every producer
validates client-side via `jsonschema` before publishing.

---

## 7. 🔍 Verifying the pipeline (beyond Step 6)

### Live message inspection

```bash
# Quick: count messages in a topic
docker exec kafka-local kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic smart-meters

# Live tail
docker exec kafka-local kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic smart-meters --max-messages 5
```

### MongoDB inspection (after P2/P3 ship)

```bash
docker exec mongodb-local mongosh \
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin \
  --eval "db.getSiblingDB('energy_db').runCommand({listCollections: 1, nameOnly: true})"
```

### Verification consumer flags (CI-friendly)

```bash
python -m ingestion.consumers.verify_topics \
  --max-seconds 60 \
  --from-beginning \
  --no-require-each
```

| Flag | Meaning |
|---|---|
| `--max-seconds N` | Stop after N seconds (good for CI / quick demo) |
| `--from-beginning` | Replay from earliest offset (default: only new messages) |
| `--no-require-each` | Don't fail when a slow topic (e.g. `market-prices` hourly) hasn't ticked yet |

Exit codes: `0` = pass, `1` = schema/sanity violation, `2` = at least one topic was silent.

---

## 8. 📁 Project structure (current state)

```
energy-big-data/
├── README.md                            ← You are here
├── requirements.txt                     ← Python deps (UTF-8, pinned)
├── .env.example                         ← Copy to .env, edit
├── .gitignore                           ← includes .venv/, .claude/, *.pkl
│
├── docker/
│   ├── docker-compose.local.yml         ← Dev mode, ephemeral
│   ├── docker-compose.pseudo.yml        ← Single broker + volumes
│   └── docker-compose.distributed.yml   ← 3 brokers, RF=3, MIN_ISR=2
│
├── ingestion/                           ← 🟢 P1 (DONE)
│   ├── config/
│   │   └── kafka_config.py              ← Shared config, env-driven
│   ├── schemas/
│   │   ├── smart_meter.schema.json
│   │   ├── weather.schema.json
│   │   ├── incident_report.schema.json
│   │   ├── rss_feed.schema.json
│   │   ├── market_price.schema.json
│   │   ├── user_feedback.schema.json
│   │   └── register_schemas.py          ← One-time Schema Registry POST
│   ├── producers/
│   │   ├── smart_meter_producer.py      ← 20 meters, 4 zones, anomalies
│   │   ├── weather_producer.py          ← Tétouan climate sim
│   │   ├── incident_producer.py         ← Weather-weighted incidents
│   │   ├── rss_producer.py              ← Industrial news templates
│   │   ├── market_price_producer.py     ← MASEN/EU/DAY_AHEAD prices
│   │   ├── user_feedback_producer.py    ← Web/Mobile/CallCenter/Email
│   │   └── shared_state.py              ← Atomic weather → consumption coupling
│   └── consumers/
│       └── verify_topics.py             ← End-to-end pipeline verifier
│
├── processing/                          ← 🟡 P2 (TODO)
│   └── README.md                        ← Detailed workflow + task list
│
├── storage/                             ← 🟡 P3 (TODO; init script done)
│   ├── README.md                        ← Collection design + HA plan
│   └── init/
│       └── init-mongo.js                ← Auto-runs on first MongoDB startup
│
├── ml/                                  ← 🟡 P5 (TODO)
│   └── README.md                        ← 3 algorithms, 2 tasks, 4 metrics
│
├── dashboard/                           ← 🟡 P4 (TODO)
│   └── README.md                        ← All 4 spec-F sections + alerts
│
├── ethics/
│   └── GDPR.md                          ← Compliance: legal basis, retention, rights
│
└── docs/                                ← (Defense slides, Rapport when ready)
```

---

## 9. 🛠️ Troubleshooting

### "ClassNotFoundException: KafkaSourceProvider" (Spark)

You forgot to add the connector packages. See [`processing/README.md` §3](processing/README.md#3--spark-connector-packaging--read-first).

### Kafka refuses to start after editing `.env`

Did you change `KAFKA_CLUSTER_ID` while a volume existed? That UUID is
written into the broker's metadata on first startup and can't change. Fix:
```bash
docker compose -f docker/docker-compose.<mode>.yml down -v
# Then start again — init runs fresh.
```

### MongoDB authentication failed

The `.env` credentials must match what was set when the volume was first
created. If you changed them after, Mongo still has the old ones. Fix:
`down -v` then `up -d` to reinit with current `.env`.

### Producer crashes with `ImportError: cannot import name 'cimpl'`

Your venv is on a different platform than where it was created (e.g.,
created on WSL, ran on Windows). Recreate the venv on the host you're
using.

### "It runs on my machine" but a teammate's it doesn't

Check:
1. They ran `pip install -r requirements.txt` (versions are pinned)
2. They have the same `.env` (especially `KAFKA_CLUSTER_ID`)
3. Their Docker Desktop has at least 4 GB RAM allocated
4. Ports 9092, 8090, 8081, 8080, 27017, 8501 are not used by other apps

### How to nuke everything and start fresh

```bash
docker compose -f docker/docker-compose.local.yml down -v
docker compose -f docker/docker-compose.pseudo.yml down -v
docker compose -f docker/docker-compose.distributed.yml down -v
docker volume prune -f       # cleans up any orphaned volumes
rm -rf ingestion/producers/.weather_state.json   # clears producer shared state
```

---

## 10. 🎓 Defense day quick reference

The professor grades on (per the spec, sections D + E + H):

1. **Tool justification** — every component must be defensible vs alternatives. Each folder README has a "Required justifications for the defense" section.
2. **Data quality framework** — section E. P2 writes `data_quality_metrics`; P4 displays them.
3. **3 algorithms compared** — section D. P5 reports Precision / Recall / F1 / inference time.
4. **Real-time dashboard with all 4 sections** — section F. P4 must have heatmap + prediction-vs-actual + word cloud + predictive alerts.
5. **GDPR + ethics** — sections G + H. See `ethics/GDPR.md` and `dashboard_alerts` audit trail.

Demo script (~10 min): start producers → open Kafka UI (show topics + live messages) → run `verify_topics` (PASS in 60 s) → start Spark job (show Spark UI) → open dashboard (show all 4 sections rendering live) → trigger an injected anomaly + show the alert + word cloud update.

---

## 11. 👀 Useful commands cheat sheet

```bash
# Status
docker compose -f docker/docker-compose.local.yml ps
docker stats                                     # live CPU/RAM per container

# Logs
docker logs kafka-local --tail 50 -f
docker logs schema-registry-local --tail 50 -f

# Kafka admin
docker exec kafka-local kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka-local kafka-topics --bootstrap-server localhost:9092 --describe --topic smart-meters

# Schema Registry
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/smart-meters-value/versions/latest

# MongoDB
docker exec -it mongodb-local mongosh -u energy_admin -p change-me-before-deploy --authenticationDatabase admin

# Python verification (quickest health check)
python -m ingestion.consumers.verify_topics --max-seconds 30 --no-require-each
```

---

## 📞 Need help?

1. Read your folder's README first (`processing/README.md`, `storage/README.md`, `dashboard/README.md`, or `ml/README.md`) — they cover all common pitfalls per role.
2. Check `ethics/GDPR.md` for any data-handling questions.
3. Pipeline-level issues: re-run §4 step by step, or use the troubleshooting section above.
4. Cross-team coordination: the 4 folder READMEs reference each other's exact data contracts; if your input doesn't match someone else's output, the discrepancy is on one of the README pages.
