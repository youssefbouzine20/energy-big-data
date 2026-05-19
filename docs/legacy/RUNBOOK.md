## RUNBOOK — `energy-big-data` (defense 2026-05-21)

> One file. Top to bottom. From a cold laptop to the prof saying "merci".
> Windows / PowerShell. Pseudo mode = your demo mode.
>
> Companion docs (deep-dives, do **not** read during the demo):
> - [README.md](README.md) — project overview + per-folder navigation
> - [DEMO_GUIDE.md](DEMO_GUIDE.md) — long-form per-section script (A–H)
> - [HANDOFF.md](HANDOFF.md) — what's done / what's owed by teammates

---

## Table of contents

1. [What you'll demo (the story in 10 lines)](#1-what-youll-demo)
2. [One-time setup (do once per machine)](#2-one-time-setup)
3. [Pick your mode](#3-pick-your-mode)
4. [Phase 0 — Bring up the infra](#4-phase-0--bring-up-the-infra)
5. [Phase 1 — MongoDB validators + healthcheck](#5-phase-1--mongodb-validators--healthcheck)
6. [Phase 2 — Schema Registry](#6-phase-2--schema-registry)
7. [Phase 3 — Start the 6 producers](#7-phase-3--start-the-6-producers)
8. [Phase 4 — Submit the Spark streaming job](#8-phase-4--submit-the-spark-streaming-job)
9. [Phase 5 — Verify MongoDB is filling up](#9-phase-5--verify-mongodb-is-filling-up)
10. [Phase 6 — Run the ML pipeline](#10-phase-6--run-the-ml-pipeline)
11. [Phase 7 — Dashboard backend + frontend](#11-phase-7--dashboard-backend--frontend)
12. [Phase 8 — Final smoke test](#12-phase-8--final-smoke-test)
13. [Demo walkthrough (15 min)](#13-demo-walkthrough)
14. [Stopping cleanly](#14-stopping-cleanly)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. What you'll demo

```
 6 PRODUCERS  →  KAFKA  →  SPARK STREAMING  →  MONGODB  →  DASHBOARD
  (Python)    (broker)   (8 streaming queries) (11 colls)  (React + Express)
                              ↑                    ↑
                       Schema Registry         ML PIPELINE
                       (6 JSON schemas)        (3 algos, sklearn)
```

| Layer | Job | What the prof sees |
|---|---|---|
| **Producers** | Generate 6 streams of fake energy data | 6 terminals printing live |
| **Kafka** | Buffer messages, decouple producer/consumer speeds | Kafka UI :8090 |
| **Spark** | Window aggregation (15 min × zone), NLP, data quality | Spark UI :8080 |
| **MongoDB** | Store raw + aggregated + ML, GDPR TTL, JSON validators | Compass or `/settings` |
| **ML** | Compare 3 algos (LinReg, RF, GB), forecast 6 windows ahead | `ml/metrics.json` + `/predictions` |
| **Dashboard** | 7 pages, JWT auth, KPIs + alerts + word cloud | http://localhost:5173 |

---

## 2. One-time setup

You normally only do this once per machine. Skip to §3 if your `.env` already has a `KAFKA_CLUSTER_ID` and `.venv` is built.

### 2.1 Clone + venv

```powershell
cd Y:\
git clone https://github.com/youssefbouzine20/energy-big-data.git
cd energy-big-data
python -m venv .venv
.venv\Scripts\Activate.ps1
.venv\Scripts\pip install --default-timeout=100 -r requirements.txt
```

### 2.2 `.env`

```powershell
copy .env.example .env
# Generate a Kafka cluster UUID and paste it into .env after KAFKA_CLUSTER_ID=
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
```

Open `.env`, replace `<paste uuid here>` with the UUID. Never change it later — Kafka refuses to boot if it mismatches what's on disk.

### 2.3 Custom Spark image (numpy + nltk + VADER)

Build once, used by all 3 modes:

```powershell
docker compose -f docker/docker-compose.local.yml --env-file .env build
```

### 2.4 Dashboard backend `.env`

```powershell
copy dashboard\backend\.env.example dashboard\backend\.env
```

Defaults are fine: `API_PORT=4000`, `CORS_ORIGIN=http://localhost:5173`, login `admin/admin123`.

### 2.5 Frontend deps (first time only)

```powershell
cd dashboard\frontend
npm install
cd ..\..
```

Done with one-time setup.

---

## 3. Pick your mode

| Mode | Compose file | Brokers | Persistent? | Use for |
|---|---|---|---|---|
| **local** | `docker-compose.local.yml` | 1 | No (no volume) | Dev. Restarts wipe data. |
| **pseudo** | `docker-compose.pseudo.yml` | 1 | Yes | **Your demo mode.** Survives `down`. |
| **distributed** | `docker-compose.distributed.yml` | 3 (RF=3, ISR=2) | Yes | Optional bonus — show broker failover. |

Container-name suffix follows the mode: `kafka-local`, `kafka-pseudo`, or three numbered `kafka1-dist` / `kafka2-dist` / `kafka3-dist`. Everywhere below, **replace `<mode>` with `local`, `pseudo`, or `distributed`** depending on which one you brought up.

> Never run two modes at once — they bind the same host ports.

---

## 4. Phase 0 — Bring up the infra

**Terminal 1** (project root, venv active):

```powershell
cd Y:\energy-big-data
.venv\Scripts\Activate.ps1
$env:PYTHONIOENCODING="utf-8"
```

The `PYTHONIOENCODING` line is mandatory in **every new PowerShell terminal** — without it, the producers crash on the `→` character (cp1252 codec error).

### 4.1 Stop anything that might already be running

```powershell
docker compose -f docker/docker-compose.local.yml --env-file .env down
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down
docker compose -f docker/docker-compose.distributed.yml --env-file .env down
```

(Each command is harmless if that mode wasn't running — just exits.)

### 4.2 Bring your chosen mode up

```powershell
docker compose -f docker/docker-compose.<mode>.yml --env-file .env up -d
```

Wait ~60 seconds, then:

```powershell
docker compose -f docker/docker-compose.<mode>.yml --env-file .env ps
```

**Expected:** `kafka-<mode>`, `schema-registry-<mode>`, `mongodb-<mode>`, `spark-master-<mode>` all `(healthy)`. `kafka-init-<mode>` shows `Exited (0)` — that is **correct**: it created the 6 topics and exited.

If anything is `(unhealthy)` or `Restarting` → §15 troubleshooting.

---

## 5. Phase 1 — MongoDB validators + healthcheck

`init-mongo.js` already ran inside the container on first boot (created 11 collections, `spark_writer` user, TTL indexes). Now apply the **Python-side validators**.

```powershell
.venv\Scripts\python -m storage.schema_validators --apply-all
```

**Expected last line:** `Résultat: 11/11 validateurs appliqués avec succès.`

Now run the health dashboard:

```powershell
.venv\Scripts\python -m storage.healthcheck
```

**Expected:** server OK, pool OK, 11 collections each `validator: ✓`, every test query shows `stage: IXSCAN` (no COLLSCAN).

If you see `0 collections` → init didn't run. `down -v` then `up -d` (this wipes Mongo data — fine on a fresh boot).

---

## 6. Phase 2 — Schema Registry

Register the 6 JSON schemas with Confluent Schema Registry:

```powershell
.venv\Scripts\python -m ingestion.schemas.register_schemas
```

**Expected:** 6 lines like `[OK] smart-meters-value id=1`, ending with all 6 registered.

Verify in Kafka UI: http://localhost:8090 → top-left dropdown → `energy-<mode>` → **Schema Registry** in sidebar → 6 subjects, version 1, BACKWARD compatibility.

---

## 7. Phase 3 — Start the 6 producers

Open **6 new PowerShell terminals**. In each, run these 3 lines first:

```powershell
cd Y:\energy-big-data
.venv\Scripts\Activate.ps1
$env:PYTHONIOENCODING="utf-8"
```

Then in each terminal, run **one** producer:

| Terminal | Command | Rate |
|---|---|---|
| 2 | `.venv\Scripts\python -m ingestion.producers.weather_producer` | every 30 s × 4 zones (**start FIRST** — writes shared state file) |
| 3 | `.venv\Scripts\python -m ingestion.producers.smart_meter_producer` | every 30 s × 20 meters |
| 4 | `.venv\Scripts\python -m ingestion.producers.incident_producer` | every 60 s |
| 5 | `.venv\Scripts\python -m ingestion.producers.rss_producer` | every 120 s |
| 6 | `.venv\Scripts\python -m ingestion.producers.market_price_producer` | every 1 h (you'll see only the boot line) |
| 7 | `.venv\Scripts\python -m ingestion.producers.user_feedback_producer` | every 45 s |

**Leave these terminals running** for the whole demo.

### Quick sanity check (back in Terminal 1)

```powershell
.venv\Scripts\python -m ingestion.consumers.verify_topics --max-seconds 60 --no-require-each
```

**Expected last line:** `[PASS] All subscribed topics delivered schema-valid messages.`

---

## 8. Phase 4 — Submit the Spark streaming job

**Terminal 8** (new PowerShell, project root). For the demo, override the window so aggregates appear in ~3 min instead of ~17:

```powershell
docker exec -e AGGREGATION_WINDOW="2 minutes" -e AGGREGATION_WATERMARK="1 minute" spark-master-<mode> /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /workspace/processing/main.py
```

(One single line. Do **not** add backticks.)

**Expected within ~45 s** (Ivy package download on first run):

```
Initialisation de la session Spark...
Lancement des flux Passthrough (Météo, RSS, Marché)...
Lancement des flux NLP (Incidents, Feedback)...
Lancement du flux principal (Compteurs & Agrégations 15min)...
Lancement du flux de Qualité des Données...
Tous les flux sont démarrés avec succès !
En écoute continue sur Kafka...
```

**Leave this terminal running.** `Ctrl+C` here stops the pipeline.

Verify in Spark UI: http://localhost:8080 → **1 worker alive** (or 2 for distributed), click the running app → **Streaming Queries** tab → **13 active queries**:

1. weather passthrough
2. rss passthrough
3. market passthrough
4. incidents (NLP)
5. feedback (NLP)
6. meters_raw
7. meters_aggregated_15min
8. data_quality (smart-meters)
9. data_quality (weather)
10. data_quality (incident-reports)
11. data_quality (rss-feeds)
12. data_quality (market-prices)
13. data_quality (user-feedback)

---

## 9. Phase 5 — Verify MongoDB is filling up

### After 1 minute (back in Terminal 1):

```powershell
docker exec mongodb-<mode> mongosh -u energy_admin -p change-me-before-deploy --authenticationDatabase admin energy_db --quiet --eval "['meters_raw','weather','incidents','feedback_nlp','rss_feeds'].forEach(c => print(c + ': ' + db[c].countDocuments()))"
```

**Expected:** `meters_raw: 100+`, others growing.

### After ~3-4 minutes (windowed aggregates appear):

```powershell
docker exec mongodb-<mode> mongosh -u energy_admin -p change-me-before-deploy --authenticationDatabase admin energy_db --quiet --eval "['meters_aggregated_15min','data_quality_metrics'].forEach(c => print(c + ': ' + db[c].countDocuments()))"
```

**Expected:** `meters_aggregated_15min: 4+`, `data_quality_metrics: 6+`.

### Inspect one aggregate row:

```powershell
docker exec mongodb-<mode> mongosh -u energy_admin -p change-me-before-deploy --authenticationDatabase admin energy_db --quiet --eval "db.meters_aggregated_15min.find().limit(1).pretty()"
```

**Expected fields:** `zone`, `window_start`, `window_end`, `avg_consumption`, `total_consumption_kwh`, `anomaly_count`, `anomaly_rate_pct`, `computed_at`.

---

## 10. Phase 6 — Run the ML pipeline

Wait until `meters_aggregated_15min` has at least 4 rows (one closed window × 4 zones). Then in **Terminal 1**:

```powershell
.venv\Scripts\python -m ml.run_pipeline
```

**Expected output:** 6 phases, ending with:
- 3 models compared in a table (LinReg / RandomForest / GradientBoosting)
- Metrics written to `ml/metrics.json`
- 72 prediction docs written to `ml_predictions` (4 zones × 6 future windows × 3 models)
- A handful of WARNING/CRITICAL alerts written to `dashboard_alerts`

Open `ml/metrics.json` in VS Code — that's what you'll show the prof.

---

## 11. Phase 7 — Dashboard backend + frontend

### 11.1 Backend (**Terminal 9**)

```powershell
cd Y:\energy-big-data\dashboard\backend
npm run dev
```

**Expected:**
```
[mongo_client] Pool connecté à mongodb://energy_admin:***@localhost:27017/...
[api] listening on http://localhost:4000
```

### 11.2 Frontend (**Terminal 10**)

```powershell
cd Y:\energy-big-data\dashboard\frontend
npm run dev
```

**Expected:**
```
VITE v5.x  ready in 800 ms
➜  Local:   http://localhost:5173/
```

### 11.3 Walk the 7 pages

Open http://localhost:5173 → redirected to `/login` → **admin / admin123**.

| # | Page | What you'll see |
|---|---|---|
| 1 | **Overview** `/` | 4 KPI cards (Total kWh, Anomalies 1h, Active Incidents, Avg Voltage) |
| 2 | **Heatmap** `/heatmap` | 4 zone tiles A/B/C/D colored by load |
| 3 | **Predictions** `/predictions` | Solid blue real line + dashed orange forecast |
| 4 | **Incidents** `/incidents` | Word cloud + recent French incidents table |
| 5 | **Alerts** `/alerts` | Active + audit alerts from `dashboard_alerts` |
| 6 | **Data Quality** `/quality` | Cards per topic (completeness, noise, coverage) |
| 7 | **Settings** `/settings` | Profile, GDPR retention table, 11 collections |

---

## 12. Phase 8 — Final smoke test

Before opening the door for the prof, run these in order:

```powershell
# 1. Infra healthy
docker compose -f docker/docker-compose.<mode>.yml --env-file .env ps

# 2. Topic delivery
.venv\Scripts\python -m ingestion.consumers.verify_topics --max-seconds 30 --no-require-each

# 3. Mongo healthcheck
.venv\Scripts\python -m storage.healthcheck

# 4. Refresh predictions (so Predictions page is fresh)
.venv\Scripts\python -m ml.run_pipeline

# 5. Click every dashboard page once in the browser
```

If all 5 pass, you're ready. Time check: this whole runbook §2 → §12 takes ~25 min on a fresh laptop.

---

## 13. Demo walkthrough

15 minutes presentation, 10 minutes Q&A. Order matters — follow the data flow.

### Order to demo (don't improvise)

| Step | Time | Open | Show / click | Say (FR) |
|---|---|---|---|---|
| 1. Architecture | 2 min | This file §1 (or printed diagram) | The 5-layer diagram | "Données → Kafka → Spark → Mongo → Dashboard. ML séparé." |
| 2. Producers | 30 s | Tile your 6 producer terminals | Live `→ smart-meters` lines | "Six producteurs Python, un par source." |
| 3. Kafka UI | 2 min | http://localhost:8090 | **Brokers** tab → **Topics** tab → click `smart-meters` → **Messages** → expand one JSON. Then **Schema Registry** → 6 subjects | "smart-meters a 3 partitions pour paralléliser. BACKWARD compat sur Schema Registry." |
| 4. Spark UI | 2 min | http://localhost:8080 | **Workers** (1 alive) → click app → **Streaming Queries** (13 actives) | "13 requêtes streaming. Watermark 2 min, fenêtre 15 min en prod, 2 min en démo. Une requête de qualité par topic." |
| 5. MongoDB | 3 min | MongoDB Compass OR Terminal 1 → `python -m storage.healthcheck` | 11 collections, click `meters_aggregated_15min` → **Documents** / **Indexes** / **Validation** tabs | "11 collections, validateurs $jsonSchema, index TTL pour GDPR, IXSCAN partout." |
| 6. ML | 2 min | `ml/metrics.json` + dashboard `/predictions` | Table of 3 models, courbe réelle vs forecast | "Étude comparative. Gradient boosting gagne R²=0.85, 3 µs/sample." |
| 7. Dashboard tour | 3 min | http://localhost:5173 | Walk 7 pages in order: Overview → Heatmap → Predictions → Incidents → Alerts → Data Quality → Settings | (see talking points in DEMO_GUIDE.md §F) |
| 8. GDPR + 3 modes | 1 min | `/settings` page + `docker/` folder | TTL retention table + 3 compose files | "TTL différenciés par collection. Trois modes : local/pseudo/distribué." |

### If the prof asks something specific

| Question | Open | Say |
|---|---|---|
| "Montrez-moi Kafka" | Kafka UI :8090 | "Six topics, smart-meters a 3 partitions." |
| "Et le Schema Registry?" | Kafka UI → Schema Registry sidebar | "Compatibilité BACKWARD — évolution de schéma sans casser les producteurs." |
| "Spark?" | Spark UI :8080 → Streaming Queries | "13 requêtes actives, mode append, watermark 2 min." |
| "Pourquoi pas de stream-stream join?" | (verbal) | "Fragile en Spark 3.5 — j'enrichis côté Mongo via `$lookup`. Spark = vélocité, Mongo = jointure de variété." |
| "MongoDB?" | Compass OR `/settings` | "11 collections, validateurs, TTL GDPR, index composé `zone + window_start`." |
| "Pourquoi pas Cassandra?" | (verbal) | "Mongo gère mieux les requêtes ad-hoc filtrées du dashboard. Cassandra serait meilleur en écriture pure." |
| "Index utilisés?" | Terminal → `python -m storage.healthcheck` | "Toutes les requêtes hit IXSCAN, pas COLLSCAN." |
| "ML?" | `ml/metrics.json` + `/predictions` | "3 algos : LinReg / RandomForest / GradientBoosting. GB gagne sur RMSE / MAE / R²." |
| "Features ML?" | (verbal) | "lag_1, lag_4, rolling mean/std, heure, jour, one-hot zone." |
| "Data quality?" | `/quality` page | "Complétude, bruit, anomalies, couverture par fenêtre 15 min par topic." |
| "RGPD?" | `/settings` GDPR table | "TTL : meters_raw 90j, agrégats 365j, incidents 730j (audit légal). `meter_id` pseudonymisé." |
| "Trois modes d'install?" | `.env.example` + `docker/` | "Local (dev), Pseudo (démo), Distributed (3 brokers RF=3 ISR=2)." |

---

## 14. Stopping cleanly

In order:

```powershell
# 1. Frontend (T10) + Backend (T9): Ctrl+C in each
# 2. Producers (T2-T7): Ctrl+C in each
# 3. Spark (T8): Ctrl+C. If stuck:
docker exec spark-master-<mode> pkill -9 -f spark-submit

# 4. Stop infra (preserves data volumes — recommended):
docker compose -f docker/docker-compose.<mode>.yml --env-file .env down

# 5. (Optional) Full wipe — next session starts empty:
docker compose -f docker/docker-compose.<mode>.yml --env-file .env down -v
```

`down -v` destroys all Kafka logs + Mongo data. Use only when you want a 100% clean restart.

---

## 15. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Producer crashes with `'charmap' codec can't encode ... '→'` | Windows cp1252 codec | `$env:PYTHONIOENCODING="utf-8"` in every new terminal |
| `MongoServerError: Authentication failed` | Volume has old credentials | `docker compose -f docker/docker-compose.<mode>.yml --env-file .env down -v && up -d` (destroys Mongo data for that mode) |
| Kafka refuses to start after editing `.env` | `KAFKA_CLUSTER_ID` changed while volume exists | `down -v` then `up -d` |
| `meters_aggregated_15min` stays at 0 after 5 min | Spark window override didn't take | Re-submit with both `-e AGGREGATION_WINDOW` AND `-e AGGREGATION_WATERMARK` (Phase 4) |
| Spark crashes `ClassNotFoundException: KafkaSourceProvider` | `--packages` flag missing | Use the full command in Phase 4 verbatim |
| Spark crashes `ModuleNotFoundError: numpy` | Custom Spark image not built | Phase 2.3 of one-time setup |
| Spark crashes `winutils.exe not found` | You ran `python -m processing.main` on host instead of `docker exec ...` | Use the `docker exec` command in Phase 4 |
| `Authentication failed` from Spark on Mongo | `MONGO_SPARK_PASS` ≠ `MONGO_PASSWORD` in `.env` | Make them identical, recreate Mongo volume |
| Dashboard shows old data | Browser cache | Ctrl+R |
| KPI cards say "loading…" forever | Backend died | Restart `npm run dev` in T9 |
| Predictions chart empty | `ml_predictions` empty | Re-run `python -m ml.run_pipeline` (Phase 6) |
| Heatmap zones empty | Spark died or window override missing | Check T8 for errors; re-submit Phase 4 |
| Kafka UI shows 0 messages | Producers died | Check T2-T7 |
| Page won't load at all | Frontend died | Restart `npm run dev` in T10 |
| Backend `EADDRINUSE :4000` | Old node still running | `Get-Process node \| Stop-Process` (warning: kills ALL node) |
| `spark-submit` path mangling on Git Bash | Git Bash converts `/opt/...` to Windows paths | Use **PowerShell**, not Git Bash. Or prefix with `MSYS_NO_PATHCONV=1` |
| Two compose files seem to conflict | You ran `up -d` on a second mode without stopping the first | Stop them all (§4.1), bring up one |

### If something breaks mid-demo

Stay calm. Switch to slides. The prof grades **methodology + justification** more than a live demo. The architecture diagram (§1) + the talking points in §13 cover 80% of the grade even with everything offline.

---

## Production vs demo cheat-sheet

| Setting | Production default (`.env.example`) | Demo override |
|---|---|---|
| `AGGREGATION_WINDOW` | `15 minutes` | `2 minutes` |
| `AGGREGATION_WATERMARK` | `2 minutes` | `1 minute` |

If asked "pourquoi 15 minutes?": *"C'est l'intervalle de règlement du marché européen de l'énergie — défendable face au jury. Pour la démo j'override à 2 min pour que les fenêtres se ferment pendant votre présence."*
