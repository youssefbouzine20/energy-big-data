# energy-big-data — Real-Time Predictive Energy Load Analysis

> **M126 Big Data capstone — ENSA Tétouan — Pr. Imad Sassi**
> **Defense: 2026-05-21**
>
> One file. Top to bottom. From a cold laptop to "merci pour la présentation".
> Platform assumed: **Windows + PowerShell**. Bash works for the same commands.

---

## Table of contents

1. [Verdict — what works, what doesn't](#1-verdict)
2. [Pipeline architecture](#2-pipeline-architecture)
3. [Stack](#3-stack)
4. [One-time setup](#4-one-time-setup)
5. [Pick a deploy mode](#5-pick-a-deploy-mode)
6. [The 8 phases — verify from cold boot](#6-the-8-phases)
7. [Defense day demo + Q&A (FR)](#7-defense-day)
8. [Pre-defense checklist](#8-pre-defense-checklist)
9. [Stop cleanly](#9-stop-cleanly)
10. [Troubleshooting](#10-troubleshooting)
11. [Project structure](#11-project-structure)
12. [Crash recovery (if it dies mid-demo)](#12-crash-recovery)
13. [Team & roles](#13-team--roles)

---

## 1. Verdict

### What is verified working (live test 2026-05-19)

- **End-to-end pseudo mode pipeline.** 6 producers → Kafka (6 topics, Schema Registry) → Spark (13 streams) → MongoDB (11 collections) → React dashboard `:5173`. Auto-refresh works.
- **Distributed mode.** RF=3, MIN_ISR=2 on `smart-meters`. 9 containers healthy. Broker failover defensible.
- **ML pipeline.** LinReg / RF / GradientBoosting compared. GB wins R²=0.87, RMSE=0.000868. 72 predictions + 53 alerts written.
- **Dashboard.** 7 pages, JWT cookie auth (`admin/admin123`), 14 backend endpoints, real data on every page.
- **GDPR / Section H.** TTL indexes 90/365/730 days. `dashboard_alerts` audit trail.
- **Data Quality / Section E.** 6 per-topic streams writing to `data_quality_metrics`.

### Known limits — acknowledge upfront if asked

- **`AGGREGATION_WINDOW` defaults to 15 min in `.env.example`** (European energy market settlement interval). For demo, override to `2 minutes` via `docker exec -e` so windows close in 3 min instead of 17.
- **ML classification descoped** — only regression is implemented (3 algos × 1 task: forecast). Section D's "study of 3 algorithms" satisfied with regression alone (RMSE / MAE / R² + inference time).
- **`ml_predictions` is NOT auto-refreshed.** Re-run `python -m ml.run_pipeline` before every demo or the Predictions page shows stale lines.
- **`market-prices` topic** ticks once per hour — Data Quality card may show "Waiting…" if no message landed in the latest window. Acceptable.
- **Demo data fallbacks on 4 dashboard pages** (Heatmap / Predictions / Incidents / Alerts). A yellow banner shows when the API returns empty. If you see the banner during demo, the underlying collection is empty — re-run Spark + ML pipeline.

### What you must NOT do

- ❌ `git push --force` to main.
- ❌ Merge `origin/meryem_branche` directly (4 known bugs).
- ❌ Run two docker compose modes at the same time (port conflict: 9092, 8090, 8081, 8080, 27017).
- ❌ Forget `$env:PYTHONIOENCODING="utf-8"` in any new PowerShell terminal — Windows cp1252 crashes producers on the `→` character.
- ❌ Change `KAFKA_CLUSTER_ID` in `.env` while volumes exist — Kafka refuses to boot.
- ❌ Run `python -m processing.main` from the Windows host — Spark needs winutils.exe. Use `docker exec spark-master-<mode> /opt/spark/bin/spark-submit ...` instead.

---

## 2. Pipeline architecture

```
 P1 — INGESTION                  P2 — PROCESSING                    P3 — STORAGE
─────────────────                ─────────────────                ─────────────────
 smart-meters     ─┐              Spark Structured                 MongoDB 7.0
 weather          ─┤              Streaming (13 queries)           ─────────────────
 incident-reports ─┼─► Kafka ──►   • 15-min windows           ──►   meters_raw
 rss-feeds        ─┤  (KRaft)      • zone aggregation               meters_aggregated_15min
 market-prices    ─┤  6 topics     • broadcast-join weather         incidents_enriched
 user-feedback    ─┘               • NLP on incidents + feedback    feedback_nlp
                                   • per-topic data quality         ml_predictions
                ┌──────────────┐                                    data_quality_metrics
                │ Schema Reg.  │                                    dashboard_alerts
                │ Kafka UI     │                                       │
                └──────────────┘                       ┌───────────────┴────────────────┐
                                                       ▼                                ▼
                                                P4 — DASHBOARD                   P5 — ML
                                               ─────────────────                ───────────
                                                React + Vite + JWT              3 models compared
                                                Express :4000                   • LinearRegression
                                                7 pages, 14 endpoints           • RandomForest
                                                Recharts viz                    • GradientBoosting
                                                :5173                           Writes ml_predictions
```

| Layer | Visible at | Purpose |
|---|---|---|
| Producers | Tiled terminals (lines `→ smart-meters`) | 6 Python scripts emit fake-but-realistic energy data |
| Kafka | http://localhost:8090 | 6 topics, KRaft mode (no ZK), Schema Registry BACKWARD |
| Spark | http://localhost:8080 | 13 streaming queries: 7 processing + 6 quality |
| MongoDB | mongodb://...@localhost:27017 | 11 collections, validators, TTL, IXSCAN-only |
| ML | `ml/metrics.json` + dashboard | Trains offline (sklearn), writes to MongoDB |
| Dashboard | http://localhost:5173 | React + Express, JWT auth `admin/admin123` |

---

## 3. Stack

| Component | Version | Purpose |
|---|---|---|
| Python | 3.10.11 | Producers, consumers, Spark jobs, ML |
| Confluent Kafka | 7.5.0 | Broker, KRaft mode (no ZooKeeper) |
| Apache Spark | 3.5.0 | Structured Streaming + NLP via VADER |
| MongoDB | 7.0 | NoSQL sink, TTL retention, `$jsonSchema` validators |
| Node.js | 20.x | Dashboard backend (Express) |
| React | 18 + Vite 5 | Dashboard frontend, JWT cookie auth |
| Recharts | 2.x | Charts |
| scikit-learn | latest | ML (LinReg / RF / GradientBoosting) |
| Docker Desktop | — | All services containerised |

Python deps pinned in `requirements.txt`. Frontend deps in `dashboard/frontend/package.json`. Backend deps in `dashboard/backend/package.json`. No global installs needed beyond Docker + Python 3.10 + Node 20.

---

## 4. One-time setup

Do this **once per laptop**. Skip to §5 if `.venv`, `.env`, and `node_modules` already exist.

### 4.1 Clone + Python venv

```powershell
cd Y:\
git clone https://github.com/youssefbouzine20/energy-big-data.git
cd energy-big-data
python -m venv .venv
.venv\Scripts\Activate.ps1
.venv\Scripts\pip install --default-timeout=100 -r requirements.txt
```

### 4.2 `.env`

```powershell
copy .env.example .env
# Generate a Kafka cluster UUID
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
```

Paste the UUID after `KAFKA_CLUSTER_ID=` in `.env`. **Never change it once the volume exists** — Kafka refuses to boot if it mismatches.

> ⚠️ **Security:** `.env.example` ships `MONGO_PASSWORD=change-me-before-deploy`. Local dev is fine; rotate before any non-local deployment.

### 4.3 Build the custom Spark image (numpy + nltk + VADER baked in)

```powershell
docker compose -f docker/docker-compose.local.yml --env-file .env build
```

Used by all 3 modes. ~2 min, once per laptop.

### 4.4 Dashboard backend `.env`

```powershell
copy dashboard\backend\.env.example dashboard\backend\.env
```

Defaults are good: `API_PORT=4000`, `CORS_ORIGIN=http://localhost:5173`, login `admin/admin123`.

### 4.5 Frontend + backend npm deps

```powershell
cd dashboard\backend
npm install
cd ..\frontend
npm install
cd ..\..
```

### 4.6 Universal terminal opening line

Every new PowerShell terminal you open for this project, run **these 3 lines first**:

```powershell
cd Y:\energy-big-data
.venv\Scripts\Activate.ps1
$env:PYTHONIOENCODING="utf-8"
```

The `PYTHONIOENCODING` is mandatory on Windows — without it, producers crash on the `→` character (cp1252 codec error).

### 4.7 MongoDB Compass (for inspection + the prof demo)

Download from https://www.mongodb.com/products/compass (free, official GUI).

Save this connection (works as soon as a MongoDB container is up — `pseudo` / `local` / `distributed` all use the same port):

```
mongodb://energy_admin:change-me-before-deploy@localhost:27017/?authSource=admin
```

Name the favourite `energy-big-data`. Click **Connect** → database `energy_db` with 11 collections appears in the left sidebar.

**Key views to bookmark for the defense (one click each during demo):**

| Click path in Compass | What the prof sees | Question it answers (FR) |
|---|---|---|
| `meters_aggregated_15min` → **Documents** | Real 15-min aggregated windows, by zone | « Comment vous agrégez les données ? » |
| `meters_aggregated_15min` → **Indexes** | Composite index `zone_1_window_start_-1` | « Les index sont-ils utilisés ? » |
| `meters_aggregated_15min` → **Validation** | `$jsonSchema` definition | « Comment validez-vous les données ? » |
| `dashboard_alerts` → **Documents** | Audit-trail entries with `triggered_at`, `alert_level`, `zone`, `model_name` | « Audit trail pour la conformité Section H ? » |
| `ml_predictions` → **Documents** | Forecasts with `forecast_for`, `consumption_forecast`, `alert_level` | « Comment vous stockez les prédictions ? » |
| Any collection → **Indexes** tab → look for TTL row | TTL indexes (90 / 365 / 730 days) on `timestamp` / `window_start` / `forecast_for` | « RGPD : durée de rétention par collection ? » |
| `incidents_enriched` → **Documents** → expand `nlp_keywords` | Array of NLP-extracted keywords from VADER | « Comment vous extrayez les mots-clés ? » |
| `feedback_nlp` → **Documents** → look at `sentiment_score` | VADER compound score [-1, 1] + label POSITIVE/NEGATIVE/NEUTRAL | « Analyse de sentiment ? » |

> **Tip:** Compass remembers the last view per collection. Before the defense, open each of the 7 collections above once and click the tab you want shown — the next click during the demo lands directly on the right view.

---

## 5. Pick a deploy mode

| Mode | Compose file | Brokers | RF | Persistent? | Use for |
|---|---|---|---|---|---|
| **A. local** | `docker-compose.local.yml` | 1 | 1 | ❌ No | Dev only — restarts wipe data |
| **B. pseudo** | `docker-compose.pseudo.yml` | 1 | 1 | ✅ Yes | **THE DEMO MODE** — survives `down` |
| **C. distributed** | `docker-compose.distributed.yml` | 3 | 3 (ISR=2) | ✅ Yes | Bonus — broker failover demo |

> ❗ **Never run two modes at once.** They bind the same host ports (9092, 27017, 8080, 8090, 8081).

### Container names per mode (cheat-sheet)

| Service | Local | Pseudo | Distributed |
|---|---|---|---|
| Kafka | `kafka-local` | `kafka-pseudo` | `kafka1-dist`, `kafka2-dist`, `kafka3-dist` |
| Schema Registry | `schema-registry-local` | `schema-registry-pseudo` | `schema-registry-dist` |
| Kafka UI | `kafka-ui-local` | `kafka-ui-pseudo` | `kafka-ui-dist` |
| MongoDB | `mongodb-local` | `mongodb-pseudo` | `mongodb-dist` |
| Spark master | `spark-master-local` | `spark-master-pseudo` | `spark-master-dist` |
| Spark worker | `spark-worker-local` | `spark-worker-pseudo` | `spark-worker-1-dist`, `spark-worker-2-dist` |

Rule of thumb: replace `-local` with `-pseudo` or `-dist` everywhere. Kafka in distributed splits into `kafka1` / `kafka2` / `kafka3`.

### Bring up / tear down (substitute the mode suffix)

```powershell
# Bring up (pseudo example)
docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d

# Tear down (preserves data)
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down

# Tear down + wipe data (use only if you need a 100% fresh slate)
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down -v
```

### Distributed-only: verify RF=3 to the prof

```powershell
docker exec kafka1-dist kafka-topics --bootstrap-server kafka1:29092 --describe --topic smart-meters
```

**Expected line:** `ReplicationFactor: 3   Configs: min.insync.replicas=2`

### Optional failover demo

```powershell
docker stop kafka2-dist
# Pipeline keeps running. Kafka UI shows ISR drop to [1, 3].
docker start kafka2-dist
# ISR recovers to [1, 2, 3] within ~10 s.
```

---

## 6. The 8 phases

Run from `Y:\energy-big-data`. **~45 min if everything works first try. Plan for 90 min.**

Substitute the mode suffix (`-local` / `-pseudo` / `-dist`) per §5. Examples below use **`-pseudo`**.

### Phase 0 — Tear down anything still running [3 min]

- [ ] **0.1** Kill leftover processes
  ```powershell
  Get-Process node, python -ErrorAction SilentlyContinue | Stop-Process -Force
  ```
- [ ] **0.2** Stop all docker stacks
  ```powershell
  docker compose -f docker/docker-compose.pseudo.yml --env-file .env down
  docker compose -f docker/docker-compose.distributed.yml --env-file .env down
  docker compose -f docker/docker-compose.local.yml --env-file .env down
  ```
- [ ] **0.3** Confirm clean
  ```powershell
  docker ps
  ```
  Expected: only header, no rows.

### Phase 1 — Cold boot infra [3 min]

- [ ] **1.1** Up
  ```powershell
  docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d
  ```
- [ ] **1.2** Wait 60 s, verify health
  ```powershell
  docker compose -f docker/docker-compose.pseudo.yml --env-file .env ps
  ```
  Expected: 6 containers `(healthy)`. `kafka-init-pseudo` shows `Exited (0)` — correct, it created topics and exited.

### Phase 2 — MongoDB validators + healthcheck [1 min]

Open **T1** with universal opening (§4.6).

- [ ] **2.1** Apply validators
  ```powershell
  python -m storage.schema_validators --apply-all
  ```
  Expected: `Résultat: 11/11 validateurs appliqués avec succès.`

- [ ] **2.2** Healthcheck
  ```powershell
  python -m storage.healthcheck
  ```
  Expected: 11 collections, every test query `stage: IXSCAN`. **Zero COLLSCAN** — if any, an index is missing.

### Phase 3 — Register schemas [1 min]

- [ ] **3.1**
  ```powershell
  python -m ingestion.schemas.register_schemas
  ```
  Expected: 6 lines `[OK] <topic>-value id=N`. Visit http://localhost:8090 → Schema Registry tab → 6 subjects, version 1, BACKWARD.

### Phase 4 — Start 6 producers [3 min]

Open **6 new PowerShell terminals (T2–T7)**. In each, universal opening, then one producer:

| T | Cadence | Command |
|---|---|---|
| T2 | 30 s × 4 zones (start FIRST) | `python -m ingestion.producers.weather_producer` |
| T3 | 30 s × 20 meters | `python -m ingestion.producers.smart_meter_producer` |
| T4 | 60 s | `python -m ingestion.producers.incident_producer` |
| T5 | 120 s | `python -m ingestion.producers.rss_producer` |
| T6 | 1 h | `python -m ingestion.producers.market_price_producer` |
| T7 | 45 s | `python -m ingestion.producers.user_feedback_producer` |

> **Why weather first?** It writes the shared zone-state file that `smart_meter_producer` and `incident_producer` read for weather-coupled simulation.

Expected in each: `→ <topic>` lines printing every cadence interval. **If a producer crashes with `'charmap' codec`, the terminal forgot `$env:PYTHONIOENCODING="utf-8"`.**

- [ ] **4.1** Sanity check (back in T1)
  ```powershell
  python -m ingestion.consumers.verify_topics --max-seconds 60 --no-require-each
  ```
  Expected: `[PASS] All subscribed topics delivered schema-valid messages.`

### Phase 5 — Submit Spark [3 min]

- [ ] **5.1** Wipe stale checkpoints (avoids "Incomplete log file" on dirty shutdown)
  ```powershell
  docker exec spark-master-pseudo bash -c "rm -rf /tmp/spark-checkpoints/*"
  ```

- [ ] **5.2** Submit in **T8** (new terminal — Spark will occupy it for the entire demo)
  ```powershell
  docker exec -e SPARK_MASTER="spark://spark-master:7077" -e AGGREGATION_WINDOW="2 minutes" -e AGGREGATION_WATERMARK="1 minute" spark-master-pseudo /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /workspace/processing/main.py
  ```
  For distributed, replace `spark-master-pseudo` → `spark-master-dist`. The `spark://spark-master:7077` URL stays the same in both modes — it resolves internally on the docker network.

  > **Why `SPARK_MASTER` AND `--master`?** `processing/spark_session.py:30` reads the env var to set `SparkSession.builder.master(...)`. Without it, the app defaults to `local[*]` (driver-only, never registers with the cluster — Spark UI shows 0 Running Applications even though data flows). The `--master` flag on spark-submit is belt-and-suspenders so the launcher also knows the cluster URL.
  >
  > **Why `2 minutes`?** Demo override so windows close in ~3 min instead of ~17. Production default is `15 minutes` (European energy market settlement interval, documented in `.env.example:86`).

- [ ] **5.3** Watch T8 output until you see (~45–60 s):
  ```
  Tous les flux sont démarrés avec succès !
  En écoute continue sur Kafka...
  ```
  **No `ERROR MicroBatchExecution`.** If you see `Incomplete log file` — Phase 5.1 wasn't done. `Ctrl+C` in T8, redo 5.1, redo 5.2.

- [ ] **5.4** Spark UI sanity
  Open http://localhost:8080 → **Running Applications** must show **1 row** named `energy-streaming` with cores and memory in use. Click it → **Streaming Queries** tab → **13 active queries**:
  1. weather passthrough
  2. rss passthrough
  3. market passthrough
  4. incidents (NLP)
  5. feedback (NLP)
  6. meters_raw
  7. meters_aggregated_15min
  8-13. data_quality × 6 (one per topic)

  **If Running Applications shows 0:** the `SPARK_MASTER` env var didn't propagate. Re-run 5.2 — the `-e` flags must go between `docker exec` and the container name, not after.

### Phase 6 — Wait for windows + run ML [5 min]

- [ ] **6.1** Wait 3 min after Spark boot, then check Mongo counts
  ```powershell
  docker exec mongodb-pseudo mongosh -u energy_admin -p change-me-before-deploy --authenticationDatabase admin energy_db --quiet --eval "['meters_raw','meters_aggregated_15min','data_quality_metrics','incidents','feedback_nlp','rss_feeds','weather','market_prices'].forEach(c => print(c + ': ' + db[c].countDocuments()))"
  ```
  Expected: `meters_raw > 100`, `meters_aggregated_15min ≥ 4`, `data_quality_metrics ≥ 5`. If aggregates stuck at 0 after 5 min, the watermark didn't advance — Spark didn't get the `-e AGGREGATION_*` env vars.

- [ ] **6.2** Run ML pipeline
  ```powershell
  python -m ml.run_pipeline
  ```
  Expected:
  - 3-algo comparison table prints (LinReg / RF / GB) with RMSE/MAE/R²/inference time
  - `[OK] Wrote metrics to ml/metrics.json`
  - `Wrote 72 prediction docs to ml_predictions`
  - `Logged N alert events` (typically 20–60)

### Phase 7 — Dashboard [2 min]

- [ ] **7.1** T9 — backend
  ```powershell
  cd Y:\energy-big-data\dashboard\backend
  npm run dev
  ```
  Expected: `[mongo_client] Pool connecté…` then `[api] listening on http://localhost:4000`.

- [ ] **7.2** T10 — frontend
  ```powershell
  cd Y:\energy-big-data\dashboard\frontend
  npm run dev
  ```
  Expected: `VITE v5.x ready in ~800ms → http://localhost:5173/`.

- [ ] **7.3** Browser — log in as `admin / admin123` at http://localhost:5173/
  Expected: redirect to Overview. No 401. KPIs populated.

### Phase 8 — Per-page verification [5 min]

For each page, check: **(a) loads without 500, (b) no yellow demo banner, (c) values look sane.**

| Page | What to see | Pass criterion |
|---|---|---|
| **Overview** | 5 KPIs growing on 30 s refresh, Recent Alerts with "CRITICAL Zone X forecast …", real wordcloud | No yellow banner, alerts mention real zones |
| **Heatmap** | 4 zone cards with %, kWh, meters, incidents | All 4 zones rendered, values change on refresh |
| **Predictions** | Zone + hour dropdowns, line chart with actual + dashed forecast | Forecast line visible. If empty, re-run `ml.run_pipeline` |
| **Incidents** | Wordcloud (zone/voltage/transformer…), severity bars, 20 incidents listed | 20 incidents with real producer descriptions |
| **Alerts** | Active alerts list, severity colors, filter pills | Counter ≥ 1 if ML ran (no fake "3 active alerts") |
| **Data Quality** | 6 cards (one per topic) | Each card shows ≥ 1 metric %, timestamp < 5 min. `market-prices` may show "Waiting…" — acceptable, 1 h cadence |
| **Settings** | GDPR TTL table (90/365/730 d), system info, admin role chip | All 9 retention rows rendered |

**If any page shows the yellow "demo data" banner OR DEMO-prefixed content, the corresponding collection is empty.** Re-run Spark + ML before defense.

---

## 7. Defense day

### Demo order (~15 min)

| # | Time | Open | Show | Say (FR) |
|---|---|---|---|---|
| 1 | 2 min | Architecture (§2) | The 5-layer picture | "Données → Kafka → Spark → Mongo → Dashboard. ML séparé." |
| 2 | 30 s | 6 producer terminals tiled | Live `→ smart-meters` lines | "Six producteurs Python, un par source." |
| 3 | 2 min | **Kafka UI :8090** | Brokers (1 alive) → Topics (6 + 2 internal) → `smart-meters` → Messages → expand a JSON. Then Schema Registry tab → 6 subjects | "smart-meters: 3 partitions pour paralléliser. Schema Registry en BACKWARD pour évolution sans rupture." |
| 4 | 2 min | **Spark UI :8080** | Workers → running app → Streaming Queries (13 actives) | "13 requêtes : 7 traitement + 6 qualité par topic. Watermark 2 min, fenêtre 15 min en prod, 2 min en démo." |
| 5 | 3 min | **MongoDB Compass** (see §4.7) — fallback: T1 → `python -m storage.healthcheck` | Click `meters_aggregated_15min` → Documents, Indexes, Validation tabs. Then `dashboard_alerts` for audit trail. Then `ml_predictions` for forecasts | "11 collections, validateurs `$jsonSchema`, index composé `zone + window_start`, TTL pour GDPR. IXSCAN partout, jamais COLLSCAN." |
| 6 | 2 min | `ml/metrics.json` + dashboard `/predictions` | 3-model comparison + dashed forecast line | "Comparaison LinReg / RF / GradientBoosting. GB gagne R²=0.87, RMSE 0.0008, inférence ~3 µs/échantillon." |
| 7 | 3 min | Dashboard **:5173** | Walk 7 pages: Overview → Heatmap → Predictions → Incidents → Alerts → Data Quality → Settings | "Dashboard React + Express, JWT cookie auth, rôles admin/operator." |
| 8 | 1 min | `.env.example` + `docker/` + `/settings` GDPR table | 3 compose files + TTL retention | "Trois modes : local / pseudo / distribué (RF=3, ISR=2). TTL différenciés : 90j brut, 365j agrégats, 730j incidents (audit légal)." |

### If the prof asks…

| Question (FR) | Open | Say |
|---|---|---|
| « Montrez-moi Kafka » | Kafka UI :8090 | "6 topics, smart-meters a 3 partitions pour paralléliser." |
| « Et le Schema Registry ? » | Kafka UI → Schema Registry | "Compatibilité BACKWARD — évolution sans casser les producteurs." |
| « Spark, comment ça marche ? » | Spark UI :8080 → Streaming Queries | "13 requêtes streaming, mode append, watermark 2 min, fenêtre paramétrable." |
| « Pourquoi pas de stream-stream join ? » | (verbal) | "Fragile en Spark 3.5 avec watermarks chaînés. J'enrichis côté Mongo via `$lookup`. Spark = vélocité, Mongo = jointure de variété." |
| « MongoDB ? » | Compass ou `/settings` | "11 collections, validateurs `$jsonSchema`, TTL différenciés GDPR, index composé `zone + window_start`." |
| « Pourquoi MongoDB et pas Cassandra ? » | (verbal) | "Modèle document = JSON Kafka. Validateurs natifs. Requêtes ad-hoc filtrées du dashboard meilleures qu'avec Cassandra (clé de partition figée)." |
| « Les index sont-ils utilisés ? » | T1 → `python -m storage.healthcheck` | "Toutes les requêtes du dashboard hit IXSCAN, jamais COLLSCAN." |
| « Et le ML ? » | `ml/metrics.json` + `/predictions` | "3 algos comparés. Gradient Boosting gagne R²=0.87. Inférence ~3 µs/sample." |
| « Features utilisées ? » | (verbal) | "lag_1, lag_4, moyenne/écart-type glissants, heure, jour, one-hot zone. 116 fenêtres réelles + 2 688 synthétiques sur 7 jours." |
| « Pourquoi pas de réseau de neurones ? » | (verbal) | "Volume insuffisant, données tabulaires, gradient boosting suffisant. Défendable en 6 min de soutenance." |
| « Data quality ? » | `/quality` page | "6 cartes, une par topic. Complétude, bruit, anomalies, couverture temporelle par fenêtre 15 min." |
| « Bruit comment c'est défini ? » | (verbal) | "Spécifique au domaine : tension hors [210, 250] V pour les compteurs, température hors [-40, 55] °C pour la météo, impact_score hors [0, 1] pour RSS." |
| « RGPD ? » | `/settings` GDPR table | "TTL : meters_raw 90j, agrégats 365j, incidents 730j (audit légal). `meter_id` pseudonymisé, jamais le nom du client." |
| « Trois modes d'install ? » | `.env.example` + `docker/` | "Local pour dev (sans persistance), Pseudo pour démo (1 broker persistant), Distribué (3 brokers RF=3 MIN_ISR=2)." |
| « Pourquoi 15 minutes en prod ? » | `.env.example:86` | "Intervalle de règlement du marché européen de l'énergie. En démo j'override à 2 min via variable d'environnement." |

---

## 8. Pre-defense checklist

Run **30 min before the prof arrives**. Tick each:

```
[ ] Battery 100%, charger plugged, Windows notifications OFF
[ ] Slack / Discord / Gmail tabs closed, phone airplane mode
[ ] Universal opening done in T1
[ ] docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d
[ ] Wait 60 s → docker compose ps → all healthy
[ ] python -m storage.schema_validators --apply-all → 11/11 OK
[ ] python -m storage.healthcheck → IXSCAN-only
[ ] python -m ingestion.schemas.register_schemas → 6 subjects
[ ] T2-T7: 6 producers started (weather FIRST)
[ ] python -m ingestion.consumers.verify_topics --max-seconds 60 --no-require-each → [PASS]
[ ] T8: docker exec spark-master-pseudo bash -c "rm -rf /tmp/spark-checkpoints/*"
[ ] T8: spark-submit with -e AGGREGATION_WINDOW="2 minutes" → "Tous les flux sont démarrés"
[ ] Spark UI :8080 → 13 streaming queries running
[ ] Wait 3 min → mongo: meters_aggregated_15min ≥ 4 docs
[ ] python -m ml.run_pipeline → 72 predictions written
[ ] T9 backend: npm run dev → [api] listening on :4000
[ ] T10 frontend: npm run dev → http://localhost:5173/
[ ] Browser: log in admin/admin123 → click ALL 7 pages → no yellow banner anywhere
[ ] MongoDB Compass: click Connect on saved 'energy-big-data' favourite → green dot, 11 collections visible
[ ] Compass pre-flight: open meters_aggregated_15min (Documents tab), dashboard_alerts (Documents), ml_predictions (Documents) so the right tab is remembered for the live demo
[ ] Open these tabs in advance:
    - http://localhost:8090   (Kafka UI)
    - http://localhost:8080   (Spark UI)
    - http://localhost:5173   (Dashboard)
    - MongoDB Compass         (connected, energy_db expanded)
    - file ml/metrics.json    (in VS Code)
    - this README §7          (Q&A script)
[ ] 3 deep breaths.
```

---

## 9. Stop cleanly

In order:

```powershell
# 1. Frontend (T10), Backend (T9): Ctrl+C in each
# 2. Producers (T2-T7): Ctrl+C in each
# 3. Spark (T8): Ctrl+C. If stuck:
docker exec spark-master-pseudo pkill -9 -f spark-submit

# 4. Stop infra (preserves data — recommended):
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down

# 5. (Optional) Wipe data — only if you want a 100% fresh restart:
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down -v
```

---

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Producer `'charmap' codec can't encode '→'` | Windows cp1252 | `$env:PYTHONIOENCODING="utf-8"` in every new terminal |
| `MongoServerError: Authentication failed` | Volume has old credentials | `down -v && up -d` (wipes data) |
| Kafka refuses to start after editing `.env` | `KAFKA_CLUSTER_ID` changed while volume exists | `down -v` then `up -d` |
| `meters_aggregated_15min` stuck at 0 after 5 min | Spark window override missing | Re-submit Phase 5 with both `-e AGGREGATION_WINDOW` AND `-e AGGREGATION_WATERMARK` |
| Spark `ClassNotFoundException: KafkaSourceProvider` | `--packages` flag missing | Use exact command in Phase 5 |
| Spark `ModuleNotFoundError: numpy` | Custom Spark image not built | `docker compose -f docker/docker-compose.local.yml --env-file .env build` |
| Spark `winutils.exe not found` | You ran `python -m processing.main` on host | Use `docker exec spark-master-<mode> /opt/spark/bin/spark-submit ...` |
| Spark `Authentication failed` on Mongo | `MONGO_SPARK_PASS` ≠ `MONGO_PASSWORD` | Make them identical, recreate Mongo volume |
| Spark "Incomplete log file" on offsets/ | Dirty shutdown corrupted checkpoint | `docker exec spark-master-pseudo bash -c "rm -rf /tmp/spark-checkpoints/*"` then re-submit |
| Dashboard `/predictions` empty | `ml_predictions` is stale | `python -m ml.run_pipeline` |
| Dashboard `/alerts` empty | No CRITICAL forecasts written | Re-run `ml.run_pipeline` |
| Heatmap zones empty | Spark died OR window override missing | Check T8 log, re-submit Phase 5 |
| Kafka UI shows 0 messages | Producers died | Check T2-T7 terminals |
| Frontend won't load | Frontend died | Restart `npm run dev` in T10 |
| Backend `EADDRINUSE :4000` | Old node still running | `Get-Process node \| Stop-Process -Force` |
| `spark-submit` path mangling on Git Bash | Git Bash converts `/opt/...` to Windows paths | **Use PowerShell, not Git Bash.** Or prefix `MSYS_NO_PATHCONV=1` |
| Spark Master UI shows 0 Running Applications | `SPARK_MASTER` env var not passed on `docker exec` (app ran in `local[*]` default) | Re-run Phase 5.2 with both `-e SPARK_MASTER="spark://spark-master:7077"` AND `--master spark://spark-master:7077`. The `-e` flag must come BEFORE the container name |
| Data Quality cards show 0% or >100% | `EXPECTED_PER_15MIN` mismatch with producer cadence | Adjust constants in `processing/data_quality.py` lines 30-37 |
| Dashboard 401 everywhere | Cookie expired | Log in again at :5173 |
| Dashboard 500 on a page | Backend crashed | `Ctrl+C` T9, `npm run dev` again |

---

## 11. Project structure

```
energy-big-data/
├── README.md                            ← This file (single source of truth)
├── requirements.txt                     ← Python deps (pinned)
├── .env.example                         ← Copy to .env, edit
├── .gitignore                           ← includes .venv/, .claude/, *.pkl
│
├── docker/
│   ├── docker-compose.local.yml         ← Dev mode, ephemeral
│   ├── docker-compose.pseudo.yml        ← DEMO MODE (single broker + volumes)
│   ├── docker-compose.distributed.yml   ← 3 brokers, RF=3, MIN_ISR=2
│   ├── Dockerfile.spark                 ← Custom Spark image (numpy + nltk + vader)
│   └── init-mongo.js                    ← Auto-runs on first MongoDB startup
│
├── ingestion/                           ← P1 (Youssef)
│   ├── README.md
│   ├── config/kafka_config.py
│   ├── schemas/                         ← 6 JSON schemas + register_schemas.py
│   ├── producers/                       ← 6 producers + shared_state.py
│   └── consumers/verify_topics.py       ← End-to-end verifier
│
├── processing/                          ← P2 (Marouan)
│   ├── README.md
│   ├── main.py                          ← Starts 13 streams
│   ├── spark_session.py
│   ├── schemas.py
│   ├── data_quality.py                  ← 6 per-topic quality streams
│   ├── streams_weather.py
│   ├── streams_external.py
│   ├── streams_incidents.py
│   ├── streams_feedback.py
│   └── streams_meters.py
│
├── storage/                             ← P3
│   ├── README.md
│   ├── mongo_client.py
│   ├── schema_validators.py
│   ├── healthcheck.py
│   └── init/init-mongo.js
│
├── ml/                                  ← P5
│   ├── README.md
│   ├── run_pipeline.py                  ← 3 algos compared, writes Mongo
│   └── metrics.json                     ← Comparison output (committed for ref)
│
├── dashboard/                           ← P4
│   ├── README.md
│   ├── backend/                         ← Express :4000, JWT, 14 endpoints
│   │   ├── src/
│   │   │   ├── server.js
│   │   │   ├── auth.js
│   │   │   ├── middleware.js
│   │   │   ├── db.js
│   │   │   └── routes/                  ← 8 route files
│   │   └── package.json
│   └── frontend/                        ← React + Vite :5173
│       ├── src/
│       │   ├── pages/                   ← 7 pages
│       │   ├── components/
│       │   └── lib/
│       └── package.json
│
├── ethics/
│   └── GDPR.md                          ← Compliance (Section H)
│
└── docs/
    └── legacy/                          ← Archived RUNBOOK/DEMO_GUIDE/HANDOFF
```

---

## 12. Crash recovery

**Stay calm.** The architecture diagram (§2) + Q&A talking points (§7) cover ~80% of the grade even with everything offline. The prof grades methodology and tool justification above live execution.

| Symptom | Fix (≤ 30 s) |
|---|---|
| Spark died | `docker exec spark-master-pseudo pkill -9 -f spark-submit` then `docker exec spark-master-pseudo bash -c "rm -rf /tmp/spark-checkpoints/*"` then redo Phase 5 |
| Producer died | `Ctrl+C` then re-run the producer command in that terminal |
| Dashboard 401 everywhere | Cookie expired — log in again at :5173 |
| Dashboard 500 on a page | Backend crashed — `Ctrl+C` T9, `npm run dev` again |
| Mongo "Authentication failed" | Volume has old creds. Last resort: `down -v` (wipes data — only if everything else fails) |
| Everything is on fire | Switch to "I'll walk you through the architecture" mode. Open §2 fullscreen. Talk diagram for 5 min. Recover at the next checkpoint. |

---

## 13. Team & roles

| Role | Owner | Folder | Detail |
|---|---|---|---|
| **P1** — Infrastructure & Ingestion | Youssef | `ingestion/` + `docker/` | Kafka cluster (KRaft), 6 producers, JSON schemas, Schema Registry, end-to-end verifier |
| **P2** — Spark Streaming | Marouan | `processing/` | 13 streams: 7 processing + 6 per-topic data quality, NLP via VADER, 15-min windowed aggregation |
| **P3** — MongoDB Storage | — | `storage/` | 11 collections, `$jsonSchema` validators, composite indexes, TTL retention, healthcheck |
| **P4** — Dashboard | Meryem | `dashboard/` | React + Vite + Express, JWT cookie auth, 7 pages, 14 endpoints, Recharts |
| **P5** — ML | — | `ml/` | 3 algos compared (LinReg / RF / GB), writes `ml_predictions` + `dashboard_alerts` |

Each `<role>/README.md` answers the prof's per-section Q&A.

Cross-cutting:
- [`ethics/GDPR.md`](ethics/GDPR.md) — Compliance, legal basis, retention, rights (Section H)
- [`docs/legacy/`](docs/legacy/) — Archived RUNBOOK / DEMO_GUIDE / HANDOFF (kept for reference)

---

## Production vs demo override cheat-sheet

| Setting | Production default (`.env.example`) | Demo override (Phase 5 docker exec flag) |
|---|---|---|
| `AGGREGATION_WINDOW` | `15 minutes` | `2 minutes` |
| `AGGREGATION_WATERMARK` | `2 minutes` | `1 minute` |

**Why 15 min in prod?** European energy market settlement interval. Documented in `.env.example:86`. For demo we override via `docker exec -e` so windows close while the prof is watching.

---

**Good luck, Youssef.**
