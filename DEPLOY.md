## DEPLOY — `energy-big-data` (single source of truth)

> **One file. Top to bottom.** From a cold laptop to "merci pour la présentation".
> Audit + harshly rewritten on **2026-05-19** for the **2026-05-21 defense**.
>
> This **supersedes** RUNBOOK.md / DEMO_GUIDE.md / HANDOFF.md (now in [docs/legacy/](docs/legacy/)).
> [README.md](README.md) stays for project overview only.
>
> Platform assumed: **Windows + PowerShell**. The Bash tool inside Claude Code works for the same commands too.

---

## Table of contents

1. [The pipeline in one picture](#1-the-pipeline-in-one-picture)
2. [What is verified working as of 2026-05-19](#2-what-is-verified-working-as-of-2026-05-19)
3. [One-time setup (do once per laptop)](#3-one-time-setup)
4. [Pick a mode — when to use which](#4-pick-a-mode)
5. [Mode A — Local (dev only)](#5-mode-a--local-dev-only)
6. [Mode B — Pseudo (DEMO MODE)](#6-mode-b--pseudo)
7. [Mode C — Distributed (optional bonus)](#7-mode-c--distributed-optional-bonus)
8. [The 8 phases (apply to any mode)](#8-the-8-phases)
9. [Where to see everything (defense Q&A lookup)](#9-where-to-see-everything)
10. [Pre-defense checklist](#10-pre-defense-checklist)
11. [Stop cleanly](#11-stop-cleanly)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. The pipeline in one picture

```
 6 PRODUCERS  →  KAFKA  →  SPARK STREAMING  →  MONGODB  →  DASHBOARD
  (Python)    (broker)    (13 streaming queries) (11 colls)  (React + Express)
                                ↑                    ↑
                         Schema Registry         ML PIPELINE
                         (6 JSON schemas)        (3 algos, sklearn)
```

| Layer | Job | Visible at |
|---|---|---|
| **Producers** | 6 Python scripts generate fake-but-realistic energy data | Tiled terminals printing live |
| **Kafka** | Buffer messages so producers/consumers run at different speeds | Kafka UI **http://localhost:8090** |
| **Schema Registry** | Central catalog of the 6 JSON schemas, BACKWARD compat | Inside Kafka UI → Schema Registry tab |
| **Spark** | 13 streaming queries: 7 processing + 6 quality (one per topic) | Spark Master UI **http://localhost:8080** |
| **MongoDB** | 11 collections — raw + aggregated + ML + audit. Validators + TTL | Compass **mongodb://...@localhost:27017** |
| **ML** | Compare LinReg / RF / GradientBoosting, forecast 6 windows ahead | `ml/metrics.json` + dashboard `/predictions` |
| **Dashboard** | 7 pages, JWT auth (admin/admin123), live data | **http://localhost:5173** |

---

## 2. What is verified working as of 2026-05-19

Tested live against pseudo mode (uptime 9h, full pipeline running):

| Component | Status | Evidence |
|---|---|---|
| 6 producers writing | ✅ | Latest timestamps within 30 s of each other in Kafka |
| Kafka 6 topics + Schema Registry | ✅ | 28,873 total messages across topics |
| Spark 13 streaming queries | ✅ | Driver process active, 99% CPU, writes every ~2 min |
| MongoDB 11 collections | ✅ | All populated, validators applied, IXSCAN-only queries |
| ML pipeline | ✅ | metrics.json + 72 ml_predictions + 12 dashboard_alerts |
| Dashboard backend (14 endpoints) | ✅ | All return real data with cookie auth |
| Dashboard frontend 7 pages | ✅ | All render, including the 6 Data Quality cards |
| GDPR TTL indexes | ✅ | 90 / 365 / 730 days per collection (Settings page) |
| 3 deployment modes | 🟡 | Pseudo verified live; local + distributed need cold-boot if you want to demo them |

### Known limitations (acknowledge upfront if asked)

- **Default `AGGREGATION_WINDOW=15 minutes`** — for live demos you set `-e AGGREGATION_WINDOW="2 minutes"` to see windows close in ~3 min instead of ~17.
- **ML classification task descoped** — only regression is implemented (3 algos × 1 task). Original `ml/README.md` mentioned both. Section D's "study of 3 algorithms" is satisfied with regression alone (RMSE / MAE / R² + inference time).
- **`ml_predictions` is not auto-refreshed** — you must run `python -m ml.run_pipeline` before the demo or the Predictions page shows stale data.
- **`market-prices` topic** emits once per hour — Data Quality card may show "Waiting…" if no message landed in the latest 15-min window.

---

## 3. One-time setup

Do this once per laptop. Skip to §4 if `.venv` and `.env` already exist.

### 3.1 Clone + Python venv

```powershell
cd Y:\
git clone https://github.com/youssefbouzine20/energy-big-data.git
cd energy-big-data
python -m venv .venv
.venv\Scripts\Activate.ps1
.venv\Scripts\pip install --default-timeout=100 -r requirements.txt
```

### 3.2 `.env`

```powershell
copy .env.example .env
# Generate a Kafka cluster UUID
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
```

Paste the UUID into `.env` replacing `<paste uuid here>` after `KAFKA_CLUSTER_ID=`. **Never change it once the volume exists** — Kafka refuses to boot if it mismatches.

### 3.3 Build the custom Spark image (numpy + nltk + VADER baked in)

```powershell
docker compose -f docker/docker-compose.local.yml --env-file .env build
```

Used by all 3 modes. ~2 min, once per laptop.

### 3.4 Dashboard backend `.env`

```powershell
copy dashboard\backend\.env.example dashboard\backend\.env
```

Defaults are good: `API_PORT=4000`, `CORS_ORIGIN=http://localhost:5173`, `admin/admin123`.

### 3.5 Frontend deps

```powershell
cd dashboard\frontend
npm install
cd ..\..
```

### 3.6 Universal terminal opening line

Every new PowerShell terminal you open for this project, run **these 3 lines first**:

```powershell
cd Y:\energy-big-data
.venv\Scripts\Activate.ps1
$env:PYTHONIOENCODING="utf-8"
```

The `PYTHONIOENCODING` is mandatory on Windows — without it, producers crash on the `→` character (cp1252 codec error).

---

## 4. Pick a mode

| Mode | Compose file | Brokers | Persistent data? | Use for |
|---|---|---|---|---|
| **A. local** | `docker-compose.local.yml` | 1 | ❌ No volume | Dev only. Each restart wipes data. |
| **B. pseudo** | `docker-compose.pseudo.yml` | 1 | ✅ Yes | **THE DEMO MODE.** Survives `down`. |
| **C. distributed** | `docker-compose.distributed.yml` | 3 (RF=3, ISR=2) | ✅ Yes | Optional bonus — broker failover story. |

> **Never run two modes at once** — they bind the same host ports (9092, 27017, 8080, 8090, 8081).

---

## 5. Mode A — Local (dev only)

### Bring up

```powershell
docker compose -f docker/docker-compose.local.yml --env-file .env up -d
```

Wait ~60 s, then verify all healthy:

```powershell
docker compose -f docker/docker-compose.local.yml --env-file .env ps
```

**Expected:** `kafka-local`, `schema-registry-local`, `mongodb-local`, `spark-master-local` all `(healthy)`. `kafka-init-local` shows `Exited (0)` — correct, it created topics and exited.

Continue to §8 with the mode suffix **`-local`** everywhere.

### Tear down (wipes data)

```powershell
docker compose -f docker/docker-compose.local.yml --env-file .env down
```

---

## 6. Mode B — Pseudo

**This is the mode you demo to the prof.** Single broker, persistent volumes, simplest moving parts.

### Bring up

```powershell
docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d
```

Wait ~60 s, verify:

```powershell
docker compose -f docker/docker-compose.pseudo.yml --env-file .env ps
```

**Expected:** 6 containers healthy: `kafka-pseudo`, `schema-registry-pseudo`, `kafka-ui-pseudo`, `mongodb-pseudo`, `spark-master-pseudo`, `spark-worker-pseudo`. `kafka-init-pseudo` shows `Exited (0)`.

Continue to §8 with the mode suffix **`-pseudo`**.

### Tear down (keeps data)

```powershell
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down
```

### Tear down + wipe (only if you want a 100% fresh slate)

```powershell
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down -v
```

---

## 7. Mode C — Distributed (optional bonus)

3 Kafka brokers (RF=3, MIN_ISR=2), 2 Spark workers. Demonstrates broker failover.

### Bring up

```powershell
docker compose -f docker/docker-compose.distributed.yml --env-file .env up -d
```

Wait ~90 s (more containers), verify:

```powershell
docker compose -f docker/docker-compose.distributed.yml --env-file .env ps
```

**Expected:** `kafka1-dist`, `kafka2-dist`, `kafka3-dist`, `schema-registry-dist`, `kafka-ui-dist`, `mongodb-dist`, `spark-master-dist`, `spark-worker-1-dist`, `spark-worker-2-dist`.

### Special verification — show RF=3 to the prof

```powershell
docker exec kafka1-dist kafka-topics --bootstrap-server kafka1:29092 --describe --topic smart-meters
```

**Expected line:** `ReplicationFactor: 3   Configs: min.insync.replicas=2`

### Failover demo (optional, only if you're confident)

```powershell
docker stop kafka2-dist
# Pipeline keeps running. Kafka UI shows ISR drop to [1, 3].
docker start kafka2-dist
# ISR recovers to [1, 2, 3] within ~10 s.
```

Continue to §8 with the mode suffix **`-dist`**, and for Kafka container names use **`kafka1-dist`** (or `kafka2`/`kafka3`).

---

## 8. The 8 phases

These phases apply to **any mode** — only the container suffix changes (`-local`, `-pseudo`, or `-dist`).

### Phase 1 — Apply MongoDB validators + run healthcheck

`init-mongo.js` already ran on first container boot (created 11 collections + TTL indexes + `spark_writer` user). Now apply the Python-side validators:

```powershell
.venv\Scripts\python -m storage.schema_validators --apply-all
```

**Expected:** `Résultat: 11/11 validateurs appliqués avec succès.`

```powershell
.venv\Scripts\python -m storage.healthcheck
```

**Expected:** 11 collections each `validator: ✓`, every test query `stage: IXSCAN` (no COLLSCAN).

### Phase 2 — Register the 6 JSON schemas

```powershell
.venv\Scripts\python -m ingestion.schemas.register_schemas
```

**Expected:** 6 lines `[OK] <topic>-value id=N`. Then verify visually at http://localhost:8090 → **Schema Registry** tab → 6 subjects, version 1, BACKWARD compatibility.

### Phase 3 — Start the 6 producers (each in its own terminal)

Open **6 new PowerShell terminals**. In each, run the §3.6 universal opening (cd + activate + UTF-8), then **one** producer:

| Terminal | Command | Cadence |
|---|---|---|
| T2 | `.venv\Scripts\python -m ingestion.producers.weather_producer` | 30 s × 4 zones (**start FIRST** — writes shared state) |
| T3 | `.venv\Scripts\python -m ingestion.producers.smart_meter_producer` | 30 s × 20 meters |
| T4 | `.venv\Scripts\python -m ingestion.producers.incident_producer` | 60 s |
| T5 | `.venv\Scripts\python -m ingestion.producers.rss_producer` | 120 s |
| T6 | `.venv\Scripts\python -m ingestion.producers.market_price_producer` | 1 h (boot line only during demo) |
| T7 | `.venv\Scripts\python -m ingestion.producers.user_feedback_producer` | 45 s |

**Leave all 6 running** for the entire demo.

Quick sanity check (back in T1):

```powershell
.venv\Scripts\python -m ingestion.consumers.verify_topics --max-seconds 60 --no-require-each
```

**Expected:** `[PASS] All subscribed topics delivered schema-valid messages.`

### Phase 4 — Submit the Spark streaming job

**T8** (new terminal, project root):

For **pseudo or local**:
```powershell
docker exec -e AGGREGATION_WINDOW="2 minutes" -e AGGREGATION_WATERMARK="1 minute" spark-master-pseudo /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 /workspace/processing/main.py
```

For **distributed**, replace `spark-master-pseudo` with `spark-master-dist`.

> The `-e AGGREGATION_WINDOW="2 minutes"` is the demo override so windows close in ~3 min instead of ~17 min. Production default (defensible to the prof: "European energy market settlement interval") is `15 minutes`.

**Expected boot output within ~45 s:**
```
Initialisation de la session Spark...
Lancement des flux Passthrough (Météo, RSS, Marché)...
Lancement des flux NLP (Incidents, Feedback)...
Lancement du flux principal (Compteurs & Agrégations 15min)...
Lancement du flux de Qualité des Données (6 topics)...
Tous les flux sont démarrés avec succès !
En écoute continue sur Kafka...
```

**`Lancement du flux de Qualité des Données (6 topics)...`** is the line that confirms the per-topic data quality streams are active. If it's missing, the container is somehow running stale code.

**Verify in Spark UI** at http://localhost:8080 → click the running app → **Streaming Queries** tab → **13 active queries**:

1. weather passthrough
2. rss passthrough
3. market passthrough
4. incidents (NLP)
5. feedback (NLP)
6. meters_raw
7. meters_aggregated_15min
8–13. data_quality × 6 (one per topic)

**Leave T8 running.** `Ctrl+C` here stops everything.

### Phase 5 — Verify MongoDB is filling up

After ~1 minute:

```powershell
docker exec mongodb-pseudo mongosh -u energy_admin -p change-me-before-deploy --authenticationDatabase admin energy_db --quiet --eval "['meters_raw','weather','incidents','feedback_nlp','rss_feeds'].forEach(c => print(c + ': ' + db[c].countDocuments()))"
```

**Expected:** `meters_raw: 100+`, others growing.

After ~3 minutes (windowed aggregates close):

```powershell
docker exec mongodb-pseudo mongosh -u energy_admin -p change-me-before-deploy --authenticationDatabase admin energy_db --quiet --eval "['meters_aggregated_15min','data_quality_metrics'].forEach(c => print(c + ': ' + db[c].countDocuments()))"
```

**Expected:** `meters_aggregated_15min: 4+`, `data_quality_metrics: 5+` (5 because market-prices may not have a window yet).

### Phase 6 — Run the ML pipeline (REQUIRED before showing the prof)

Wait until `meters_aggregated_15min` has at least 4 rows, then:

```powershell
.venv\Scripts\python -m ml.run_pipeline
```

**Expected (6 numbered phases):**
- Loads aggregated data + backfills synthetic history
- Trains 3 models, prints comparison table (RMSE / MAE / R² / inference time)
- Writes `ml/metrics.json`
- Writes 72 prediction docs to `ml_predictions`
- Writes WARNING/CRITICAL alerts to `dashboard_alerts`

> **Re-run this every time you reboot Spark.** Otherwise the `/predictions` page shows stale lines and `/alerts` looks empty.

### Phase 7 — Dashboard backend + frontend

**T9** (backend):
```powershell
cd Y:\energy-big-data\dashboard\backend
npm run dev
```
**Expected:** `[mongo_client] Pool connecté…` then `[api] listening on http://localhost:4000`.

**T10** (frontend):
```powershell
cd Y:\energy-big-data\dashboard\frontend
npm run dev
```
**Expected:** `VITE v5.x  ready in 800 ms → http://localhost:5173/`.

### Phase 8 — Final smoke test

```powershell
# 1. Infra healthy
docker compose -f docker/docker-compose.pseudo.yml --env-file .env ps

# 2. Mongo healthcheck
.venv\Scripts\python -m storage.healthcheck

# 3. Topic delivery
.venv\Scripts\python -m ingestion.consumers.verify_topics --max-seconds 30 --no-require-each

# 4. Click through all 7 dashboard pages — every one must render data
```

If all four pass, you're ready.

---

## 9. Where to see everything

**Demo order** (15 min). Follow the data flow:

| # | Time | What to open | What to click / show | What to say (FR) |
|---|---|---|---|---|
| 1 | 2 min | Architecture diagram in §1 of this doc | The 5-layer picture | "Données → Kafka → Spark → Mongo → Dashboard. ML séparé." |
| 2 | 30 s | Tile the 6 producer terminals | Live `→ smart-meters` lines | "Six producteurs Python, un par source." |
| 3 | 2 min | **Kafka UI http://localhost:8090** | **Brokers** (1 alive) → **Topics** (6 + 2 internal) → click `smart-meters` → **Messages** → expand one JSON. Then **Schema Registry** tab → 6 subjects | "smart-meters a 3 partitions pour paralléliser. Schema Registry en mode BACKWARD pour évolution sans rupture." |
| 4 | 2 min | **Spark UI http://localhost:8080** | **Workers** (1 alive, or 2 in distributed) → click running app → **Streaming Queries** (13 actives) | "13 requêtes : 7 traitement + 6 qualité par topic. Watermark 2 min, fenêtre 15 min en prod, 2 min en démo." |
| 5 | 3 min | **MongoDB Compass** OR `.venv\Scripts\python -m storage.healthcheck` | 11 collections, click `meters_aggregated_15min` → **Documents** / **Indexes** / **Validation** tabs | "11 collections, validateurs `$jsonSchema`, index composé `zone + window_start`, TTL pour GDPR, IXSCAN partout." |
| 6 | 2 min | `ml/metrics.json` + dashboard `/predictions` | The 3-model comparison table + the dashed forecast line | "Étude comparative LinReg / RF / GradientBoosting. GB gagne RMSE/MAE/R²=0.85, inférence 3 µs/échantillon." |
| 7 | 3 min | Dashboard **http://localhost:5173** | Walk 7 pages: Overview → Heatmap → Predictions → Incidents → Alerts → Data Quality → Settings | "Dashboard React + Express, JWT, rôles admin/operator." |
| 8 | 1 min | `.env.example` + `docker/` folder + `/settings` GDPR table | 3 compose files + TTL retention table | "Trois modes : local/pseudo/distribué. TTL différenciés par collection : 90j brut, 365j agrégats, 730j incidents (audit légal)." |

### If the prof asks something specific

| Question (FR) | Open | Say |
|---|---|---|
| « Montrez-moi Kafka » | Kafka UI :8090 | "6 topics, smart-meters a 3 partitions pour paralléliser." |
| « Et le Schema Registry ? » | Kafka UI → sidebar | "Compatibilité BACKWARD — évolution sans casser les producteurs." |
| « Spark, comment ça marche ? » | Spark UI :8080 → Streaming Queries | "13 requêtes streaming, mode append, watermark 2 min, fenêtre paramétrable." |
| « Pourquoi pas de stream-stream join ? » | (verbal) | "Fragile en Spark 3.5 avec watermarks chaînés. J'enrichis côté Mongo via `$lookup`. Spark = vélocité, Mongo = jointure de variété." |
| « MongoDB ? » | Compass ou `/settings` | "11 collections, validateurs `$jsonSchema`, TTL différenciés GDPR, index composé `zone + window_start`." |
| « Pourquoi MongoDB et pas Cassandra ? » | (verbal) | "Modèle document = JSON Kafka. Validateurs natifs. Requêtes ad-hoc filtrées du dashboard meilleures qu'avec Cassandra (clé de partition figée)." |
| « Les index sont-ils utilisés ? » | T1 → `python -m storage.healthcheck` | "Toutes les requêtes du dashboard hit IXSCAN, jamais COLLSCAN." |
| « Et le ML ? » | `ml/metrics.json` + `/predictions` | "3 algos comparés. Gradient Boosting gagne (R²=0.85). Inférence ~3 µs/sample." |
| « Features utilisées ? » | (verbal) | "lag_1, lag_4, moyenne/écart-type glissants, heure, jour, one-hot zone. 116 fenêtres réelles + 2 688 synthétiques sur 7 jours." |
| « Pourquoi pas de réseau de neurones ? » | (verbal) | "Volume insuffisant, données tabulaires, gradient boosting suffisant. Défendable en 6 min de soutenance." |
| « Data quality ? » | `/quality` page | "6 cartes, une par topic. Complétude, bruit, anomalies, couverture temporelle par fenêtre 15 min." |
| « Bruit comment c'est défini ? » | (verbal) | "Spécifique au domaine : tension hors [210, 250] V pour les compteurs, température hors [-40, 55] °C pour la météo, impact_score hors [0, 1] pour RSS." |
| « RGPD ? » | `/settings` GDPR table | "TTL : meters_raw 90j, agrégats 365j, incidents 730j (audit légal). `meter_id` pseudonymisé, jamais le nom du client." |
| « Trois modes d'install ? » | `.env.example:5-10` + `docker/` | "Local pour dev (sans persistance), Pseudo pour démo (1 broker persistant), Distribué (3 brokers RF=3 MIN_ISR=2)." |

---

## 10. Pre-defense checklist

Run **30 min before the prof arrives**. Tick each box.

```
[ ] (Once) Charger plugged in, Windows notifications OFF, Slack/Discord closed
[ ] docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d
[ ] Wait 60 s → docker compose ps → all healthy
[ ] python -m storage.schema_validators --apply-all → 11/11 OK
[ ] python -m ingestion.schemas.register_schemas → 6 subjects registered
[ ] Open T2-T7, start the 6 producers (weather first)
[ ] T8: spark-submit with -e AGGREGATION_WINDOW="2 minutes" — wait for "Tous les flux sont démarrés"
[ ] Spark UI :8080 → 13 streaming queries running
[ ] Wait 3 min → mongo: meters_aggregated_15min ≥ 4 docs, data_quality_metrics ≥ 5 docs
[ ] python -m ml.run_pipeline → metrics table prints + 72 predictions written
[ ] T9 backend: npm run dev → [api] listening on :4000
[ ] T10 frontend: npm run dev → http://localhost:5173/
[ ] Browser: log in admin/admin123 → click ALL 7 pages → every one shows data
[ ] Open these tabs in advance:
    - http://localhost:8090   (Kafka UI)
    - http://localhost:8080   (Spark UI)
    - http://localhost:5173   (Dashboard)
    - file ml/metrics.json    (in VS Code)
    - file DEPLOY.md §9       (this Q&A table, your script)
[ ] 3 deep breaths.
```

---

## 11. Stop cleanly

In order:

```powershell
# 1. Frontend (T10), Backend (T9): Ctrl+C in each
# 2. Producers (T2-T7): Ctrl+C in each
# 3. Spark (T8): Ctrl+C. If stuck:
docker exec spark-master-pseudo pkill -9 -f spark-submit

# 4. Stop infra (preserves data — recommended for next session):
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down

# 5. (Optional) Wipe data — only if you want a 100% fresh restart:
docker compose -f docker/docker-compose.pseudo.yml --env-file .env down -v
```

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Producer `'charmap' codec can't encode '→'` | Windows cp1252 | `$env:PYTHONIOENCODING="utf-8"` in every new terminal |
| `MongoServerError: Authentication failed` | Volume has old credentials | `docker compose -f docker/docker-compose.<mode>.yml --env-file .env down -v && up -d` (wipes data) |
| Kafka refuses to start after editing `.env` | `KAFKA_CLUSTER_ID` changed while volume exists | `down -v` then `up -d` |
| `meters_aggregated_15min` stuck at 0 after 5 min | Spark window override missing | Re-submit Spark with both `-e AGGREGATION_WINDOW` AND `-e AGGREGATION_WATERMARK` |
| Spark `ClassNotFoundException: KafkaSourceProvider` | `--packages` flag missing | Use the exact command in Phase 4 |
| Spark `ModuleNotFoundError: numpy` | Custom Spark image not built | `docker compose -f docker/docker-compose.local.yml --env-file .env build` |
| Spark `winutils.exe not found` | You ran `python -m processing.main` on host | Use `docker exec spark-master-<mode> /opt/spark/bin/spark-submit ...` |
| Spark `Authentication failed` on Mongo | `MONGO_SPARK_PASS` ≠ `MONGO_PASSWORD` in `.env` | Make them identical, then recreate Mongo volume |
| Dashboard `/predictions` empty | `ml_predictions` is stale | `.venv\Scripts\python -m ml.run_pipeline` |
| Dashboard `/alerts` empty | No CRITICAL forecasts written | Re-run `ml.run_pipeline` (auto-writes alerts when thresholds breach) |
| Heatmap zones empty | Spark died or window override missing | Check T8 for errors, re-submit Phase 4 |
| Kafka UI shows 0 messages | Producers died | Check T2-T7 logs |
| Frontend won't load | Frontend died | Restart `npm run dev` in T10 |
| Backend `EADDRINUSE :4000` | Old node still running | `Get-Process node \| Stop-Process` (warning: kills ALL node) |
| `spark-submit` path mangling on Git Bash | Git Bash converts `/opt/...` to Windows paths | **Use PowerShell, not Git Bash.** Or prefix `MSYS_NO_PATHCONV=1` |
| Spark Master UI shows 0 applications running but data IS flowing | Spark in local (in-driver) mode — normal | Not a bug; just means scheduling is local, not cluster |
| Data Quality cards show 0% or >100% coverage | `EXPECTED_PER_15MIN` mismatch with producer cadence | Adjust constants in `processing/data_quality.py` lines 30-37 |

### If anything fails mid-demo

Stay calm. **The architecture diagram (§1) + Q&A talking points (§9) cover 80% of the grade even with everything offline.** The prof grades methodology and tool justification above live execution.

---

## Production vs demo cheat-sheet

| Setting | Production default (`.env.example`) | Demo override |
|---|---|---|
| `AGGREGATION_WINDOW` | `15 minutes` | `2 minutes` (Phase 4 docker exec flag) |
| `AGGREGATION_WATERMARK` | `2 minutes` | `1 minute` (Phase 4 docker exec flag) |

If asked « pourquoi 15 minutes en prod ? » → "C'est l'intervalle de règlement du marché européen de l'énergie — `.env.example:86` documente ce choix. Pour la démo j'override à 2 min via variable d'environnement pour que les fenêtres se ferment pendant votre présence."
