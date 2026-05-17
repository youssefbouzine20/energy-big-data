# Team Handoff — `energy-big-data`

> Snapshot taken **2026-05-17**, 4 days before defense (2026-05-21).
> Branch: `p1/infrastructure-ingestion`. Two new commits ship the work
> described below.

This file tells the next teammate **what's done, what's left, and exactly
which commands to run**. It is intentionally short — each role's deep dive
lives in its folder's `README.md`. Use this as your map.

---

## 1. What's in this branch (last 2 commits)

| SHA | Title | Why it matters |
|---|---|---|
| `d171263` | `fix(infra): bring pseudo + distributed Spark services to parity with local` | Marouan's Spark pipeline now runs on **all 3 modes**, not just local. Pseudo + distributed compose files now use the custom `energy/spark:3.5.0` image, mount `/workspace`, set `PYTHONPATH`, and pass the right Kafka/Mongo env vars. Verified end-to-end with a producer round-trip on distributed (800 docs in `meters_raw`). |
| `38df2e8` | `feat(p4): pivot dashboard to React SPA + Express API with JWT auth` | The P4 dashboard is no longer Streamlit — it's a React + TypeScript SPA with a Node/Express REST API. Login page, 7 protected pages, JWT in httpOnly cookie. Backend smoke-tested against Mongo; frontend scaffolded but `npm install` not yet run end-to-end. |

Pull and verify:
```powershell
git pull
git log --oneline -5
# Should show d171263 and 38df2e8 at the top
```

---

## 2. Current state at a glance

| Part | Owner | State | Folder |
|---|---|---|---|
| **P1** Infrastructure & Ingestion | Youssef | ✅ Done | `ingestion/`, `docker/` |
| **P2** Spark Streaming | Marouan | ✅ Done — verified on all 3 modes | `processing/` |
| **P3** MongoDB Storage | _(next reader)_ | 🟡 Partial — `init-mongo.js` covers 11 collections + indexes + TTL. **Missing the Python helpers** (see §4). | `storage/` |
| **P4** React + Express Dashboard | _(next reader)_ | 🟡 Scaffolded — backend tested, frontend NOT `npm install`-verified. (see §6) | `dashboard/` |
| **P5** ML Models | _(next reader)_ | 🔴 Not started — only the README. (see §5) | `ml/` |

Defense rubric (sections D/E/F/H of the spec) — what earns points:
1. ✅ Tool justifications — covered in each role's README
2. ✅ Data quality framework — P2 writes `data_quality_metrics` per window
3. 🔴 3-algorithm comparison with 4 metrics (Precision/Recall/F1/inference) — **P5 owes this**
4. 🟡 Dashboard with 4 mandatory sections — **frontend needs npm install + manual verify**
5. ✅ GDPR + ethics — `ethics/GDPR.md` + `dashboard_alerts` collection ready

---

## 3. Quick start — runs in any mode (5 min)

```powershell
# 1. From the project root
cd Y:\energy-big-data

# 2. Activate venv (already installed)
.venv\Scripts\Activate.ps1

# 3. Pick a mode (local for daily dev, see §7 for when to use which)
docker compose -f docker/docker-compose.local.yml --env-file .env up -d

# 4. Wait ~30s, then verify
docker compose -f docker/docker-compose.local.yml --env-file .env ps
# All entries should show "Up (healthy)" except the few that don't have healthchecks

# 5. Register schemas (one-time per fresh cluster)
.venv\Scripts\python -m ingestion.schemas.register_schemas

# 6. (Optional) Start a producer in one terminal
.venv\Scripts\python -m ingestion.producers.smart_meter_producer

# 7. Start the Spark pipeline in another terminal
docker exec spark-master-local /opt/spark/bin/spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 `
  /workspace/processing/main.py

# Wait for: "Tous les flux sont démarrés avec succès !"
```

For pseudo or distributed, replace `local` with `pseudo` / `distributed`
and `spark-master-local` with `spark-master-pseudo` / `spark-master-dist`.
See the cheat-sheet table in [README.md §5](README.md#-container-names-per-mode-cheat-sheet).

---

## 4. 👋 If you're picking up **P3 (MongoDB Storage)**

**What's already in the repo:**
- `storage/init/init-mongo.js` — creates **11 collections**, `spark_writer` user, **compound + single-field query indexes**, **TTL indexes** for GDPR retention. Auto-runs on first Mongo startup in any mode.
- `storage/README.md` — full design rationale + per-mode mongosh examples (§10).

**What you need to deliver** (listed in `storage/README.md §7`):

| # | File | Purpose |
|---|---|---|
| 1 | `storage/mongo_client.py` | Shared Python helper — `get_db()` returns a connected `MongoClient`. **P4 and P5 both depend on this.** Starter code is in `storage/README.md §8`. |
| 2 | `storage/extend_init.py` | Idempotent script to add validators / extra indexes after `init-mongo.js` has run. Optional but nice. |
| 3 | `storage/replica_setup.md` | Markdown describing how to convert single-node Mongo to a 3-member replica set for production. Required for the defense Rapport (HA story). |
| 4 | `storage/healthcheck.py` | Print collection sizes + index stats + TTL countdown. Nice-to-have for the demo. |

**First thing to do** — write `mongo_client.py` (30 minutes max, code is in the
README). Without it, P5's ML training scripts and the production dashboard
backend can't share a single Mongo connection helper.

**Verifying your work** — for any mode:

```powershell
# Find which mode is running
docker ps --filter "name=mongodb" --format "table {{.Names}}\t{{.Status}}"
# → exactly one of mongodb-local / mongodb-pseudo / mongodb-dist

# Replace <mongodb-container> below with the matching name.
# List all collections
docker exec <mongodb-container> mongosh `
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin `
  --quiet energy_db --eval "db.getCollectionNames().sort().forEach(c => print(c))"
# Should print 11 collection names

# Confirm spark_writer user exists
docker exec <mongodb-container> mongosh `
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin `
  --quiet energy_db `
  --eval "db.getSiblingDB('energy_db').getUsers().users.forEach(u => print(u.user))"
# Should print: spark_writer

# After Spark + producers run for a while, check data is flowing
docker exec <mongodb-container> mongosh `
  -u energy_admin -p change-me-before-deploy --authenticationDatabase admin `
  --quiet energy_db `
  --eval "db.getCollectionNames().forEach(c => print(c.padEnd(28) + ': ' + db[c].countDocuments() + ' docs'))"
# meters_raw grows fastest, meters_aggregated_15min appears after first 15-min window
```

**Or use MongoDB Compass** (GUI, recommended):
1. Download free from https://www.mongodb.com/products/compass
2. Connect with URI: `mongodb://energy_admin:change-me-before-deploy@localhost:27017/?authSource=admin`
3. Same URI works for all 3 modes (host port is always `27017`).

---

## 5. 👋 If you're picking up **P5 (ML)**

**What's already in the repo:**
- `ml/README.md` — complete spec: **3 algorithms × 2 tasks × 4 metrics**, feature engineering recipe, starter code.
- The data is already flowing into MongoDB (`meters_raw` is populated when producers + Spark are running). No need to wait for anything.

**What you need to deliver** (listed in `ml/README.md §8`):

| Order | File | What it does |
|---|---|---|
| 1 | `ml/load_data.py` | Read `meters_raw` + join `weather` / `incidents` / `market_prices` from Mongo into a Pandas DataFrame |
| 2 | `ml/features.py` | One-hot zones, circular-encode hour/day, lag features, scale numerics |
| 3 | `ml/quality.py` | 5 data-quality checks → write to `ml_data_quality` |
| 4 | `ml/train_classifier.py` | LogReg / RF / GB for `is_anomaly` — save `models/anomaly_*.pkl` |
| 5 | `ml/train_regressor.py` | Linear / RF / GB for next-window `consumption_kwh` — save `models/forecast_*.pkl` |
| 6 | `ml/evaluate.py` | Print the **Precision / Recall / F1 / inference-time** comparison table for the Rapport |
| 7 | `ml/predict.py` | Load best models, predict next 15-min window per zone, upsert into `ml_predictions`. Run every 15 min via cron or `ml/scheduler.py`. |

**Critical path**: P4 dashboard's `/predictions` and `/alerts` pages currently
show empty because `ml_predictions` is empty. As soon as `predict.py` upserts
the first forecast docs, those pages light up — no other coordination needed.

**Defense answers you must memorize** — already drafted in `ml/README.md §11`:
- Why these 3 algos (not SVM / NN)
- Why temporal split (not random shuffle)
- Why `class_weight='balanced'`
- Where the operating threshold comes from
- Inference-time budget

**The exact comparison table the prof expects** is in `ml/README.md §3`.

---

## 6. 👋 If you're picking up **P4 (Dashboard verification / extension)**

**What's already in the repo (committed but not all verified):**
- **Backend** (`dashboard/backend/`) — Express 4 + `mongodb` driver + JWT cookie auth. **Smoke-tested**: server starts, connects to Mongo, login + `/api/overview/kpis` returns 200.
- **Frontend** (`dashboard/frontend/`) — Vite + React + TypeScript + Tailwind + shadcn/ui + Recharts. **Scaffolded but not yet npm-installed end-to-end.** All 8 pages compile-clean in isolation, but no one has run `npm run dev` against a live backend.

**First task** — verify the frontend boots and login flow works (~15 min):

```powershell
# Terminal 1: backend (already smoke-tested, just confirm it still runs)
cd Y:\energy-big-data\dashboard\backend
# (.env was copied earlier; if missing: copy .env.example .env)
npm install                       # if you haven't yet
npm run dev
# → [api] listening on http://localhost:4000
# → [db] connected to mongodb (energy_db)  (assumes docker mongo is up)

# Terminal 2: frontend
cd Y:\energy-big-data\dashboard\frontend
npm install                       # first time — installs ~250 packages, ~1 min
npm run dev
# → opens http://localhost:5173

# Browser:
# 1. Open http://localhost:5173 → redirected to /login
# 2. Login with: admin / admin123  (or operator / operator123)
# 3. Should land on Overview page with KPI cards + alerts banner
# 4. Click each of the 7 sidebar links; verify pages render (some will be
#    empty until P5 writes ml_predictions — that's expected)
```

**If `npm install` fails on the frontend**: the `package.json` pins versions
that should be compatible with Node 18+. If you're on an older Node, upgrade.

**Pages that will look empty until P5 ships**:
- **Predictions** — needs `ml_predictions.consumption_forecast` per zone
- **Alerts (Active)** — needs `ml_predictions.alert_level` set to WARNING/CRITICAL

All other pages light up from collections P2 already populates (heatmap from
`meters_aggregated_15min`, incidents/word cloud from `incidents` +
`incidents_enriched`, data quality from `data_quality_metrics`).

---

## 7. The 3 deployment modes — when to use which

| Mode | Use when | Persists data? | Container name suffix |
|---|---|---|---|
| **Local** | Daily dev. Restarts wipe the slate. | ❌ No (no volume) | `-local` |
| **Pseudo** | Multi-day work, you need data to survive `docker compose down`. | ✅ Yes | `-pseudo` |
| **Distributed** | Defense demo — show RF=3 + broker failover. | ✅ Yes | Kafka: `kafka1-dist` / `kafka2-dist` / `kafka3-dist`; everything else `-dist` |

Full table of container names per mode lives in
[README.md §5](README.md#-container-names-per-mode-cheat-sheet).

**Never run two compose files at once** — they all bind to the same host ports.

To switch:
```powershell
docker compose -f docker/docker-compose.<current>.yml --env-file .env down
docker compose -f docker/docker-compose.<new>.yml --env-file .env up -d
```

---

## 8. Gotchas we hit (and how to avoid them)

### 8.1 — Stale MongoDB volume after `.env` password change

**Symptom**: `MongoServerError: Authentication failed` after bringing up
pseudo or distributed for the first time in a while.

**Cause**: the Mongo volume (`mongo-pseudo-data` or `mongo-dist-data`) was
created in a previous run with a different `MONGO_PASSWORD`. Mongo "remembers"
the original creds; changing `.env` later has no effect.

**Fix**: destroy the volume and let `init-mongo.js` re-run:
```powershell
docker compose -f docker/docker-compose.<mode>.yml --env-file .env rm -sf mongodb
docker volume rm energy-<mode>_mongo-<mode>-data
docker compose -f docker/docker-compose.<mode>.yml --env-file .env up -d mongodb
```

This **destroys whatever data was in Mongo** for that mode. For local mode
this is irrelevant (no volume). For pseudo/distributed, only do this if
you've never written anything important — otherwise back up first.

### 8.2 — Git Bash mangles `/opt/spark/...` paths

**Symptom**: running `docker exec spark-master-local /opt/spark/bin/spark-submit ...`
inside Git Bash on Windows fails with:
```
exec: "C:/Program Files/Git/opt/spark/bin/spark-submit": no such file
```

**Cause**: MSYS2/Git Bash auto-converts unix-style paths to Windows paths
before passing to docker.

**Fix**: prefix the docker exec with `MSYS_NO_PATHCONV=1` OR wrap the spark
command in `bash -c` (which executes inside the container, bypassing host
shell):
```bash
docker exec spark-master-local bash -c '/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /workspace/processing/main.py'
```

PowerShell (which most teammates use) does not have this problem.

### 8.3 — `'charmap' codec can't encode character '→'` from producer

**Symptom**: `smart_meter_producer.py` (and other producers) spam ERROR logs
on Windows about the `→` arrow character.

**Cause**: Python's default Windows stdout codec (cp1252) can't print U+2192.
The messages are **still sent to Kafka correctly** — only the local `print`
fails.

**Fix (optional, cosmetic)**: set `PYTHONIOENCODING=utf-8` before running the
producer:
```powershell
$env:PYTHONIOENCODING="utf-8"
.venv\Scripts\python -m ingestion.producers.smart_meter_producer
```

Or fix the producer to strip the arrow from its log format. Not blocking.

---

## 9. Defense day demo script (10 minutes)

Picking distributed mode for the demo since it shows the RF=3 story.

```powershell
# 1. (At your desk before the demo) verify cluster is healthy
docker compose -f docker/docker-compose.distributed.yml --env-file .env ps
# All entries "Up (healthy)"

# 2. Show Kafka topics with RF=3
docker exec kafka1-dist kafka-topics --bootstrap-server kafka1:29092 --describe --topic smart-meters
# → "ReplicationFactor: 3   Configs: min.insync.replicas=2"

# 3. Start 6 producers in 6 terminals (use the README §4 step 5 commands)

# 4. Run end-to-end verifier
.venv\Scripts\python -m ingestion.consumers.verify_topics --max-seconds 60 --no-require-each
# → [PASS] All subscribed topics delivered schema-valid messages.

# 5. Submit the Spark pipeline (wait for "Tous les flux sont démarrés")
docker exec spark-master-dist bash -c '/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /workspace/processing/main.py'

# 6. Browser tour:
#    - Kafka UI         :8090  → show 6 topics receiving live messages
#    - Spark Master UI  :8080  → show 7 streaming queries running
#    - Dashboard        :5173  → show login, then the 7 protected pages

# 7. Demonstrate broker failover (impressive — only works on distributed!)
docker stop kafka2-dist
# Pipeline keeps running. Show Kafka UI: ISR drops to 1,3 but topic still alive.
docker start kafka2-dist
# Show ISR recovers to 1,2,3 within ~10s.

# 8. Show Mongo collections filling up
docker exec mongodb-dist mongosh -u energy_admin -p change-me-before-deploy `
  --authenticationDatabase admin --quiet energy_db `
  --eval "db.getCollectionNames().forEach(c => print(c.padEnd(28) + ': ' + db[c].countDocuments() + ' docs'))"

# 9. Trigger a synthetic CRITICAL alert (insert directly into ml_predictions
#    if P5's predict.py isn't running yet) → show the dashboard alert banner
#    + the audit log entry in dashboard_alerts

# 10. Take questions. Each role's README has the "Required justifications
#     for the defense" section already drafted.
```

---

## 10. Where to look for what

| If you want to know... | Read |
|---|---|
| The big picture | [README.md](README.md) (top-level) |
| Kafka topics + schemas | [README.md §6](README.md#-the-6-kafka-topics-p1s-data-contract-for-the-team) + `ingestion/schemas/*.json` |
| Spark stream details | [processing/README.md](processing/README.md) |
| MongoDB collections + indexes + TTL | [storage/README.md](storage/README.md) + `storage/init/init-mongo.js` |
| Dashboard architecture (React + Express + JWT) | [dashboard/README.md](dashboard/README.md) |
| ML tasks + algorithms + metrics | [ml/README.md](ml/README.md) |
| GDPR + ethics audit | [ethics/GDPR.md](ethics/GDPR.md) |
| Container names per mode | [README.md §5](README.md#-container-names-per-mode-cheat-sheet) |
| MongoDB commands per mode | [storage/README.md §10](storage/README.md#10-run--verify) |

---

## 11. Push & pull workflow

```powershell
# Pull latest
git checkout p1/infrastructure-ingestion
git pull

# Make your changes, commit, push
git add <files>
git commit -m "feat(p3): add storage/mongo_client.py and healthcheck"
git push

# When your part is done, open a PR to main via GitHub UI
```

**Never commit**:
- `.env` (has credentials)
- `node_modules/`, `dist/`, `*.pkl`, `__pycache__/` (already gitignored)
- The producer's `.weather_state.json` (transient)

Good luck. Ask in the team chat if anything in this file is wrong or missing.
