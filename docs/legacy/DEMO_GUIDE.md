# Demo Guide — 25-min defense (M126 Big Data, Pr. I. Sassi)

## 🎯 Defense timing — 15 min presentation + 10 min Q&A

This guide tells you **what URL to open**, **what to click**, and **what to say** for every section of the professor's spec (A through H). Read it line-by-line during the defense if needed.

Before the defense: open every browser tab below and every terminal in this guide. **Don't open anything live in front of the prof** — switch between tabs that are already loaded.

---

## 0. Architecture diagram (open first, ~2 min)

> **What to say:** "Voici l'architecture globale de notre pipeline temps réel. Les données arrivent par Kafka, sont traitées par Spark Structured Streaming, stockées dans MongoDB, puis exposées via un dashboard React. Nous avons aussi un pipeline ML séparé qui produit des prédictions à 90 minutes."

```
   6 PRODUCTEURS ───► KAFKA (3 partitions/topic)  ───► SPARK ───► MONGODB ──► DASHBOARD
   (Python, 6 sources)    │       │                    │           │            │
                          │       │                    │           │            │
   smart-meters (20 ×30s) │       │                    │   11 collections      React + Express
   weather (4 × 30s)      │       │                    │   - meters_raw        7 pages, JWT auth
   incidents (1/min)      │       │                    │   - meters_aggreg     /heatmap, /alerts, ...
   rss-feeds (1/2min)     │       │                    │   - incidents_enrich
   market-prices (1/h)    │       │                    │   - feedback_nlp
   user-feedback (1/45s)  │       │                    │   - ml_predictions
                          │       │                    │   - data_quality
                          │  Schema Registry          │   + 4 more
                          │   (6 JSON schemas)        │
                                                  ML PIPELINE
                                              (sklearn — separate)
```

---

## A. Ingestion / Kafka — ~2 min

### What to open
- **Kafka UI:** http://localhost:8090

### What to click
1. Top-left dropdown → `energy-pseudo` (or `energy-distributed` if running distributed)
2. **Brokers** tab → 1 broker (pseudo) or 3 brokers (distributed)
3. **Topics** tab → 6 business topics + 2 internal
   - `smart-meters` (3 partitions, ~10 000+ messages)
   - `weather`, `incident-reports`, `rss-feeds`, `market-prices`, `user-feedback`
4. Click **`smart-meters`** → **Messages** tab → click any message → JSON payload with `meter_id`, `zone`, `consumption_kwh`, `voltage_v`, `is_anomaly`
5. **Schema Registry** tab → exactly **6 subjects** all version 1, type JSON, BACKWARD compatibility

### Talking points
- "Six producteurs Python, un par source de données, écrivent vers Kafka."
- "smart-meters a 3 partitions pour répartir la charge — chaque consommateur Spark peut paralléliser sa lecture."
- "Compatibilité BACKWARD au niveau du Schema Registry : les nouvelles versions de schéma peuvent lire les anciennes données, donc on peut faire évoluer sans interrompre les producteurs."

---

## B. Spark Structured Streaming — ~3 min

### What to open
- **Spark Master UI:** http://localhost:8080
- **Terminal 8** (running `spark-submit`) for the live logs

### What to click in the Spark UI
1. **Workers**: 1 alive (pseudo) or 2 alive (distributed)
2. Cores in use, Memory in use
3. (If running in cluster mode) Running Applications → click `energy-streaming-main` → **Streaming Queries** tab → 13 active queries (7 traitement + 6 qualité, une par topic)

### Talking points
- "Spark Structured Streaming consomme les 6 topics Kafka en parallèle via 8 requêtes streaming."
- "Pour chaque flux : on lit avec un watermark de 2 minutes, on groupe par fenêtre de 15 minutes et par zone, on calcule moyenne, max, min, écart-type, total — et on écrit le résultat dans MongoDB en mode append."
- "Pour la démo j'utilise une fenêtre de 2 minutes au lieu de 15 (variable d'environnement `AGGREGATION_WINDOW`) pour qu'on voie les agrégats se remplir pendant la présentation."
- "Pourquoi pas de stream-stream join avec météo et incidents ? J'ai testé : les joins entre streams agrégés en mode append en Spark 3.5 sont fragiles — les watermarks avancent, les batches committent, mais aucune ligne n'est émise. J'ai donc choisi d'enrichir au moment de la requête côté MongoDB via `$lookup`. Spark gère la vélocité, Mongo gère la jointure de variété. Pattern classique en production."

---

## C. MongoDB — ~3 min

### What to open
- **MongoDB Compass** (download from https://www.mongodb.com/products/compass)
  - Connection URI: `mongodb://energy_admin:change-me-before-deploy@localhost:27017/?authSource=admin`
- OR **dashboard Settings page** (http://localhost:5173/settings) — shows all 11 collections with TTL

### What to click in Compass
1. Database `energy_db` → **11 collections** visible
2. Click `meters_aggregated_15min`
   - **Documents** tab → live docs with `zone`, `window_start`, `avg_consumption`, ...
   - **Indexes** tab → compound index `zone_1_window_start_-1` (used by dashboard) + TTL index
   - **Validation** tab → `$jsonSchema` with required fields, enums (zone A/B/C/D), bsonType constraints
3. Click `meters_raw` → show same 3 tabs
4. Click `ml_predictions` → 72 prediction docs with `consumption_forecast`, `alert_level`, `model_name`

### Optional CLI proof
```powershell
.venv\Scripts\python -m storage.healthcheck
```
Output shows: 11 collections, all validators OK, all queries hit IXSCAN (not COLLSCAN), TTL active.

### Talking points
- "Onze collections : cinq pour les données brutes — meters_raw, weather, incidents, rss_feeds, market_prices — et six dérivées."
- "Pourquoi MongoDB ? Trois raisons : (1) le modèle document JSON match parfaitement nos messages Kafka hétérogènes. (2) Les validateurs `$jsonSchema` au niveau collection nous donnent une troisième couche de validation après les producteurs (jsonschema) et le Schema Registry. (3) Les index TTL gèrent automatiquement la rétention GDPR — meters_raw expire à 90 jours, les agrégats à 365, les incidents à 730 (audit légal)."
- "Pourquoi pas Cassandra ou HBase ? Cassandra serait plus performant pour les écritures séquentielles, mais MongoDB gère mieux les requêtes ad-hoc dont le dashboard a besoin (filtre par zone + plage de temps + agrégation). HBase imposerait une dépendance Hadoop trop lourde pour notre cluster."

---

## D. Modélisation IA / Analytics — ~3 min

### What to open
- **File `ml/metrics.json`** (open in VS Code or browser)
- **Dashboard Predictions page** (http://localhost:5173/predictions)

### What to click on Predictions page
1. Zone selector top-right → try A, B, C, D
2. Chart shows **solid blue line** (real consumption from `meters_aggregated_15min`) + **dashed orange line** (prediction from `ml_predictions`)

### Talking points — table to memorize
| Model | RMSE | MAE | R² | Inf time (µs/sample) |
|---|---|---|---|---|
| linear_regression | 0.001033 | 0.000723 | 0.7082 | 2.81 |
| random_forest | 0.000769 | 0.000557 | 0.8384 | 137.68 |
| gradient_boosting | 0.000747 | 0.000538 | **0.8474** | 3.13 |

- "Étude comparative de trois algorithmes distincts comme demandé en Section D : régression linéaire, forêt aléatoire, et gradient boosting."
- "Le gradient boosting gagne sur les trois métriques d'erreur : RMSE le plus bas, MAE le plus bas, R² le plus haut à 0.85. Et son temps d'inférence est de seulement 3 microsecondes par échantillon — quasi-identique à la régression linéaire, donc on garde la rapidité du modèle simple plus la précision du modèle complexe."
- "La forêt aléatoire est compétitive en précision mais 40 fois plus lente en inférence — un trade-off classique entre précision et latence en production."
- "Features utilisées : `lag_1` (consommation 15 min plus tôt), `lag_4` (1 h plus tôt), moyenne et écart-type glissants sur 4 fenêtres, heure de la journée, jour de la semaine, et un encodage one-hot de la zone."
- "Données d'entraînement : 116 fenêtres réelles depuis Kafka + 2 688 fenêtres synthétiques sur 7 jours générées avec un modèle de pic matinal à 8 h et soir à 19 h, atténuation week-end, et bruit gaussien. Split chronologique 80/20 — on n'entraîne jamais sur le futur."

---

## E. Data Quality Framework — ~2 min

### What to open
- **Dashboard Data Quality page** (http://localhost:5173/quality)

### What to show
- **6 cartes** : smart-meters, weather, incident-reports, rss-feeds, market-prices, user-feedback.
- Chaque carte : Completeness, Noise rate, Anomaly rate, Temporal coverage en %.
- Badge OK / WARN / CRIT en haut à droite si seuils dépassés.

### Talking points
- "Le framework de qualité de données calcule **6 streams Spark en parallèle**, un par topic, fenêtre 15 minutes :"
  - "**Complétude** — pourcentage de messages avec tous les champs vitaux non-null (par topic : consumption_kwh + zone pour meters, incident_id + zone + severity pour incidents, etc.)."
  - "**Taux de bruit** — pourcentage de valeurs physiquement impossibles, spécifique au domaine de chaque topic (ex: tension hors [210, 250] V pour les compteurs, température hors [-40, 55] °C pour la météo, impact_score hors [0, 1] pour RSS)."
  - "**Taux d'anomalies** — pourcentage de messages 'business-interesting' par topic : `is_anomaly=True` pour meters, `severity in (HIGH, CRITICAL)` pour incidents, `sentiment=NEGATIVE` pour feedback, `weather_severity in (STORM, EXTREME)` pour météo."
  - "**Couverture temporelle** — ratio messages reçus / messages attendus par topic (600 pour smart-meters avec 20 compteurs, 120 pour weather, 15 pour incidents, etc.)."
- "Un système d'alerte métier déclenche **CRIT** si la couverture chute sous 80 % (perte de capteur) et **WARN** si le bruit dépasse 5 % (calibration nécessaire). Les alertes sont écrites par-topic dans `data_quality_metrics.alert`."

---

## F. Visualisation & Reporting — ~2 min

### What to open
- **Dashboard** (http://localhost:5173 — already logged in as `admin`)

### Walk through 7 pages (in this order)
1. **Overview** — 4 KPI cards en haut, alerte prédictive en bas
2. **Heatmap** — 4 tuiles coloriées par charge moyenne
3. **Predictions** — courbe réelle + prévision dashed (Section D)
4. **Incidents** — word cloud NLP + table des incidents récents
5. **Alerts** — alertes actives + log d'audit
6. **Data Quality** — cartes par topic (Section E)
7. **Settings** — profil + table GDPR (Section H)

### Talking points
- "Dashboard React + Express, authentification JWT, rôles `admin` et `operator`."
- "L'utilisateur ouvre une page → backend Express requête MongoDB → renvoie JSON → React affiche."
- "Page Predictions : la courbe pleine est la consommation moyenne réelle par fenêtre, la courbe dashed est la prévision ML à 90 minutes. Sélecteur de zone à droite."
- "Page Incidents : le nuage de mots est généré côté backend depuis `incidents.description` — fréquence des mots-clés des 7 derniers jours."

---

## G. Modes d'Installation Hybride — ~1 min

### What to show
- **`.env.example`** lines 5-10: documentation des 3 modes (local / pseudo / distributed)
- **`docker/`** dossier: 3 fichiers compose distincts

### Talking points
- "Trois modes d'installation, tous fonctionnels :"
  1. "**Local** — Python pur, Kafka et Mongo en containers, Spark embarqué dans le driver. Pour le développement."
  2. "**Pseudo-distribué** — un broker Kafka, un master Spark, un worker Spark, une instance Mongo, tous en containers Docker, persistants. C'est le mode démontré aujourd'hui."
  3. "**Fully-distributed** — 3 brokers Kafka avec RF=3 et MIN_ISR=2, 2 workers Spark, prêt pour multi-machine. Démontré dans `docker-compose.distributed.yml`."

---

## H. Éthique & Conformité — ~1 min

### What to show
- **Dashboard Settings page** — table GDPR retention policies (8 collections visibles avec TTL en jours)
- **`ethics/GDPR.md`** (if exists, otherwise reference the Settings page table)

### Talking points
- "Conformité GDPR implémentée via des index TTL MongoDB :"
  - "meters_raw : 90 jours (fenêtre d'entraînement ML)"
  - "meters_aggregated_15min : 365 jours (analyse de tendance saisonnière)"
  - "incidents : 730 jours = 2 ans (obligation légale d'audit)"
  - "ml_predictions : 180 jours (audit modèle)"
- "Aucune donnée nominative dans les producteurs — `meter_id` est un identifiant pseudonymisé `SM-001`, jamais le nom du client."
- "Authentification JWT côté dashboard, mots de passe non en clair dans les logs (masqués `***@`)."

---

## 🛠️ If something breaks during the demo

| Symptom | Quick fix |
|---|---|
| Dashboard shows old data | Refresh browser (Ctrl+R) |
| KPI cards say "loading…" | Backend died — restart `npm run dev` in T9 |
| Predictions chart empty | Re-run `.venv\Scripts\python -m ml.run_pipeline` |
| Heatmap zones empty | Spark died — check T8 for errors, restart spark-submit |
| Kafka UI shows 0 messages | Producers died — check T2-T7 |
| Page won't load at all | Frontend died — `cd dashboard/frontend && npm run dev` |

If ANYTHING goes wrong, **stay calm and switch to slides**. The prof grades methodology + justification more than a live demo.

---

## 📋 Pre-defense checklist (30 min before the prof arrives)

1. [ ] `docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d` (bring infra up)
2. [ ] Wait 5 min, then open Kafka UI :8090 — confirm 6 topics with messages > 100
3. [ ] Open Spark UI :8080 — confirm worker alive
4. [ ] Open dashboard :5173 — log in `admin`/`admin123`
5. [ ] Click through 7 pages — all render with data
6. [ ] Run `.venv\Scripts\python -m ml.run_pipeline` to refresh predictions
7. [ ] Refresh Predictions page — dashed line appears
8. [ ] Refresh Alerts page — entries appear
9. [ ] Open `ml/metrics.json` in a tab (for Section D)
10. [ ] Open this DEMO_GUIDE.md in a tab (your script)
11. [ ] Close Slack / Discord / personal browser tabs
12. [ ] Plug in your charger — Docker drains battery fast
13. [ ] Disable Windows notifications during the defense

Then take 3 deep breaths. You've got this.
