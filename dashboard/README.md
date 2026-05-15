# P4 — Streamlit Dashboard

Interactive real-time monitoring dashboard. Reads from MongoDB (P3), displays
KPIs, geographic heatmap, **prediction-vs-actual curves**, word cloud, and
**predictive saturation alerts** — all four are explicit professor requirements
in spec Section F.

## Purpose in the pipeline

```
MongoDB (P3) + ml_predictions (P5) ──► Streamlit (P4) ──► http://localhost:8501
```

## Required sections (all from professor's Section F)

The professor explicitly lists 4 dashboard requirements. ALL must be present:

| # | Section F requirement | My section | Data source |
|---|---|---|---|
| 1 | "cartes de chaleur de la consommation en temps réel" | **Geographic heatmap** | `meters_aggregated_15min` + zone coordinates |
| 2 | "courbes de prédiction versus consommation réelle" | **Prediction vs actual chart** | `ml_predictions` + `meters_aggregated_15min` |
| 3 | "nuage de mots dynamique sur les incidents techniques" | **Dynamic word cloud** | `incidents_enriched` |
| 4 | "alertes prédictives ... anticiper la saturation du réseau" | **Predictive alerts banner** | `ml_predictions` thresholds |

Additional (not in spec but useful for KPI overview):
5. KPI cards (totals, anomaly count, active incidents)
6. Recent incidents table

## Why Streamlit? (REQUIRED justification)

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **Streamlit** | Python-native (same stack as P2/P5), fast prototyping (<1 day for full dashboard), built-in MongoDB compatibility via PyMongo, free | Single-user UX (no auth), limited layout customization | ✅ **Chosen** for academic project |
| Grafana | Best-in-class time-series + alerting | Steep learning curve, awkward MongoDB integration (needs plugin), more ops overhead | ❌ Wrong fit — too heavy |
| Dash (Plotly) | More customizable than Streamlit, same Python stack | More boilerplate (callbacks, layout) — 3× the code | ❌ Overkill |
| Custom Flask + React | Full control, multi-user | Weeks of work, not the focus of the project | ❌ Out of scope |

## Required env vars

- `MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DB_NAME`
- `STREAMLIT_PORT` (defaults to 8501)

## What to build

1. **`dashboard/app.py`** — main entry point, page layout, auto-refresh
2. **`dashboard/queries.py`** — cached MongoDB queries (`@st.cache_data(ttl=30)`)
3. **`dashboard/components/`** — reusable chart functions (one per section)
   - `kpi_cards.py`
   - `heatmap.py`
   - `prediction_chart.py`
   - `wordcloud_widget.py`
   - `alerts_banner.py`

## Predictive alerts logic (Section F — REQUIRED)

The professor's spec: "**alertes prédictives ... anticiper la saturation du réseau et éviter les délestages**".

```python
def compute_alert_level(zone: str, forecast_kwh: float, historical_peak: float) -> str:
    ratio = forecast_kwh / historical_peak
    if ratio >= 0.95: return "CRITICAL"   # red banner — load shedding likely
    if ratio >= 0.85: return "WARNING"    # yellow banner — proactive regulation needed
    if ratio >= 0.70: return "INFO"       # blue — elevated load
    return "NORMAL"

# Read latest forecast per zone from ml_predictions, render alert banner at top of dashboard
# Log every CRITICAL+WARNING event to dashboard_alerts for the Rapport audit trail
```

## Starter snippet (full app skeleton)

```python
import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta, timezone
from wordcloud import WordCloud
import matplotlib.pyplot as plt

from storage.mongo_client import get_db

st.set_page_config(page_title="Energy Grid Monitor", layout="wide", page_icon="⚡")
st.title("⚡ Energy Grid Monitor — Tétouan")

db = get_db()
now = datetime.now(timezone.utc)

# ── 1. Predictive alerts banner (top — most visible) ─────────────────────
forecasts = list(db.ml_predictions.find(
    {"forecast_for": {"$gte": now}}, sort=[("forecast_for", 1)]
).limit(4))
for fc in forecasts:
    level = fc.get("alert_level", "NORMAL")
    color = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(level, "🟢")
    if level in ("CRITICAL", "WARNING"):
        st.error(f"{color} Zone {fc['zone']}: forecast {fc['consumption_forecast']:.2f} kWh "
                 f"({fc['ratio']*100:.0f}% of peak) — {level}")

# ── 2. KPI cards ──────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total kWh (24h)", f"{query_total_consumption(db):.2f}")
col2.metric("Anomalies (1h)", query_anomaly_count(db, hours=1))
col3.metric("Active incidents", db.incidents.count_documents({"resolved": False}))
col4.metric("Avg voltage", f"{query_avg_voltage(db):.1f} V")

# ── 3. Geographic heatmap ─────────────────────────────────────────────────
st.subheader("🗺️ Real-time consumption heatmap")
zone_data = pd.DataFrame(list(db.meters_aggregated_15min.aggregate([
    {"$match": {"window_start": {"$gte": now - timedelta(minutes=15)}}},
    {"$group": {"_id": "$zone", "avg_kwh": {"$avg": "$avg_consumption"},
                "lat": {"$first": "$zone_lat"}, "lon": {"$first": "$zone_lon"}}}
])))
fig = px.density_mapbox(zone_data, lat="lat", lon="lon", z="avg_kwh", radius=40,
                        center={"lat": 35.578, "lon": -5.368}, zoom=12,
                        mapbox_style="carto-positron")
st.plotly_chart(fig, use_container_width=True)

# ── 4. Prediction vs actual chart (Section F — REQUIRED) ─────────────────
st.subheader("📈 Prediction vs Actual Consumption")
actual = pd.DataFrame(list(db.meters_aggregated_15min.find(
    {"window_start": {"$gte": now - timedelta(hours=6)}}
).sort("window_start", 1)))
predicted = pd.DataFrame(list(db.ml_predictions.find(
    {"forecast_for": {"$gte": now - timedelta(hours=6)}}
).sort("forecast_for", 1)))
fig = px.line(title="Consumption: forecast vs actual (last 6h)")
fig.add_scatter(x=actual["window_start"], y=actual["avg_consumption"], name="Actual", mode="lines")
fig.add_scatter(x=predicted["forecast_for"], y=predicted["consumption_forecast"],
                name="Predicted", mode="lines", line=dict(dash="dash"))
st.plotly_chart(fig, use_container_width=True)

# ── 5. Word cloud (Section F — REQUIRED) ─────────────────────────────────
st.subheader("☁️ Incident technical keywords")
texts = " ".join(d["description"] for d in db.incidents.find(
    {"timestamp": {"$gte": now - timedelta(days=7)}}, {"description": 1}
))
wc = WordCloud(width=1000, height=400, background_color="white").generate(texts)
fig, ax = plt.subplots(figsize=(10, 4)); ax.imshow(wc); ax.axis("off")
st.pyplot(fig)

# ── 6. Recent incidents table ─────────────────────────────────────────────
st.subheader("⚠️ Recent incidents")
recent = pd.DataFrame(list(db.incidents.find().sort("timestamp", -1).limit(10)))
st.dataframe(recent[["timestamp", "zone", "severity", "type", "description"]],
             use_container_width=True)

# ── Auto-refresh every 30 seconds ─────────────────────────────────────────
import time
time.sleep(30)
st.rerun()
```

## Required justifications for the defense

1. **Why Streamlit, not Grafana / Dash / custom?** — use the table above
2. **Why these 4+2 sections?** — sections 1–4 are mandated by spec F; KPI + incidents table support the operator's situational awareness
3. **Why 30-second refresh?** — Matches producer cycle (smart-meters every 30s); finer refresh would hammer Mongo without showing new data
4. **How do alerts get triggered?** — Threshold-based on `ml_predictions` ratio to historical peak; all events logged to `dashboard_alerts` for audit (Section H Ethics)
5. **Why these visualizations?** — heatmap = geographic intuition; line chart = temporal trend; word cloud = NLP output visibility for non-technical stakeholders

## Data Quality awareness (Section E)

Dashboard should DISPLAY data quality status, not just hide it:
- A small badge in header: "Data Quality: 98% ✅" sourced from latest `data_quality_metrics`
- If completeness < 95% or noise > 5%, show a warning banner so the operator knows data is degraded

## Dependencies (already in `requirements.txt`)

- `streamlit==1.35.0`, `plotly==5.22.0`, `wordcloud==1.9.3`, `nltk==3.8.1`, `pymongo==4.7.3`, `pandas==2.2.2`

## Run

```bash
.venv/bin/streamlit run dashboard/app.py --server.port 8501
```

Open http://localhost:8501.

## Verification (defense checklist)

- [ ] All 4 sections from spec F render: heatmap, prediction-vs-actual, word cloud, alerts banner
- [ ] KPIs update every 30s when producers are running
- [ ] Heatmap shows 4 distinct zone clusters around Tétouan coordinates (35.578, -5.368)
- [ ] Prediction vs actual chart shows two overlapping lines (dashed = predicted)
- [ ] Word cloud has 20+ distinct technical terms
- [ ] Triggering a CRITICAL alert (manually inject a high forecast) renders the red banner
- [ ] Page load completes in < 3 seconds (use `@st.cache_data(ttl=30)`)
