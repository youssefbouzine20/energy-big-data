# P4 — Streamlit Dashboard

This folder is for the real-time monitoring dashboard. Reads from MongoDB (P3),
displays KPIs, heatmaps, time-series, and an NLP word cloud of incidents.

## Purpose in the pipeline

```
MongoDB (P3) ──► Streamlit (P4 — this folder) ──► Browser at http://localhost:8501
```

## Required pages / sections

1. **KPI cards (top of page):**
   - Total consumption today (sum of `consumption_kwh`)
   - Anomalies in the last hour (count where `is_anomaly=true`)
   - Active incidents (count where `resolved=false`)
   - Average grid voltage (last 5 minutes)

2. **Geographic heatmap:**
   - Use Plotly `density_mapbox` or `scattermapbox`
   - Center on Tétouan (35.578, -5.368)
   - Color intensity = avg `consumption_kwh` per zone
   - Data from `meters_aggregated_15min` joined on `zone_lat`/`zone_lon`

3. **Time-series charts:**
   - Consumption per zone (line chart, last 24h)
   - Voltage trend (line chart, last 4h, with anomaly thresholds 215/245)
   - Anomaly count per hour (bar chart, last 24h)

4. **Word cloud:**
   - Built from `incidents.description` text
   - Use `wordcloud==1.9.3` library (already in `requirements.txt`)
   - Filter stopwords with `nltk` stopwords

5. **Recent incidents table:**
   - Last 10 incidents with severity color-coding
   - Columns: `timestamp`, `zone`, `severity`, `type`, `description` (truncated)

## Required env vars

- `MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DB_NAME`
- `STREAMLIT_PORT` (defaults to 8501)

## What to build

1. **`dashboard/app.py`** — main Streamlit entry point.
2. **`dashboard/queries.py`** — MongoDB query helpers (cached with `@st.cache_data`).
3. **`dashboard/components.py`** — reusable chart/KPI components.

## Starter snippet

```python
# dashboard/app.py
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta, timezone
from storage.mongo_client import get_db

st.set_page_config(page_title="Energy Grid Monitor", layout="wide")
st.title("⚡ Energy Grid Monitor — Tétouan")

db = get_db()
now = datetime.now(timezone.utc)
one_hour_ago = now - timedelta(hours=1)

# ── KPI cards ──────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total kWh (today)",
    f"{db.meters_raw.aggregate([{'$match': {'timestamp': {'$gte': now - timedelta(days=1)}}},
                                {'$group': {'_id': None, 'total': {'$sum': '$consumption_kwh'}}}]).next()['total']:.2f}")
col2.metric("Anomalies (1h)",
    db.meters_raw.count_documents({"timestamp": {"$gte": one_hour_ago}, "is_anomaly": True}))
col3.metric("Active incidents",
    db.incidents.count_documents({"resolved": False}))
col4.metric("Avg voltage",
    f"{list(db.meters_raw.aggregate([{'$group': {'_id': None, 'avg': {'$avg': '$voltage_v'}}}]))[0]['avg']:.1f} V")

# ── Auto-refresh every 30s ────────────────────────────────────────────────
import time
time.sleep(30)
st.rerun()
```

## Auto-refresh

Use `st.rerun()` after `time.sleep(30)` OR install `streamlit-autorefresh` for non-blocking refresh.

## Dependencies

All in `requirements.txt`:
- `streamlit==1.35.0`
- `plotly==5.22.0`
- `wordcloud==1.9.3`
- `nltk==3.8.1`
- `pymongo==4.7.3`
- `pandas==2.2.2`

## Run

```bash
.venv/bin/streamlit run dashboard/app.py --server.port 8501
```

Open http://localhost:8501 in a browser.

## Verification

- All 5 sections render without errors
- KPIs update every 30s when producers are running
- Heatmap shows 4 distinct zone clusters at Tétouan coordinates
- Word cloud has > 20 distinct words from incident descriptions

## Notes

- Use `@st.cache_data(ttl=30)` on expensive MongoDB queries to avoid hammering the DB
- For the demo, keep the dashboard responsive — 5+ second page loads will look bad to the professor
