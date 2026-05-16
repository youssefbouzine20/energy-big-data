# P4 — Streamlit Dashboard  (FULL WORKFLOW + WHAT TO BUILD)

> Owner: P4 teammate.
> Reads from: MongoDB collections (P3) — never Kafka directly.
> Writes to:  `dashboard_alerts` collection (audit log of alert triggers).
> Used by:    end-user (operator at http://localhost:8501), defense jury.

---

## 0. The pipeline in one picture

```
   P3 MongoDB                              P4 Streamlit (you)              browser
  ─────────────                           ──────────────────              ─────────
   meters_aggregated_15min  ───┐
   incidents_enriched       ───┤  pymongo            ┌──────────────┐
   feedback_nlp             ───┼─── @st.cache_data ──► app.py       │
   data_quality_metrics     ───┤    (ttl=30)         │   layout +   │
   ml_predictions (P5)      ───┘                     │   plotly +   │  http GET
                                                     │   wordcloud  ├───────────► http://localhost:8501
                                                     └───────┬──────┘  every 30s
                                                             │
                                                  on CRITICAL/WARNING:
                                                             │
                                                             ▼
                                                  dashboard_alerts collection
                                                  (audit log for Section H)
```

**Your job in one sentence:** build a real-time dashboard with the **4 sections
explicitly required by the professor's Section F** (heatmap, prediction-vs-actual,
word cloud, predictive alerts), plus a few KPI cards on top, that auto-refreshes
every 30 seconds and reads only from MongoDB.

---

## 1. The 4 mandatory sections (Section F of the spec)

The professor's spec lists **4 dashboard requirements**. Missing any one
will cost points. Each one maps to a Mongo collection.

| # | Spec text | Your section | Source collection | Library |
|---|---|---|---|---|
| 1 | "**cartes de chaleur** de la consommation en temps réel" | Geographic heatmap of zones | `meters_aggregated_15min` (last 15 min) | `plotly.express.density_mapbox` |
| 2 | "**courbes de prédiction** versus consommation réelle" | Prediction-vs-actual line chart | `ml_predictions` + `meters_aggregated_15min` | `plotly.express.line` |
| 3 | "**nuage de mots dynamique** sur les incidents techniques" | Dynamic word cloud | `incidents_enriched` (or `incidents.description`) | `wordcloud.WordCloud` |
| 4 | "**alertes prédictives** … anticiper la saturation du réseau et éviter les délestages" | Banner of WARNING/CRITICAL alerts | `ml_predictions` + threshold logic | `st.error` / `st.warning` |

Add 2 more for situational awareness (operator usefulness):

| 5 | KPI cards | Top of page | `meters_aggregated_15min` + `incidents` | `st.metric` |
| 6 | Recent incidents table | Bottom | `incidents` (last 10) | `st.dataframe` |

---

## 2. Why Streamlit? — REQUIRED defense answer

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **Streamlit** | Python-native (same stack as P2/P5), <1 day for full dashboard, native PyMongo support, free, auto-rerun loop built in | Single-user (no auth), limited layout customization | ✅ **Chosen** for academic project |
| Grafana | Best-in-class time-series + alerting | Steep learning curve, MongoDB plugin is third-party, more ops overhead | ❌ Wrong fit — too heavy |
| Dash (Plotly) | More customizable, same Python stack | 3× the code (callbacks + layout boilerplate) | ❌ Overkill |
| Custom Flask + React | Full control, multi-user | Weeks of work, not the project's focus | ❌ Out of scope |

---

## 3. Connection details

| Param | Value | Set by |
|---|---|---|
| MongoDB host | `localhost` (running from host) or `mongodb` (running in compose) | `MONGO_HOST` env |
| MongoDB port | `27017` | `MONGO_PORT` env |
| MongoDB user | `energy_admin` (read-heavy work; no need for spark_writer) | `MONGO_USERNAME` env |
| MongoDB password | from `.env` | `MONGO_PASSWORD` env |
| MongoDB DB | `energy_db` | `MONGO_DB_NAME` env |
| Streamlit port | `8501` (mapped to host) | `STREAMLIT_PORT` env |

Use the **shared helper from P3**: `from storage.mongo_client import get_db`.
Don't re-implement Mongo connection logic in the dashboard.

---

## 4. The complete workflow — what happens on each refresh

Every 30 seconds (set by `time.sleep(30); st.rerun()`), Streamlit reruns the
entire script top-to-bottom. The `@st.cache_data(ttl=30)` decorator on each
query function means the actual Mongo round-trip happens **once per 30s
window**, even if multiple chart components re-read the same data.

### Per-refresh flow

```
t=0      st.rerun() called
         │
         ▼
t=0.001  st.set_page_config + title
         │
         ▼
t=0.005  query_alerts(now)              ◄── pymongo.find on ml_predictions
                                            (CACHED 30s)
         │
         ▼
t=0.020  render alerts banner  ─────── st.error / st.warning / st.success
         │
         ▼
t=0.025  query_kpis(now)                ◄── 4 small aggregation queries
         │
         ▼
t=0.040  render 4 metric cards
         │
         ▼
t=0.050  query_zone_aggregates(now)     ◄── pymongo aggregate
         │                                  (CACHED 30s)
         ▼
t=0.100  px.density_mapbox(...)         ─── render heatmap
         │
         ▼
t=0.110  query_predictions(now-6h..now+1h) ◄── ml_predictions
         │
         ▼
t=0.130  query_actuals(now-6h..now)        ◄── meters_aggregated_15min
         │
         ▼
t=0.180  px.line(actual + predicted)    ─── render prediction-vs-actual
         │
         ▼
t=0.190  query_incident_descriptions(now-7d..now) ◄── incidents.description
         │
         ▼
t=0.300  WordCloud(...).generate(text)  ─── render word cloud
         │
         ▼
t=0.350  query_recent_incidents(limit=10)
         │
         ▼
t=0.380  st.dataframe(df)
         │
         ▼
t=0.400  query_data_quality()            ◄── data_quality_metrics
         │
         ▼
t=0.420  st.caption("Data Quality: 98%")
         │
         ▼
t=30     time.sleep(30) → st.rerun()
```

Total render time should be **under 1 second** when caches are warm.

---

## 5. What you must build — explicit task list

| # | File | Purpose | Done when |
|---|---|---|---|
| 1 | `dashboard/__init__.py` | Empty marker | File exists |
| 2 | `dashboard/queries.py` | All `@st.cache_data` Mongo query functions | Each chart pulls data via 1 query function |
| 3 | `dashboard/components/kpi_cards.py` | Renders the 4 metric cards | `render_kpis(db, now)` returns a 4-column row |
| 4 | `dashboard/components/heatmap.py` | Renders Plotly mapbox | `render_heatmap(db, now)` shows 4 zone clusters |
| 5 | `dashboard/components/prediction_chart.py` | Prediction-vs-actual line chart | Two overlapping lines, dashed = predicted |
| 6 | `dashboard/components/wordcloud_widget.py` | NLP word cloud | At least 20 distinct terms render |
| 7 | `dashboard/components/alerts_banner.py` | Top banner with active alerts | CRITICAL renders red, WARNING yellow |
| 8 | `dashboard/components/incidents_table.py` | Last 10 incidents table | Sortable, color-coded by severity |
| 9 | `dashboard/components/quality_badge.py` | "Data Quality: 98% ✅" caption | Reads `data_quality_metrics` |
| 10 | `dashboard/app.py` | Wires all components together with auto-refresh | `streamlit run dashboard/app.py` opens at :8501 |
| 11 | `dashboard/README.md` (this file) | Already written | — |

---

## 6. Predictive alerts logic (Section F — REQUIRED)

The professor's spec literally says: "*alertes prédictives ... anticiper
la saturation du réseau et éviter les délestages*". Here is the contract:

### Alert thresholds (per zone, computed from `ml_predictions.consumption_forecast`)

```python
def compute_alert_level(forecast_kwh: float, historical_peak_kwh: float) -> str:
    """
    historical_peak_kwh = max consumption seen in this zone over the last 30 days
    (compute once, cache for 1h via @st.cache_data(ttl=3600))
    """
    if historical_peak_kwh <= 0:
        return "NORMAL"
    ratio = forecast_kwh / historical_peak_kwh
    if ratio >= 0.95: return "CRITICAL"   # red banner — load shedding likely
    if ratio >= 0.85: return "WARNING"    # yellow — proactive regulation needed
    if ratio >= 0.70: return "INFO"       # blue — elevated load
    return "NORMAL"
```

### What renders

```python
# Top of dashboard, above KPIs
forecasts = list(db.ml_predictions.find(
    {"forecast_for": {"$gte": now}},
    sort=[("forecast_for", 1)]
).limit(4))   # next prediction per zone

for fc in forecasts:
    level = fc.get("alert_level", "NORMAL")
    if level == "CRITICAL":
        st.error(f"🔴 **Zone {fc['zone']}**: forecast {fc['consumption_forecast']:.3f} kWh "
                 f"({fc['ratio_to_peak']*100:.0f}% of historical peak) — saturation risk in 15 min")
    elif level == "WARNING":
        st.warning(f"🟡 **Zone {fc['zone']}**: forecast {fc['consumption_forecast']:.3f} kWh "
                   f"({fc['ratio_to_peak']*100:.0f}% of peak) — elevated load expected")
```

### Audit logging (Section H Ethics)

Every CRITICAL or WARNING alert must be logged to `dashboard_alerts` for
the audit trail:

```python
db.dashboard_alerts.insert_one({
    "triggered_at":  datetime.now(timezone.utc),
    "zone":          fc["zone"],
    "alert_level":   level,
    "forecast_kwh":  fc["consumption_forecast"],
    "ratio_to_peak": fc["ratio_to_peak"],
    "model_name":    fc["model_name"],
})
```

---

## 7. Starter snippet — full app skeleton

```python
"""
P4 — Streamlit dashboard for the energy grid.
Run: streamlit run dashboard/app.py --server.port 8501
"""
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from wordcloud import WordCloud
import matplotlib.pyplot as plt

from storage.mongo_client import get_db


# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Energy Grid Monitor — Tetouan",
                   layout="wide", page_icon="⚡")
st.title("⚡ Energy Grid Monitor — Tetouan")

db  = get_db()
now = datetime.now(timezone.utc)


# ── Cached queries (Mongo round-trip happens at most once per 30s) ──────────
@st.cache_data(ttl=30)
def q_active_alerts():
    return list(db.ml_predictions.find(
        {"forecast_for": {"$gte": now},
         "alert_level":  {"$in": ["WARNING", "CRITICAL"]}},
        sort=[("forecast_for", 1)]).limit(8))

@st.cache_data(ttl=30)
def q_kpis():
    last_24h = now - timedelta(hours=24)
    last_1h  = now - timedelta(hours=1)
    return {
        "total_kwh_24h":    sum(d["total_consumption_kwh"]
                                for d in db.meters_aggregated_15min.find(
                                    {"window_start": {"$gte": last_24h}}, {"total_consumption_kwh": 1})),
        "anomalies_1h":     db.meters_raw.count_documents(
                                {"timestamp": {"$gte": last_1h}, "is_anomaly": True}),
        "active_incidents": db.incidents.count_documents({"resolved": False}),
        "avg_voltage":      list(db.meters_aggregated_15min.aggregate([
                                {"$match": {"window_start": {"$gte": last_1h}}},
                                {"$group": {"_id": None, "v": {"$avg": "$voltage_avg"}}}]))[0]["v"]
    }

@st.cache_data(ttl=30)
def q_heatmap_data():
    last_15m = now - timedelta(minutes=15)
    return pd.DataFrame(list(db.meters_aggregated_15min.aggregate([
        {"$match": {"window_start": {"$gte": last_15m}}},
        {"$group": {"_id": "$zone",
                    "avg_kwh": {"$avg": "$avg_consumption"},
                    "lat":     {"$first": "$zone_lat"},
                    "lon":     {"$first": "$zone_lon"}}},
        {"$project": {"zone": "$_id", "avg_kwh": 1, "lat": 1, "lon": 1, "_id": 0}}
    ])))

@st.cache_data(ttl=30)
def q_actual_vs_predicted():
    actual = pd.DataFrame(list(db.meters_aggregated_15min.find(
        {"window_start": {"$gte": now - timedelta(hours=6)}},
        sort=[("window_start", 1)])))
    predicted = pd.DataFrame(list(db.ml_predictions.find(
        {"forecast_for": {"$gte": now - timedelta(hours=6)}},
        sort=[("forecast_for", 1)])))
    return actual, predicted

@st.cache_data(ttl=120)
def q_incidents_text():
    descs = db.incidents.find({"timestamp": {"$gte": now - timedelta(days=7)}},
                              {"description": 1, "_id": 0})
    return " ".join(d["description"] for d in descs)

@st.cache_data(ttl=30)
def q_recent_incidents():
    rows = list(db.incidents.find().sort("timestamp", -1).limit(10))
    return pd.DataFrame(rows)[["timestamp", "zone", "severity", "type", "description"]]

@st.cache_data(ttl=120)
def q_data_quality():
    docs = list(db.data_quality_metrics.find().sort("window_start", -1).limit(6))
    if not docs: return None
    return sum(d["completeness_pct"] for d in docs) / len(docs)


# ── Section 1 (REQUIRED F): predictive alerts banner ─────────────────────────
alerts = q_active_alerts()
if alerts:
    for a in alerts:
        if a["alert_level"] == "CRITICAL":
            st.error(f"🔴 **Zone {a['zone']}** — forecast {a['consumption_forecast']:.3f} kWh "
                     f"({a['ratio_to_peak']*100:.0f}% of peak) — saturation risk")
        else:
            st.warning(f"🟡 **Zone {a['zone']}** — forecast {a['consumption_forecast']:.3f} kWh "
                       f"({a['ratio_to_peak']*100:.0f}% of peak)")
else:
    st.success("✅ All zones operating within normal load")


# ── Section 5 (extra): KPI cards ─────────────────────────────────────────────
k = q_kpis()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total kWh (24h)", f"{k['total_kwh_24h']:.2f}")
c2.metric("Anomalies (1h)",  k["anomalies_1h"])
c3.metric("Active incidents", k["active_incidents"])
c4.metric("Avg voltage",     f"{k['avg_voltage']:.1f} V")


# ── Section 1 (REQUIRED F): geographic heatmap ───────────────────────────────
st.subheader("🗺️ Real-time consumption heatmap (last 15 min)")
df_zone = q_heatmap_data()
if not df_zone.empty:
    fig = px.density_mapbox(df_zone, lat="lat", lon="lon", z="avg_kwh",
                            radius=40, center={"lat": 35.578, "lon": -5.368},
                            zoom=12, mapbox_style="carto-positron",
                            color_continuous_scale="YlOrRd")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Waiting for first 15-min window of aggregated data ...")


# ── Section 2 (REQUIRED F): prediction vs actual ─────────────────────────────
st.subheader("📈 Prediction vs actual consumption (last 6h)")
actual, predicted = q_actual_vs_predicted()
if not actual.empty:
    zone_filter = st.selectbox("Zone", ["A", "B", "C", "D"], key="z")
    actual_z = actual[actual["zone"] == zone_filter]
    pred_z   = predicted[predicted["zone"] == zone_filter] if not predicted.empty else pd.DataFrame()
    fig = px.line(title=f"Zone {zone_filter}")
    fig.add_scatter(x=actual_z["window_start"], y=actual_z["avg_consumption"],
                    name="Actual", mode="lines")
    if not pred_z.empty:
        fig.add_scatter(x=pred_z["forecast_for"], y=pred_z["consumption_forecast"],
                        name="Predicted", mode="lines", line=dict(dash="dash"))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No aggregated data yet.")


# ── Section 3 (REQUIRED F): word cloud ───────────────────────────────────────
st.subheader("☁️ Incident technical keywords (last 7 days)")
text = q_incidents_text()
if text:
    wc = WordCloud(width=1000, height=400, background_color="white",
                   colormap="viridis").generate(text)
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.imshow(wc); ax.axis("off")
    st.pyplot(fig)
else:
    st.info("No incidents in the last 7 days.")


# ── Section 6 (extra): recent incidents table ────────────────────────────────
st.subheader("⚠️ Recent incidents")
df_inc = q_recent_incidents()
if not df_inc.empty:
    st.dataframe(df_inc, use_container_width=True, hide_index=True)


# ── Footer: data quality badge (Section E awareness) ─────────────────────────
dq = q_data_quality()
if dq:
    badge = "✅" if dq >= 98 else "⚠️" if dq >= 90 else "❌"
    st.caption(f"Data Quality: {dq:.1f}% {badge}  •  Auto-refresh every 30s  •  Last updated {now.strftime('%H:%M:%S UTC')}")


# ── Auto-refresh ─────────────────────────────────────────────────────────────
time.sleep(30)
st.rerun()
```

---

## 8. Required justifications for the defense

1. **Why Streamlit, not Grafana / Dash / custom?** — use the table in §2
2. **Why these 4+2 sections?** — sections 1–4 mandated by spec F; KPI + incidents support situational awareness
3. **Why 30-second refresh?** — Matches the smart-meter producer cycle (30s); finer would hammer Mongo with no new data
4. **How are alerts triggered?** — Threshold on `ml_predictions.ratio_to_peak`; logged to `dashboard_alerts` for ethics audit (Section H)
5. **Why these visualizations?** — heatmap = geographic intuition; line chart = temporal trend; word cloud = NLP visibility for non-technical stakeholders; table = drill-down detail
6. **How does the dashboard handle stale data?** — `data_quality_metrics` badge exposes completeness; alerts go away when forecasts age past `forecast_for`
7. **Why no real-time WebSocket push?** — 30s polling is sufficient for energy-grid monitoring (settlement interval is 15 min anyway)

---

## 9. Run + verify

### Local development

```bash
.venv/bin/streamlit run dashboard/app.py --server.port 8501
# Opens http://localhost:8501 in your browser
```

### Inside Docker (production-like)

Add to `docker-compose.local.yml` (after P4 ships):

```yaml
streamlit:
  image: python:3.10-slim
  container_name: streamlit-local
  command: bash -c "pip install -r /workspace/requirements.txt && streamlit run /workspace/dashboard/app.py --server.port 8501 --server.address 0.0.0.0"
  ports:
    - "${STREAMLIT_PORT:-8501}:8501"
  volumes:
    - ../:/workspace
  environment:
    MONGO_HOST: mongodb
    MONGO_PORT: 27017
    MONGO_USERNAME: ${MONGO_USERNAME}
    MONGO_PASSWORD: ${MONGO_PASSWORD}
    MONGO_DB_NAME: ${MONGO_DB_NAME}
  depends_on:
    mongodb:
      condition: service_healthy
  networks:
    - energy-net
```

### Defense verification checklist

- [ ] All 4 sections from spec F render: heatmap, prediction-vs-actual, word cloud, alerts banner
- [ ] KPIs update every 30s when producers are running
- [ ] Heatmap shows 4 distinct zone clusters around Tétouan (35.578, -5.368)
- [ ] Prediction-vs-actual chart shows two overlapping lines (dashed = predicted)
- [ ] Word cloud has 20+ distinct technical terms
- [ ] Triggering a synthetic CRITICAL alert (insert one manually) renders the red banner
- [ ] Page first-load < 3 seconds with caches warm
- [ ] CSV export available via `st.download_button` (nice-to-have)

---

## 10. Common pitfalls

1. **`pymongo.errors.ServerSelectionTimeoutError`** → wrong MONGO_HOST. From host use `localhost`, from compose use `mongodb`.
2. **Charts don't update** → forgot `time.sleep(30); st.rerun()` at the end. Streamlit doesn't auto-refresh on its own.
3. **Heatmap shows nothing** → no aggregated data yet (Spark hasn't computed first 15-min window). Wait or use `--from-beginning` consumer flag for backfill.
4. **Word cloud crashes on empty text** → guard with `if text:` before `WordCloud(...).generate(text)`.
5. **Cache returns stale data after restart** → `@st.cache_data` is in-memory; clears on `st.rerun()` after TTL. Set `ttl=30` not higher.
6. **CRITICAL alert fires every refresh** → without `dashboard_alerts` deduplication, you'll double-log. Track last-fired-time per zone.
7. **Plotly heatmap is blank on dark map theme** → `mapbox_style="carto-positron"` is light; for dark mode use `"carto-darkmatter"`.

---

## 11. Dashboard ↔ Section H Ethics

The professor's Section H requires **transparent ML decisions**. The dashboard is
your transparency surface:

- Show alert thresholds explicitly (`>=95% peak = CRITICAL`)
- Show which model produced each forecast (`model_name` field)
- Log every alert trigger to `dashboard_alerts` for review
- Display data quality so operators don't blindly trust degraded data

These add up to a **defensible audit trail** ready for the GDPR/ethics
section of the Rapport.

---

## 12. Dependencies (already in root `requirements.txt`)

`streamlit==1.35.0`, `plotly==5.22.0`, `wordcloud==1.9.3`, `pymongo==4.7.3`, `pandas==2.2.2`. Matplotlib comes bundled with wordcloud.
