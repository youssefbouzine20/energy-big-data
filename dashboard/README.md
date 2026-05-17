# P4 — React Dashboard + Express API  (FULL WORKFLOW + WHAT TO BUILD)

> Owner: P4 teammate.
> Reads from: MongoDB (P3) — through the Express API in `backend/`.
> Writes to:  `dashboard_alerts` collection (audit log of alert triggers).
> Used by:    end-user (operator at http://localhost:5173), defense jury.

---

## 0. Architecture

```
   P3 MongoDB                  P4 Express API (backend/)              P4 React SPA (frontend/)         browser
  ─────────────                ──────────────────────────             ──────────────────────────       ─────────
   meters_aggregated_15min ──┐
   incidents_enriched       ──┤   /api/overview/kpis                  AuthProvider (JWT cookie)
   feedback_nlp             ──┼─► /api/heatmap                  ───►  Layout (Sidebar + Header)  ───► http://localhost:5173
   data_quality_metrics     ──┤   /api/predictions                    Pages: Overview / Heatmap /
   ml_predictions (P5)      ──┤   /api/incidents{,/wordcloud}         Predictions / Incidents /
   dashboard_alerts         ──┘   /api/alerts/{active,history,log}    Alerts / Quality / Settings
                                  /api/quality/{latest,history}                                  
                                  /api/settings/{retention,system}
```

- **Backend:** Node.js + Express + the official `mongodb` driver. JWT in an
  httpOnly cookie. Mock user store (`AUTH_USERS` in `.env`) — 2 demo accounts.
- **Frontend:** Vite + React + TypeScript + Tailwind + shadcn/ui + Recharts.
  Auto-refreshes every 30 s. All 4 sections required by spec F are covered.

**Why this stack** (defense answer):

| Choice | Why |
|---|---|
| **React SPA** | Industry-standard, multi-page, supports role-based access (admin vs operator), strong demo polish |
| **Express + REST** | Same Node ecosystem as the SPA, ~150 LOC for 12 endpoints, easy to extend |
| **JWT in httpOnly cookie** | Resists XSS exfil (vs `localStorage`), CSRF mitigated by `SameSite=Lax` |
| **shadcn/ui + Tailwind** | Copy-paste components, zero runtime overhead, modern look |
| **Recharts** | Pure React, no D3 wrapper complexity, sufficient for the prediction-vs-actual chart |

---

## 1. The 4 mandatory sections (Section F of the spec)

| # | Spec text | Page | Source collection |
|---|---|---|---|
| 1 | "cartes de chaleur de la consommation en temps réel" | `Heatmap.tsx` | `meters_aggregated_15min` |
| 2 | "courbes de prédiction versus consommation réelle" | `Predictions.tsx` | `meters_aggregated_15min` + `ml_predictions` |
| 3 | "nuage de mots dynamique sur les incidents techniques" | `Incidents.tsx` | `incidents_enriched` (NLP keywords) |
| 4 | "alertes prédictives … éviter les délestages" | `Alerts.tsx` + Overview banner | `ml_predictions` |

Plus three supporting pages: Overview (KPIs), Data Quality (Section E), Settings (Section G/H surface).

---

## 2. Folder layout

```
dashboard/
├── README.md                    ← this file
├── backend/                     ← Node.js + Express + MongoDB driver
│   ├── package.json
│   ├── .env.example
│   └── src/
│       ├── server.js            ← Express bootstrap, route mounting
│       ├── db.js                ← MongoClient singleton
│       ├── auth.js              ← JWT issue/verify, mock user store
│       ├── middleware.js        ← requireAuth, requireRole
│       └── routes/
│           ├── auth.js          ← POST /login, /logout, GET /me
│           ├── overview.js      ← GET /kpis, /recent-windows
│           ├── heatmap.js       ← GET /
│           ├── predictions.js   ← GET / (zone, hours)
│           ├── incidents.js     ← GET /, /wordcloud
│           ├── alerts.js        ← GET /active, /history, POST /log
│           ├── quality.js       ← GET /latest, /history
│           └── settings.js      ← GET /retention, /system
└── frontend/                    ← Vite + React + TypeScript
    ├── package.json
    ├── vite.config.ts           ← proxies /api → http://localhost:4000
    ├── tailwind.config.js
    ├── components.json          ← shadcn config
    ├── index.html
    └── src/
        ├── main.tsx             ← entry point
        ├── App.tsx              ← router
        ├── index.css            ← Tailwind + CSS vars
        ├── lib/
        │   ├── utils.ts         ← cn() + number/time helpers
        │   ├── api.ts           ← fetch wrapper with credentials
        │   └── auth.tsx         ← AuthProvider context
        ├── components/
        │   ├── ui/              ← shadcn components (button, card, …)
        │   ├── Sidebar.tsx
        │   ├── Header.tsx
        │   ├── Layout.tsx
        │   └── ProtectedRoute.tsx
        └── pages/
            ├── Login.tsx
            ├── Overview.tsx
            ├── Heatmap.tsx
            ├── Predictions.tsx
            ├── Incidents.tsx
            ├── Alerts.tsx
            ├── Quality.tsx
            └── Settings.tsx
```

---

## 3. Install + run (first time)

The dashboard runs as two processes. From the project root:

```powershell
# 1) Backend (Express on :4000)
cd dashboard\backend
copy .env.example .env
npm install
npm run dev

# 2) Frontend (Vite on :5173) — open a SECOND terminal
cd dashboard\frontend
npm install
npm run dev
```

Open http://localhost:5173 in your browser. Login with one of:
- `admin` / `admin123`  (role: admin)
- `operator` / `operator123`  (role: operator)

Both credentials are defined in `dashboard/backend/.env`. Change them before deploy.

### What the backend reads from `.env`

The backend loads `../../.env` (root) FIRST, then `.env` (in `backend/`) which
overrides anything role-specific. So your existing `MONGO_*` config from the
root `.env` is reused — no duplication. The backend `.env.example` only
documents the extras (`API_PORT`, `JWT_SECRET`, `AUTH_USERS`, `CORS_ORIGIN`).

---

## 4. The data flow on a single page load

```
t=0      User hits /predictions?zone=B
         │
         ▼
t=0.001  React Router renders <PredictionsPage>
         │
         ▼
t=0.005  useEffect → api.get("/api/predictions?zone=B&hours=6")
         │  (fetch with credentials: 'include' → cookie sent)
         ▼
t=0.020  Vite dev server proxies /api/* to http://localhost:4000
         │
         ▼
t=0.025  Express → requireAuth middleware verifies JWT in cookie
         │
         ▼
t=0.030  routes/predictions.js queries two collections via MongoClient
         │  - meters_aggregated_15min (actuals)
         │  - ml_predictions (forecasts)
         ▼
t=0.080  JSON response: { actual: [...], predicted: [...], zone, hours }
         │
         ▼
t=0.100  React reshapes both arrays into a single timeseries by timestamp
         │
         ▼
t=0.120  Recharts <LineChart> renders two lines (solid = actual, dashed = predicted)
         │
         ▼
t=30     setInterval fires → load() again → React re-renders if data changed
```

---

## 5. Auth flow

```
Login.tsx:                        backend/routes/auth.js:
  POST /api/auth/login   ─────►   findUser(username, password)
                                   if not found → 401
                                   issueToken({sub, role}) → JWT
                                   setSessionCookie(res, token)
                                                                ◄───── 200 + Set-Cookie

AuthProvider on app mount:
  GET /api/auth/me       ─────►   verifyToken(cookie)
                                   if valid → 200 { user }
                                   else → 401
                                                                ◄───── { user } or 401

ProtectedRoute:
  if !user && !loading → <Navigate to="/login" />

Logout button → POST /api/auth/logout → clearCookie → setUser(null)
```

---

## 6. Required justifications for the defense

1. **Why React over Streamlit?** Industry-standard multi-page architecture; supports real auth (mock here, real in production); separation of concerns (API decoupled from UI lets P5 ML reuse the same endpoints).
2. **Why Express, not FastAPI?** Same JS runtime as the frontend; no second language to maintain; Node's official `mongodb` driver is mature.
3. **Why JWT in httpOnly cookie?** Resists XSS exfiltration (vs `localStorage`); CSRF protection via `SameSite=Lax`; works with proxy in dev.
4. **Why two hardcoded users for demo?** Academic scope — a real `users` collection with bcrypt is documented in the Rapport as the production path.
5. **Why these 7 pages?** Pages 2–5 cover the 4 mandatory Section F items; Overview is the operator's landing page; Data Quality surfaces Section E; Settings makes Section G+H tangible (TTL policies visible, alert audit log reviewable).
6. **Why 30-second auto-refresh?** Matches the smart-meter producer cycle and the 15-min Spark window granularity; faster polling wastes Mongo CPU.
7. **What's the audit trail?** Every CRITICAL/WARNING alert displayed by the dashboard can be logged to `dashboard_alerts` via `POST /api/alerts/log`. Reviewable in the Alerts page.

---

## 7. Common pitfalls

1. **`401 not authenticated` on every API call** → check that `vite.config.ts` proxy is hitting `localhost:4000` and that `credentials: "include"` is set on fetch (it is, in `lib/api.ts`).
2. **`CORS error`** → `CORS_ORIGIN` in `backend/.env` must include the frontend origin exactly. Default is `http://localhost:5173`.
3. **`MongoServerError: Authentication failed`** → backend reads `MONGO_USERNAME`/`MONGO_PASSWORD` from root `.env`; verify they match what was set when the Mongo volume was first created.
4. **Empty Overview/Heatmap pages** → producers + Spark haven't filled `meters_aggregated_15min` yet. Wait for the first 15-min window to close.
5. **Word cloud is empty** → P2's NLP stream hasn't written `incidents_enriched`. The backend falls back to plain `incidents.description` tokenization if `incidents_enriched` is empty.
6. **Login works locally but Set-Cookie is rejected** → if you change the dev port, update `CORS_ORIGIN` AND ensure SameSite is correct for cross-origin (we use Lax which works for same-site dev via proxy).

---

## 8. Defense day checklist

- [ ] Both `npm run dev` processes are healthy (`:4000/api/health` returns `{ ok: true }`)
- [ ] Login with `admin / admin123` works → redirected to Overview
- [ ] Sidebar shows 7 pages, header shows current time + user
- [ ] Overview: KPI cards populated, active alerts banner shows current state
- [ ] Heatmap: 4 zone tiles colored by load
- [ ] Predictions: line chart renders both actual + predicted for at least one zone
- [ ] Incidents: word cloud shows ≥ 20 terms, recent incidents table fills
- [ ] Alerts: audit log shows entries (or "empty" if no alerts triggered)
- [ ] Data Quality: 6 topic cards rendered with completeness %
- [ ] Settings: retention table + collection counts visible
- [ ] Logout returns to login screen

---

## 9. Dependencies

Backend: `express`, `mongodb`, `jsonwebtoken`, `cookie-parser`, `cors`, `dotenv`.
Frontend: `react`, `react-router-dom`, `recharts`, `tailwindcss`, `@radix-ui/*`, `lucide-react`, `class-variance-authority`, `clsx`, `tailwind-merge`.

Run `npm install` once in each of `backend/` and `frontend/`.
