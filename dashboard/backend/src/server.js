import express from "express";
import cors from "cors";
import cookieParser from "cookie-parser";
import dotenv from "dotenv";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { startMongo, closeMongo, isConnected } from "./db.js";
import authRoutes from "./routes/auth.js";
import overviewRoutes from "./routes/overview.js";
import heatmapRoutes from "./routes/heatmap.js";
import predictionsRoutes from "./routes/predictions.js";
import incidentsRoutes from "./routes/incidents.js";
import alertsRoutes from "./routes/alerts.js";
import qualityRoutes from "./routes/quality.js";
import settingsRoutes from "./routes/settings.js";
import { requireAuth } from "./middleware.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

dotenv.config({ path: path.resolve(__dirname, "../../../.env") });
dotenv.config({ path: path.resolve(__dirname, "../.env"), override: true });

const PORT = Number(process.env.API_PORT || 4000);
const CORS_ORIGIN = (process.env.CORS_ORIGIN || "http://localhost:5173")
  .split(",").map(s => s.trim());

const app = express();

app.use(cors({ origin: CORS_ORIGIN, credentials: true }));
app.use(express.json({ limit: "1mb" }));
app.use(cookieParser());

app.get("/api/health", (_req, res) => res.json({
  ok: true,
  service: "energy-dashboard-api",
  db: isConnected() ? "connected" : "disconnected",
}));

app.use("/api/auth", authRoutes);
app.use("/api/overview", requireAuth, overviewRoutes);
app.use("/api/heatmap", requireAuth, heatmapRoutes);
app.use("/api/predictions", requireAuth, predictionsRoutes);
app.use("/api/incidents", requireAuth, incidentsRoutes);
app.use("/api/alerts", requireAuth, alertsRoutes);
app.use("/api/quality", requireAuth, qualityRoutes);
app.use("/api/settings", requireAuth, settingsRoutes);

app.use((err, _req, res, _next) => {
  const status = err.status || 500;
  if (status >= 500 && status !== 503) {
    console.error("[api] error:", err);
  }
  res.status(status).json({ error: err.message || "internal error" });
});

const server = app.listen(PORT, () => {
  startMongo();
  console.log(`[api] listening on http://localhost:${PORT}`);
  console.log(`[api] CORS allowed for: ${CORS_ORIGIN.join(", ")}`);
});

const shutdown = async () => {
  console.log("[api] shutting down...");
  await closeMongo();
  server.close(() => process.exit(0));
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
