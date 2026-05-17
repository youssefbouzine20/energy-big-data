import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

const RETENTION = [
  { collection: "meters_raw", ttl_days: 90, reason: "ML training window" },
  { collection: "meters_aggregated_15min", ttl_days: 365, reason: "Seasonal trend analysis" },
  { collection: "weather", ttl_days: 365, reason: "Long-term correlation" },
  { collection: "incidents", ttl_days: 730, reason: "Operational + regulatory" },
  { collection: "incidents_enriched", ttl_days: 730, reason: "NLP audit trail" },
  { collection: "feedback_nlp", ttl_days: 365, reason: "Sentiment trend analysis" },
  { collection: "ml_predictions", ttl_days: 180, reason: "Prediction audit trail" },
  { collection: "data_quality_metrics", ttl_days: 365, reason: "Quality history" },
  { collection: "dashboard_alerts", ttl_days: 365, reason: "Ethics audit trail (Section H)" },
];

router.get("/retention", (_req, res) => res.json({ items: RETENTION }));

router.get("/system", async (_req, res, next) => {
  try {
    const db = getDb();
    const collections = await db.listCollections({}, { nameOnly: true }).toArray();
    const stats = [];
    for (const c of collections) {
      try {
        const count = await db.collection(c.name).estimatedDocumentCount();
        stats.push({ name: c.name, estimated_count: count });
      } catch {
        stats.push({ name: c.name, estimated_count: null });
      }
    }
    stats.sort((a, b) => a.name.localeCompare(b.name));
    res.json({ db_name: db.databaseName, collections: stats });
  } catch (err) { next(err); }
});

export default router;
