import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/active", async (_req, res, next) => {
  try {
    const db = getDb();
    // "Active" = forecasts within the recent window (last 6h) or the future.
    // ml.run_pipeline writes forecasts at a moment in time; if it ran 30 min ago,
    // strictly-future filter wipes everything. Widen to ±6h so alerts stay visible.
    const since = new Date(Date.now() - 6 * 60 * 60 * 1000);
    const rows = await db.collection("ml_predictions")
      .find(
        { forecast_for: { $gte: since }, alert_level: { $in: ["WARNING", "CRITICAL"] } },
        { projection: { _id: 0 } }
      )
      .sort({ forecast_for: -1 })
      .limit(20)
      .toArray();
    res.json({ items: rows });
  } catch (err) { next(err); }
});

router.get("/history", async (req, res, next) => {
  try {
    const db = getDb();
    const limit = Math.min(Number(req.query.limit) || 100, 500);
    const rows = await db.collection("dashboard_alerts")
      .find({}, { projection: { _id: 0 } })
      .sort({ triggered_at: -1 })
      .limit(limit)
      .toArray();
    res.json({ items: rows });
  } catch (err) { next(err); }
});

router.post("/log", async (req, res, next) => {
  try {
    const db = getDb();
    const { zone, alert_level, forecast_kwh, ratio_to_peak, model_name } = req.body || {};
    if (!zone || !alert_level) return res.status(400).json({ error: "zone and alert_level required" });
    const doc = {
      triggered_at: new Date(),
      zone,
      alert_level,
      forecast_kwh: Number(forecast_kwh) || null,
      ratio_to_peak: Number(ratio_to_peak) || null,
      model_name: model_name || null,
      triggered_by: req.user?.username || "system",
    };
    await db.collection("dashboard_alerts").insertOne(doc);
    res.status(201).json({ ok: true });
  } catch (err) { next(err); }
});

export default router;
