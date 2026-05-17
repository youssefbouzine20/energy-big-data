import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/active", async (_req, res, next) => {
  try {
    const db = getDb();
    const now = new Date();
    const rows = await db.collection("ml_predictions")
      .find(
        { forecast_for: { $gte: now }, alert_level: { $in: ["WARNING", "CRITICAL"] } },
        { projection: { _id: 0 } }
      )
      .sort({ forecast_for: 1 })
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
