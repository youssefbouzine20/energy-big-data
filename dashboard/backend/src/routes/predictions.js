import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/", async (req, res, next) => {
  try {
    const db = getDb();
    const hours = Math.min(Number(req.query.hours) || 6, 48);
    const zone = req.query.zone || null;
    const since = new Date(Date.now() - hours * 60 * 60 * 1000);

    const actualFilter = { window_start: { $gte: since } };
    if (zone) actualFilter.zone = zone;
    const actual = await db.collection("meters_aggregated_15min")
      .find(actualFilter, { projection: { _id: 0, zone: 1, window_start: 1, avg_consumption: 1 } })
      .sort({ window_start: 1 })
      .toArray();

    const predFilter = { forecast_for: { $gte: since } };
    if (zone) predFilter.zone = zone;
    const predicted = await db.collection("ml_predictions")
      .find(predFilter, { projection: { _id: 0, zone: 1, forecast_for: 1, consumption_forecast: 1, model_name: 1, alert_level: 1 } })
      .sort({ forecast_for: 1 })
      .toArray();

    res.json({ actual, predicted, zone, hours });
  } catch (err) { next(err); }
});

export default router;
