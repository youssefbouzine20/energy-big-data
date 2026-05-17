import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/kpis", async (_req, res, next) => {
  try {
    const db = getDb();
    const now = new Date();
    const last24h = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const last1h = new Date(now.getTime() - 60 * 60 * 1000);

    const [agg24h] = await db.collection("meters_aggregated_15min").aggregate([
      { $match: { window_start: { $gte: last24h } } },
      { $group: { _id: null, total: { $sum: "$total_consumption_kwh" } } }
    ]).toArray();

    const anomalies1h = await db.collection("meters_raw").countDocuments({
      timestamp: { $gte: last1h }, is_anomaly: true
    });

    const activeIncidents = await db.collection("incidents").countDocuments({ resolved: false });

    const [voltage] = await db.collection("meters_aggregated_15min").aggregate([
      { $match: { window_start: { $gte: last1h } } },
      { $group: { _id: null, avg: { $avg: "$voltage_avg" } } }
    ]).toArray();

    res.json({
      total_kwh_24h: agg24h?.total ?? 0,
      anomalies_1h: anomalies1h,
      active_incidents: activeIncidents,
      avg_voltage: voltage?.avg ?? 0,
      as_of: now.toISOString(),
    });
  } catch (err) { next(err); }
});

router.get("/recent-windows", async (_req, res, next) => {
  try {
    const db = getDb();
    const rows = await db.collection("meters_aggregated_15min")
      .find({}, { projection: { _id: 0 } })
      .sort({ window_start: -1 })
      .limit(24)
      .toArray();
    res.json({ items: rows.reverse() });
  } catch (err) { next(err); }
});

export default router;
