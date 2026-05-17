import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/", async (_req, res, next) => {
  try {
    const db = getDb();
    const since = new Date(Date.now() - 30 * 60 * 1000);
    const rows = await db.collection("meters_aggregated_15min").aggregate([
      { $match: { window_start: { $gte: since } } },
      { $group: {
          _id: "$zone",
          avg_kwh: { $avg: "$avg_consumption" },
          total_kwh: { $sum: "$total_consumption_kwh" },
          anomalies: { $sum: "$anomaly_count" },
          lat: { $first: "$zone_lat" },
          lon: { $first: "$zone_lon" },
      } },
      { $project: { _id: 0, zone: "$_id", avg_kwh: 1, total_kwh: 1, anomalies: 1, lat: 1, lon: 1 } },
      { $sort: { zone: 1 } }
    ]).toArray();
    res.json({ items: rows, as_of: new Date().toISOString() });
  } catch (err) { next(err); }
});

export default router;
