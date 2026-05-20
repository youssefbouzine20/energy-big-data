import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/", async (_req, res, next) => {
  try {
    const db = getDb();
    // Show the MOST RECENT window per zone — no time filter. Survives any gap
    // in the Spark stream. Freshness is exposed via `last_seen` so the frontend
    // and the prof both know how recent the data is.
    const rows = await db.collection("meters_aggregated_15min").aggregate([
      { $sort: { window_start: -1 } },
      { $group: {
          _id: "$zone",
          avg_kwh:   { $first: "$avg_consumption" },
          total_kwh: { $first: "$total_consumption_kwh" },
          anomalies: { $first: "$anomaly_count" },
          meters:    { $first: "$meter_count" },
          lat:       { $first: "$zone_lat" },
          lon:       { $first: "$zone_lon" },
          last_seen: { $first: "$window_start" },
      } },
      { $project: { _id: 0, zone: "$_id", avg_kwh: 1, total_kwh: 1, anomalies: 1, meters: 1, lat: 1, lon: 1, last_seen: 1 } },
      { $sort: { zone: 1 } }
    ]).toArray();
    res.json({ items: rows, as_of: new Date().toISOString() });
  } catch (err) { next(err); }
});

export default router;
