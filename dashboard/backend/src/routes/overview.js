import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/kpis", async (req, res) => {
  try {
    const db = getDb();
    const now = new Date();
    const startOfMonth     = new Date(now.getFullYear(), now.getMonth(), 1);
    const startOfPrevMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const startOfWeek      = new Date(now);
    startOfWeek.setDate(now.getDate() - now.getDay());
    startOfWeek.setHours(0, 0, 0, 0);

    const [consoCurrent, consoPrev, metersNow, metersPrev, sparkWindows, predictions, activeAlerts] =
      await Promise.all([
        db.collection("meters_aggregated_15min").aggregate([
          { $match: { window_start: { $gte: startOfMonth } } },
          { $group: { _id: null, total: { $sum: "$total_kwh" } } },
        ]).toArray(),
        db.collection("meters_aggregated_15min").aggregate([
          { $match: { window_start: { $gte: startOfPrevMonth, $lt: startOfMonth } } },
          { $group: { _id: null, total: { $sum: "$total_kwh" } } },
        ]).toArray(),
        db.collection("meters_aggregated_15min").distinct("meter_id", {
          window_start: { $gte: startOfMonth },
        }),
        db.collection("meters_aggregated_15min").distinct("meter_id", {
          window_start: { $gte: startOfPrevMonth, $lt: startOfMonth },
        }),
        db.collection("meters_aggregated_15min")
          .countDocuments({ window_start: { $gte: startOfWeek } }),
        db.collection("ml_predictions").estimatedDocumentCount(),
        db.collection("dashboard_alerts")
          .countDocuments({ acked: { $ne: true }, level: { $in: ["CRITICAL", "WARNING"] } }),
      ]);

    const totalConsumption = Math.round(consoCurrent[0]?.total ?? 0);
    const prevTotal        = Math.round(consoPrev[0]?.total ?? 1);
    const consumptionDelta = prevTotal > 0
      ? +((( totalConsumption - prevTotal) / prevTotal) * 100).toFixed(1)
      : 0;
    const metersDelta = metersPrev.length > 0
      ? +((( metersNow.length - metersPrev.length) / metersPrev.length) * 100).toFixed(1)
      : 0;

    res.json({
      totalConsumption,
      consumptionDelta,
      activeMeters: metersNow.length,
      metersDelta,
      sparkWindows,
      predictions,
      activeAlerts,
    });
  } catch (err) {
    console.error("[overview/kpis]", err);
    res.status(500).json({ error: "Failed to fetch KPIs" });
  }
});

router.get("/recent-windows", async (req, res) => {
  try {
    const db    = getDb();
    const limit = Math.min(parseInt(req.query.limit ?? "24"), 96);
    const docs  = await db.collection("meters_aggregated_15min")
      .find({})
      .sort({ window_start: -1 })
      .limit(limit)
      .toArray();
    res.json(docs.reverse());
  } catch (err) {
    console.error("[overview/recent-windows]", err);
    res.status(500).json({ error: "Failed to fetch windows" });
  }
});

export default router;
