import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/latest", async (_req, res, next) => {
  try {
    const db = getDb();
    const topics = ["smart-meters", "weather", "incident-reports", "rss-feeds", "market-prices", "user-feedback"];
    const items = [];
    for (const topic of topics) {
      const [doc] = await db.collection("data_quality_metrics")
        .find({ topic }, { projection: { _id: 0 } })
        .sort({ window_start: -1 })
        .limit(1)
        .toArray();
      items.push({ topic, latest: doc || null });
    }
    res.json({ items, as_of: new Date().toISOString() });
  } catch (err) { next(err); }
});

router.get("/history", async (req, res, next) => {
  try {
    const db = getDb();
    const topic = req.query.topic || "smart-meters";
    const limit = Math.min(Number(req.query.limit) || 48, 200);
    const rows = await db.collection("data_quality_metrics")
      .find({ topic }, { projection: { _id: 0 } })
      .sort({ window_start: -1 })
      .limit(limit)
      .toArray();
    res.json({ topic, items: rows.reverse() });
  } catch (err) { next(err); }
});

export default router;
