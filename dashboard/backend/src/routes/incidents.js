import { Router } from "express";
import { getDb } from "../db.js";

const router = Router();

router.get("/", async (req, res, next) => {
  try {
    const db = getDb();
    const limit = Math.min(Number(req.query.limit) || 20, 200);
    const rows = await db.collection("incidents")
      .find({}, { projection: { _id: 0 } })
      .sort({ timestamp: -1 })
      .limit(limit)
      .toArray();
    res.json({ items: rows });
  } catch (err) { next(err); }
});

router.get("/wordcloud", async (req, res, next) => {
  try {
    const db = getDb();
    const days = Math.min(Number(req.query.days) || 7, 90);
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const counts = new Map();
    const enriched = await db.collection("incidents_enriched")
      .find({ timestamp: { $gte: since } }, { projection: { _id: 0, nlp_keywords: 1 } })
      .limit(2000)
      .toArray();
    for (const doc of enriched) {
      for (const kw of (doc.nlp_keywords || [])) {
        const k = String(kw).toLowerCase();
        if (k.length < 3) continue;
        counts.set(k, (counts.get(k) || 0) + 1);
      }
    }

    if (counts.size === 0) {
      const descs = await db.collection("incidents")
        .find({ timestamp: { $gte: since } }, { projection: { _id: 0, description: 1 } })
        .limit(2000)
        .toArray();
      const stop = new Set(["the","and","for","with","that","this","from","have","has","was","were","are","but","not","you","your","can","will","been","near","into","over","under","when","where","what","which","they","them","there","than","then","also","more","most","some","such","very","only","just","made","make","being","done"]);
      for (const d of descs) {
        for (const w of String(d.description || "").toLowerCase().split(/[^a-z]+/)) {
          if (w.length < 4 || stop.has(w)) continue;
          counts.set(w, (counts.get(w) || 0) + 1);
        }
      }
    }

    const items = [...counts.entries()]
      .map(([text, value]) => ({ text, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 60);
    res.json({ items });
  } catch (err) { next(err); }
});

export default router;
