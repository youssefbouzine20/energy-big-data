import { Router } from "express";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const router = Router();

// dashboard/backend/src/routes/ml.js -> repo root is 4 levels up.
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const METRICS_PATH = path.resolve(__dirname, "../../../../ml/metrics.json");

router.get("/metrics", async (_req, res, next) => {
  try {
    const raw = await readFile(METRICS_PATH, "utf-8");
    const metrics = JSON.parse(raw);
    const models = metrics.models ?? {};
    const entries = Object.entries(models);

    let best = null;
    for (const [name, m] of entries) {
      if (typeof m?.r2 !== "number") continue;
      if (best == null || m.r2 > best.r2) best = { name, ...m };
    }

    res.json({
      evaluated_at: metrics.evaluated_at ?? null,
      best,
      all: models,
    });
  } catch (err) {
    if (err.code === "ENOENT") {
      return res.status(404).json({
        error: "ml/metrics.json not found — run 'python -m ml.run_pipeline' first",
      });
    }
    next(err);
  }
});

export default router;
