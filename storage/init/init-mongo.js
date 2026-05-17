// =============================================================================
// MongoDB initialization script for energy_db (P3 Storage layer)
//
// Mongo runs every *.js in /docker-entrypoint-initdb.d/ ONCE on first startup
// (when /data/db is empty). It is NOT re-run on subsequent starts. To re-init,
// destroy the volume:  docker compose -f docker/docker-compose.<mode>.yml down -v
//
// Idempotent: every operation either silently does nothing or replaces the same
// end-state. Safe to run twice against the same volume (e.g. via mongosh) to
// retrofit indexes onto a database that already has data.
//
// What this script does:
//   1. Creates a writer service-account (spark_writer) with readWrite on energy_db.
//      The Spark Structured Streaming job authenticates as this user — never as
//      the root admin. Rotate the password before any non-local deployment.
//   2. Creates ALL 11 collections P2/P3/P4/P5 will write to (5 raw + 6 derived).
//   3. Adds compound + single-field indexes for time-range and zone queries.
//   4. Adds TTL indexes that enforce GDPR retention automatically (see ethics/GDPR.md).
//
// All durations are in seconds. Adjust if the GDPR.md retention policy changes.
// =============================================================================

const RETENTION_RAW_DAYS         = 90;   // smart-meter raw points
const RETENTION_AGGREGATED_DAYS  = 365;  // 15-min Spark windows
const RETENTION_WEATHER_DAYS     = 365;
const RETENTION_INCIDENTS_DAYS   = 730;  // 2 years (audit trail)
const RETENTION_PREDICTIONS_DAYS = 180;
const RETENTION_FEEDBACK_DAYS    = 365;
const RETENTION_RSS_DAYS         = 365;
const RETENTION_MARKET_DAYS      = 365;
const RETENTION_QUALITY_DAYS     = 365;
const RETENTION_ALERTS_DAYS      = 365;  // dashboard audit log

const SECONDS_PER_DAY = 86400;

// Switch to the application database (Mongo creates it on first write).
db = db.getSiblingDB('energy_db');

// ── 1. Service account ──────────────────────────────────────────────────────
// Created idempotently — drop+recreate so re-running on a fresh volume always
// produces the same end-state.
try { db.dropUser('spark_writer'); } catch (e) { /* user did not exist */ }
db.createUser({
    user: 'spark_writer',
    pwd:  'change-me-before-deploy',  // override via Spark job secrets in production
    roles: [{ role: 'readWrite', db: 'energy_db' }],
});

// ── 2. Collections ──────────────────────────────────────────────────────────
// Listing them up front (instead of lazy-creating on first insert) lets P3 set
// per-collection options (validators, capped, etc.) here in one place.
[
    // P1 producers + P2 streams_meters
    'meters_raw',
    'meters_aggregated_15min',
    // P1 producer + P2 streams_weather
    'weather',
    // P1 producer + P2 streams_incidents (raw passthrough)
    'incidents',
    // P2 streams_incidents (NLP enrichment with nlp_keywords + tokens)
    'incidents_enriched',
    // P1 producer + P2 streams_feedback (with sentiment_predicted + tokens)
    'feedback_nlp',
    // P1 producer + P2 streams_external (rss passthrough)
    'rss_feeds',
    // P1 producer + P2 streams_external (market-prices passthrough)
    'market_prices',
    // P2 data_quality (one doc per topic per 15-min window)
    'data_quality_metrics',
    // P4 dashboard audit log (ack of predictive alerts)
    'dashboard_alerts',
    // P5 ML (forecast + anomaly probability per zone per window)
    'ml_predictions',
].forEach(name => {
    if (!db.getCollectionNames().includes(name)) {
        db.createCollection(name);
    }
});

// ── 3. Query indexes ────────────────────────────────────────────────────────
// Compound (zone, timestamp) covers heatmap queries by zone + time range.
// Compound (meter_id, timestamp) covers per-meter history queries.
// Single (timestamp) is needed for the TTL index AND for global time-range scans.
// createIndex is idempotent — re-running just confirms the index exists.
db.meters_raw.createIndex({ meter_id: 1, timestamp: -1 });
db.meters_raw.createIndex({ zone:     1, timestamp: -1 });

db.meters_aggregated_15min.createIndex({ zone:        1, window_start: -1 });
db.meters_aggregated_15min.createIndex({ window_start: 1 });

db.weather.createIndex({ timestamp: -1 });
db.weather.createIndex({ weather_severity: 1, timestamp: -1 });

db.incidents.createIndex({ zone: 1,  timestamp: -1 });
db.incidents.createIndex({ severity: 1, timestamp: -1 });
db.incidents.createIndex({ resolved: 1 });

db.incidents_enriched.createIndex({ zone: 1, timestamp: -1 });
db.incidents_enriched.createIndex({ severity: 1, timestamp: -1 });
db.incidents_enriched.createIndex({ nlp_keywords: 1 });  // for word-cloud lookup

db.feedback_nlp.createIndex({ zone: 1, timestamp: -1 });
db.feedback_nlp.createIndex({ sentiment_predicted: 1, timestamp: -1 });
db.feedback_nlp.createIndex({ category: 1, timestamp: -1 });

db.rss_feeds.createIndex({ timestamp: -1 });
db.rss_feeds.createIndex({ category: 1, timestamp: -1 });
db.rss_feeds.createIndex({ impact_score: -1, timestamp: -1 });

db.market_prices.createIndex({ timestamp: -1 });
db.market_prices.createIndex({ market: 1, timestamp: -1 });

db.data_quality_metrics.createIndex({ topic: 1, window_start: -1 });
db.data_quality_metrics.createIndex({ alert: 1, computed_at: -1 });

db.dashboard_alerts.createIndex({ alert_level: 1, triggered_at: -1 });
db.dashboard_alerts.createIndex({ zone: 1, triggered_at: -1 });

db.ml_predictions.createIndex({ model_name: 1, prediction_time: -1 });
db.ml_predictions.createIndex({ zone: 1, prediction_time: -1 });

// ── 4. TTL indexes (GDPR-enforced retention) ────────────────────────────────
// expireAfterSeconds tells Mongo to delete documents whose indexed field is
// older than the threshold. Mongo runs the cleanup ~every 60 seconds.
// Named indexes so we can identify them in db.<col>.getIndexes() output.
db.meters_raw.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: RETENTION_RAW_DAYS * SECONDS_PER_DAY,
      name: 'ttl_raw_90d' }
);
db.meters_aggregated_15min.createIndex(
    { window_start: 1 },
    { expireAfterSeconds: RETENTION_AGGREGATED_DAYS * SECONDS_PER_DAY,
      name: 'ttl_aggregated_365d' }
);
db.weather.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: RETENTION_WEATHER_DAYS * SECONDS_PER_DAY,
      name: 'ttl_weather_365d' }
);
db.incidents.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: RETENTION_INCIDENTS_DAYS * SECONDS_PER_DAY,
      name: 'ttl_incidents_730d' }
);
db.incidents_enriched.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: RETENTION_INCIDENTS_DAYS * SECONDS_PER_DAY,
      name: 'ttl_incidents_enriched_730d' }
);
db.feedback_nlp.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: RETENTION_FEEDBACK_DAYS * SECONDS_PER_DAY,
      name: 'ttl_feedback_365d' }
);
db.rss_feeds.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: RETENTION_RSS_DAYS * SECONDS_PER_DAY,
      name: 'ttl_rss_365d' }
);
db.market_prices.createIndex(
    { timestamp: 1 },
    { expireAfterSeconds: RETENTION_MARKET_DAYS * SECONDS_PER_DAY,
      name: 'ttl_market_365d' }
);
db.data_quality_metrics.createIndex(
    { window_start: 1 },
    { expireAfterSeconds: RETENTION_QUALITY_DAYS * SECONDS_PER_DAY,
      name: 'ttl_quality_365d' }
);
db.dashboard_alerts.createIndex(
    { triggered_at: 1 },
    { expireAfterSeconds: RETENTION_ALERTS_DAYS * SECONDS_PER_DAY,
      name: 'ttl_alerts_365d' }
);
db.ml_predictions.createIndex(
    { prediction_time: 1 },
    { expireAfterSeconds: RETENTION_PREDICTIONS_DAYS * SECONDS_PER_DAY,
      name: 'ttl_predictions_180d' }
);

print('[init-mongo] energy_db initialized: 11 collections, indexes, TTL policies, spark_writer user.');
