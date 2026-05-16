// =============================================================================
// MongoDB initialization script for energy_db (P3 Storage layer)
//
// Mongo runs every *.js in /docker-entrypoint-initdb.d/ ONCE on first startup
// (when /data/db is empty). It is NOT re-run on subsequent starts. To re-init,
// destroy the volume:  docker compose -f docker/docker-compose.<mode>.yml down -v
//
// What this script does:
//   1. Creates a writer service-account (spark_writer) with readWrite on energy_db.
//      The Spark Structured Streaming job authenticates as this user — never as
//      the root admin. Rotate the password before any non-local deployment.
//   2. Creates the 5 collections P2/P3/P5 will write to.
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
['meters_raw',
 'meters_aggregated_15min',
 'weather',
 'incidents',
 'ml_predictions'].forEach(name => {
    if (!db.getCollectionNames().includes(name)) {
        db.createCollection(name);
    }
});

// ── 3. Query indexes ────────────────────────────────────────────────────────
// Compound (zone, timestamp) covers heatmap queries by zone + time range.
// Compound (meter_id, timestamp) covers per-meter history queries.
// Single (timestamp) is needed for the TTL index AND for global time-range scans.
db.meters_raw.createIndex({ meter_id: 1, timestamp: -1 });
db.meters_raw.createIndex({ zone:     1, timestamp: -1 });

db.meters_aggregated_15min.createIndex({ zone:        1, window_start: -1 });
db.meters_aggregated_15min.createIndex({ window_start: 1 });

db.weather.createIndex({ timestamp: -1 });
db.weather.createIndex({ weather_severity: 1, timestamp: -1 });

db.incidents.createIndex({ zone: 1,  timestamp: -1 });
db.incidents.createIndex({ severity: 1, timestamp: -1 });
db.incidents.createIndex({ resolved: 1 });

db.ml_predictions.createIndex({ model_name: 1, prediction_time: -1 });
db.ml_predictions.createIndex({ zone: 1, prediction_time: -1 });

// ── 4. TTL indexes (GDPR-enforced retention) ────────────────────────────────
// expireAfterSeconds tells Mongo to delete documents whose indexed field is
// older than the threshold. Mongo runs the cleanup ~every 60 seconds.
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
db.ml_predictions.createIndex(
    { prediction_time: 1 },
    { expireAfterSeconds: RETENTION_PREDICTIONS_DAYS * SECONDS_PER_DAY,
      name: 'ttl_predictions_180d' }
);

print('[init-mongo] energy_db initialized: 5 collections, indexes, TTL policies, spark_writer user.');
