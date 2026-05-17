import { MongoClient } from "mongodb";

let client = null;
let db = null;
let connecting = false;
let retryTimer = null;
let lastFailureLog = 0;

function buildUri() {
  const host = process.env.MONGO_HOST || "localhost";
  const port = process.env.MONGO_PORT || "27017";
  const user = process.env.MONGO_USERNAME || "energy_admin";
  const pass = process.env.MONGO_PASSWORD || "change-me-before-deploy";
  return `mongodb://${encodeURIComponent(user)}:${encodeURIComponent(pass)}@${host}:${port}/?authSource=admin`;
}

async function attemptConnect() {
  if (connecting || db) return;
  connecting = true;
  const dbName = process.env.MONGO_DB_NAME || "energy_db";
  const tentative = new MongoClient(buildUri(), { serverSelectionTimeoutMS: 3000 });
  try {
    await tentative.connect();
    await tentative.db(dbName).command({ ping: 1 });
    client = tentative;
    db = client.db(dbName);
    console.log(`[db] connected to mongodb (${dbName})`);
    lastFailureLog = 0;
  } catch (err) {
    try { await tentative.close(); } catch { /* ignore */ }
    const now = Date.now();
    if (now - lastFailureLog > 30_000) {
      console.warn(`[db] mongo unavailable: ${err.message} — will retry every 5s`);
      lastFailureLog = now;
    }
  } finally {
    connecting = false;
  }
}

export function startMongo() {
  attemptConnect();
  if (!retryTimer) retryTimer = setInterval(attemptConnect, 5000);
}

export function isConnected() {
  return db != null;
}

export function getDb() {
  if (!db) {
    const err = new Error("database temporarily unavailable — is MongoDB running?");
    err.status = 503;
    throw err;
  }
  return db;
}

export async function closeMongo() {
  if (retryTimer) { clearInterval(retryTimer); retryTimer = null; }
  if (client) {
    try { await client.close(); } catch { /* ignore */ }
    client = null;
    db = null;
  }
}
