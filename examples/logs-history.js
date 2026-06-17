/**
 * Fetch device log history for "mc-01" (info / warn / error).
 *
 * Goes through app.log.history(), which now hits the influx-db-service REST
 * API (POST /iot/db/log/history) under the hood — same inputs/outputs as
 * before.
 *
 * Run:  RELAY_API_KEY=... RELAY_SECRET=... node examples/logs-history.js
 */

import { RelayApp } from "../src/index.js";

const DEVICE_IDENT = "mc-01";
const LEVELS = ["info", "warn", "error"];
const START = "2026-06-16T00:00:00.000Z";
const END = "2026-06-18T00:00:00.000Z";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

await app.connect();
console.log("Connected to RelayX");

try {
  const t0 = performance.now();
  const data = await app.log.history({
    device_ident: DEVICE_IDENT,
    levels: LEVELS,
    start: START,
    end: END,
  });
  const elapsedMs = Math.round(performance.now() - t0);

  // data = { info: [{ value, timestamp }, ...], warn: [...], error: [...] }
  console.log(JSON.stringify(data, null, 2));
  for (const level of LEVELS) {
    console.log(`mc-01 "${level}": ${(data[level] ?? []).length} log(s)`);
  }
  console.log(`fetched in ${elapsedMs}ms`);
} catch (err) {
  console.error("log.history failed:", err.message);
} finally {
  await app.disconnect();
}
