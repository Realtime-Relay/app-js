/**
 * Fetch event history for device "mc-01", event name "state".
 *
 * Goes through app.events.history(), which now hits the influx-db-service REST
 * API (POST /iot/db/event/history) under the hood — same inputs/outputs as
 * before.
 *
 * Run:  RELAY_API_KEY=... RELAY_SECRET=... node examples/events-history.js
 */

import { RelayApp } from "../src/index.js";

const DEVICE_IDENT = "mc-01";
const EVENT_NAMES = ["state"];
const START = "2026-06-16T00:00:00.000Z";
const END = "2026-06-17T00:00:00.000Z";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

await app.connect();
console.log("Connected to RelayX");

try {
  const t0 = performance.now();
  const data = await app.events.history({
    device_ident: DEVICE_IDENT,
    event_names: EVENT_NAMES,
    start: START,
    end: END,
  });
  const elapsedMs = Math.round(performance.now() - t0);

  // data = { state: [{ value, timestamp }, ...] }
  const points = data.state ?? [];
  console.log(JSON.stringify(data, null, 2));
  console.log(`mc-01 "state": ${points.length} event(s) in ${elapsedMs}ms`);
} catch (err) {
  console.error("events.history failed:", err.message);
} finally {
  await app.disconnect();
}
