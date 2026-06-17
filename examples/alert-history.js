/**
 * Fetch alert event history for device "mc-01" (fire / resolved / ack).
 *
 * Goes through app.alert.history(), which now hits the influx-db-service REST
 * API (POST /iot/db/alerts/history) under the hood — same inputs/outputs as
 * before.
 *
 * Run:  RELAY_API_KEY=... RELAY_SECRET=... node examples/alert-history.js
 */

import { RelayApp } from "../src/index.js";

const DEVICE_IDENT = "mc-01";
const RULE_STATES = ["fire", "resolved", "ack"];
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
  const data = await app.alert.history({
    rule_type: "DEVICE",
    device_ident: DEVICE_IDENT,
    rule_states: RULE_STATES,
    start: START,
    end: END,
  });
  const elapsedMs = Math.round(performance.now() - t0);

  // data = { events: [{ state, value, timestamp, incident_id, rule_id, device_id }, ...] }
  const events = data.events ?? [];
  console.log(JSON.stringify(data, null, 2));
  console.log(`mc-01 alerts: ${events.length} event(s)`);
  console.log(`fetched in ${elapsedMs}ms`);
} catch (err) {
  console.error("alerts.history failed:", err.message);
} finally {
  await app.disconnect();
}
