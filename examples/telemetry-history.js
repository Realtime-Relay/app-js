/**
 * Fetch telemetry history for device "mc-01", field "current_s1".
 *
 * Goes through app.telemetry.history(), which now hits the influx-db-service
 * REST API (POST /iot/db/telemetry/history) under the hood — same
 * inputs/outputs as before.
 *
 * Run:  RELAY_API_KEY=... RELAY_SECRET=... node examples/telemetry-history.js
 */

import { RelayApp } from "../src/index.js";

const DEVICE_IDENT = "mc-01";
const FIELDS = ["current_s1", "wifi_rssi"];
const START = "2026-06-16T00:00:00.000Z";
const END = "2026-06-17T00:00:00.000Z";

// Also fetch the latest reading per field over the window (telemetry.latest()).
const SHOW_LATEST = true;

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

await app.connect();
console.log("Connected to RelayX");

try {
  const t0 = performance.now();
  const data = await app.telemetry.history({
    device_ident: DEVICE_IDENT,
    fields: FIELDS,
    start: START,
    end: END,
  });
  const elapsedMs = Math.round(performance.now() - t0);

  // data = { current_s1: [{ value, timestamp }, ...] }
  const cPoints = data.current_s1 ?? [];
  const wPoints = data.wifi_rssi ?? [];
  console.log(JSON.stringify(data, null, 2));
  console.log(`mc-01 "current_s1": ${cPoints.length} reading(s)`);
  console.log(`mc-01 "wifi_rssi": ${wPoints.length} reading(s)`);
  console.log(`history fetched in ${elapsedMs}ms`);

  if (SHOW_LATEST) {
    const lt0 = performance.now();
    const latest = await app.telemetry.latest({
      device_ident: DEVICE_IDENT,
      fields: FIELDS,
      start: START,
      end: END,
    });
    const latestMs = Math.round(performance.now() - lt0);

    // latest = { current_s1: { value, timestamp }, wifi_rssi: { value, timestamp } }
    console.log("\nlatest:");
    console.log(JSON.stringify(latest, null, 2));
    console.log(`latest fetched in ${latestMs}ms`);
  }
} catch (err) {
  console.error("telemetry.history failed:", err.message);
} finally {
  await app.disconnect();
}
