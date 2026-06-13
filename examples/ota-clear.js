/**
 * OTA reset - clear all firmware and rollouts for the org. Handy for wiping
 * test state between OTA runs.
 *
 * Rollouts: DRAFT rollouts are deleted; ACTIVE/PAUSED are STOPPED. STOPPED
 * rollouts are permanent audit history (the engine never deletes them), but
 * once stopped they no longer block firmware deletion.
 * Firmware: every listed artifact is deleted.
 *
 * Run:  RELAY_API_KEY=... RELAY_SECRET=... node examples/ota-clear.js
 */

import { RelayApp } from "../src/index.js";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

await app.connect();
console.log("Connected to RelayX");

await app.ota.init(); // firmwareDelete goes over HTTP

// Walk every page of a paginated list into one array.
async function listAll(fn, key) {
  const out = [];
  let page = 1;
  for (;;) {
    const res = await fn({ page, limit: 200 });
    out.push(...res[key]);
    if (!res.page?.has_more) break;
    page++;
  }
  return out;
}

// ── Rollouts ─────────────────────────────────────────────────
const rollouts = await listAll((p) => app.ota.rolloutList(p), "rollouts");
console.log(`\nRollouts: ${rollouts.length}`);

let deleted = 0, stopped = 0, kept = 0;
for (const r of rollouts) {
  try {
    if (r.status === "DRAFT") {
      await app.ota.deleteRollout({ rollout_id: r.rollout_id });
      deleted++;
      console.log(`  deleted draft ${r.rollout_id}`);
    } else if (r.status === "ACTIVE" || r.status === "PAUSED") {
      await app.ota.toggleRollout({ rollout_id: r.rollout_id, state: "STOPPED" });
      stopped++;
      console.log(`  stopped ${r.status.toLowerCase()} ${r.rollout_id}`);
    } else {
      kept++; // already STOPPED - permanent history, not deletable
    }
  } catch (err) {
    console.error(`  rollout ${r.rollout_id}: ${err.message}`);
  }
}
console.log(`  ${deleted} deleted, ${stopped} stopped, ${kept} already stopped (kept as history)`);

// ── Firmware ─────────────────────────────────────────────────
const firmware = await listAll((p) => app.ota.firmwareList(p), "firmwares");
console.log(`\nFirmware: ${firmware.length}`);

let fwDeleted = 0, fwFailed = 0;
for (const f of firmware) {
  try {
    await app.ota.firmwareDelete({ id: f.firmware_id });
    fwDeleted++;
    console.log(`  deleted ${f.name} v${f.version} [${f.firmware_id}]`);
  } catch (err) {
    fwFailed++;
    console.error(`  failed ${f.firmware_id}: ${err.message}`);
  }
}
console.log(`  ${fwDeleted} deleted${fwFailed ? `, ${fwFailed} failed` : ""}`);

await app.disconnect();
console.log("\nDone.");
