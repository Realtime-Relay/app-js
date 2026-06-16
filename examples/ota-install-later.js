/**
 * OTA install-later + live phase updates.
 *
 * Two operator-side features, working together:
 *   1. app.ota.onJobPhaseUpdate(cb) — live push of every device job phase
 *      transition (no polling jobsList). Drives this whole script.
 *   2. app.ota.installRollout({ rollout_id }) — tell devices that STAGED the
 *      image (phase DOWNLOADED, via a DOWNLOAD_ONLY rollout) to install it now,
 *      without re-downloading.
 *
 * Flow: upload -> DOWNLOAD_ONLY rollout (stage only) -> wait for DOWNLOADED
 * (seen live) -> installRollout() -> wait for INSTALLED (seen live).
 *
 * Run:  RELAY_API_KEY=... RELAY_SECRET=... node examples/ota-install-later.js
 */

import { RelayApp } from "../src/index.js";
import { readFileSync } from "fs";

// ── Edit for your test ───────────────────────────────────────────────────
const BIN_PATH =
  "/Users/arjun/Code/Relay/DeviceSDK/device-cpp/example/build/device-cpp-example.bin";
const VERSION = "1.0.4"; // must differ from the running version, unique per (name, version)
const TARGET = { type: "all" };
const STEP_TIMEOUT_MS = 120_000;
// ───────────────────────────────────────────────────────────────────────────

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

await app.connect();
await app.ota.init();
console.log("Connected + OTA initialized");

// ── Live phase updates: log every transition, resolve any phase waiters ──
const waiters = [];
app.ota.onJobPhaseUpdate((u) => {
  console.log(`[phase] device=${u.device_id} -> ${u.phase}` + (u.error ? ` (err: ${u.error})` : ""));
  for (let i = waiters.length - 1; i >= 0; i--) {
    if (waiters[i].phase === u.phase) waiters.splice(i, 1)[0].resolve(u);
  }
});

// Resolve when any device reports `phase`; reject after STEP_TIMEOUT_MS.
function waitForPhase(phase) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      const i = waiters.indexOf(w);
      if (i >= 0) waiters.splice(i, 1);
      reject(new Error(`timed out waiting for ${phase}`));
    }, STEP_TIMEOUT_MS);
    const w = { phase, resolve: (u) => { clearTimeout(timer); resolve(u); } };
    waiters.push(w);
  });
}

async function bail(msg) {
  console.error(msg);
  app.ota.offJobPhaseUpdate();
  await app.disconnect();
  process.exit(1);
}

// ── 1. Upload + DOWNLOAD_ONLY rollout (stage only, don't install) ──
const file = readFileSync(BIN_PATH);
console.log(`Uploading ${BIN_PATH} (${file.length} bytes) as v${VERSION}...`);
const fw = await app.ota.firmwareUpload({
  name: `install-later v${VERSION}`,
  version: VERSION,
  file,
  file_name: BIN_PATH,
});
console.log(`Uploaded: ${fw.firmware_id}`);

const draft = await app.ota.createRollout({
  firmware_id: fw.firmware_id,
  request_type: "DOWNLOAD_ONLY", // stage only — device parks at DOWNLOADED
  target: TARGET,
});
const active = await app.ota.toggleRollout({ rollout_id: draft.rollout_id, state: "ACTIVE" });
console.log(`Staging rollout ${draft.rollout_id} -> ${active.device_count} device(s)`);

// ── 2. Wait (live) for a device to stage the image ──
console.log("Waiting for DOWNLOADED...");
await waitForPhase("DOWNLOADED").catch(() => bail("No device staged the image — is it online?"));

// ── 3. Install-later: nudge the staged device(s) to install now ──
console.log("\nImage staged. Installing now (INSTALL_ONLY, no re-download)...");
const res = await app.ota.installRollout({ rollout_id: draft.rollout_id });
console.log(`installRollout -> ${res.installing} device(s) nudged to install`);

// ── 4. Wait (live) for the install to complete ──
console.log("Waiting for INSTALLED...");
await waitForPhase("INSTALLED")
  .then(() => console.log("\nDone — installed without re-downloading."))
  .catch(() => console.error("Did not see INSTALLED in time."));

app.ota.offJobPhaseUpdate();
await app.disconnect();
