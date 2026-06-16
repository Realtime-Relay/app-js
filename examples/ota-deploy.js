/**
 * OTA deploy — push one firmware build to your device(s) and leave it live.
 *
 * Upload a hardcoded .bin -> create a DOWNLOAD_INSTALL rollout -> activate it.
 * Unlike examples/ota.js (the full API tour), this does NOT stop/delete at the
 * end, so the device actually receives and installs the update.
 *
 * Run:  RELAY_API_KEY=... RELAY_SECRET=... node examples/ota-deploy.js
 */

import { RelayApp } from "../src/index.js";
import { readFileSync } from "fs";

// ── Edit these for your test ─────────────────────────────────────────────
// The app image produced by the device-cpp example build (NOT the merged
// image / bootloader). See the build output path in the device example.
const BIN_PATH =
  "/Users/arjun/Code/Relay/DeviceSDK/device-cpp/example/build/device-cpp-example.bin";

// MUST match CONFIG_APP_PROJECT_VER of the built bin, AND differ from the
// version the device is currently running (else the device dedups and skips).
// Firmware is UNIQUE PER (org, name, version) — reusing the same NAME + VERSION
// fails with VERSION_EXISTS, so bump VERSION each run (or change NAME below).
const VERSION = "1.0.1";

// Who gets it. { type: "all" } = every device in the org. To hit one device:
//   { type: "devices", device_ids: ["<device ObjectId>"] }
const TARGET = { type: "all" };
// ─────────────────────────────────────────────────────────────────────────

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

await app.connect();
console.log("Connected to RelayX");

// init() exchanges the NATS credential for the HTTP token used by the upload.
await app.ota.init();
console.log("OTA initialized");

// ── 1. Upload the binary ─────────────────────────────────────
const file = readFileSync(BIN_PATH); // Buffer
console.log(`Uploading ${BIN_PATH} (${file.length} bytes) as v${VERSION}...`);

let fw;
try {
  fw = await app.ota.firmwareUpload({
    name: `device-cpp-example v${VERSION}`,
    version: VERSION,
    file,
    file_name: BIN_PATH, // full path OK — SDK sends just the basename + ext
  });
} catch (err) {
  // Most common trip-up: re-running without bumping VERSION.
  console.error(`Upload failed: ${err.message} (code: ${err.code ?? "?"})`);
  if (err.code === "VERSION_EXISTS") {
    console.error("→ this name + version already exists. Bump VERSION (or change name) and rebuild, or delete the old firmware first.");
  }
  await app.disconnect();
  process.exit(1);
}
console.log(`Uploaded: id=${fw.firmware_id} sha256=${fw.sha256} size=${fw.size}`);

// ── 2. Create a DRAFT rollout (intent only — no jobs yet) ────
const draft = await app.ota.createRollout({
  firmware_id: fw.firmware_id,
  request_type: "DOWNLOAD_INSTALL", // download AND install (vs DOWNLOAD_ONLY to pre-stage)
  target: TARGET,
  force_download: false, // true overrides the device on_download veto
  force_install: false,  // true overrides the device on_pre_install veto
  user_config: { apply: "app_gated" }, // device hooks gate the install
});
console.log(`Draft rollout ${draft.rollout_id} → ~${draft.device_count} device(s) (preview)`);

// ── 3. Activate — THE snapshot moment: jobs created + nudges blasted ─
const active = await app.ota.toggleRollout({
  rollout_id: draft.rollout_id,
  state: "ACTIVE",
});
console.log(`ACTIVE → ${active.device_count} device(s) snapshotted.`);
console.log(`\nRollout is LIVE. rollout_id = ${draft.rollout_id}`);
console.log("Head devices get an immediate nudge; others pick it up on their next poll.");
console.log("Watch the device serial log for the firmware version after it reboots.");

await app.disconnect();
