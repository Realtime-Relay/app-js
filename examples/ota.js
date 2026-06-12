import { RelayApp } from "../src/index.js";
import { readFileSync } from "fs";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "test",
});

await app.connect();
console.log("Connected to RelayX\n");

// ── Init: get the HTTP token + file-handler URL ──────────────
// Required once before firmwareUpload / firmwareDelete (token lives ~2h —
// call init() again to refresh). firmwareList works without it.

await app.ota.init();
console.log("OTA initialized\n");

// ── Upload a firmware binary ─────────────────────────────────

const filePath = "/Users/arjun/Downloads/MMA2604_WB_FD.pdf";
const file = readFileSync(filePath); // Buffer; Blob/File also works

console.log("Uploading...")

const uploaded = await app.ota.firmwareUpload({
  name: "Boiler controller fix", // display name shown in UI pickers
  version: "1.0.4",              // unique per org — duplicate → VERSION_EXISTS
  file,
  file_name: filePath,           // full path OK — SDK sends just the basename
});
console.log("Uploaded:");
console.log(`  id:      ${uploaded.firmware_id}`);
console.log(`  version: ${uploaded.version}`);
console.log(`  sha256:  ${uploaded.sha256}`);
console.log(`  size:    ${uploaded.size} bytes\n`);

// ── List firmware (paginated, newest first) ──────────────────

const { firmwares, page } = await app.ota.firmwareList({ page: 1 });
console.log(`Firmware (${page.count}${page.has_more ? "+" : ""}):`);
firmwares.forEach((f) =>
  console.log(`  - ${f.name} v${f.version} (${f.size} bytes) [${f.firmware_id}]`),
);

// Walk remaining pages, if any
let p = 1;
let more = page.has_more;
while (more) {
  p += 1;
  const next = await app.ota.firmwareList({ page: p });
  next.firmwares.forEach((f) => console.log(`  - ${f.name} v${f.version}`));
  more = next.page.has_more;
}
console.log();

// ── Error handling: duplicate version ────────────────────────

try {
  await app.ota.firmwareUpload({ name: "dupe", version: "1.0.3", file });
} catch (err) {
  console.log(`Expected failure: ${err.message} (code: ${err.code})\n`);
  // → firmware upload failed: Firmware version already exists (code: VERSION_EXISTS)
}

// ── Create a DRAFT rollout ───────────────────────────────────
// Pure intent — no jobs exist yet. device_count is a PREVIEW; the real
// snapshot happens at activation, against the fleet as it exists then.

const rollout = await app.ota.createRollout({
  firmware_id: uploaded.firmware_id,
  request_type: "DOWNLOAD_INSTALL", // or "DOWNLOAD_ONLY" to pre-stage
  target: { type: "all" }, // or devices (device_ids) / logical_group / hierarchy_group
  force_install: false,
  user_config: { apply: "app_gated" },
});
console.log(`Draft rollout: ${rollout.rollout_id} → ~${rollout.device_count} devices (preview)`);

// ── Update the draft (anything past DRAFT is immutable) ──────

await app.ota.updateRollout({
  rollout_id: rollout.rollout_id,
  request_type: "DOWNLOAD_ONLY",
});
console.log("Rollout updated to DOWNLOAD_ONLY");

// ── Activate: THE snapshot moment — jobs created + blast ─────

const active = await app.ota.toggleRollout({
  rollout_id: rollout.rollout_id,
  state: "ACTIVE",
});
console.log(`Rollout ACTIVE → ${active.device_count} devices snapshotted\n`);

// ── Wind down: stop the rollout, then delete the firmware ────
// Activated rollouts can't be deleted (permanent history) — STOP them.
// A STOPPED rollout no longer blocks firmware deletion.

await app.ota.toggleRollout({ rollout_id: rollout.rollout_id, state: "STOPPED" });
console.log("Rollout stopped (jobs preserved as history)");

const deleted = await app.ota.firmwareDelete({ id: uploaded.firmware_id });
console.log(`Deleted: ${deleted.firmware_id} (${deleted.deleted})`);

await app.disconnect();
