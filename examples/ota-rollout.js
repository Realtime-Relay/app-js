import { RelayApp } from "../src/index.js";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "test",
});

await app.connect();
console.log("Connected to RelayX\n");

// Rollouts are pure NATS — no app.ota.init() needed (that's only for
// firmware upload/delete, which go over HTTP).

// ── Pick a firmware to roll out ──────────────────────────────

const { firmwares } = await app.ota.firmwareList({ page: 1 });

if (firmwares.length === 0) {
  console.log("No firmware uploaded yet — run examples/ota.js first.");
  await app.disconnect();
  process.exit(0);
}

const fw = firmwares[0]; // newest first
console.log(`Rolling out: ${fw.name} v${fw.version} (${fw.firmware_id})\n`);

// ── Create a DRAFT rollout (intent only — no jobs yet) ───────
// The returned device_count is a PREVIEW. The target is re-resolved and
// frozen into per-device jobs only at activation.

const draft = await app.ota.createRollout({
  firmware_id: fw.firmware_id,
  request_type: "DOWNLOAD_ONLY", // pre-stage only; devices download, don't install

  // Target shapes (device ObjectIds, not idents):
  //   { type: "devices", device_ids: ["665f..."] }
  //   { type: "logical_group", group_id: "<LogicalGroup id>" }
  //   { type: "hierarchy_group", group_id: "<HeirarchyGroup id>" }
  //   { type: "all" }                       — every device in the org
  // Any shape + optional exclude: ["<device_id>"]
  target: { type: "all" },

  force_download: false, // true = override the device's download() veto
  force_install: false,  // true = override the device's pre_install veto
  user_config: { apply: "app_gated" }, // passed through to device hooks
  // created_by defaults to this api_key's id; env is sent automatically
});
console.log(`Draft: ${draft.rollout_id} → ~${draft.device_count} devices (preview)`);

// ── Edit the draft (only drafts are editable) ────────────────

const updated = await app.ota.updateRollout({
  rollout_id: draft.rollout_id,
  request_type: "DOWNLOAD_INSTALL", // changed our mind: stage AND install
});
console.log(`Draft updated: ${updated.rollout_id}`);

// ── Activate — THE snapshot moment ───────────────────────────
// Target re-resolved NOW → 1 job per device (PENDING) → activated_at set
// (the FIFO order key) → full-job nudges blasted. Devices reconcile via
// their desired queue and work rollouts in activation order (FIFO).

const active = await app.ota.toggleRollout({
  rollout_id: draft.rollout_id,
  state: "ACTIVE",
});
console.log(`ACTIVE → ${active.device_count} devices snapshotted\n`);

// ── Pause / resume ───────────────────────────────────────────
// Pause freezes PENDING jobs AND blocks each device's queue head (rollouts
// behind this one wait — FIFO). In-flight downloads finish and park.

await app.ota.toggleRollout({ rollout_id: draft.rollout_id, state: "PAUSED" });
console.log("PAUSED — pending jobs frozen, queue head held");

await app.ota.toggleRollout({ rollout_id: draft.rollout_id, state: "ACTIVE" });
console.log("RESUMED — same snapshot, no re-resolution\n");

// ── Manual retry ─────────────────────────────────────────────
// Re-arms terminal jobs (FAILED / ROLLED_BACK / VETOED → PENDING,
// attempts+1, history logged) and nudges those devices. A re-armed job
// keeps its original FIFO position.

const retried = await app.ota.retryRollout({
  rollout_id: draft.rollout_id,
  phases: ["FAILED", "VETOED"], // omit to retry all three terminal phases
});
console.log(`Retried ${retried.retried} jobs\n`);

// ── Guard rails, demonstrated ────────────────────────────────

try {
  await app.ota.toggleRollout({ rollout_id: draft.rollout_id, state: "ACTIVE" });
} catch (err) {
  console.log(`Re-activate refused: ${err.code}`); // INVALID_TRANSITION
}

try {
  await app.ota.updateRollout({ rollout_id: draft.rollout_id, force_install: true });
} catch (err) {
  console.log(`Editing a live rollout refused: ${err.code}`); // ROLLOUT_NOT_DRAFT
}

try {
  await app.ota.deleteRollout({ rollout_id: draft.rollout_id });
} catch (err) {
  console.log(`Deleting a live rollout refused: ${err.code}`); // ROLLOUT_NOT_DRAFT
}

// ── Stop: terminal — jobs preserved as permanent history ─────

await app.ota.toggleRollout({ rollout_id: draft.rollout_id, state: "STOPPED" });
console.log("\nSTOPPED — never served again; job ledger kept for audit.");

try {
  await app.ota.retryRollout({ rollout_id: draft.rollout_id });
} catch (err) {
  console.log(`Retry on stopped rollout refused: ${err.code}`); // ROLLOUT_NOT_LIVE
}

await app.disconnect();
