/**
 * Area J — Wiring (T67..T69). Infra-level: device identity scheme,
 * NATS account subject mappings, ESP partition table.
 */

import {
	getApp,
	uploadFirmwareFor,
	activateRollout,
	waitForPhase,
	cleanupRollouts,
	cfg,
	pass,
	block,
	step,
	ask,
} from "./_lib.js";

const need = () => (cfg.deviceId ? null : block("set OTA_TEST_DEVICE_ID"));

// T67 — device identity = api_key_id (used by nudge / job.poll / status).
//
// Can't be verified from the app-SDK alone. The Job row stores
// Devices._id (engine translates api_key_id → _id at the status boundary
// via db.resolveDeviceId), so jobsList shows the _id either way and the
// app never sees the api_key_id round-trip. Verify by inspecting the
// three device-facing subjects directly:
//   1. NUDGE     export.<org>.<env>.ota.<api_key_id>.firmware_update
//   2. JOB POLL  api.iot.ota.<org>.job.poll  payload { device_id: <api_key_id> }
//   3. STATUS    <org>.<env>.ota.<api_key_id>.status
// All three <device> tokens / payload fields must equal the device's JWT
// api_key_id (nats.org_data.api_key_id). Cross-check via the device serial
// log ("[ota] firmware_update nudge received" + the subject string) and
// the engine docker log ("[ota] job update rollout=… device=<mongo_id>"
// — engine's already-resolved side).
export async function T67() {
	const skip = need();
	if (skip) return skip;
	step(`configured device id (api_key_id) = ${cfg.deviceId}`);
	step("activating a rollout to generate the three device-facing subjects…");
	const fw = await uploadFirmwareFor("T67");
	const { rollout_id } = await activateRollout("T67", { firmware_id: fw.firmware_id });
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);

	step("now confirm all three carry the api_key_id above as the <device> token:");
	step(`  1. nudge   serial: 'subscribed firmware_update on import.<org>.<env>.ota.${cfg.deviceId}.firmware_update'`);
	step(`  2. poll    the job.poll REQ payload device_id = ${cfg.deviceId}`);
	step(`  3. status  serial publishes '<org>.<env>.ota.${cfg.deviceId}.status'`);
	const ans = await ask("do all three use that exact api_key_id (from the device JWT nats.org_data.api_key_id)? [y/N]: ");
	if (ans.toLowerCase().startsWith("y")) {
		return pass(`one identity end-to-end: nudge/poll/status all use ${cfg.deviceId}`);
	}
	return block("subjects do not all use the api_key_id — identity is inconsistent across nudge/poll/status");
}
T67.testName = "Device identity = api_key_id";

// T68 — account subject mappings work BOTH directions: engine->device nudge
// (export->import) and device->engine status (status->route.*). The harness
// runs a full install (so both directions exercise) and gates on you
// confirming the two signals.
export async function T68() {
	const skip = need();
	if (skip) return skip;
	step("tail the engine: docker compose logs -f ota-engine | grep '[ota] job update'");
	await ask("press Enter to run a full install (exercises both nudge and status mappings)…");

	const fw = await uploadFirmwareFor("T68");
	const { rollout_id } = await activateRollout("T68", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED", "DOWNLOADED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);

	const a1 = await ask("Direction 1 (export->import): did the device serial show 'firmware_update nudge received'? [y/N]: ");
	const a2 = await ask("Direction 2 (status->route.*): did the engine log '[ota] job update' for this rollout? [y/N]: ");
	const ok1 = a1.toLowerCase().startsWith("y");
	const ok2 = a2.toLowerCase().startsWith("y");
	if (ok1 && ok2) {
		return pass(`both account mappings healthy (nudge arrived, status reached engine; job hit ${j.phase})`);
	}
	return block(`mapping gap: nudge(export->import)=${ok1 ? "ok" : "MISSING"}, status(->route.*)=${ok2 ? "ok" : "MISSING"}`);
}
T68.testName = "Account subject mappings";

// T69 — partition table + flash size. Pure device inspection (idf.py +
// boot banner); the harness gates on what you read off the device.
export async function T69() {
	const skip = need();
	if (skip) return skip;
	step("on the device: cd example && idf.py partition-table");
	step("check sdkconfig: CONFIG_ESPTOOLPY_FLASHSIZE_4MB=y");
	const tbl = await ask("does the partition table list ota_0 AND ota_1 AND otadata? [y/N]: ");
	const sz = await ask("is the flash size 4MB? [y/N]: ");
	const boot = await ask("does the boot banner show a 'RUNNING PARTITION: ota_0/ota_1' (next_update_partition non-null)? [y/N]: ");
	const all = [tbl, sz, boot].every((a) => a.toLowerCase().startsWith("y"));
	if (all) {
		return pass("valid A/B OTA layout: ota_0 + ota_1 + otadata, 4MB flash, inactive slot present");
	}
	return block(`layout incomplete: ota_0/1+otadata=${tbl.trim()}, 4MB=${sz.trim()}, boot-slot=${boot.trim()} — without all three, OTA fails 'no ota partition' (T29)`);
}
T69.testName = "Partition table + flash size";
