/**
 * Area E — Install (T35..T42 standard, T70..T72 INSTALL_ONLY). pre/post
 * install hooks, partition flip, downgrade (partition-based detection),
 * rollbacks, and the install-only flow.
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
	sleep,
	ask,
} from "./_lib.js";

const need = () => (cfg.deviceId ? null : block("set OTA_TEST_DEVICE_ID"));

// T35 — install happy path
export async function T35() {
	const skip = need();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T35");
	const { rollout_id } = await activateRollout("T35", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`installed in attempts=${j.attempts ?? 1}`);
}
T35.testName = "Install happy path";

// T36 — pre_install veto; force_install overrides
export async function T36() {
	const skip = need();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T36");

	const a = await activateRollout("T36a", { firmware_id: fw.firmware_id });
	const j1 = await waitForPhase({
		rollout_id: a.rollout_id,
		device_id: cfg.deviceId,
		phases: ["VETOED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});

	const b = await activateRollout("T36b", {
		firmware_id: fw.firmware_id,
		force_install: true,
	});
	const j2 = await waitForPhase({
		rollout_id: b.rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([a.rollout_id, b.rollout_id]);
	if (j1.phase !== "VETOED") {
		return block(`first run reached ${j1.phase} — wire on_pre_install to return false`);
	}
	return pass(`veto=${j1.phase}  force=${j2.phase}`);
}
T36.testName = "pre_install veto + force_install";

// T37 — post_install reject: the NEW image's on_post_install returns false,
// so the bootloader reverts and the OLD image reports ROLLED_BACK.
export async function T37() {
	const skip = need();
	if (skip) return skip;
	step("device prep: make on_post_install return false (e.g. return false when running == target version), rebuild+flash the CURRENT image so the hook ships");
	await ask("confirm on_post_install will return false on the new image — press Enter…");

	const fw = await uploadFirmwareFor("T37");
	const { rollout_id } = await activateRollout("T37", { firmware_id: fw.firmware_id });

	step("new image boots PENDING_VERIFY, post_install rejects -> revert -> old image reports ROLLED_BACK");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`post_install reject -> ${j.phase}; device back on the old image. Restore the healthy hook after.`);
}
T37.testName = "post_install reject";

// T38 — crash-loop new image: a build that resets before commit is reverted
// by the native bootloader rollback watchdog (no app logic).
export async function T38() {
	const skip = need();
	if (skip) return skip;
	step("device prep: build a 'bad' firmware that resets/aborts BEFORE esp_ota_mark_app_valid_cancel_rollback");
	step("  (e.g. esp_restart() early in app_main, or abort() in on_post_install before returning). CONFIG_BOOTLOADER_APP_ROLLBACK_ENABLE=y must be set.");
	await ask("confirm the deployed image will crash before commit — press Enter…");

	const fw = await uploadFirmwareFor("T38");
	const { rollout_id } = await activateRollout("T38", { firmware_id: fw.firmware_id });

	step("new image never marks valid -> bootloader rollback watchdog reverts -> ROLLED_BACK");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`crash-loop image reverted by the native watchdog -> ${j.phase}`);
}
T38.testName = "Crash-loop new image";

// T39 — downgrade: installing an OLDER version label still reports INSTALLED,
// proving the boot check is partition+PENDING_VERIFY based, not version-string.
export async function T39() {
	const skip = need();
	if (skip) return skip;
	step("device prep: 1) install vN (e.g. CONFIG_APP_PROJECT_VER 1.0.2); 2) take the SAME healthy source, set an OLDER label (e.g. 1.0.0), build that bin");
	await ask("set OTA_TEST_BIN to the older-labeled .bin (or confirm the example build is the older one), then press Enter…");

	const fw = await uploadFirmwareFor("T39");
	const { rollout_id } = await activateRollout("T39", { firmware_id: fw.firmware_id });

	step("installing the older-labeled image — expect INSTALLED (NOT ROLLED_BACK)");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	if (j.phase !== "INSTALLED") {
		return block(`downgrade reported ${j.phase} (expected INSTALLED) — boot check may be comparing version strings instead of partition address`);
	}
	return pass("downgrade -> INSTALLED (partition-based boot check, not version-string)");
}
T39.testName = "Downgrade (partition-based check)";

// T40 — A/B partition flip: each install lands in the opposite slot.
export async function T40() {
	const skip = need();
	if (skip) return skip;
	step("note the current 'RUNNING PARTITION: ota_X' from the device boot banner");
	const before = await ask("which slot is it running now? (ota_0 / ota_1): ");

	const fw1 = await uploadFirmwareFor("T40a");
	const r1 = await activateRollout("T40", { firmware_id: fw1.firmware_id });
	await waitForPhase({ rollout_id: r1.rollout_id, device_id: cfg.deviceId, phases: ["INSTALLED"], timeoutMs: cfg.waitInstallMs });
	const mid = await ask("after install #1, which slot does the boot banner show now?: ");

	const fw2 = await uploadFirmwareFor("T40b");
	const r2 = await activateRollout("T40", { firmware_id: fw2.firmware_id });
	await waitForPhase({ rollout_id: r2.rollout_id, device_id: cfg.deviceId, phases: ["INSTALLED"], timeoutMs: cfg.waitInstallMs });
	const after = await ask("after install #2, which slot now?: ");

	await cleanupRollouts([r1.rollout_id, r2.rollout_id]);
	const flipped = before.trim() !== mid.trim() && mid.trim() !== after.trim() && before.trim() === after.trim();
	if (!flipped) {
		return block(`slots did not alternate cleanly: ${before} -> ${mid} -> ${after} (expected X -> Y -> X)`);
	}
	return pass(`A/B flip confirmed: ${before} -> ${mid} -> ${after}`);
}
T40.testName = "A/B partition flip";

// T41 — power-cut in PENDING_VERIFY: a reboot loss before commit rolls back.
export async function T41() {
	const skip = need();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T41");
	const { rollout_id } = await activateRollout("T41", { firmware_id: fw.firmware_id });

	step("watch serial: the device will reboot into the new image (new version banner)");
	await ask("the INSTANT the new version banner appears (before on_post_install commits, ~1-2s), PULL POWER. Power back on, then press Enter…");

	step("on the power-on boot the bootloader should revert (image never marked valid) -> ROLLED_BACK");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["ROLLED_BACK", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	if (j.phase !== "ROLLED_BACK") {
		return block(`got ${j.phase} — either the commit had already happened (cut earlier) or rollback didn't trigger`);
	}
	return pass("power-cut in PENDING_VERIFY -> ROLLED_BACK on next boot");
}
T41.testName = "Power-cut in PENDING_VERIFY";

// T42 — custom on_install that stages without rebooting, then a manual reboot
// completes the install.
export async function T42() {
	const skip = need();
	if (skip) return skip;
	step("device prep: wire on_install to esp_ota_set_boot_partition(partition) but NOT reboot (reboot on a button/delay), rebuild+flash");
	await ask("confirm on_install stages without rebooting — press Enter…");

	const fw = await uploadFirmwareFor("T42");
	const { rollout_id } = await activateRollout("T42", { firmware_id: fw.firmware_id });

	step("waiting for INSTALLING (device stages, logs 'on_install returned without reboot (staged)')");
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLING"],
		timeoutMs: cfg.waitDownloadMs,
	});
	await ask("now trigger your manual reboot on the device (button/delay), then press Enter…");

	step("new image boots, post_install commits -> INSTALLED");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`deferred-reboot install completed on manual reboot -> ${j.phase}`);
}
T42.testName = "Custom on_install (deferred reboot)";

// ───────────────────────────────────────────────────────────
// INSTALL_ONLY flow (T70-T72) — new request_type that skips
// the download step. Device's handle_install_only verifies the
// inactive partition before flipping otadata. Triggered today
// via PAUSE→RESUME on a DOWNLOAD_ONLY rollout (until an explicit
// rollout.install operator API lands).
// ───────────────────────────────────────────────────────────

// T70 — INSTALL_ONLY happy path. End-to-end the install-later flow:
// DOWNLOAD_ONLY stages → PAUSE → RESUME → engine sends INSTALL_ONLY to
// the DOWNLOADED device → device installs without re-downloading.
// Key assertion: history has exactly ONE DOWNLOADING entry (the initial
// stage), not two. A second DOWNLOADING would mean the device ignored
// INSTALL_ONLY and re-downloaded.
export async function T70() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T70");

	step("phase 1 — stage image via DOWNLOAD_ONLY");
	const { rollout_id } = await activateRollout("T70", {
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_ONLY",
	});
	const staged = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADED"],
		timeoutMs: cfg.waitDownloadMs,
	});
	step(`staged at DOWNLOADED (job=${staged.job_id})`);

	step("phase 2 — PAUSE the rollout");
	await app.ota.toggleRollout({ rollout_id, state: "PAUSED" });
	await sleep(500);

	step("phase 3 — RESUME → engine dispatches INSTALL_ONLY to DOWNLOADED-phase devices");
	await app.ota.toggleRollout({ rollout_id, state: "ACTIVE" });

	const installed = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});

	// Verify: history shouldn't show a SECOND DOWNLOADING entry.
	const hist = await app.ota.jobHistory({ job_id: installed.job_id, limit: 200 });
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);
	const dlCount = phases.filter((p) => p === "DOWNLOADING").length;

	await cleanupRollouts([rollout_id]);

	if (dlCount > 1) {
		return block(
			`device re-downloaded — history has ${dlCount} DOWNLOADING entries, expected 1: ${phases.join("→")}`,
		);
	}
	return pass(`INSTALL_ONLY installed without re-download: ${phases.join("→")}`);
}
T70.testName = "INSTALL_ONLY happy path (stage + resume)";

// T71 — INSTALL_ONLY dedup. Device receives INSTALL_ONLY for a version
// it's already running → handle_install_only short-circuits with
// INSTALLED (no flash write, no partition flip). Can't be triggered
// from the app SDK today — needs either rollout.install operator API
// or a direct NATS publish helper. The PAUSE/RESUME trick won't reach
// this branch because a fresh DOWNLOAD_ONLY rollout for the same
// version would dedup at reconcile (running == job.version) before
// even staging.
export async function T71() {
	return block(
		"requires triggering an INSTALL_ONLY push for the device's currently-running version. Today the engine only sends INSTALL_ONLY on PAUSE→RESUME of a rollout where the device's job is at DOWNLOADED — and the device never reaches DOWNLOADED for a version it's already running (reconcile's dedup short-circuits first). Set up by either: (a) building a `rollout.install` operator API that lets you re-send INSTALL_ONLY for an already-INSTALLED job, or (b) using the nats CLI to publish msgpack {request_type:'INSTALL_ONLY', rollout_id, version: <running version>, sha256} to import.<org>.<env>.ota.<api_key_id>.firmware_update. Expected: device serial logs 'install_only: already running <v>', engine receives single INSTALLED status, no DOWNLOADING/INSTALLING phases.",
	);
}
T71.testName = "INSTALL_ONLY dedup (already running)";

// T72 — INSTALL_ONLY negatives. Two FAILED phases to verify:
//   "no staged image" — inactive partition has no app_desc
//   "staged version mismatch" — inactive partition has a different version
// Same triggering problem as T71; needs direct push or rollout.install.
export async function T72() {
	return block(
		"requires direct INSTALL_ONLY push (see T71) for a version NOT staged on the device's inactive partition. Setup options: (a) wipe the inactive partition before sending; (b) send INSTALL_ONLY for v9.0.0 while the device's staged image is some other version. Expected: device phase → FAILED, error string 'no staged image' or 'staged version mismatch'; otadata untouched; no reboot.",
	);
}
T72.testName = "INSTALL_ONLY negatives (no staged / mismatch)";
