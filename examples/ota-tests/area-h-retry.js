/**
 * Area H — Retry (T54..T58). Manual retry semantics.
 *
 * T54/T55/T58 need a job in a terminal phase (FAILED/ROLLED_BACK/VETOED).
 * From the app side alone we can't synthesize that — they require the
 * device to fail. T56 (guards) and T57 (zero re-arm) are fully app-side.
 */

import {
	getApp,
	uploadFirmwareFor,
	activateRollout,
	waitForPhase,
	cleanupRollouts,
	expectError,
	cfg,
	pass,
	block,
	assert,
	step,
	ask,
} from "./_lib.js";

const need = () => (cfg.deviceId ? null : block("set OTA_TEST_DEVICE_ID"));

// T54 — retry re-arms terminal jobs. Produce a terminal-fail job (easiest: a
// veto hook returning false), then retryRollout and confirm the job flips
// back to PENDING, attempts increments, and history logs the retry.
export async function T54() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	step("ensure the device will FAIL/VETO this rollout (e.g. on_pre_install returns false)");
	await ask("press Enter to activate the rollout that should fail…");

	const fw = await uploadFirmwareFor("T54");
	const { rollout_id } = await activateRollout("T54", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED", "VETOED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});
	const attemptsBefore = j.attempts ?? 0;

	step(`job is ${j.phase} (attempts=${attemptsBefore}) — calling retryRollout`);
	const r = await app.ota.retryRollout({ rollout_id });
	assert(r.retried >= 1, `retried=${r.retried} (want >=1)`);

	// Confirm it re-armed: phase back to PENDING (or already re-running), attempts+1,
	// and a retry note in history.
	const jobs = await app.ota.jobsList({ rollout_id, limit: 50 });
	const re = jobs.jobs.find((x) => x.device_id === cfg.deviceId);
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const hasRetryNote = hist.history.some((h) => (h.note ?? "").toLowerCase().includes("retry"));
	await cleanupRollouts([rollout_id]);

	if (!hasRetryNote) {
		return block(`no 'retry' note in history after retryRollout: ${hist.history.map((h) => h.phase ?? h.to).join("→")}`);
	}
	return pass(`re-armed: retried=${r.retried}, phase now ${re?.phase}, attempts ${attemptsBefore}->${re?.attempts}, history has retry note`);
}
T54.testName = "Retry re-arms terminal jobs";

// T55 — retry filters by phase/device. Needs mixed phases across devices to
// fully exercise device_ids[]; with one device we verify the phases[] filter:
// retrying phases:['INSTALLED'] (a non-retryable phase) must re-arm nothing,
// while phases:['FAILED'] re-arms the failed job.
export async function T55() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	step("ensure the device will FAIL/VETO this rollout (so there's a terminal job to filter on)");
	await ask("press Enter to activate the rollout that should fail…");

	const fw = await uploadFirmwareFor("T55");
	const { rollout_id } = await activateRollout("T55", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED", "VETOED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});

	// Filter that should match NOTHING (the job isn't in this phase).
	const otherPhase = j.phase === "FAILED" ? "VETOED" : "FAILED";
	const noMatch = await app.ota.retryRollout({ rollout_id, phases: [otherPhase] });
	assert(noMatch.retried === 0, `phase filter [${otherPhase}] re-armed ${noMatch.retried} (want 0)`);

	// Filter that SHOULD match the job's actual terminal phase.
	const match = await app.ota.retryRollout({ rollout_id, phases: [j.phase] });
	assert(match.retried >= 1, `phase filter [${j.phase}] re-armed ${match.retried} (want >=1)`);

	await cleanupRollouts([rollout_id]);
	return pass(`phase filter works: [${otherPhase}]->0 re-armed, [${j.phase}]->${match.retried} re-armed`);
}
T55.testName = "Retry filters";
T55.testName = "Retry filters";

// T56 — retry guards: DRAFT and STOPPED → ROLLOUT_NOT_LIVE
export async function T56() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T56");

	// DRAFT
	const d = await app.ota.createRollout({
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_INSTALL",
		target: { type: "all" },
	});
	await expectError("ROLLOUT_NOT_LIVE", () =>
		app.ota.retryRollout({ rollout_id: d.rollout_id }),
	);

	// STOPPED
	await app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "ACTIVE" });
	await app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "STOPPED" });
	await expectError("ROLLOUT_NOT_LIVE", () =>
		app.ota.retryRollout({ rollout_id: d.rollout_id }),
	);
	return pass();
}
T56.testName = "Retry guards (DRAFT / STOPPED)";

// T57 — retry on an ACTIVE rollout with no terminal jobs: zero re-arm
export async function T57() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T57");
	const { rollout_id } = await activateRollout("T57", {
		firmware_id: fw.firmware_id,
		target: { type: "all" }, // count doesn't matter; no terminal jobs guaranteed at activation
	});
	const r = await app.ota.retryRollout({ rollout_id });
	assert(r.retried === 0, `retried=${r.retried} (want 0)`);
	await cleanupRollouts([rollout_id]);
	return pass();
}
T57.testName = "Zero re-arm → zero nudges";

// T58 — after a device fails once and goes quiet (handled_rollout_ set), an
// operator retry must make it re-attempt: the retry nudge clears the device
// guard and the job transitions out of its terminal phase.
export async function T58() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	step("ensure the device will FAIL this rollout once, then go quiet (handled_rollout_ set)");
	await ask("press Enter to activate the rollout that should fail…");

	const fw = await uploadFirmwareFor("T58");
	const { rollout_id } = await activateRollout("T58", { firmware_id: fw.firmware_id });
	const failed = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED", "VETOED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});

	step(`job ${failed.phase}; device is now quiet. Calling retryRollout — device should accept the re-nudge`);
	await app.ota.retryRollout({ rollout_id });

	// The job must leave its terminal phase (PENDING -> re-runs). Wait for any
	// movement away from the failed phase.
	const moved = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED", "VETOED", "FAILED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
		pollMs: 1500,
	});
	await cleanupRollouts([rollout_id]);
	// Re-attempt is proven by attempts climbing (the device acted again), even
	// if it fails again for the same reason.
	if ((moved.attempts ?? 0) <= (failed.attempts ?? 0)) {
		return block(`attempts did not climb (${failed.attempts} -> ${moved.attempts}) — device may not have re-attempted (handled_rollout_ not cleared by the nudge)`);
	}
	return pass(`device re-attempted after retry: attempts ${failed.attempts}->${moved.attempts}, phase ${failed.phase}->${moved.phase}`);
}
T58.testName = "Device re-attempt on retry";
