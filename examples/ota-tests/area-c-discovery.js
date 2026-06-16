/**
 * Area C — Discovery (T20..T26). How a device finds out it has work:
 * nudges, polls, manual check(), FIFO gating, lost-nudge backstop.
 *
 * All require a live device; the app side orchestrates and polls jobsList
 * for the expected end state.
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
	sleep,
	step,
	ask
} from "./_lib.js";

function needDevice() {
	return cfg.deviceId
		? null
		: block("set OTA_TEST_DEVICE_ID (Devices._id, used for targeting + job matching)");
}

// T20 — nudge → immediate action (no poll wait)
export async function T20() {
	const skip = needDevice();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T20");
	const t0 = Date.now();
	const { rollout_id } = await activateRollout("T20", { firmware_id: fw.firmware_id });
	const job = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: 30_000, // well under the 30-min poll floor
	});
	await cleanupRollouts([rollout_id]);
	return pass(`acted in ${Date.now() - t0}ms, phase=${job.phase}`);
}
T20.testName = "Nudge → immediate action";

// T21 — poll on connect: a device that was OFFLINE at activation discovers
// the job via its first job.poll on reconnect (not via a nudge — the nudge
// was published while it was down and lost).
export async function T21() {
	const skip = needDevice();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T21");

	await ask("power OFF the device, then press Enter…");
	const { rollout_id } = await activateRollout("T21", { firmware_id: fw.firmware_id });
	step("rollout active while device is offline — the nudge is published into the void");
	await ask("power ON the device — watch serial for 'job.poll ->' on connect; press Enter once it boots…");

	step("waiting for the on-connect poll to discover + install the job");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`offline->online: on-connect poll served the job (reached ${j.phase})`);
}
T21.testName = "Poll on connect";

// T22 — periodic re-poll fires on the timer. With no rollout and no nudge,
// the device must still emit a 'job.poll ->' every RELAY_OTA_POLL_INTERVAL_MIN.
// This is a serial-timing observation (the app side can't see the device's
// timer), so the harness gates on you watching two spaced polls.
export async function T22() {
	const skip = needDevice();
	if (skip) return skip;
	step("ensure NO rollout is active for this device (so polls return idle)");
	step(`watch the device serial for 'job.poll -> ...' lines spaced ~RELAY_OTA_POLL_INTERVAL_MIN apart`);
	await ask("note the timestamp of one 'job.poll ->' line, then press Enter and wait for the next…");
	const ans = await ask("did a SECOND 'job.poll ->' fire ~one interval later (idle, no nudge)? [y/N]: ");
	if (ans.toLowerCase().startsWith("y")) {
		return pass("periodic re-poll confirmed on the timer");
	}
	return block("no second timer poll observed — check RELAY_OTA_POLL_INTERVAL_MIN and that tick() runs");
}
T22.testName = "Poll interval (timer)";

// T23 — manual check() forces an immediate job.poll. The device-side trigger
// lives in example/main.cpp (the // T23 one-shot ota.check() ~30s after boot).
// check() only sets reconcile_due_, so the proof is a 'job.poll ->' on the
// very next tick — visible on serial, not from the app side.
export async function T23() {
	const skip = needDevice();
	if (skip) return skip;
	step("device example must have the // T23 ota.check() trigger (fires ~30s after boot)");
	await ask("watch serial ~30s after boot for: 'T23: calling ota.check()' then an IMMEDIATE 'job.poll ->'. Press Enter when you've watched…");
	const ans = await ask("did 'job.poll ->' fire on the very next tick after check() (not waiting for the timer)? [y/N]: ");
	if (ans.toLowerCase().startsWith("y")) {
		return pass("check() forced an immediate poll");
	}
	return block("no immediate poll after check() — verify check() sets reconcile_due_ and tick() consumes it");
}
T23.testName = "Manual check()";

// T24 — FIFO-gated nudge: a 2nd rollout activated while the device is busy on
// an earlier one must NOT be nudged (it isn't the device's FIFO head). The
// app-side proof: R2's job sits at PENDING — it does not jump to DOWNLOADING —
// for a window while R1 is in-flight.
export async function T24() {
	const skip = needDevice();
	if (skip) return skip;
	const app = await getApp();
	const fw1 = await uploadFirmwareFor("T24a");
	const fw2 = await uploadFirmwareFor("T24b");

	const { rollout_id: r1 } = await activateRollout("T24", { firmware_id: fw1.firmware_id });
	step("waiting for R1 to go in-flight (DOWNLOADING) so the device is busy");
	await waitForPhase({
		rollout_id: r1,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "INSTALLING"],
		timeoutMs: cfg.waitDownloadMs,
	});

	step("activating R2 while R1 is in-flight — engine should HOLD the R2 nudge");
	const { rollout_id: r2 } = await activateRollout("T24", { firmware_id: fw2.firmware_id });

	// R2 must NOT be nudged (not the FIFO head). Confirm its job stays PENDING
	// for ~12s rather than jumping to DOWNLOADING.
	step("verifying R2 stays PENDING (not nudged) for 12s");
	let r2phase = "PENDING";
	const until = Date.now() + 12_000;
	while (Date.now() < until) {
		const jl = await app.ota.jobsList({ rollout_id: r2, limit: 50 });
		const j2 = jl.jobs.find((x) => x.device_id === cfg.deviceId) ?? jl.jobs[0];
		r2phase = j2?.phase ?? "PENDING";
		if (r2phase !== "PENDING") break;
		await sleep(2000);
	}
	await cleanupRollouts([r1, r2]);
	if (r2phase !== "PENDING") {
		return block(`R2 advanced to ${r2phase} while R1 in-flight — FIFO-gated nudge not holding (R2 should wait)`);
	}
	return pass("R2 held at PENDING (not nudged) while R1 in-flight — engine logs 'held N nudge(s)'");
}
T24.testName = "FIFO-gated nudge";

// T25 — lost-nudge backstop: even with the push nudge disabled, job.poll still
// discovers and installs the job.
export async function T25() {
	const skip = needDevice();
	if (skip) return skip;
	step("device prep: disable the nudge so ONLY poll can discover work —");
	step("  in ota_engine.h subscribe_updates(), comment out transport_->subscribe(subject, tramp); rebuild+flash");
	await ask("confirm the device is running WITHOUT the firmware_update subscription — press Enter…");

	const fw = await uploadFirmwareFor("T25");
	const { rollout_id } = await activateRollout("T25", { firmware_id: fw.firmware_id });
	step("nudge is dropped (no subscription) — device must discover via job.poll (on-connect / timer / check())");

	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`installed via poll despite no nudge (reached ${j.phase}). Restore the subscribe line after.`);
}
T25.testName = "Lost-nudge backstop";

// T26 — duplicate nudge: a 2nd firmware_update arriving mid-download is
// dropped by the device's in-flight guard, so only ONE download runs.
//
// retryRollout can't be used here: it only re-arms TERMINAL jobs
// (FAILED/ROLLED_BACK/VETOED), and a DOWNLOADING job isn't terminal, so it
// would re-arm 0 and nudge nobody. The only app-SDK way to re-nudge a LIVE
// job is PAUSE -> RESUME: the engine's resume re-dispatch sends a fresh
// firmware_update to any device whose job is at PENDING/DOWNLOADING. Pausing
// does NOT stop the device's download (DOWNLOAD_INSTALL is uninterruptible
// once started), so the resume nudge lands while the download is in flight =
// a genuine duplicate nudge.
export async function T26() {
	const skip = needDevice();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T26");

	const { rollout_id } = await activateRollout("T26", { firmware_id: fw.firmware_id });

	// Catch the device early in the download so the 2nd nudge lands while it's
	// still DOWNLOADING (not after it has moved to INSTALLING/INSTALLED).
	step("waiting for DOWNLOADING so the duplicate nudge lands mid-download");
	const dl = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING"],
		timeoutMs: cfg.waitDownloadMs,
	});

	step("PAUSE -> RESUME to re-dispatch a 2nd firmware_update to the DOWNLOADING device");
	await app.ota.toggleRollout({ rollout_id, state: "PAUSED" });
	await sleep(300);
	await app.ota.toggleRollout({ rollout_id, state: "ACTIVE" }); // resume re-nudges PENDING/DOWNLOADING

	step("waiting for INSTALLED");
	const done = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});

	// The duplicate nudge must NOT have started a second download: exactly one
	// DOWNLOADING entry in the history (in-flight guard collapsed the re-nudge).
	const hist = await app.ota.jobHistory({ job_id: done.job_id, limit: 200 });
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);
	const dlCount = phases.filter((p) => p === "DOWNLOADING").length;

	await cleanupRollouts([rollout_id]);

	if (dl.phase !== "DOWNLOADING") {
		return block(`never caught DOWNLOADING (got ${dl.phase}) — can't prove the dup nudge was mid-download`);
	}
	if (dlCount !== 1) {
		return block(`expected 1 DOWNLOADING entry, got ${dlCount} — duplicate nudge may have restarted the download: ${phases.join("→")}`);
	}
	return pass(`duplicate nudge collapsed to one download: ${phases.join("→")}`);
}
T26.testName = "Duplicate nudge (in-flight guard)";
