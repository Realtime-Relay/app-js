/**
 * Area F — Status (T43..T47). Phase ledger / failures / engine guards /
 * jobHistory pagination / identity matching.
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
	assert,
	step,
	ask,
	sleep,
} from "./_lib.js";

const need = () => (cfg.deviceId ? null : block("set OTA_TEST_DEVICE_ID"));

// T43 — phase ledger: a complete install walks the expected timeline
export async function T43() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T43");
	const { rollout_id } = await activateRollout("T43", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);
	for (const want of ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"]) {
		assert(phases.includes(want), `history missing ${want}: ${phases.join(",")}`);
	}
	await cleanupRollouts([rollout_id]);
	return pass(phases.join("→"));
}
T43.testName = "Phase ledger (complete timeline)";

// T44 — any failure phase + its error string surface in jobsList/jobHistory.
// Drive it with whatever failure is easiest to produce on your bench: a veto
// (on_download/on_pre_install returns false), a sha mismatch (T28 prep), or a
// missing token (T30 prep). The harness just asserts a terminal-failure phase
// lands with a non-empty error.
export async function T44() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	step("ensure a failure condition is set on the device (veto hook returns false, or sha/token broken)");
	await ask("press Enter to activate a rollout that should FAIL/VETO/ROLL_BACK…");

	const fw = await uploadFirmwareFor("T44");
	const { rollout_id } = await activateRollout("T44", { firmware_id: fw.firmware_id });

	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED", "VETOED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const entry = hist.history.find((h) => ["FAILED", "VETOED", "ROLLED_BACK"].includes(h.phase ?? h.to));
	const err = entry?.error ?? j.error ?? "";
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);
	await cleanupRollouts([rollout_id]);
	if (!err) {
		return block(`reached ${j.phase} but no error string in history: ${phases.join("→")}`);
	}
	return pass(`failure reflected: phase=${j.phase} error="${err}" (${phases.join("→")})`);
}
T44.testName = "Failure phases reflected";

// T45 — a device cannot set the server-only PENDING phase. We inject a
// phase=PENDING status for a live job and confirm the engine drops it
// (DEVICE_PHASES guard) — the job's phase does not revert to PENDING.
export async function T45() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T45");
	const { rollout_id } = await activateRollout("T45", { firmware_id: fw.firmware_id });

	// Let the job reach a non-PENDING phase we can watch for reversion.
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	const before = j.phase;
	step(`job is at ${before}. Now inject a forbidden PENDING status for it.`);
	step(`  nats pub "<org>.<env>.ota.${cfg.deviceId}.status" '<msgpack {rollout_id:"${rollout_id}", phase:"PENDING"}>'`);
	await ask("publish the PENDING status (nats CLI / custom fw), then press Enter…");

	await sleep(3000);
	const after = (await app.ota.jobsList({ rollout_id, limit: 50 })).jobs
		.find((x) => x.device_id === cfg.deviceId);
	await cleanupRollouts([rollout_id]);
	if (after?.phase === "PENDING") {
		return block(`job reverted to PENDING — DEVICE_PHASES guard FAILED (engine accepted a device-set PENDING)`);
	}
	return pass(`device-set PENDING dropped: phase stayed ${after?.phase} (was ${before}, not reverted)`);
}
T45.testName = "PENDING from device dropped";

// T46 — status matches by api_key_id; cross-org silently dropped
export async function T46() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T46");
	const { rollout_id } = await activateRollout("T46", { firmware_id: fw.firmware_id });

	// Positive: status from THIS device must hit the job (so the timeline moves).
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADED", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);

	const note = cfg.org2.apiKey
		? "positive verified; for negative (cross-org / unknown device id silently dropped), inspect engine docker logs for '[ota] unknown device' / 'no job for ...'"
		: "positive verified; cross-org drop check needs OTA_TEST_ORG2_* (look for engine log: '[ota] unknown device …')";
	return pass(`device→${j.phase}; ${note}`);
}
T46.testName = "Status identity match";

// T47 — jobHistory pagination
export async function T47() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T47");
	const { rollout_id } = await activateRollout("T47", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED", "DOWNLOADED"],
		timeoutMs: cfg.waitInstallMs,
	});

	const p1 = await app.ota.jobHistory({ job_id: j.job_id, page: 1, limit: 2 });
	assert(p1.history.length <= 2, "page exceeded limit");
	assert(typeof p1.page.total === "number", "page.total missing");
	if (p1.page.has_more) {
		const p2 = await app.ota.jobHistory({ job_id: j.job_id, page: 2, limit: 2 });
		assert(p2.history.length > 0, "page 2 empty despite has_more");
		// chronological: page2 entries should come AFTER page1's last
		const lastP1 = p1.history.at(-1);
		const firstP2 = p2.history[0];
		const tsKey = "at" in lastP1 ? "at" : "created_at" in lastP1 ? "created_at" : null;
		if (tsKey) {
			assert(
				new Date(firstP2[tsKey]).getTime() >= new Date(lastP1[tsKey]).getTime(),
				"page 2 starts before page 1 ends (not chronological)",
			);
		}
	}
	await cleanupRollouts([rollout_id]);
	return pass(`total=${p1.page.total}`);
}
T47.testName = "jobHistory pagination";
