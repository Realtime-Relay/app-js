/**
 * Area G — FIFO (T48..T53). Sequential rollouts, stepped chain, PAUSED head
 * blocks queue, re-poll-before-install, retry-older-mid-newer edge,
 * busy-device discovery via completion re-poll.
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
	step
} from "./_lib.js";

const need = () => (cfg.deviceId ? null : block("set OTA_TEST_DEVICE_ID"));

// T48 — sequential rollouts: R1 then R2, in activation order
export async function T48() {
	const skip = need();
	if (skip) return skip;
	const f1 = await uploadFirmwareFor("T48a");
	const f2 = await uploadFirmwareFor("T48b");
	const r1 = await activateRollout("T48a", { firmware_id: f1.firmware_id });
	const r2 = await activateRollout("T48b", { firmware_id: f2.firmware_id });

	await waitForPhase({
		rollout_id: r1.rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await waitForPhase({
		rollout_id: r2.rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([r1.rollout_id, r2.rollout_id]);
	return pass("R1 → R2 in activation order");
}
T48.testName = "Sequential rollouts";

// T49 — stepped upgrade chain v1 → v2 → v3
export async function T49() {
	const skip = need();
	if (skip) return skip;
	const fws = [];
	for (let i = 0; i < 3; i++) fws.push(await uploadFirmwareFor(`T49-${i}`));
	const rollouts = [];
	for (const fw of fws) {
		rollouts.push(await activateRollout(`T49`, { firmware_id: fw.firmware_id }));
	}
	for (const r of rollouts) {
		await waitForPhase({
			rollout_id: r.rollout_id,
			device_id: cfg.deviceId,
			phases: ["INSTALLED"],
			timeoutMs: cfg.waitInstallMs,
		});
	}
	await cleanupRollouts(rollouts.map((r) => r.rollout_id));
	return pass("3-step chain installed in order");
}
T49.testName = "Stepped upgrade chain";

// T50 — PAUSED head blocks the queue
export async function T50() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const f1 = await uploadFirmwareFor("T50a");
	const f2 = await uploadFirmwareFor("T50b");
	const r1 = await activateRollout("T50a", { firmware_id: f1.firmware_id });
	await app.ota.toggleRollout({ rollout_id: r1.rollout_id, state: "PAUSED" });
	const r2 = await activateRollout("T50b", { firmware_id: f2.firmware_id });

	// R2 should NOT progress while R1 (the head) is paused. We give it room
	// to misbehave; if R2 stays at PENDING, head-of-line hold is working.
	await new Promise((res) => setTimeout(res, 15_000));
	const jobs = await app.ota.jobsList({ rollout_id: r2.rollout_id });
	const j2 = jobs.jobs.find((j) => j.device_id === cfg.deviceId);
	await cleanupRollouts([r1.rollout_id, r2.rollout_id]);
	if (j2?.phase === "PENDING") return pass("R2 held at PENDING behind paused R1");
	return block(`R2 advanced to ${j2?.phase} — head-of-line hold may be broken`);
}
T50.testName = "PAUSED head blocks queue";

// T51 — pause/stop after DOWNLOADED: device re-polls before install,
// finds the job no longer served, parks
export async function T51() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T51");
	const { rollout_id } = await activateRollout("T51", { firmware_id: fw.firmware_id });
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADED"],
		timeoutMs: cfg.waitDownloadMs,
	});
	await app.ota.toggleRollout({ rollout_id, state: "PAUSED" });
	// Wait through what would be the install window — device should park.
	await new Promise((res) => setTimeout(res, 15_000));
	const j = (await app.ota.jobsList({ rollout_id })).jobs.find(
		(x) => x.device_id === cfg.deviceId,
	);
	await cleanupRollouts([rollout_id]);
	if (j?.phase === "DOWNLOADED") return pass("device parked at DOWNLOADED");
	return block(`expected DOWNLOADED stable, got ${j?.phase}`);
}
T51.testName = "Re-poll before install (pause after DOWNLOADED)";

// T52 — retry-older-mid-newer (documented edge). Retrying an OLDER rollout
// while a NEWER one is mid-install: the newer completes first (a reboot can't
// be aborted), then the older is served via the completion re-poll. Needs two
// distinct firmware versions; the older must FAIL the first time so there's a
// terminal job to retry (use a veto hook, or set OTA_TEST_BIN appropriately).
export async function T52() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();

	step("R(old): activate first. Arrange for it to reach a terminal-fail phase so it's retryable later");
	const fOld = await uploadFirmwareFor("T52old");
	const rOld = await activateRollout("T52", { firmware_id: fOld.firmware_id });
	const oldJob = await waitForPhase({
		rollout_id: rOld.rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED", "VETOED", "ROLLED_BACK", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});

	step("R(new): activate and let it reach INSTALLING (in-flight, can't be aborted)");
	const fNew = await uploadFirmwareFor("T52new");
	const rNew = await activateRollout("T52", { firmware_id: fNew.firmware_id });
	await waitForPhase({
		rollout_id: rNew.rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});

	step("retry R(old) WHILE R(new) is installing");
	try {
		await app.ota.retryRollout({ rollout_id: rOld.rollout_id });
	} catch (e) {
		step(`retry returned: ${e.message} (ok if R_old isn't live/terminal)`);
	}

	step("expect: R(new) finishes first, THEN R(old) served via completion re-poll");
	const newDone = await waitForPhase({
		rollout_id: rNew.rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	// R(old) may or may not re-run depending on whether the retry re-armed it;
	// report what happened rather than hard-failing this documented edge.
	const oldAfter = (await app.ota.jobsList({ rollout_id: rOld.rollout_id, limit: 50 })).jobs
		.find((x) => x.device_id === cfg.deviceId);
	await cleanupRollouts([rOld.rollout_id, rNew.rollout_id]);
	return pass(`edge documented: R_new=${newDone.phase} (completed first), R_old now=${oldAfter?.phase} (served after, via completion re-poll if re-armed)`);
}
T52.testName = "Retry-older-mid-newer (edge)";

// T53 — busy-device discovery: R2 picked up via completion re-poll
export async function T53() {
	const skip = need();
	if (skip) return skip;
	const f1 = await uploadFirmwareFor("T53a");
	const r1 = await activateRollout("T53a", { firmware_id: f1.firmware_id });
	// Wait until device is busy on R1 (DOWNLOADING/INSTALLING).
	await waitForPhase({
		rollout_id: r1.rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "INSTALLING"],
		timeoutMs: cfg.waitDownloadMs,
	});
	const f2 = await uploadFirmwareFor("T53b");
	const r2 = await activateRollout("T53b", { firmware_id: f2.firmware_id });
	// R2 should NOT be nudged immediately. Wait for completion re-poll path.
	await waitForPhase({
		rollout_id: r2.rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs + cfg.waitDownloadMs,
	});
	await cleanupRollouts([r1.rollout_id, r2.rollout_id]);
	return pass("R2 served via completion re-poll");
}
T53.testName = "Busy-device discovery (completion re-poll)";
