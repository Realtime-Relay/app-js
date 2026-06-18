/**
 * Area B — Lifecycle (T09..T19). Rollout state machine, target shapes,
 * cross-org isolation. All app-SDK; no device required.
 */

import { RelayApp } from "../../src/index.js";
import {
	getApp,
	uploadFirmwareFor,
	activateRollout,
	waitForPhase,
	cleanupRollouts,
	expectError,
	assert,
	pass,
	block,
	fail,
	step,
	ask,
	sleep,
	cfg,
} from "./_lib.js";

// T09 — createRollout (DRAFT only, no jobs yet)
export async function T09() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T09");
	const draft = await app.ota.createRollout({
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_INSTALL",
		target: { type: "all" },
	});
	assert(draft.status === "DRAFT", `status=${draft.status} (want DRAFT)`);
	assert(typeof draft.device_count === "number", "device_count missing");
	// No jobs visible yet — jobsList against a DRAFT returns empty.
	const jobs = await app.ota.jobsList({ rollout_id: draft.rollout_id });
	assert(jobs.jobs.length === 0, `jobs leaked at DRAFT: ${jobs.jobs.length}`);
	await cleanupRollouts([draft.rollout_id]);
	return pass(`preview_count=${draft.device_count}`);
}
T09.testName = "Create DRAFT (intent only)";

// T10 — updateRollout works on DRAFT, blocked once ACTIVE
export async function T10() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T10");
	const d = await app.ota.createRollout({
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_INSTALL",
		target: { type: "all" },
	});
	const upd = await app.ota.updateRollout({
		rollout_id: d.rollout_id,
		force_download: true,
	});
	assert(upd.rollout_id === d.rollout_id, "update returned wrong rollout");
	await app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "ACTIVE" });
	await expectError("ROLLOUT_NOT_DRAFT", () =>
		app.ota.updateRollout({ rollout_id: d.rollout_id, force_install: true }),
	);
	await cleanupRollouts([d.rollout_id]);
	return pass();
}
T10.testName = "Update DRAFT-only";

// T11 — deleteRollout works on DRAFT only
export async function T11() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T11");

	// (a) DRAFT delete succeeds
	const dDraft = await app.ota.createRollout({
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_INSTALL",
		target: { type: "all" },
	});
	await app.ota.deleteRollout({ rollout_id: dDraft.rollout_id });

	// (b) ACTIVE delete fails ROLLOUT_NOT_DRAFT
	const dActive = await app.ota.createRollout({
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_INSTALL",
		target: { type: "all" },
	});
	await app.ota.toggleRollout({ rollout_id: dActive.rollout_id, state: "ACTIVE" });
	await expectError("ROLLOUT_NOT_DRAFT", () =>
		app.ota.deleteRollout({ rollout_id: dActive.rollout_id }),
	);

	// (c) STOPPED delete also fails (history preserved)
	await app.ota.toggleRollout({ rollout_id: dActive.rollout_id, state: "STOPPED" });
	await expectError("ROLLOUT_NOT_DRAFT", () =>
		app.ota.deleteRollout({ rollout_id: dActive.rollout_id }),
	);
	return pass();
}
T11.testName = "Delete DRAFT-only";

// T12 — activate is THE snapshot moment: 1 job/device, all PENDING.
// Uses { type: "all" } so the test works without OTA_TEST_DEVICE_ID.
// (Engine target resolver keys on Devices._id; api_key_id won't resolve.)
export async function T12() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T12");
	const { rollout_id, active } = await activateRollout("T12", {
		firmware_id: fw.firmware_id,
		target: { type: "all" },
	});
	if (active.device_count === 0) {
		await cleanupRollouts([rollout_id]);
		return block("org has zero devices — register a device first");
	}
	const jobs = await app.ota.jobsList({ rollout_id });
	assert(
		jobs.jobs.length === active.device_count,
		`jobs ${jobs.jobs.length} != device_count ${active.device_count}`,
	);
	for (const j of jobs.jobs) {
		// PENDING at the moment of activation. (A fast device can transition
		// to DOWNLOADING between toggleRollout and our jobsList — accept either.)
		assert(
			j.phase === "PENDING" || j.phase === "DOWNLOADING",
			`job phase=${j.phase} at activate (want PENDING)`,
		);
	}
	await cleanupRollouts([rollout_id]);
	return pass(`jobs=${jobs.jobs.length} all PENDING (or transitioning)`);
}
T12.testName = "Activate = snapshot moment";

// T13 — empty target → NO_TARGET_DEVICES (fail closed)
export async function T13() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T13");
	// Empty devices[] would fail validation client-side, so use a target
	// that resolves to 0 on the engine side: { type:"all", exclude:[every device] }
	// is annoying. Instead, target a known-bogus device id — engine resolves it
	// to 0 → NO_TARGET_DEVICES on activation.
	const d = await app.ota.createRollout({
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_INSTALL",
		target: { type: "devices", device_ids: ["000000000000000000000000"] },
	});
	await expectError("NO_TARGET_DEVICES", () =>
		app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "ACTIVE" }),
	);
	await cleanupRollouts([d.rollout_id]);
	return pass();
}
T13.testName = "Empty target fails closed";

// T14 — Method A: offline-device pause proves "serving stops". Once a
// DOWNLOAD_INSTALL job picks up on the device it runs to completion (no
// engine-side checkpoint between DOWNLOADED and INSTALLING), so we test
// pause by keeping the device offline during the transition and asserting
// the job doesn't progress. Head-of-line hold is T50.
export async function T14() {
	const app = await getApp();
	if (!cfg.deviceId) return block("set OTA_TEST_DEVICE_ID");

	const fw = await uploadFirmwareFor("T14");

	await ask("power OFF the device, then press Enter…");

	const { rollout_id } = await activateRollout("T14", { firmware_id: fw.firmware_id });

	step("PAUSE rollout (device is offline; job stays PENDING)");
	const paused = await app.ota.toggleRollout({ rollout_id, state: "PAUSED" });
	assert(paused.state === "PAUSED", `state=${paused.state} (want PAUSED)`);

	await ask("power ON the device — watch serial for NO 'nudge received'; press Enter when booted");

	step("verifying serving stops: poll jobsList for 20s — job must stay PENDING");
	const stableUntil = Date.now() + 20_000;
	while (Date.now() < stableUntil) {
		const list = await app.ota.jobsList({ rollout_id, limit: 50 });
		const j = list.jobs.find((x) => x.device_id === cfg.deviceId) ?? list.jobs[0];
		if (j && j.phase !== "PENDING") {
			await cleanupRollouts([rollout_id]);
			fail(`job advanced to ${j.phase} while rollout PAUSED — serving did not stop`);
		}
		await sleep(2000);
	}
	step("job stable at PENDING for 20s ✓ — serving is stopped");

	step("RESUME rollout (state → ACTIVE)");
	await app.ota.toggleRollout({ rollout_id, state: "ACTIVE" });

	step(`waiting for INSTALLED (up to ${Math.round(cfg.waitInstallMs / 1000)}s)…`);
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});

	await cleanupRollouts([rollout_id]);
	return pass("paused→PENDING-stable→resume→INSTALLED");
}
T14.testName = "Pause (serving stops, offline-device)";

// T15 — Resume must satisfy TWO guarantees:
//   (a) same snapshot served — job count and identities unchanged
//   (b) re-dispatches nudges to FIFO-head devices so they actually act
//       (without this, devices would only discover resume on their next
//       periodic poll, up to 30 min later)
// Uses the offline-device pattern (same as T14) so the PAUSE window
// actually exists — otherwise an online device may race past PENDING
// before we can pause.
export async function T15() {
	const app = await getApp();
	if (!cfg.deviceId) return block("set OTA_TEST_DEVICE_ID");
	const fw = await uploadFirmwareFor("T15");

	await ask("power OFF the device, then press Enter…");

	const { rollout_id, active } = await activateRollout("T15", {
		firmware_id: fw.firmware_id,
	});
	const before = active.device_count;

	step("PAUSE rollout (device still offline)");
	await app.ota.toggleRollout({ rollout_id, state: "PAUSED" });

	await ask("power ON the device — wait for it to fully boot, then press Enter…");

	// (a) Snapshot stability — count must match what we activated with.
	const paused = await app.ota.jobsList({ rollout_id, limit: 200 });
	assert(
		paused.jobs.length === before,
		`snapshot changed during pause (${before} → ${paused.jobs.length})`,
	);
	step(`snapshot stable at ${before} job(s)`);

	step("RESUME rollout — engine must re-dispatch nudges to FIFO-head devices");
	await app.ota.toggleRollout({ rollout_id, state: "ACTIVE" });

	// (b) Re-dispatch verification: forward progress in <60s. If we sit at
	// PENDING for 60s, the resume did not nudge — the device would only
	// discover the resume on the next periodic poll (>=30 min).
	step("verifying re-dispatch fires: expecting non-PENDING progress within 60s");
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: 60_000,
	});

	step("waiting for INSTALLED to clean up");
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});

	await cleanupRollouts([rollout_id]);
	return pass(`count stable (${before}) + resume re-dispatched + INSTALLED`);
}
T15.testName = "Resume (no re-snapshot + re-dispatches nudges)";

// T73 — Resume picks the right push per device phase:
//   PENDING / DOWNLOADING → firmware_update with rollout's own request_type
//   DOWNLOADED → firmware_update with request_type=INSTALL_ONLY (no re-download)
// Single-device single-rollout flow can't observe both branches at once;
// requires multi-device coordination or engine docker-log inspection.
export async function T73() {
	return block(
		"multi-device verification: target two devices (devA, devB) in one rollout. Power off devA, leave devB online. Activate DOWNLOAD_INSTALL rollout → devB starts downloading, devA stays PENDING. Pause; wait for devB to reach DOWNLOADED. Resume; tail ota-engine docker logs and look for TWO '[ota] dispatched rollout=… nudges=N' lines whose payloads carry different request_type values: DOWNLOAD_INSTALL (to devA) and INSTALL_ONLY (to devB). Single-device version is covered by T70.",
	);
}
T73.testName = "Resume routes by phase (firmware_update vs INSTALL_ONLY)";

// T16 — STOPPED is terminal: can't resume / retry / delete
export async function T16() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T16");
	const { rollout_id } = await activateRollout("T16", { firmware_id: fw.firmware_id });
	await app.ota.toggleRollout({ rollout_id, state: "STOPPED" });
	await expectError("INVALID_TRANSITION", () =>
		app.ota.toggleRollout({ rollout_id, state: "ACTIVE" }),
	);
	await expectError("ROLLOUT_NOT_LIVE", () =>
		app.ota.retryRollout({ rollout_id }),
	);
	await expectError("ROLLOUT_NOT_DRAFT", () =>
		app.ota.deleteRollout({ rollout_id }),
	);
	return pass();
}
T16.testName = "Stop (terminal)";

// T17 — invalid transitions: DRAFT→PAUSED, STOPPED→ACTIVE, ACTIVE→ACTIVE
export async function T17() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T17");
	const d = await app.ota.createRollout({
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_INSTALL",
		target: { type: "all" },
	});
	await expectError("INVALID_TRANSITION", () =>
		app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "PAUSED" }),
	);
	await app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "ACTIVE" });
	await expectError("INVALID_TRANSITION", () =>
		app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "ACTIVE" }),
	);
	await app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "STOPPED" });
	await expectError("INVALID_TRANSITION", () =>
		app.ota.toggleRollout({ rollout_id: d.rollout_id, state: "ACTIVE" }),
	);
	return pass();
}
T17.testName = "Invalid transitions";

// T18 — group-target end-to-end. Activates a real rollout against the
// logical (or hierarchy) group, waits for the device to actually install.
// Proves the engine's KV-backed group resolution works all the way
// through: cohorts KV bucket → resolveTargetDeviceIds → createJobs →
// dispatchToHead → device installs → INSTALLED. Run this whenever you
// touch group resolution or the cohorts service.
export async function T18() {
	if (!cfg.groupId) {
		return block("set OTA_TEST_GROUP_ID (and OTA_TEST_GROUP_TYPE if hierarchy)");
	}
	const fw = await uploadFirmwareFor("T18");

	step(`activating against ${cfg.groupType} ${cfg.groupId}`);
	const { rollout_id, active } = await activateRollout("T18", {
		firmware_id: fw.firmware_id,
		target: { type: cfg.groupType, group_id: cfg.groupId },
	});

	if (active.device_count === 0) {
		await cleanupRollouts([rollout_id]);
		return block(
			`group resolved to 0 devices — verify the cohorts KV entry GROUP_<orgID>_${cfg.groupId} has the device's id in its device_ids array`,
		);
	}

	step(`group resolved → ${active.device_count} device(s), waiting for INSTALLED`);
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`${cfg.groupType}/${cfg.groupId} → ${active.device_count} device(s) installed`);
}
T18.testName = "Group target end-to-end (KV-backed)";

// T19 — cross-org isolation. Needs second-org creds.
export async function T19() {
	if (!cfg.org2.apiKey || !cfg.org2.secret) {
		return block("set OTA_TEST_ORG2_API_KEY/SECRET to verify cross-org NOT_FOUND");
	}
	const appA = await getApp();
	const fw = await uploadFirmwareFor("T19");
	const { rollout_id } = await activateRollout("T19", { firmware_id: fw.firmware_id });

	const appB = new RelayApp({
		api_key: cfg.org2.apiKey,
		secret: cfg.org2.secret,
		mode: cfg.mode,
	});
	await appB.connect();
	try {
		await expectError("ROLLOUT_NOT_FOUND", () =>
			appB.ota.toggleRollout({ rollout_id, state: "PAUSED" }),
		);
		await expectError("ROLLOUT_NOT_FOUND", () =>
			appB.ota.jobsList({ rollout_id }),
		);
	} finally {
		await appB.disconnect();
	}
	await cleanupRollouts([rollout_id]);
	return pass();
}
T19.testName = "Cross-org isolation";
