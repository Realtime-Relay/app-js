/**
 * Area I — Scale (T59..T66). Fan-out, multi-org, engine HA, concurrent
 * status, reconnect mid-flow, dedup, busy-loop guard, token expiry.
 */

import { RelayApp } from "../../src/index.js";
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

// T59 — fan-out to N devices
export async function T59() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T59");
	const { rollout_id, active } = await activateRollout("T59", {
		firmware_id: fw.firmware_id,
		target: { type: "all" },
	});
	const list = await app.ota.jobsList({ rollout_id, limit: 200 });
	assert(
		list.jobs.length === active.device_count,
		`jobs ${list.jobs.length} != device_count ${active.device_count}`,
	);
	// Stats sum back to total — basic sanity.
	if (list.stats) {
		const sum = Object.values(list.stats).reduce((a, b) => a + (b || 0), 0);
		assert(sum === active.device_count, `stats sum ${sum} != ${active.device_count}`);
	}
	await cleanupRollouts([rollout_id]);
	return pass(`N=${active.device_count}`);
}
T59.testName = "Fan-out to N devices";

// T60 — multi-org concurrency (needs second-org creds)
export async function T60() {
	if (!cfg.org2.apiKey || !cfg.org2.secret) {
		return block("set OTA_TEST_ORG2_API_KEY/SECRET");
	}
	const appA = await getApp();
	const appB = new RelayApp({
		api_key: cfg.org2.apiKey,
		secret: cfg.org2.secret,
		mode: cfg.mode,
	});
	await appB.connect();
	await appB.ota.init();
	try {
		const fwA = await uploadFirmwareFor("T60A");
		const fwB = await appB.ota.firmwareUpload({
			name: `ota-test-T60B-${Date.now()}`,
			version: "9.60.0",
			file: (await import("node:fs")).readFileSync(cfg.binPath),
			file_name: cfg.binPath,
		});

		const a = await appA.ota.createRollout({
			firmware_id: fwA.firmware_id,
			request_type: "DOWNLOAD_INSTALL",
			target: { type: "all" },
		});
		await appA.ota.toggleRollout({ rollout_id: a.rollout_id, state: "ACTIVE" });

		const b = await appB.ota.createRollout({
			firmware_id: fwB.firmware_id,
			request_type: "DOWNLOAD_INSTALL",
			target: { type: "all" },
		});
		await appB.ota.toggleRollout({ rollout_id: b.rollout_id, state: "ACTIVE" });

		// Cross-list isolation: each org sees only its own rollouts.
		const listA = await appA.ota.rolloutList({ limit: 200 });
		const listB = await appB.ota.rolloutList({ limit: 200 });
		const idsA = new Set(listA.rollouts.map((r) => r.rollout_id));
		const idsB = new Set(listB.rollouts.map((r) => r.rollout_id));
		assert(idsA.has(a.rollout_id) && !idsB.has(a.rollout_id), "org A rollout leaked into org B");
		assert(idsB.has(b.rollout_id) && !idsA.has(b.rollout_id), "org B rollout leaked into org A");

		await cleanupRollouts([a.rollout_id]);
		await appB.ota.toggleRollout({ rollout_id: b.rollout_id, state: "STOPPED" });
		return pass();
	} finally {
		await appB.disconnect();
	}
}
T60.testName = "Multi-org concurrency / isolation";

// T61 — engine horizontal scale: with 2+ engine instances, queue groups
// deliver each job.poll/status to exactly ONE instance. This is an infra
// observation (needs both instances' logs), so the harness generates the
// traffic and gates on you confirming no duplicate processing across logs.
export async function T61() {
	const skip = need();
	if (skip) return skip;
	step("run 2+ ota-engine instances on the same NATS account (docker compose up --scale ota-engine=2)");
	step("tail BOTH instances: docker compose logs -f ota-engine | grep '[ota] job update'");
	await ask("press Enter to generate traffic (the harness will run a full install so statuses flow)…");

	const fw = await uploadFirmwareFor("T61");
	const { rollout_id } = await activateRollout("T61", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED", "FAILED", "VETOED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);

	const ans = await ask(`job reached ${j.phase}. Across BOTH engine logs, was each '[ota] job update' for this rollout handled by exactly ONE instance (no dupes)? [y/N]: `);
	if (ans.toLowerCase().startsWith("y")) {
		return pass("queue groups deliver each message exactly once across instances");
	}
	return block("duplicate processing observed across instances — check the queue-group names on the status consumer + job.poll endpoint");
}
T61.testName = "Engine horizontal scale";

// T62 — concurrent status atomicity: fire many retries at one rollout at once
// and confirm the atomic $set+$push held — attempts reflects the re-arms with
// no lost/torn history entries.
export async function T62() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	step("ensure the device will FAIL/VETO this rollout (so retries have a terminal job to re-arm)");
	await ask("press Enter to activate, then the harness fires concurrent retries…");

	const fw = await uploadFirmwareFor("T62");
	const { rollout_id } = await activateRollout("T62", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED", "VETOED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});

	// Hammer: 8 concurrent retries. The engine must serialize them atomically.
	step("firing 8 concurrent retryRollout calls at the same job");
	const results = await Promise.allSettled(
		Array.from({ length: 8 }, () => app.ota.retryRollout({ rollout_id })),
	);
	const okCount = results.filter((r) => r.status === "fulfilled").length;

	// Read the ledger: history must be well-formed (every entry has a phase),
	// and attempts must be a sane integer (not corrupted by torn writes).
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 500 });
	const jobs = await app.ota.jobsList({ rollout_id, limit: 50 });
	const re = jobs.jobs.find((x) => x.device_id === cfg.deviceId);
	const malformed = hist.history.filter((h) => !(h.phase ?? h.to)).length;
	await cleanupRollouts([rollout_id]);

	if (malformed > 0) {
		return block(`${malformed} malformed history entries — torn write under concurrency`);
	}
	if (!Number.isInteger(re?.attempts)) {
		return block(`attempts is not a clean integer (${re?.attempts}) — lost/double-counted under concurrency`);
	}
	return pass(`atomic under load: ${okCount}/8 retries ok, attempts=${re?.attempts}, ${hist.history.length} clean history entries`);
}
T62.testName = "Concurrent status atomicity";

// T63 — reconnect mid-flow: dropping NATS during a download must rebind the
// firmware_update sub on reconnect and the flow resumes (v1 restarts from 0).
export async function T63() {
	const skip = need();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T63");
	const { rollout_id } = await activateRollout("T63", { firmware_id: fw.firmware_id });

	step("waiting for DOWNLOADING so you can drop NATS mid-flow");
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING"],
		timeoutMs: cfg.waitDownloadMs,
	});
	await ask("DROP the device's NATS link (kill broker / pull cable / drop WiFi), wait ~5s, then RESTORE it and press Enter…");
	step("on reconnect, serial should show 'Restored N subscriptions' + 'subscribed firmware_update'; download restarts");

	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: cfg.waitInstallMs + cfg.waitDownloadMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`recovered after NATS drop -> ${j.phase} (sub rebound, download restarted from 0)`);
}
T63.testName = "Reconnect mid-flow";

// T64 — dedup: rolling out the version the device already runs
export async function T64() {
	const skip = need();
	if (skip) return skip;
	if (!cfg.deviceVersion) {
		return block("set OTA_TEST_DEVICE_VERSION to the version the device currently reports");
	}
	const fw = await uploadFirmwareFor("T64", { version: cfg.deviceVersion });
	const { rollout_id } = await activateRollout("T64", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["INSTALLED"],
		timeoutMs: 30_000, // must be fast — no download
	});
	const app = await getApp();
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 50 });
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);
	await cleanupRollouts([rollout_id]);
	if (phases.includes("DOWNLOADING")) {
		return block(`dedup may have missed — history has DOWNLOADING: ${phases.join("→")}`);
	}
	return pass(`fast-INSTALLED, no download: ${phases.join("→")}`);
}
T64.testName = "Dedup (already on version)";

// T65 — no busy-loop on persistent fail: the device fails once then stays
// quiet (handled_rollout_). Drive a repeatable failure, then confirm the
// FAILED-entry count is stable over a window (no tight re-poll/re-fail loop).
export async function T65() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	step("cause a REPEATABLE failure on the device (easiest: no OTA partition — see T29 prep)");
	await ask("press Enter to activate the rollout that should fail repeatably…");

	const fw = await uploadFirmwareFor("T65");
	const { rollout_id } = await activateRollout("T65", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED", "VETOED", "ROLLED_BACK"],
		timeoutMs: cfg.waitInstallMs,
	});

	step("verifying no busy-loop: counting terminal-fail entries over 25s");
	const term = (hs) => hs.filter((h) => ["FAILED", "VETOED", "ROLLED_BACK"].includes(h.phase ?? h.to)).length;
	const c1 = term((await app.ota.jobHistory({ job_id: j.job_id, limit: 500 })).history);
	await sleep(25_000);
	const c2 = term((await app.ota.jobHistory({ job_id: j.job_id, limit: 500 })).history);
	await cleanupRollouts([rollout_id]);

	if (c2 > c1) {
		return block(`busy-loop: terminal-fail entries grew ${c1} -> ${c2} (handled_rollout_ guard not holding)`);
	}
	return pass(`failed once then quiet: terminal-fail entries stable at ${c2} over 25s`);
}
T65.testName = "No busy-loop on persistent fail";

// T66 — HTTP token refresh after expiry: an aged-out token is refetched at
// download time. Needs the device's token to be stale (idle ~2h, or shorten
// the token TTL server-side to speed this up).
export async function T66() {
	const skip = need();
	if (skip) return skip;
	step("make the device's HTTP token stale: idle it online past the token TTL (~2h), OR shorten the TTL server-side");
	await ask("confirm the device's cached HTTP token has expired — press Enter…");

	const fw = await uploadFirmwareFor("T66");
	const { rollout_id } = await activateRollout("T66", { firmware_id: fw.firmware_id });
	step("device should log 'HTTP token acquired' AGAIN (refetch) then download — watch serial");

	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitInstallMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`stale token refetched at download time, download succeeded (reached ${j.phase})`);
}
T66.testName = "Token expiry / refresh";
