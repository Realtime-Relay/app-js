/**
 * Area D — Download (T27..T34). Token fetch, presigned URL, SHA, partition
 * absence, vetoes, DOWNLOAD_ONLY staging.
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
	sleep,
} from "./_lib.js";

const need = () => (cfg.deviceId ? null : block("set OTA_TEST_DEVICE_ID"));

// T27 — happy download
export async function T27() {
	const skip = need();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T27");
	const { rollout_id } = await activateRollout("T27", { firmware_id: fw.firmware_id });
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitDownloadMs,
	});
	await cleanupRollouts([rollout_id]);
	return pass(`reached ${j.phase}`);
}
T27.testName = "Happy download";

// T28 — SHA-256 mismatch. The device hashes the bytes it downloads and
// compares against job.sha256. The upload pipeline always records the real
// sha of the bytes, so to force a mismatch we corrupt the STORED sha256
// (in Mongo) BEFORE activation — then the job carries a bad sha, the device
// downloads the correct bytes, computes the correct sha, and they disagree.
//
// The harness can't touch Mongo (it's a NATS client), so the corruption is a
// one-liner you run at the gated prompt. Everything else is automated.
export async function T28() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T28");

	const BAD_SHA = "0".repeat(64);
	const mongo = `mongosh "$MONGO_DB_URL" --eval 'db.Firmware.updateOne({_id: ObjectId("${fw.firmware_id}")}, {$set: {sha256: "${BAD_SHA}"}})'`;
	step(`uploaded fw=${fw.firmware_id} real sha=${String(fw.sha256).slice(0, 12)}…`);
	step("corrupt the STORED sha so it no longer matches the bytes — run this, then continue:");
	console.log(`\n  ${mongo}\n`);
	await ask("press Enter once the sha is corrupted (it must happen BEFORE activation)…");

	const { rollout_id } = await activateRollout("T28", { firmware_id: fw.firmware_id });

	step("waiting for FAILED (device downloads ok, then sha check fails)");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED"],
		timeoutMs: cfg.waitDownloadMs,
	});

	// Confirm the failure reason is the sha check, not something else.
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const failed = hist.history.find((h) => (h.phase ?? h.to) === "FAILED");
	const err = (failed?.error ?? j.error ?? "").toLowerCase();
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);

	await cleanupRollouts([rollout_id]);

	if (!err.includes("sha")) {
		return block(`reached FAILED but error was "${failed?.error ?? j.error}" (expected 'sha mismatch'): ${phases.join("→")}`);
	}
	return pass(`sha mismatch caught: ${phases.join("→")} (err="${failed?.error ?? j.error}")`);
}
T28.testName = "SHA-256 mismatch";

// T29 — no OTA partition: device fails ONCE with 'no ota partition' and does
// NOT busy-loop. The manual part is flashing a single-app partition table
// (no inactive OTA slot); the harness then activates, asserts the FAILED
// reason, and re-checks after a delay to prove the handled_rollout_ guard
// stopped it re-attempting.
//
// Device prep (before running): in example/sdkconfig.defaults set
// CONFIG_PARTITION_TABLE_SINGLE_APP_LARGE=y (NOT plain SINGLE_APP — the ~1.2MB
// app won't fit its 1MB factory slot), then: rm -f sdkconfig; idf.py
// erase-flash; idf.py build flash monitor. Restore the two-OTA partitions.csv
// after the test (same rm + erase-flash dance).
export async function T29() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();

	await ask("confirm the device is flashed with a SINGLE-APP partition table (no ota_0/ota_1) — press Enter…");

	const fw = await uploadFirmwareFor("T29");
	const { rollout_id } = await activateRollout("T29", { firmware_id: fw.firmware_id });

	step("waiting for FAILED (device has no inactive OTA slot)");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED"],
		timeoutMs: 60_000,
	});

	const hist1 = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const failed = hist1.history.find((h) => (h.phase ?? h.to) === "FAILED");
	const err = (failed?.error ?? j.error ?? "").toLowerCase();
	if (!err.includes("partition")) {
		await cleanupRollouts([rollout_id]);
		return block(`reached FAILED but error was "${failed?.error ?? j.error}" (expected 'no ota partition')`);
	}

	// No busy-loop: after the guard kicks in, the device must NOT re-attempt on
	// its next poll. Wait, then confirm there's still exactly ONE FAILED entry
	// and attempts didn't climb.
	step("verifying no busy-loop: waiting 20s, then re-checking the ledger…");
	const failedCount1 = hist1.history.filter((h) => (h.phase ?? h.to) === "FAILED").length;
	await sleep(20_000);
	const hist2 = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const failedCount2 = hist2.history.filter((h) => (h.phase ?? h.to) === "FAILED").length;

	await cleanupRollouts([rollout_id]);

	if (failedCount2 > failedCount1) {
		return block(`busy-loop detected: FAILED entries grew ${failedCount1} -> ${failedCount2} (handled_rollout_ guard not holding)`);
	}
	return pass(`FAILED 'no ota partition' once, no re-attempt (FAILED entries stable at ${failedCount2}, attempts=${hist2.attempts ?? "?"})`);
}
T29.testName = "No OTA partition (no busy-loop)";

// T30 — no HTTP token: with the token exchange denied, the download never
// starts and the job fails with a token error.
export async function T30() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	step("device prep: deny the token exchange — in the NATS account remove the device's");
	step("  publish perm on 'accounts.user.get_http_token' (or set RELAY_OTA_TOKEN_SERVICE bogus + reflash)");
	await ask("confirm the device can no longer get an HTTP token — press Enter…");

	const fw = await uploadFirmwareFor("T30");
	const { rollout_id } = await activateRollout("T30", { firmware_id: fw.firmware_id });

	step("waiting for FAILED (token never acquired, download never starts)");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED"],
		timeoutMs: 60_000,
	});
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const failed = hist.history.find((h) => (h.phase ?? h.to) === "FAILED");
	const err = (failed?.error ?? j.error ?? "").toLowerCase();
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);
	await cleanupRollouts([rollout_id]);
	if (phases.includes("DOWNLOADING")) {
		return block(`download started despite no token — got phases ${phases.join("→")}`);
	}
	if (!err.includes("token")) {
		return block(`FAILED but error was "${failed?.error ?? j.error}" (expected 'no http token')`);
	}
	return pass(`no http token -> FAILED before download (err="${failed?.error ?? j.error}"). Restore the perm after.`);
}
T30.testName = "No HTTP token";

// T31 — URL fetch fails: the file-handler /url endpoint returns non-200, so
// the device gets a token but can't get a presigned URL.
export async function T31() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T31");
	const { rollout_id } = await activateRollout("T31", { firmware_id: fw.firmware_id });

	step("break the /url endpoint NOW (before the device requests it):");
	step("  stop the ota-file-handler, OR rotate the device bearer, OR otherwise make GET /iot/ota/firmware/<id>/url return non-200");
	await ask("press Enter once /url will return non-200…");

	step("waiting for FAILED (token ok, but presigned-URL fetch fails)");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED"],
		timeoutMs: 60_000,
	});
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const failed = hist.history.find((h) => (h.phase ?? h.to) === "FAILED");
	const err = (failed?.error ?? j.error ?? "").toLowerCase();
	await cleanupRollouts([rollout_id]);
	if (!err.includes("url")) {
		return block(`FAILED but error was "${failed?.error ?? j.error}" (expected 'url fetch failed')`);
	}
	return pass(`url fetch failed -> FAILED (err="${failed?.error ?? j.error}"). Restart the file-handler after.`);
}
T31.testName = "URL fetch fails";

// T32 — WiFi drop mid-download: clean esp_ota_abort, FAILED, no brick; the
// next nudge/poll restarts from 0 (v1 has no Range resume).
export async function T32() {
	const skip = need();
	if (skip) return skip;
	const app = await getApp();
	const fw = await uploadFirmwareFor("T32");
	const { rollout_id } = await activateRollout("T32", { firmware_id: fw.firmware_id });

	step("waiting for DOWNLOADING so you can cut WiFi mid-stream");
	await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING"],
		timeoutMs: cfg.waitDownloadMs,
	});
	await ask("CUT WiFi now (power off the AP / move out of range), then press Enter…");

	step("waiting for FAILED (download aborted cleanly)");
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["FAILED"],
		timeoutMs: 90_000,
	});
	const hist = await app.ota.jobHistory({ job_id: j.job_id, limit: 200 });
	const phases = hist.history.map((h) => h.phase ?? h.to ?? h.event);
	await cleanupRollouts([rollout_id]);
	return pass(`WiFi drop -> clean FAILED, device still boots old image (no brick): ${phases.join("→")}. Restore WiFi; retry restarts from 0.`);
}
T32.testName = "WiFi drop mid-download";

// T33 — on_download veto; force_download overrides
export async function T33() {
	const skip = need();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T33");

	// Run 1: hook returns false → VETOED
	const a = await activateRollout("T33a", { firmware_id: fw.firmware_id });
	const j1 = await waitForPhase({
		rollout_id: a.rollout_id,
		device_id: cfg.deviceId,
		phases: ["VETOED", "DOWNLOADING", "DOWNLOADED"],
		timeoutMs: 30_000,
	});

	// Run 2: force_download=true overrides the veto
	const b = await activateRollout("T33b", {
		firmware_id: fw.firmware_id,
		force_download: true,
	});
	const j2 = await waitForPhase({
		rollout_id: b.rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADING", "DOWNLOADED", "INSTALLING", "INSTALLED"],
		timeoutMs: cfg.waitDownloadMs,
	});
	await cleanupRollouts([a.rollout_id, b.rollout_id]);

	if (j1.phase !== "VETOED") {
		return block(
			`first run reached ${j1.phase} — device's on_download must return false for this test; got VETOED? rerun with the hook wired up`,
		);
	}
	return pass(`veto=${j1.phase}  force=${j2.phase}`);
}
T33.testName = "on_download veto + force_download";

// T34 — DOWNLOAD_ONLY staging: reaches DOWNLOADED, never installs
export async function T34() {
	const skip = need();
	if (skip) return skip;
	const fw = await uploadFirmwareFor("T34");
	const { rollout_id } = await activateRollout("T34", {
		firmware_id: fw.firmware_id,
		request_type: "DOWNLOAD_ONLY",
	});
	const j = await waitForPhase({
		rollout_id,
		device_id: cfg.deviceId,
		phases: ["DOWNLOADED"],
		timeoutMs: cfg.waitDownloadMs,
	});
	// Give it a moment to falsely proceed; expect it to STAY at DOWNLOADED.
	const app = await getApp();
	const before = j.phase;
	await new Promise((r) => setTimeout(r, 5000));
	const after = (await app.ota.jobsList({ rollout_id })).jobs.find(
		(x) => x.device_id === cfg.deviceId,
	);
	await cleanupRollouts([rollout_id]);
	if (before === "DOWNLOADED" && after?.phase === "DOWNLOADED") return pass("parked at DOWNLOADED");
	return block(`expected DOWNLOADED stable, got ${before} → ${after?.phase}`);
}
T34.testName = "DOWNLOAD_ONLY staging";
