/**
 * Shared helpers for the OTA test scripts. Every test file imports from here.
 *
 * One connection per `node run.js …` invocation; tests share `app` to avoid
 * re-handshaking and re-issuing HTTP tokens.
 *
 * Env (all optional except RELAY_API_KEY/SECRET):
 *   RELAY_API_KEY, RELAY_SECRET, RELAY_MODE (default "production")
 *   OTA_TEST_BIN              path to a small valid firmware bin (default = ota-deploy.js path)
 *   OTA_TEST_DEVICE_ID        the device's id — same value the device puts on the wire
 *                             (JWT nats.org_data.api_key_id), and the same value the
 *                             engine stores as Devices._id in this setup. Used for
 *                             targeting ({type:"devices"}) and matching jobs in jobsList.
 *   OTA_TEST_DEVICE_VERSION   version label the device currently runs (T64 dedup)
 *   OTA_TEST_GROUP_ID         logical or hierarchy group containing the device (T18)
 *   OTA_TEST_GROUP_TYPE       "logical_group" | "hierarchy_group" (default logical_group)
 *   OTA_TEST_ORG2_API_KEY, OTA_TEST_ORG2_SECRET   second-org creds (T19, T60)
 *   OTA_TEST_WAIT_DOWNLOAD_MS device-download timeout (default 90_000)
 *   OTA_TEST_WAIT_INSTALL_MS  install + reboot + commit timeout (default 180_000)
 */

import { RelayApp } from "../../src/index.js";
import { readFileSync, existsSync } from "node:fs";

// ── env / config ─────────────────────────────────────────────────────────

export const cfg = {
	apiKey: process.env.RELAY_API_KEY,
	secret: process.env.RELAY_SECRET,
	mode: process.env.RELAY_MODE || "production",
	binPath: "/Users/arjun/Code/Relay/DeviceSDK/device-cpp/example/build/device-cpp-example.bin",
	deviceId: process.env.OTA_TEST_DEVICE_ID || null,
	deviceVersion: process.env.OTA_TEST_DEVICE_VERSION || null,
	groupId: process.env.OTA_TEST_GROUP_ID || null,
	groupType: process.env.OTA_TEST_GROUP_TYPE || "logical_group",
	org2: {
		apiKey: process.env.OTA_TEST_ORG2_API_KEY || null,
		secret: process.env.OTA_TEST_ORG2_SECRET || null,
	},
	waitDownloadMs: Number(process.env.OTA_TEST_WAIT_DOWNLOAD_MS || 190_000),
	waitInstallMs: Number(process.env.OTA_TEST_WAIT_INSTALL_MS || 1_080_000),
};

// ── step logging ─────────────────────────────────────────────────────────
//
// Helpers emit one `step` line per meaningful action so you can see exactly
// where a test is at any moment (no more silent 90s waits). The runner sets
// _testStart at the top of each test so timestamps reset per-test. Output
// uses dim color when stdout is a TTY; plain text otherwise.

let _testStart = Date.now();
const _isTTY = process.stdout.isTTY;
const _dim = (s) => (_isTTY ? `\x1b[2m${s}\x1b[0m` : s);
const _yellow = (s) => (_isTTY ? `\x1b[33m${s}\x1b[0m` : s);

export function resetStepClock() {
	_testStart = Date.now();
}

function _ts() {
	const ms = Date.now() - _testStart;
	if (ms < 1000) return `+${ms}ms`.padStart(7);
	return `+${(ms / 1000).toFixed(1)}s`.padStart(7);
}

export function step(msg) {
	console.log(`  ${_dim(_ts() + "  step ")}${msg}`);
}

export function note(msg) {
	console.log(`  ${_dim(_ts() + "  ")}${_yellow(msg)}`);
}

// ── connection ───────────────────────────────────────────────────────────

let _app = null;

export async function getApp() {
	if (_app) return _app;
	if (!cfg.apiKey || !cfg.secret) {
		throw new Error(
			"RELAY_API_KEY and RELAY_SECRET are required (export them or put them in your shell rc)",
		);
	}
	step(`connecting to relay (mode=${cfg.mode})…`);
	_app = new RelayApp({ api_key: cfg.apiKey, secret: cfg.secret, mode: cfg.mode });
	await _app.connect();
	const orgID = _app?._ctx?.orgID ?? _app?.ctx?.orgID ?? "?";
	step(`NATS connected (orgID=${String(orgID).slice(0, 8)}…)`);
	step("OTA init: requesting HTTP token…");
	await _app.ota.init();
	step("OTA init done");
	return _app;
}

export async function disconnectApp() {
	if (_app) {
		try {
			await _app.disconnect();
		} catch {
			/* ignore */
		}
		_app = null;
	}
}

// ── result helpers (every test returns one of these or throws) ──────────

export const PASS = { result: "PASS" };
export const pass = (note) => (note ? { result: "PASS", note } : PASS);
export const block = (reason) => ({ result: "BLOCKED", reason });
export const fail = (msg) => {
	throw new Error(msg);
};

export function assert(cond, msg) {
	if (!cond) fail(msg);
}

// errCode("VERSION_EXISTS", () => app.ota.firmwareUpload(...))
export async function expectError(code, fn) {
	step(`expect error code=${code}`);
	let threw = null;
	try {
		await fn();
	} catch (e) {
		threw = e;
	}
	assert(threw, `expected error ${code}, got success`);
	const got = threw.code || threw.message || "";
	assert(
		got === code || (typeof got === "string" && got.includes(code)),
		`expected error ${code}, got: ${got}`,
	);
	step(`error matched (${code})`);
}

// ── unique names / versions so reruns don't collide ─────────────────────

const RUN_TAG = `t${Date.now().toString(36)}`; // one tag per process

export function uniqueName(test) {
	return `ota-test-${test}-${RUN_TAG}`;
}

// Monotonic-ish semver-ish versions per test so the file-handler dedupe
// uniqueness ([org, name, version]) never trips us up on rerun. The minor
// is the test number, the patch increments within a single test run.
let _patch = 0;
export function uniqueVersion(test) {
	const major = 9; // namespace away from real builds
	const minor = parseInt(String(test).replace(/[^\d]/g, ""), 10) || 0;
	return `${major}.${minor}.${_patch++}`;
}

// ── bin helpers ──────────────────────────────────────────────────────────

let _binCache = null;

export function loadTestBin() {
	if (_binCache) return _binCache;
	if (!existsSync(cfg.binPath)) {
		fail(
			`test bin not found at ${cfg.binPath} — build the device-cpp example or set OTA_TEST_BIN`,
		);
	}
	_binCache = readFileSync(cfg.binPath);
	return _binCache;
}

// 65MB > the 64MB file-handler cap. Generated lazily — held briefly.
export function makeOversizeBin() {
	return Buffer.alloc(65 * 1024 * 1024 + 1, 0);
}

export function makeEmptyBin() {
	return Buffer.alloc(0);
}

// ── high-level orchestration ────────────────────────────────────────────

/**
 * Upload a fresh firmware with a unique (name, version) for the calling
 * test. Returns the full response (firmware_id, sha256, size, …).
 */
export async function uploadFirmwareFor(test, { file, name, version } = {}) {
	const app = await getApp();
	const data = {
		name: name ?? uniqueName(test),
		version: version ?? uniqueVersion(test),
		file: file ?? loadTestBin(),
		file_name: cfg.binPath,
	};
	step(`upload  name=${data.name} version=${data.version} bytes=${data.file?.length ?? "?"}`);
	const r = await app.ota.firmwareUpload(data);
	step(`uploaded  fw=${r.firmware_id} sha=${String(r.sha256 || "").slice(0, 12)}… size=${r.size}`);
	return r;
}

/**
 * Create + activate in one shot. Target defaults to the configured test
 * device if set, else falls back to { type: "all" }.
 */
export async function activateRollout(test, params) {
	const app = await getApp();
	// Default target: pin to the configured test device if we have one; else
	// "all" (the whole org). Engine's resolveTargetDeviceIds matches against
	// Devices._id — same value as the api_key_id the device knows itself by.
	const target =
		params.target ??
		(cfg.deviceId
			? { type: "devices", device_ids: [cfg.deviceId] }
			: { type: "all" });
	const reqType = params.request_type ?? "DOWNLOAD_INSTALL";
	step(`createRollout  fw=${params.firmware_id} req=${reqType} target=${target.type}`);
	const draft = await app.ota.createRollout({
		firmware_id: params.firmware_id,
		request_type: reqType,
		target,
		force_download: params.force_download ?? false,
		force_install: params.force_install ?? false,
		user_config: params.user_config,
	});
	step(`draft created  rollout_id=${draft.rollout_id} preview_count=${draft.device_count}`);
	step(`toggleRollout  state=ACTIVE rollout_id=${draft.rollout_id}`);
	const active = await app.ota.toggleRollout({
		rollout_id: draft.rollout_id,
		state: "ACTIVE",
	});
	step(`ACTIVE  jobs=${active.device_count ?? "?"} (head nudges dispatched)`);
	return { rollout_id: draft.rollout_id, draft, active };
}

/**
 * Poll jobsList until one of `phases` is reached for the given device, or
 * timeout. Returns the job row. Throws on timeout.
 */
export async function waitForPhase({
	rollout_id,
	device_id,
	phases,
	timeoutMs,
	pollMs = 2000,
}) {
	const app = await getApp();
	const budget = timeoutMs ?? cfg.waitInstallMs;
	const deadline = Date.now() + budget;
	const targets = Array.isArray(phases) ? phases : [phases];
	// Match by the configured device id. Fall back to "first job" so a
	// 1-device org works without OTA_TEST_DEVICE_ID set.
	const matchId = device_id || cfg.deviceId || null;
	step(`waitForPhase  target=[${targets.join("|")}] timeout=${Math.round(budget / 1000)}s device=${matchId ? matchId.slice(0, 8) + "…" : "first-job"}`);
	let last = null;
	let lastSeen = null;
	let polls = 0;
	while (Date.now() < deadline) {
		polls++;
		const res = await app.ota.jobsList({ rollout_id, limit: 200 });
		const job = (matchId && res.jobs.find((j) => j.device_id === matchId)) || res.jobs[0];
		if (job) {
			last = job;
			// Only emit a step when the phase changes — quiet but informative.
			if (job.phase !== lastSeen) {
				step(`phase  ${lastSeen ?? "(start)"} → ${job.phase}  (poll #${polls}, attempts=${job.attempts ?? 0})`);
				lastSeen = job.phase;
			}
			if (targets.includes(job.phase)) {
				step(`waitForPhase done  phase=${job.phase} after ${polls} poll(s)`);
				return job;
			}
		} else if (polls === 1) {
			step(`no jobs yet for rollout — polling…`);
		}
		await sleep(pollMs);
	}
	fail(
		`timeout waiting for phase ${targets.join("|")} — last: ${last?.phase ?? "(no job)"} after ${budget}ms / ${polls} polls`,
	);
}

export function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

// Interactive prompt for tests that need a hands-on step ("power off the
// device, then press Enter"). Trims the answer. Resolves to "" on Enter.
import readline from "node:readline";
export async function ask(label) {
	const tag = _isTTY ? `\x1b[36m  ASK  \x1b[0m` : "  ASK  ";
	const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
	const ans = await new Promise((r) => rl.question(`${tag}${label} `, r));
	rl.close();
	return ans.trim();
}

// Best-effort cleanup. STOP rollouts created by tests so they don't pollute
// the next run. Never throws — cleanup is advisory.
export async function cleanupRollouts(ids) {
	const real = ids.filter(Boolean);
	if (real.length === 0) return;
	const app = await getApp();
	step(`cleanup  STOP ${real.length} rollout(s)`);
	for (const id of real) {
		try {
			await app.ota.toggleRollout({ rollout_id: id, state: "STOPPED" });
		} catch {
			/* already stopped / not draft / already gone */
		}
	}
}

// Pretty-print a row for a runner. Module-level so areas can share format.
export function fmtTest(id, area, name) {
	return `${id}  ${area}  ${name}`;
}
