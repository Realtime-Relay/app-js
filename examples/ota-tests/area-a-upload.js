/**
 * Area A — Upload (T01..T08). Pure app-SDK / file-handler tests.
 * Mirrors the Miro "OTA Pipeline — Test Plan" rows for A·Upload.
 */

import {
	getApp,
	uploadFirmwareFor,
	uniqueName,
	uniqueVersion,
	loadTestBin,
	makeEmptyBin,
	makeOversizeBin,
	activateRollout,
	cleanupRollouts,
	expectError,
	assert,
	pass,
	block,
	sleep,
} from "./_lib.js";

// T01 — upload valid bin
export async function T01() {
	const r = await uploadFirmwareFor("T01");
	assert(r.firmware_id, "no firmware_id returned");
	assert(/^[a-f0-9]{64}$/.test(r.sha256), "sha256 not a 64-hex string");
	assert(r.size > 0, "size <= 0");
	return pass(`fw=${r.firmware_id} size=${r.size}`);
}
T01.testName = "Upload valid bin";

// T02 — duplicate (name, version) → VERSION_EXISTS
export async function T02() {
	const name = uniqueName("T02");
	const version = uniqueVersion("T02");
	await uploadFirmwareFor("T02", { name, version });
	await expectError("VERSION_EXISTS", () =>
		uploadFirmwareFor("T02", { name, version }),
	);
	return pass();
}
T02.testName = "Duplicate (name,version) rejected";

// T03 — empty body. The SDK fails fast in #requireFile ("file is empty")
// before any HTTP request reaches the file-handler. The server-side guard
// ("Empty body — no firmware uploaded") covers any client that sends a
// 0-byte body anyway, but the SDK never gets there. Either is a valid pass.
export async function T03() {
	await expectError("file is empty", () =>
		uploadFirmwareFor("T03", { file: makeEmptyBin() }),
	);
	return pass("rejected client-side (SDK guard)");
}
T03.testName = "Empty file rejected";

// T04 — > 64MB → FILE_TOO_LARGE
export async function T04() {
	await expectError("FILE_TOO_LARGE", () =>
		uploadFirmwareFor("T04", { file: makeOversizeBin() }),
	);
	return pass();
}
T04.testName = "Oversize (>64MB) rejected";

// T05 — list pagination + size-0 (unfinalized) exclusion. We upload a few
// fresh entries so pagination is non-trivial even on a fresh org.
export async function T05() {
	const app = await getApp();
	for (let i = 0; i < 3; i++) await uploadFirmwareFor(`T05-${i}`);
	const page1 = await app.ota.firmwareList({ page: 1, limit: 2 });
	assert(Array.isArray(page1.firmwares), "firmwares not array");
	assert(page1.firmwares.length <= 2, "page exceeded limit");
	assert(
		typeof page1.page.has_more === "boolean",
		"page.has_more missing",
	);
	for (const fw of page1.firmwares) {
		assert(fw.size > 0, `size-0 firmware leaked: ${fw.firmware_id}`);
	}
	return pass(`page1=${page1.firmwares.length} has_more=${page1.page.has_more}`);
}
T05.testName = "firmwareList pagination + no size-0";

// T06 — firmwareDelete; GET /url should 404 afterward
export async function T06() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T06");
	const del = await app.ota.firmwareDelete({ id: fw.firmware_id });
	assert(del.deleted === true, "deleted != true");
	// Trying to make a rollout off it should now fail with FIRMWARE_NOT_FOUND.
	await expectError("FIRMWARE_NOT_FOUND", () =>
		app.ota.createRollout({
			firmware_id: fw.firmware_id,
			request_type: "DOWNLOAD_INSTALL",
			target: { type: "all" },
		}),
	);
	return pass();
}
T06.testName = "firmwareDelete";

// T07 — delete blocked by ACTIVE rollout, allowed once STOPPED
export async function T07() {
	const app = await getApp();
	const fw = await uploadFirmwareFor("T07");
	const { rollout_id } = await activateRollout("T07", {
		firmware_id: fw.firmware_id,
		target: { type: "all" }, // count doesn't matter — just needs to be ACTIVE
	});
	await expectError("FIRMWARE_IN_ACTIVE_ROLLOUT", () =>
		app.ota.firmwareDelete({ id: fw.firmware_id }),
	);
	await app.ota.toggleRollout({ rollout_id, state: "STOPPED" });
	// Tiny grace so the engine commits the rollout state change before
	// the file-handler re-checks (different services, same DB).
	await sleep(500);
	const del = await app.ota.firmwareDelete({ id: fw.firmware_id });
	assert(del.deleted === true, "delete after STOP returned !deleted");
	await cleanupRollouts([rollout_id]);
	return pass();
}
T07.testName = "Delete blocked by live rollout";

// T08 — HTTP token exchange. The app side has already gone through
// accounts.user.get_http_token in getApp() (init()); if we got this far the
// exchange worked. We assert by exercising a token-only path (upload).
export async function T08() {
	const r = await uploadFirmwareFor("T08", { file: loadTestBin() });
	assert(r.firmware_id, "upload via HTTP token path failed");
	return pass("app-side token verified; device-side requires hardware");
}
T08.testName = "HTTP token exchange (app side)";
