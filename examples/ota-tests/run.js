/**
 * OTA test runner. Maps to the Miro "OTA Pipeline — Test Plan" table.
 *
 *   node run.js T01                # one test
 *   node run.js T01 T02 T05        # several
 *   node run.js A                  # whole area (A..J)
 *   node run.js T20-T26            # range
 *   node run.js all                # everything
 *   node run.js app-only           # skip tests that need device hardware
 *
 * Add --step anywhere to pause after each test for inspection:
 *   node run.js A --step           # Enter = next, s = skip, q = quit
 *
 * Each test is a named async function exported by an area file. It returns
 * { result: "PASS"|"BLOCKED", reason?, note? } or throws on FAIL.
 */

import readline from "node:readline";

import * as A from "./area-a-upload.js";
import * as B from "./area-b-lifecycle.js";
import * as C from "./area-c-discovery.js";
import * as D from "./area-d-download.js";
import * as E from "./area-e-install.js";
import * as F from "./area-f-status.js";
import * as G from "./area-g-fifo.js";
import * as H from "./area-h-retry.js";
import * as I from "./area-i-scale.js";
import * as J from "./area-j-wiring.js";
import { disconnectApp, resetStepClock } from "./_lib.js";

const AREAS = { A, B, C, D, E, F, G, H, I, J };

// Tests that need real device hardware. `node run.js app-only` skips these.
const DEVICE_REQUIRED = new Set([
	"T14", "T15", "T18", "T20", "T21", "T22", "T23", "T24", "T25", "T26", "T27", "T28", "T29", "T30",
	"T31", "T32", "T33", "T34", "T35", "T36", "T37", "T38", "T39", "T40",
	"T41", "T42", "T48", "T49", "T50", "T51", "T52", "T53", "T58", "T63",
	"T64", "T65", "T66", "T69",
	// INSTALL_ONLY flow:
	"T70",
]);

function buildIndex() {
	// Map "T##" → { fn, area, name } by scanning area exports for symbols
	// starting with "T". Area files export the test number; the function's
	// .testName property carries the human label.
	const index = new Map();
	for (const [areaLetter, mod] of Object.entries(AREAS)) {
		for (const [key, fn] of Object.entries(mod)) {
			if (!/^T\d+$/.test(key)) continue;
			index.set(key, { fn, area: areaLetter, name: fn.testName || key });
		}
	}
	return index;
}

function resolve(args, index) {
	if (args.length === 0 || args.includes("all")) {
		return [...index.keys()].sort(byNum);
	}
	if (args.includes("app-only")) {
		return [...index.keys()].filter((k) => !DEVICE_REQUIRED.has(k)).sort(byNum);
	}
	const out = new Set();
	for (const arg of args) {
		if (/^[A-J]$/.test(arg)) {
			for (const [k, v] of index) {
				if (v.area === arg) out.add(k);
			}
			continue;
		}
		const range = arg.match(/^T(\d+)-T(\d+)$/);
		if (range) {
			const [, lo, hi] = range;
			for (let n = +lo; n <= +hi; n++) {
				const k = `T${String(n).padStart(2, "0")}`;
				if (index.has(k)) out.add(k);
			}
			continue;
		}
		if (index.has(arg)) out.add(arg);
		else console.warn(`unknown test id: ${arg}`);
	}
	return [...out].sort(byNum);
}

const byNum = (a, b) => parseInt(a.slice(1), 10) - parseInt(b.slice(1), 10);

// ── ANSI dye (skip when piped) ──────────────────────────────────────────
const c = process.stdout.isTTY
	? {
		green: (s) => `\x1b[32m${s}\x1b[0m`,
		red: (s) => `\x1b[31m${s}\x1b[0m`,
		yellow: (s) => `\x1b[33m${s}\x1b[0m`,
		dim: (s) => `\x1b[2m${s}\x1b[0m`,
		bold: (s) => `\x1b[1m${s}\x1b[0m`,
	}
	: { green: s => s, red: s => s, yellow: s => s, dim: s => s, bold: s => s };

// ── main ────────────────────────────────────────────────────────────────

const rawArgs = process.argv.slice(2);
const stepMode = rawArgs.includes("--step");
const args = rawArgs.filter((a) => a !== "--step");

const index = buildIndex();
const tests = resolve(args, index);

// Prompt for input between tests in --step mode.
async function prompt(label) {
	const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
	const answer = await new Promise((res) => rl.question(label, res));
	rl.close();
	return answer.trim().toLowerCase();
}

if (tests.length === 0) {
	console.error("no tests selected. Try `node run.js all` or `node run.js T01`");
	process.exit(2);
}

console.log(
	c.bold(`running ${tests.length} test(s)`) +
		(stepMode ? c.dim("  [step mode: Enter=next, s=skip, q=quit]") : "") +
		"\n",
);

const summary = { pass: 0, fail: 0, block: 0, skip: 0, errors: [] };

let stopped = false;
for (let i = 0; i < tests.length; i++) {
	if (stopped) break;
	const id = tests[i];
	const entry = index.get(id);
	const label = `${c.bold(id)} ${c.dim(entry.area)}  ${entry.name}`;
	const counter = c.dim(`(${i + 1}/${tests.length})`);

	if (stepMode) {
		const answer = await prompt(`${counter} ${label}\n  > Enter to run, s to skip, q to quit: `);
		if (answer === "q") { stopped = true; break; }
		if (answer === "s") { summary.skip++; console.log(`  ${c.dim("skipped")}\n`); continue; }
	} else {
		console.log(`${counter} ${label}`);
	}

	resetStepClock();
	const t0 = Date.now();
	try {
		const r = await entry.fn();
		const dt = `${Date.now() - t0}ms`;
		if (r?.result === "BLOCKED") {
			summary.block++;
			console.log(`  ${c.yellow("BLOCK")} ${c.dim(dt)} — ${r.reason ?? ""}`);
		} else {
			summary.pass++;
			const note = r?.note ? ` — ${r.note}` : "";
			console.log(`  ${c.green("PASS")}  ${c.dim(dt)}${note}`);
		}
	} catch (e) {
		summary.fail++;
		summary.errors.push({ id, msg: e?.message ?? String(e) });
		console.log(`  ${c.red("FAIL")}  ${c.dim(`${Date.now() - t0}ms`)} — ${e?.message ?? e}`);
	}

	console.log();
}

console.log();
console.log(
	c.bold("summary  ") +
		`${c.green(summary.pass + " pass")}  ` +
		`${c.red(summary.fail + " fail")}  ` +
		`${c.yellow(summary.block + " blocked")}  ` +
		(summary.skip ? `${c.dim(summary.skip + " skipped")}  ` : "") +
		c.dim(`of ${tests.length}`),
);

if (summary.errors.length) {
	console.log("\n" + c.bold("failures:"));
	for (const e of summary.errors) console.log(`  ${c.red(e.id)}: ${e.msg}`);
}

await disconnectApp();
process.exit(summary.fail > 0 ? 1 : 0);
