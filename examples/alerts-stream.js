/**
 * app.alert.stream() — global subscription to alert lifecycle events
 * across ALL rules in the org, with optional filters.
 *
 * Run:
 *   RELAY_API_KEY=... RELAY_SECRET=... node examples/alerts-stream.js
 *
 * Try the filter variations near the bottom by uncommenting one block.
 */

import { RelayApp } from "../src/index.js";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

app.connection.listeners((event) => {
  console.log(`[connection] ${event}`);
});

await app.connect();
console.log("Connected to RelayX\n");

// Warm the rule cache so the first events are enriched without an extra
// network round-trip. (stream() will self-heal on miss too — this is just
// a small startup nicety.)
await app.alert.list();

// ─── 1. Dump raw, pretty-printed JSON ─────────────────────────

function render(e) {
  console.log(JSON.stringify(e, null, 2));
}

// ─── 2. Subscribe ─────────────────────────────────────────────

// (a) Everything in the org — no filters.
const sub = await app.alert.stream({
  callback: (e) => render(e),
});

console.log("Listening for alert events. Ctrl+C to stop.\n");

// ─── Filter examples — uncomment to try ───────────────────────

// (b) Only specific rules:
//
// const sub = await app.alert.stream({
//   filters: { ruleIds: ["<rule_id_1>", "<rule_id_2>"] },
//   callback: (e) => render(e),
// });

// (c) Only events for specific devices (matched against payload device_id;
//     idents are resolved up-front via the device cache):
//
// const sub = await app.alert.stream({
//   filters: { deviceIdents: ["sensor-001", "gateway-02"] },
//   callback: (e) => render(e),
// });

// (d) Only rules whose scope is a particular logical group / hierarchy node:
//
// const sub = await app.alert.stream({
//   filters: { groupIds: ["<group_id>"] },
//   callback: (e) => render(e),
// });

// (e) Combined — AND-ed filters: specific device, specific rules:
//
// const sub = await app.alert.stream({
//   filters: {
//     deviceIdents: ["sensor-001"],
//     ruleIds: ["<rule_id>"],
//   },
//   callback: (e) => render(e),
// });

// ─── 3. Graceful shutdown ─────────────────────────────────────

async function shutdown() {
  console.log("\nStopping stream…");
  await sub.off();
  await app.disconnect();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
