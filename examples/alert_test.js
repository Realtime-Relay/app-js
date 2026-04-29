/**
 * Interactive end-to-end test harness for the new alerting changes.
 *
 * Drive a device with telemetry separately (e.g. device-js/examples/simulator.js
 * publishing the `temperature` and `humidity` metrics for `device_ident`).
 *
 * Usage:
 *   RELAY_API_KEY=...  RELAY_SECRET=...  DEVICE_IDENT=sensor_01 \
 *     node examples/alert_test.js
 *
 * Optional:
 *   RELAY_MODE=test|production    (default: test)
 *
 * Once running, type a number from the menu and Enter.
 */
import { RelayApp } from "../src/index.js";
import { createInterface } from "readline";

const DEVICE_IDENT = process.env.DEVICE_IDENT;
if (!DEVICE_IDENT) {
  console.error("DEVICE_IDENT env var required");
  process.exit(1);
}

const RULE_THRESHOLD_NAME = "high_temp_test";
const RULE_EPHEMERAL_NAME = "high_humidity_test_eph";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

app.connection.listeners((evt) => console.log(`[connection] ${evt}`));

await app.connect();
console.log("Connected to RelayX\n");

// ── Resolve device id ──────────────────────────────────────────
const device = await app.device.get({ ident: DEVICE_IDENT });
if (!device) {
  console.error(`Device "${DEVICE_IDENT}" not found`);
  process.exit(1);
}
const DEVICE_ID = device.id;
console.log(`Using device "${DEVICE_IDENT}" (id=${DEVICE_ID})\n`);

// ── Hold references for cleanup ────────────────────────────────
let thresholdAlert = null;
let ephemeralAlert = null;

// ── Backend THRESHOLD rule ─────────────────────────────────────
async function ensureThresholdRule() {
  thresholdAlert = await app.alert.get(RULE_THRESHOLD_NAME);
  if (thresholdAlert == null) {
    thresholdAlert = await app.alert.create({
      name: RULE_THRESHOLD_NAME,
      description: "test rig: temp > 30 for 5s, recovery 5s, cooldown 10s",
      type: "THRESHOLD",
      metric: "temperature",
      config: {
        scope: { type: "DEVICE", value: DEVICE_ID },
        operator: ">",
        value: 30,
        duration: 5,
        recovery_duration: 5,
        cooldown: 10,
      },
      notification_channel: [],
    });
    console.log(`[setup] created backend rule "${RULE_THRESHOLD_NAME}"`);
  } else {
    console.log(
      `[setup] reusing existing backend rule "${RULE_THRESHOLD_NAME}"`,
    );
  }

  await thresholdAlert.listen({
    onFire: (d) =>
      console.log(`[backend] FIRE     incident=${d.incident_id ?? "-"}`),
    onResolved: (d) =>
      console.log(`[backend] RESOLVED incident=${d.incident_id ?? "-"}`),
    onAck: (d) =>
      console.log(
        `[backend] ACK      incident=${d.incident_id ?? "-"}  by=${d.ack?.acked_by}`,
      ),
  });
}

// ── Ephemeral rule (owner-side evaluator) ─────────────────────
async function ensureEphemeralRule() {
  ephemeralAlert = await app.alert.get(RULE_EPHEMERAL_NAME);
  if (ephemeralAlert == null) {
    ephemeralAlert = await app.alert.createEphemeral({
      name: RULE_EPHEMERAL_NAME,
      description: "test rig: humidity > 70 for 5s, recovery 5s, cooldown 10s",
      config: {
        topic: {
          source: "TELEMETRY",
          device_ident: DEVICE_IDENT,
          last_token: "humidity",
        },
        duration: 5,
        recovery_duration: 5,
        cooldown: 10,
        recovery_eval_type: "VALUE",
      },
      notification_channel: [],
    });
    console.log(`[setup] created ephemeral rule "${RULE_EPHEMERAL_NAME}"`);
  } else {
    console.log(`[setup] reusing existing ephemeral rule "${RULE_EPHEMERAL_NAME}"`);
  }

  ephemeralAlert.setEvaluator((rolling) => {
    const v = rolling[DEVICE_IDENT]?.humidity?.value ?? 0;
    return v > 70;
  });

  await ephemeralAlert.listen({
    onFire: (d) =>
      console.log(`[ephem]   FIRE     incident=${d.incident_id ?? "-"}`),
    onResolved: (d) =>
      console.log(`[ephem]   RESOLVED incident=${d.incident_id ?? "-"}`),
    onAck: (d) =>
      console.log(
        `[ephem]   ACK      incident=${d.incident_id ?? "-"}  by=${d.ack?.acked_by}`,
      ),
  });
}

await ensureThresholdRule();
await ensureEphemeralRule();
console.log("");

// ── Menu actions ──────────────────────────────────────────────
const actions = {
  async "1"() {
    console.log("\n→ ack backend incident");
    const ok = await app.alert.ack({
      alert_id: thresholdAlert.id,
      device_id: DEVICE_ID,
      acked_by: "alice",
      ack_notes: "test ack from menu",
    });
    console.log(`   result: ${ok}`);
  },

  async "2"() {
    console.log("\n→ ack ephemeral incident");
    const ok = await app.alert.ack({
      alert_id: ephemeralAlert.id,
      device_id: DEVICE_ID,
      acked_by: "bob",
      ack_notes: "test ack from menu",
    });
    console.log(`   result: ${ok}`);
  },

  async "3"() {
    console.log("\n→ alert.history (backend, all states, last hour)");
    const end = new Date().toISOString();
    const start = new Date(Date.now() - 360 * 60 * 1000).toISOString();

    const { events } = await app.alert.history({
      rule_type: "DEVICE",
      device_ident: DEVICE_IDENT,
      rule_states: ["fire", "resolved", "ack"],
      start,
      end
    });

    console.log(`   ${events.length} events`);
    const byIncident = {};
    for (const e of events) {
      const k = e.incident_id ?? "(none)";
      (byIncident[k] = byIncident[k] || []).push(e.state);
    }
    for (const [inc, states] of Object.entries(byIncident)) {
      console.log(`   incident ${inc}: ${states.join(" → ")}`);
    }
  },

  async "4"() {
    console.log("\n→ alert.history (rule-scoped, fires only, count per hour)");
    const end = new Date().toISOString();
    const start = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

    const { events } = await app.alert.history({
      rule_type: "RULE",
      rule_id: thresholdAlert.id,
      rule_states: ["fire"],
      start,
      end,
      interval: "1h",
      aggregate_fn: "count",
    });
    console.log(`   ${events.length} buckets`);
    for (const e of events) {
      console.log(`   ${new Date(e.timestamp).toISOString()}  ${e.value}`);
    }
  },

  async "5"() {
    const inc = await prompt("   incident_id: ");
    if (!inc.trim()) return console.log("   skipped");

    console.log(`\n→ alert.history filtered by incident_id=${inc.trim()}`);
    const end = new Date().toISOString();
    const start = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

    // Use rule_type: DEVICE so we match incidents from any rule on this device
    // (backend OR ephemeral). incident_id is unique enough on its own.
    const { events } = await app.alert.history({
      rule_type: "DEVICE",
      device_ident: DEVICE_IDENT,
      incident_id: inc.trim(),
      rule_states: ["fire", "resolved", "ack"],
      start,
      end,
    });
    for (const e of events) {
      console.log(`   ${new Date(e.timestamp).toISOString()}  ${e.state}`);
    }
  },

  async "6"() {
    console.log("\n→ mute backend rule for 60s");
    const res = await app.alert.mute({
      id: thresholdAlert.id,
      mute_config: {
        type: "TIME_BASED",
        mute_till: new Date(Date.now() + 60_000).toISOString(),
      },
    });
    console.log(`   ${res.status}`);
  },

  async "7"() {
    console.log("\n→ unmute backend rule");
    const res = await app.alert.unmute(thresholdAlert.id);
    console.log(`   ${res.status}`);
  },

  // ── Negative tests ──────────────────────────────────────────
  async "8"() {
    console.log("\n→ NEGATIVE: ack backend without device_id (should throw)");
    try {
      await app.alert.ack({
        alert_id: thresholdAlert.id,
        acked_by: "x",
      });
      console.log("   ✗ DID NOT throw");
    } catch (err) {
      console.log(`   ✓ threw: ${err.message}`);
    }
  },

  async "9"() {
    console.log("\n→ NEGATIVE: history with rule_states=[\"ack_all\"]");
    try {
      await app.alert.history({
        rule_type: "DEVICE",
        device_ident: DEVICE_IDENT,
        rule_states: ["ack_all"],
        start: new Date(Date.now() - 60_000).toISOString(),
        end: new Date().toISOString(),
      });
      console.log("   ✗ DID NOT throw");
    } catch (err) {
      console.log(`   ✓ threw: ${err.message}`);
    }
  },

  async "10"() {
    console.log("\n→ NEGATIVE: history with aggregate_fn=mean (should throw)");
    try {
      await app.alert.history({
        rule_type: "DEVICE",
        device_ident: DEVICE_IDENT,
        rule_states: ["fire"],
        start: new Date(Date.now() - 60_000).toISOString(),
        end: new Date().toISOString(),
        interval: "1h",
        aggregate_fn: "mean",
      });
      console.log("   ✗ DID NOT throw");
    } catch (err) {
      console.log(`   ✓ threw: ${err.message}`);
    }
  },

  async "11"() {
    console.log("\n→ NEGATIVE: ackAll / ackHistory presence");
    console.log(`   app.alert.ackAll      = ${typeof app.alert.ackAll}`);
    console.log(`   app.alert.ackHistory  = ${typeof app.alert.ackHistory}`);
    console.log(`   (both should be "undefined")`);
  },

  async "12"() {
    console.log("\n→ list all alerts on this org");
    const alerts = await app.alert.list();
    for (const a of alerts) {
      console.log(`   - ${a.name}  (${a.type ?? "?"})  id=${a.id}`);
    }
  },

  async "q"() {
    console.log("\n→ cleanup");
    try {
      if (thresholdAlert?.stop) await thresholdAlert.stop();
    } catch {}
    try {
      if (ephemeralAlert?.stop) await ephemeralAlert.stop();
    } catch {}
    if (thresholdAlert?.id) {
      await app.alert.delete(thresholdAlert.id);
      console.log(`   deleted ${RULE_THRESHOLD_NAME}`);
    }
    if (ephemeralAlert?.id) {
      await app.alert.delete(ephemeralAlert.id);
      console.log(`   deleted ${RULE_EPHEMERAL_NAME}`);
    }
    await app.disconnect();
    console.log("   disconnected");
    process.exit(0);
  },
};

// ── Menu loop ─────────────────────────────────────────────────
const rl = createInterface({ input: process.stdin, output: process.stdout });
function prompt(q) {
  return new Promise((res) => rl.question(q, res));
}

function printMenu() {
  console.log("\n──────────── alert_test menu ────────────");
  console.log("  Drives: temp>30 fires backend; humidity>70 fires ephemeral");
  console.log("  Trigger fires by publishing telemetry from your device.");
  console.log("");
  console.log("  1   ack backend incident");
  console.log("  2   ack ephemeral incident (no device_id)");
  console.log("  3   alert.history all states (last 1h)");
  console.log("  4   alert.history fires bucketed by hour (last 24h)");
  console.log("  5   alert.history filtered by incident_id");
  console.log("  6   mute backend rule (60s)");
  console.log("  7   unmute backend rule");
  console.log("  8   neg: ack backend w/o device_id");
  console.log("  9   neg: history with ack_all");
  console.log("  10  neg: history with aggregate_fn=mean");
  console.log("  11  neg: ackAll/ackHistory presence");
  console.log("  12  list all alerts");
  console.log("  q   cleanup + quit");
  console.log("─────────────────────────────────────────");
}

(async () => {
  printMenu();
  while (true) {
    const choice = (await prompt("> ")).trim();
    if (!choice) {
      printMenu();
      continue;
    }
    const fn = actions[choice];
    if (!fn) {
      console.log(`unknown: ${choice}`);
      continue;
    }
    try {
      await fn();
    } catch (err) {
      console.error("action failed:", err.message);
    }
  }
})();

process.on("SIGINT", async () => {
  await actions["q"]();
});
