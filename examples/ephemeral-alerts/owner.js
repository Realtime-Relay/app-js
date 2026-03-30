import { RelayApp } from "../../src/index.js";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "test",
});

app.connection.listeners((event) => {
  console.log(`[connection] ${event}`);
});

await app.connect();
console.log("Connected to RelayX\n");

// ── Create an ephemeral alert ────────────────────────────────
// This alert monitors temperature from device s-3.
// The evaluator runs client-side — no backend evaluation.

const DEVICE_IDENT = "s-3";
const TEMP_THRESHOLD = 85;
const HUMIDITY_THRESHOLD = 70;

let ephAlert = await app.alert.get("ephemeral_multi_metric");

if (ephAlert == null) {
  ephAlert = await app.alert.createEphemeral({
    name: "ephemeral_multi_metric",
    description: `Client-side alert: temperature > ${TEMP_THRESHOLD}°C AND humidity > ${HUMIDITY_THRESHOLD}% on ${DEVICE_IDENT}`,
    config: {
      topic: {
        source: "TELEMETRY",
        device_ident: DEVICE_IDENT,
        last_token: "*", // Subscribe to ALL metrics from this device
      },
      duration: 5,
      recovery_duration: 10,
      cooldown: 10,
    },
    notification_channel: [],
  });
}

console.log(`Ephemeral alert: ${ephAlert.name} (id: ${ephAlert.id})\n`);

// ── Set the evaluator ────────────────────────────────────────
// Rolling state with last_token: "*" accumulates all metrics:
// { "s-3": { "temperature": { value: 90.5, timestamp: ... }, "humidity": { value: 75.2, timestamp: ... } } }
//
// Breach condition: BOTH temperature > 85°C AND humidity > 70%

ephAlert.setEvaluator((data) => {
  const device = data[DEVICE_IDENT];
  if (!device) return false;

  const temp = device.temperature?.value;
  const humidity = device.humidity?.value;

  // Need both metrics to have data before evaluating
  if (temp == null || humidity == null) {
    console.log(
      `  [eval] Waiting for data... temp=${temp ?? "?"} humidity=${humidity ?? "?"}`,
    );
    return false;
  }

  const tempBreached = temp > TEMP_THRESHOLD;
  const humidityBreached = humidity > HUMIDITY_THRESHOLD;
  const breached = tempBreached && humidityBreached;

  console.log(
    `  [eval] temp=${temp}°C ${tempBreached ? ">" : "<="} ${TEMP_THRESHOLD}°C | ` +
      `humidity=${humidity}% ${humidityBreached ? ">" : "<="} ${HUMIDITY_THRESHOLD}% → ${breached ? "BREACH" : "CLEAR"}`,
  );

  return breached;
});

// ── Start listening ──────────────────────────────────────────
// Since setEvaluator() was called, this instance becomes the OWNER.
// It subscribes to telemetry, runs the evaluator, and manages the state machine.

await ephAlert.listen({
  onFire: (data) => {
    console.log("\n[FIRE]", JSON.stringify(data, null, 2));
  },

  onResolved: (data) => {
    console.log("\n[RESOLVED]", JSON.stringify(data, null, 2));
  },

  onAck: (data) => {
    console.log("\n[ACK]", data);
  },

  onAckAll: (data) => {
    console.log("\n[ACK_ALL]", data);
  },

  onError: (err) => {
    console.error("\n[EVALUATOR ERROR]", err.message);
  },
});

console.log("Ephemeral alert engine started (owner mode).");
console.log(
  `Monitoring ${DEVICE_IDENT}: temperature > ${TEMP_THRESHOLD}°C AND humidity > ${HUMIDITY_THRESHOLD}%`,
);
console.log("Run the device simulator to send telemetry data.\n");
console.log("Press Ctrl+C to stop.\n");

// ── Graceful shutdown ────────────────────────────────────────
process.on("SIGINT", async () => {
  console.log("\nStopping ephemeral engine...");
  await ephAlert.stop();
  console.log("Engine stopped. Cleaning up alert...");

  // Uncomment to delete the alert on exit:
  // await app.alert.delete(ephAlert.id);

  await app.disconnect();
  console.log("Disconnected.");
  process.exit(0);
});
