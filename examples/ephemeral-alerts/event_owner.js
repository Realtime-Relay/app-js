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

// ── Create an ephemeral alert (EVENT source) ─────────────────
// Monitors "door_opened" events from device s-3.
// Fires when events are received (duration=1s means nearly instant).

const DEVICE_IDENT = "s-3";
const EVENT_NAME = "door_opened";
const MAX_OPENS = 3;

let ephAlert = await app.alert.get("ephemeral_event_door");

if (ephAlert == null) {
  ephAlert = await app.alert.createEphemeral({
    name: "ephemeral_event_door",
    description: `Client-side alert: ${EVENT_NAME} events from ${DEVICE_IDENT}`,
    config: {
      topic: {
        source: "EVENT",
        device_ident: DEVICE_IDENT,
        last_token: EVENT_NAME,
      },
      duration: 0,
      recovery_duration: 30,
      cooldown: 15,
    },
    notification_channel: [],
  });
}

console.log(`Ephemeral alert: ${ephAlert.name} (id: ${ephAlert.id})\n`);

// ── Set the evaluator ────────────────────────────────────────
// Rolling state for EVENT source stores raw event data:
// { "s-3": { "door_opened": { action: "open", zone: "warehouse", count: 5, ... } } }
//
// Each new event overwrites the previous in rolling state.
// We use a local counter to track total event occurrences.

let eventCount = 0;

ephAlert.setEvaluator((data) => {
  const device = data[DEVICE_IDENT];
  if (!device) return false;

  const eventData = device[EVENT_NAME];
  if (!eventData) return false;

  eventCount++;

  const breached = eventCount > MAX_OPENS;
  const result = breached ? "BREACH" : "CLEAR";

  console.log(
    `  [eval] ${EVENT_NAME} count=${eventCount} (threshold=${MAX_OPENS}) → ${result}`,
  );

  return breached;
});

// ── Start listening ──────────────────────────────────────────

await ephAlert.listen({
  onFire: (data) => {
    console.log("\n[FIRE]", JSON.stringify(data, null, 2));
  },

  onResolved: (data) => {
    console.log("\n[RESOLVED]", JSON.stringify(data, null, 2));
  },

  onError: (err) => {
    console.error("\n[EVALUATOR ERROR]", err.message);
  },
});

console.log("Ephemeral alert engine started (owner mode — EVENT source).");
console.log(
  `Monitoring ${DEVICE_IDENT}: fires when ${EVENT_NAME} count > ${MAX_OPENS}`,
);
console.log("Run the event simulator to send events.\n");
console.log("Press Ctrl+C to stop.\n");

// ── Graceful shutdown ────────────────────────────────────────
process.on("SIGINT", async () => {
  console.log("\nStopping ephemeral engine...");
  await ephAlert.stop();
  console.log("Engine stopped.");

  await app.disconnect();
  console.log("Disconnected.");
  process.exit(0);
});
