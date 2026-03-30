import { RelayApp } from "../src/index.js";

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

// ── Create a THRESHOLD alert ─────────────────────────────────

var thresholdAlert = await app.alert.get("high_temp");

if (thresholdAlert == null) {
  thresholdAlert = await app.alert.create({
    name: "high_temp",
    description: "Fires when temperature exceeds 85°C for 60s",
    type: "THRESHOLD",
    metric: "temperature",
    config: {
      scope: { type: "DEVICE", value: "69bffcb28cc30a4f716936bc" },
      operator: ">",
      value: 85,
      duration: 5,
      recovery_duration: 10,
      cooldown: 10,
    },
    notification_channel: ["ops_webhook"],
  });
}

console.log("Created THRESHOLD alert:", thresholdAlert.name);

// // ── Listen for alert events ──────────────────────────────────

var firstFire = true;

await thresholdAlert.listen({
  onFire: (data) => {
    console.log("[ALERT FIRED]", data);

    if (firstFire) {
      setTimeout(async () => {
        // Mute for 1 hour
        const muteResult = await app.alert.mute({
          id: data.alert.id,
          mute_config: {
            type: "TIME_BASED",
            mute_till: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
          },
        });
        console.log("Muted alert:", muteResult.status);

        // Unmute after 10 seconds
        setTimeout(async () => {
          const unmuteResult = await app.alert.unmute(data.alert.id);
          console.log("Unmuted alert:", unmuteResult.status);
        }, 10000);
      }, 8000);

      firstFire = false;
    }
  },
  onResolved: (data) => {
    console.log("[ALERT RESOLVED]", data);

    firstFire = true;
  },
  onAck: (data) => {
    console.log("[ALERT ACK]", data);
  },
  onAckAll: (data) => {
    console.log("[ALERT ACK_ALL]", data);
  },
});

console.log('Listening for alert events on "high_temp"...');

// ── Acknowledge an alert ─────────────────────────────────────
// Simulate ack after 5 seconds
setTimeout(async () => {
  const acked = await app.alert.ackAll({
    alert_id: thresholdAlert.id,
    acked_by: "operator_jane",
    ack_notes: "Investigating device cooling system",
  });
  console.log("Ack result:", acked);
}, 5000);

// // ── List all alerts ──────────────────────────────────────────

const allAlerts = await app.alert.list();
console.log(`\nTotal alerts: ${allAlerts.length}`);
allAlerts.forEach((a) => console.log(`  - ${a.name} (${a.type})`));

// // ── Get a specific alert ─────────────────────────────────────

const fetched = await app.alert.get("high_temp");
console.log(`\nFetched alert: ${fetched.name}, type: ${fetched.type}`);

// ── Update an alert ──────────────────────────────────────────

const updated = await app.alert.update({
  id: thresholdAlert.id,
  description: "Updated: fires when temperature exceeds 85°C",
  config: {
    ...thresholdAlert.config,
    duration: 10,
  },
});
console.log(
  `\nUpdated alert: ${updated.name}, new duration: ${updated.config?.duration}`,
);

// ── Delete an alert ──────────────────────────────────────────
// Uncomment to test delete (will stop the listener above)
// const deleted = await app.alert.delete(thresholdAlert.id);
// console.log(`\nDeleted alert: ${deleted}`);

console.log("\nListening for events... (Ctrl+C to stop)");
