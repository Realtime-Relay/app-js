# Ephemeral Alerting Guide

Ephemeral alerts let you define custom alert rules with your own evaluation logic. Unlike standard alerts (THRESHOLD, RATE_CHANGE) which are evaluated server-side, ephemeral alerts run your evaluator function client-side, giving you full control over when alerts fire and resolve.

## Architecture: Owner & Listener

Ephemeral alerts use an **owner/listener** model:

- **Owner**: Subscribes to raw data (telemetry, events, or commands), runs your evaluator function, and publishes alert state changes (fire, resolved). Only one owner can run per alert rule at a time.
- **Listener**: Subscribes to alert state changes published by the owner. Multiple listeners can run simultaneously. Useful for dashboards or notification services that need to react to alerts without running the evaluation logic.

## Creating an Ephemeral Alert

```js
const alert = await app.alert.createEphemeral({
  name: "high-temperature",
  description: "Fires when temperature exceeds 85°C",
  config: {
    topic: {
      source: "TELEMETRY",
      device_ident: "sensor-1",
      last_token: "temperature",
    },
    duration: 5,
    recovery_duration: 10,
    recovery_eval_type: "VALUE",
  },
  notification_channel: ["ops-webhook"],
});
```

### Config Reference

| Field                | Type   | Description                                                              |
| -------------------- | ------ | ------------------------------------------------------------------------ |
| `topic.source`       | string | Data source: `'TELEMETRY'`, `'EVENT'`, or `'COMMAND'`                    |
| `topic.device_ident` | string | Device identifier to monitor                                             |
| `topic.last_token`   | string | Metric name (telemetry), event name (events), or command name (commands) |
| `duration`           | number | Seconds the breach condition must hold before the alert fires            |
| `recovery_duration`  | number | Seconds the clear condition must hold before the alert resolves          |
| `cooldown`           | number | Minimum seconds between consecutive fires (optional)                     |
| `recovery_eval_type` | string | `'VALUE'` (default) or `'TIMER'` — see [Recovery Modes](#recovery-modes) |

### Updating

```js
const updated = await app.alert.updateEphemeral({
  id: alert.id,
  config: { duration: 10, recovery_duration: 20 },
});
```

## Owner Mode

Set an evaluator function before calling `listen()` to run in owner mode. The evaluator receives the rolling state (accumulated data from the data source) and returns `true` (breached) or `false` (clear).

```js
const alert = await app.alert.get("high-temperature");

// Define your evaluator
alert.setEvaluator((rollingState) => {
  const temp = rollingState["sensor-1"]?.temperature?.value ?? 0;
  return temp > 85;
});

await alert.listen({
  on_fire: (data) => console.log("ALERT FIRED:", data),
  on_resolved: (data) => console.log("ALERT RESOLVED:", data),
  on_ack: (data) => console.log("ACKNOWLEDGED:", data),
  on_error: (err) => console.log("ERROR:", err),
});
```

The evaluator is called every time new data arrives. The rolling state is an object keyed by device identifier:

```js
// Rolling state structure
{
  'sensor-1': {
    temperature: {
      value: 87.5,
      timestamp: 1711382400000,
    }
  }
}
```

### Stopping

```js
await alert.stop();
```

## Listener Mode

Call `listen()` without setting an evaluator to run in listener mode. You receive fire/resolved events published by the owner running elsewhere.

```js
const alert = await app.alert.get("high-temperature");

await alert.listen({
  on_fire: (data) => console.log("FIRED:", data),
  on_resolved: (data) => console.log("RESOLVED:", data),
  on_ack: (data) => console.log("ACK:", data),
  on_ack_all: (data) => console.log("ACK ALL:", data),
});
```

## Recovery Modes

The `recovery_eval_type` config controls how alerts recover (transition from fired back to normal).

### VALUE (default)

Recovery happens when the evaluator returns `false` for `recovery_duration` seconds. This requires data to keep flowing — if data stops arriving, the evaluator never runs and the alert stays in the fired state.

**Best for**: Telemetry monitoring where sensors publish continuously. Example: temperature drops back below threshold.

```js
config: {
  recovery_eval_type: 'VALUE',
  recovery_duration: 10, // evaluator must return false for 10s
}
```

### TIMER

Recovery happens automatically after `recovery_duration` seconds of silence (no new messages on the data topic). If data does arrive and the evaluator returns `false`, that also triggers recovery.

**Best for**: Event-based alerts where silence means "the problem stopped." Example: door opened events stop occurring.

```js
config: {
  recovery_eval_type: 'TIMER',
  recovery_duration: 30, // no events for 30s = resolved
}
```

## Callbacks

All callbacks are optional and support both sync and async functions.

| Callback      | Triggered When                             | Payload                                           |
| ------------- | ------------------------------------------ | ------------------------------------------------- |
| `on_fire`     | Alert transitions to fired state           | Alert payload with rule, device, value, timestamp |
| `on_resolved` | Alert transitions back to normal           | Alert payload with rule, device, value, timestamp |
| `on_ack`      | A specific device instance is acknowledged | Ack payload with acked_by, notes                  |
| `on_ack_all`  | All instances are acknowledged             | Ack payload with acked_by, notes                  |
| `on_error`    | An error occurs during evaluation          | Error object                                      |

```js
await alert.listen({
  on_fire: (data) => {
    console.log(`Alert ${data.alert.name} fired!`);
    console.log(`Value: ${JSON.stringify(data.last_value)}`);
  },
});
```

## Acknowledge & Mute

### Acknowledge

Acknowledge transitions the alert from `alerting` to `acknowledged`. The alert stays acknowledged until it resolves naturally.

```js
// Acknowledge for a specific device
await alert.ack("operator-1", "Looking into it");

// Acknowledge all instances
await alert.ackAll("operator-1", "Team notified");
```

### Mute / Unmute

Muting suppresses notification dispatch but the alert continues to evaluate and fire.

```js
// Mute forever
await app.alert.mute({
  id: alert.id,
  mute_config: { type: "FOREVER" },
});

// Mute for a duration
await app.alert.mute({
  id: alert.id,
  mute_config: {
    type: "TIME_BASED",
    mute_till: "2026-03-26T00:00:00.000Z",
  },
});

// Unmute
await app.alert.unmute(alert.id);
```

## Data Sources

### TELEMETRY

Monitor continuous numeric sensor data.

```js
topic: {
  source: 'TELEMETRY',
  device_ident: 'sensor-1',
  last_token: 'temperature', // metric name from device schema
}
```

### EVENT

Monitor discrete events published by devices.

```js
topic: {
  source: 'EVENT',
  device_ident: 'sensor-1',
  last_token: 'door_opened', // event name
}
```

### COMMAND

Monitor commands sent to devices.

```js
topic: {
  source: 'COMMAND',
  device_ident: 'sensor-1',
  last_token: 'firmware_update', // command name
}
```

## Example: Temperature Threshold

An owner monitors temperature and fires when it exceeds 85°C for 5 seconds. Resolves when it stays below 85°C for 10 seconds.

```js
import { RelayApp } from "relayx-app-js";

const app = new RelayApp({
  api_key: "<YOUR_API_KEY>",
  secret: "<YOUR_SECRET>",
  mode: "test",
});

app.connection.listeners((event) => console.log(`[connection] ${event}`));
await app.connect();

// Fetch or create the ephemeral alert
let alert = await app.alert.get("high-temp-alert");

if (!alert) {
  alert = await app.alert.createEphemeral({
    name: "high-temp-alert",
    config: {
      topic: {
        source: "TELEMETRY",
        device_ident: "sensor-1",
        last_token: "temperature",
      },
      duration: 5,
      recovery_duration: 10,
      recovery_eval_type: "VALUE",
    },
  });
}

// Set evaluator — fires when temp > 85
alert.setEvaluator(
  (state) => (state["sensor-1"]?.temperature?.value ?? 0) > 85,
);

// Start
await alert.listen({
  on_fire: (data) =>
    console.log(`FIRE: temp=${JSON.stringify(data.last_value)}`),
  on_resolved: () => console.log("RESOLVED"),
});

console.log("Monitoring... Press Ctrl+C to stop.");

process.on("SIGINT", async () => {
  await alert.stop();
  await app.disconnect();
});
```

## Example: Event Counting (with TIMER Recovery)

An owner monitors door_opened events and fires when more than 3 occur. Uses TIMER recovery — resolves after 30 seconds of no events.

```js
import { RelayApp } from "relayx-app-js";

const app = new RelayApp({
  api_key: "<YOUR_API_KEY>",
  secret: "<YOUR_SECRET>",
  mode: "test",
});

app.connection.listeners((event) => console.log(`[connection] ${event}`));
await app.connect();

let alert = await app.alert.get("door-alert");

if (!alert) {
  alert = await app.alert.createEphemeral({
    name: "door-alert",
    config: {
      topic: {
        source: "EVENT",
        device_ident: "sensor-1",
        last_token: "door_opened",
      },
      duration: 0,
      recovery_duration: 30,
      recovery_eval_type: "TIMER",
    },
  });
}

let eventCount = 0;

alert.setEvaluator(() => {
  eventCount++;
  return eventCount > 3;
});

await alert.listen({
  on_fire: () => console.log(`ALERT: ${eventCount} door events!`),
  on_resolved: () => console.log("RESOLVED: no events for 30s"),
});

console.log("Monitoring door events... Press Ctrl+C to stop.");

process.on("SIGINT", async () => {
  await alert.stop();
  await app.disconnect();
});
```
