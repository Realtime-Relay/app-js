# RelayX App SDK for JavaScript

Official JavaScript SDK for building applications on the RelayX platform.

> **[View Full Documentation →](https://docs.relay-x.io/app-sdk/overview)**

## Installation

```bash
npm install @relay-x/app-sdk
```

## Quick Start

```js
import { RelayApp } from "@relay-x/app-sdk";

const app = new RelayApp({
  api_key: "<YOUR_API_KEY>",
  secret: "<YOUR_SECRET>",
  mode: "production",
});

app.connection.listeners((event) => console.log(`[connection] ${event}`));
await app.connect();

await app.telemetry.stream({
  device_ident: "sensor-1",
  metric: "temperature",
  callback: (data) => console.log(`temp: ${JSON.stringify(data)}`),
});

// ... your application logic ...

await app.disconnect();
```

## Configuration

```js
const app = new RelayApp({
  api_key: "<YOUR_API_KEY>", // JWT credential from RelayX console
  secret: "<YOUR_SECRET>", // Secret key
  mode: "production", // 'production' | 'test'
  debug: false, // Enable debug logging (default: false)
});
```

Get your credentials at [console.relay-x.io](https://console.relay-x.io).

<!-- TODO: Link to sign-up tutorial -->

## Connection

```js
await app.connect();
await app.disconnect();

// Connection lifecycle events
app.connection.listeners((event) => console.log(event));
// Events: 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'auth_failed'
```

### Presence

Subscribe to device connect/disconnect events.

```js
await app.connection.presence((data) => {
  console.log(`${data.device_ident} ${data.event}`);
  // data.event: 'connected' | 'disconnected'
});

// Unsubscribe
await app.connection.presenceOff();
```

## Devices

```js
// List all devices
const devices = await app.device.list();

// Get a single device
const device = await app.device.get({ ident: "sensor-1" });

// Create a device
const newDevice = await app.device.create({
  ident: "sensor-1",
  schema: {
    temperature: { type: "number", unit: "Celsius", unit_symbol: "°C" },
    humidity: { type: "number", unit: "Percentage", unit_symbol: "%" },
  },
  config: {},
});

// Update a device
const updated = await app.device.update({
  id: device.id,
  config: { interval: 5000 },
});

// Delete a device
await app.device.delete("sensor-1");
```

## Telemetry

### Live Streaming

Stream real-time telemetry from a device. The `metric` is validated against the device schema.

```js
// Stream a specific metric
await app.telemetry.stream({
  device_ident: "sensor-1",
  metric: "temperature",
  callback: (data) =>
    console.log(`[${data.metric}] ${JSON.stringify(data.data)}`),
});

// Stream all metrics
await app.telemetry.stream({
  device_ident: "sensor-1",
  metric: "*",
  callback: (data) => console.log(data),
});

// Unsubscribe from specific metrics
await app.telemetry.off({ device_ident: "sensor-1", metric: ["temperature"] });

// Unsubscribe from all metrics for a device
await app.telemetry.off({ device_ident: "sensor-1" });
```

### History

```js
const history = await app.telemetry.history({
  device_ident: "sensor-1",
  fields: ["temperature", "humidity"],
  start: "2026-03-01T00:00:00.000Z",
  end: "2026-03-25T00:00:00.000Z",
});
```

### Latest

Fetches the most recent telemetry values (last 24 hours).

```js
const latest = await app.telemetry.latest({
  device_ident: "sensor-1",
  fields: ["temperature", "humidity"],
});
```

## Commands

Send one-way commands to devices.

```js
// Send to one or more devices
const result = await app.command.send({
  name: "set_interval",
  device_ident: ["sensor-1", "sensor-2"],
  data: { interval: 5000 },
});
// result: { 'sensor-1': { sent: true }, 'sensor-2': { sent: true } }

// Command history
const history = await app.command.history({
  name: "set_interval",
  device_idents: ["sensor-1"],
  start: "2026-03-01T00:00:00.000Z",
  end: "2026-03-25T00:00:00.000Z",
});
```

## RPC

Make request/reply calls to devices.

```js
const response = await app.rpc.call({
  device_ident: "sensor-1",
  name: "get_status",
  data: { verbose: true },
  timeout: 10, // seconds (default: 10)
});
```

## Events

Subscribe to device-published events.

```js
await app.events.stream({
  name: "door_opened",
  callback: (data) => console.log(`Event: ${JSON.stringify(data)}`),
});

await app.events.off({ name: "door_opened" });
```

## Alerts

### CRUD

```js
// Create a threshold alert
const alert = await app.alert.create({
  name: "high-temp",
  type: "THRESHOLD", // 'THRESHOLD' | 'RATE_CHANGE'
  metric: "temperature",
  config: { threshold: 85, duration: 5 },
  notification_channel: ["ops-webhook"],
});

// Get, update, delete
const fetched = await app.alert.get("high-temp");
const updated = await app.alert.update({
  id: fetched.id,
  config: { threshold: 90 },
});
await app.alert.delete(fetched.id);

// List all alerts
const alerts = await app.alert.list();
```

### Listening

```js
const alert = await app.alert.get("high-temp");

await alert.listen({
  on_fire: (data) => console.log("FIRED:", data),
  on_resolved: (data) => console.log("RESOLVED:", data),
  on_ack: (data) => console.log("ACK:", data),
  on_ack_all: (data) => console.log("ACK ALL:", data),
});
```

### History

```js
const history = await app.alert.history({
  rule_type: "RULE", // 'RULE' | 'DEVICE'
  rule_id: alert.id,
  rule_states: ["fire", "resolved"],
  start: "2026-03-01T00:00:00.000Z",
  end: "2026-03-25T00:00:00.000Z",
});
```

### Acknowledge

```js
// Acknowledge for a specific device
await app.alert.ack({
  device_id: "<device_id>",
  alert_id: alert.id,
  acked_by: "operator-1",
  ack_notes: "Investigating",
});

// Acknowledge all instances
await app.alert.ackAll({
  alert_id: alert.id,
  acked_by: "operator-1",
});
```

### Mute / Unmute

```js
await app.alert.mute({
  id: alert.id,
  mute_config: { type: "FOREVER" },
  // or { type: 'TIME_BASED', mute_till: '2026-04-01T00:00:00.000Z' }
});

await app.alert.unmute(alert.id);
```

## Ephemeral Alerts

Ephemeral alerts let you define custom alert rules that are evaluated client-side with your own logic. See the full guide: [Ephemeral Alerting Guide](docs/ephemeral-alerting.md).

```js
// Create an ephemeral alert
const alert = await app.alert.createEphemeral({
  name: "custom-temp-alert",
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

// Set your evaluator
alert.setEvaluator(
  (state) => (state["sensor-1"]?.temperature?.value ?? 0) > 85,
);

// Start monitoring
await alert.listen({
  on_fire: (data) => console.log("ALERT:", data),
  on_resolved: (data) => console.log("RESOLVED:", data),
});

// Stop
await alert.stop();
```

## Logical Groups

Group devices by tags for batch operations and streaming.

```js
// Create
const group = await app.logicalGroup.create({
  name: "floor-1-sensors",
  tags: ["floor_1", "temperature"],
  device_idents: ["sensor-1", "sensor-2"],
});

// Update membership
const updated = await app.logicalGroup.update({
  id: group.id,
  devices: { add: ["sensor-3"], remove: ["sensor-1"] },
  tags: { add: ["humidity"], remove: ["floor_1"] },
});

// List, get, delete
const groups = await app.logicalGroup.list();
const fetched = await app.logicalGroup.get(group.id);
const devices = await app.logicalGroup.listDevices(group.id);
await app.logicalGroup.delete(group.id);
```

### Group Streaming

Each group instance has `stream()` and `off()` methods.

```js
const group = await app.logicalGroup.get("<group_id>");

await group.stream({
  callback: (data) => console.log(data),
});

await group.off();
```

## Hierarchy Groups

Organize devices in a hierarchy path (e.g., `building_1.floor_2.zone_a`).

```js
// Create
const group = await app.heirarchyGroup.create({
  name: "zone-a-sensors",
  heirarchy: "building_1.floor_2.zone_a",
  device_idents: ["sensor-1", "sensor-2"],
});

// Update
const updated = await app.heirarchyGroup.update({
  id: group.id,
  devices: { add: ["sensor-3"], remove: [] },
  heirarchy: "building_1.floor_3.zone_a",
});

// List, get, delete
const groups = await app.heirarchyGroup.list();
const fetched = await app.heirarchyGroup.get(group.id);
const devices = await app.heirarchyGroup.listDevices(group.id);
await app.heirarchyGroup.delete(group.id);
```

### Hierarchy Group Streaming

Supports metric and hierarchy path filtering with wildcards.

```js
const group = await app.heirarchyGroup.get("<group_id>");

// Stream all data
await group.stream({ callback: (data) => console.log(data) });

// Filter by metric
await group.stream({
  metric: "temperature",
  callback: (data) => console.log(data),
});

// Filter by hierarchy path (supports * and > wildcards)
await group.stream({
  heirarchy: "building_1.*.zone_a",
  callback: (data) => console.log(data),
});

await group.off();
```

## Notifications

Create webhook or email notification channels for alerts.

```js
// Webhook
const notif = await app.notification.create({
  name: "ops-webhook",
  type: "WEBHOOK",
  config: { endpoint: "https://hooks.example.com/alerts" },
});

// Email
const emailNotif = await app.notification.create({
  name: "ops-email",
  type: "EMAIL",
  config: {
    recipients: ["ops@example.com"],
    subject: "Alert Notification",
    template: "Alert {{alert_name}} fired on {{device_ident}}",
  },
});

// Update, delete, list, get
const updated = await app.notification.update({
  name: "ops-webhook",
  type: "WEBHOOK",
  config: { endpoint: "https://new-url.com" },
});
await app.notification.delete(notif.id);
const all = await app.notification.list();
const fetched = await app.notification.get("<notif_id>");
```

## Offline Behavior

- **Commands**: Buffered in memory while disconnected and flushed automatically on reconnect.
- **Subscriptions**: All active telemetry, event, presence, alert, and group stream subscriptions are automatically restored on reconnect.
- **Ephemeral Alerts**: Alert state events (fire, resolved, ack) are buffered and published on reconnect.

## Error Handling

The SDK throws standard `Error` objects with descriptive messages.

```js
try {
  await app.telemetry.stream({
    device_ident: "sensor-1",
    metric: "nonexistent",
    callback: () => {},
  });
} catch (err) {
  console.log(`Validation error: ${err.message}`);
}
```

Common error scenarios:

- Invalid arguments or missing required fields
- Operations attempted while disconnected
- Schema validation failures (metric not in device schema)

## License

Apache-2.0
