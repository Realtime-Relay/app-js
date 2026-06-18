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

await app.log.stream({
  device_ident: "sensor-1",
  callback: (entry) => console.log(`[${entry.level}] ${entry.data}`),
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

Returns each requested field as an array of `{value, timestamp}` points.

```js
const history = await app.telemetry.history({
  device_ident: "sensor-1",
  fields: ["temperature", "humidity"],
  start: "2026-03-01T00:00:00.000Z",
  end: "2026-03-25T00:00:00.000Z",
});
```

#### Aggregation

Bucket by time with `interval` + `aggregate_fn`. Both must be supplied
together. `interval` is a Flux duration (`"30s"`, `"5m"`, `"1h"`, `"1d"`).
`aggregate_fn` is one of:

| function | meaning |
|---|---|
| `mean` | arithmetic mean per bucket |
| `min` / `max` | extrema per bucket |
| `sum` | total per bucket |
| `count` | number of points per bucket |
| `first` / `last` | first or last point per bucket |
| `median` | median per bucket |
| `stddev` | standard deviation per bucket |

```js
// Hourly average temperature for the past day
const hourlyAvg = await app.telemetry.history({
  device_ident: "sensor-1",
  fields: ["temperature"],
  start: "2026-04-28T00:00:00.000Z",
  end: "2026-04-29T00:00:00.000Z",
  interval: "1h",
  aggregate_fn: "mean",
});

// Daily peak humidity for the past month
const dailyMax = await app.telemetry.history({
  device_ident: "sensor-1",
  fields: ["humidity"],
  start, end,
  interval: "1d",
  aggregate_fn: "max",
});
```

Numeric aggregates (`mean`, `min`, `max`, `sum`, `median`, `stddev`)
require numeric metric values; non-numeric points are ignored.
`count`, `first`, and `last` work on any value type.

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

Subscribe to device-published events. `device_ident` accepts:

- `"*"` — all devices in your org
- `[ident]` — a single device
- `[a, b, …]` — a specific list of devices

The callback receives `{ <device_ident>: <event_data> }` so you always
know which device fired the event.

```js
// One device
await app.events.stream({
  name: "door_opened",
  device_ident: ["entry-sensor"],
  callback: (payload) => {
    for (const [ident, data] of Object.entries(payload)) {
      console.log(`${ident} fired:`, data);
    }
  },
});

// All devices
await app.events.stream({
  name: "boot",
  device_ident: "*",
  callback: (payload) => console.log(payload),
});

await app.events.off({ name: "door_opened" });
```

### History

```js
const events = await app.events.history({
  device_ident: "sensor-1",
  event_names: ["door_opened", "boot"],
  start: "2026-03-01T00:00:00.000Z",
  end: "2026-03-25T00:00:00.000Z",
});
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
});
```

Each fire / resolved / ack event carries an `incident_id` that's stable
across the lifetime of an alerting episode — minted when the alert
goes from `normal → alerting`, persisted across cooldown re-fires and
acks, and cleared only on resolution. Use it to group related events.

### History

History fires through the same streaming protocol as telemetry/events
and supports filtering by alert state (`fire`, `resolved`, `ack`) and
optionally by `incident_id`.

```js
const history = await app.alert.history({
  rule_type: "RULE", // 'RULE' | 'DEVICE'
  rule_id: alert.id,
  rule_states: ["fire", "resolved", "ack"],
  start: "2026-03-01T00:00:00.000Z",
  end: "2026-03-25T00:00:00.000Z",
});

// Walk a single incident end-to-end
const incident = await app.alert.history({
  rule_type: "DEVICE",
  device_ident: "sensor-1",
  incident_id: "<incident_uuid>",
  start,
  end,
});
```

### Acknowledge

`device_id` is required — it identifies which device's incident gets
acknowledged. After ack, cooldown re-fires for the same incident are
recorded for audit but do not dispatch notifications until the alert
resolves and a new incident begins.

```js
await app.alert.ack({
  device_id: "<device_id>",
  alert_id: alert.id,
  acked_by: "operator-1",
  ack_notes: "Investigating",
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

## Logs

Subscribe to live device logs and query history. Each log entry carries
a `level` (`info` | `warn` | `error`), a `data` payload, and a
device-side `timestamp`.

### Live Streaming

```js
// All levels
await app.log.stream({
  device_ident: "sensor-1",
  callback: (entry) => console.log(`[${entry.level}] ${entry.data}`),
});

// Errors only
await app.log.stream({
  device_ident: "sensor-1",
  levels: ["error"],
  callback: (entry) => console.error(entry.data),
});

await app.log.off({ device_ident: "sensor-1" });
```

### History

Returns logs grouped by level. Optionally bucket with `interval` +
`aggregate_fn: "count"` for per-level counts over time.

```js
const logs = await app.log.history({
  device_ident: "sensor-1",
  start: "2026-04-28T00:00:00.000Z",
  end: "2026-04-29T00:00:00.000Z",
});
// { info: [...], warn: [...], error: [...] }

// Hourly error counts
const hourly = await app.log.history({
  device_ident: "sensor-1",
  levels: ["error"],
  start,
  end,
  interval: "1h",
  aggregate_fn: "count",
});
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

## OTA (Firmware Updates)

Manage over-the-air firmware from the backend: upload binaries, create rollouts
that target a fleet, and track each device's job as it downloads and installs.
The matching device-side flow lives in the RelayX Device SDK.

### Firmware

Call `init()` once after `connect()`; call it again to refresh. `firmwareList()`
works without it.

```js
await app.ota.init();

const file = readFileSync("./firmware-1.0.4.bin"); // Buffer; Blob/File also work
const fw = await app.ota.firmwareUpload({
  name: "Boiler controller fix", // display name shown in UI pickers
  version: "1.0.4",              // unique per org — a duplicate throws VERSION_EXISTS
  file,
  file_name: "firmware-1.0.4.bin",
});
// → { firmware_id, version, sha256, size }

// Paginated, newest first
const { firmwares, page } = await app.ota.firmwareList({ page: 1 });
// page → { count, has_more }

await app.ota.firmwareDelete({ id: fw.firmware_id });
```

### Rollouts

A rollout is the unit of deployment. It starts as a `DRAFT` (pure intent — no
jobs exist yet) and can be edited freely. Activating it (`toggleRollout` →
`ACTIVE`) is **the** snapshot moment: the target is resolved against the fleet as
it exists then, one job per device is created, and devices are nudged. Anything
past `DRAFT` is immutable — adjust by stopping and creating a new rollout.

```js
const rollout = await app.ota.createRollout({
  firmware_id: fw.firmware_id,
  request_type: "DOWNLOAD_INSTALL", // or "DOWNLOAD_ONLY" to pre-stage (install later)
  target: { type: "all" },          // see Targeting below
  force_install: false,
  user_config: { apply: "app_gated" },
});
// → { rollout_id, status: "DRAFT", device_count }
//   device_count here is a PREVIEW — the real snapshot happens at activation
```

Edit a rollout while it is still `DRAFT`:

```js
await app.ota.updateRollout({
  rollout_id: rollout.rollout_id,
  request_type: "DOWNLOAD_ONLY",
});
```

Activate it — this creates the per-device jobs and nudges the fleet. The
returned `device_count` is now the real snapshot, not a preview:

```js
await app.ota.toggleRollout({
  rollout_id: rollout.rollout_id,
  state: "ACTIVE",
});
```

A live rollout moves between three states: `ACTIVE`, `PAUSED`, and `STOPPED`.
`PAUSED` freezes pending jobs and holds the device's queue head — rollouts are
processed FIFO per device, so the ones behind it wait too:

```js
// Pause, then resume against the same snapshot (no re-resolution)
await app.ota.toggleRollout({
  rollout_id: rollout.rollout_id,
  state: "PAUSED",
});

await app.ota.toggleRollout({
  rollout_id: rollout.rollout_id,
  state: "ACTIVE",
});
```

`STOPPED` is terminal — the rollout is never served again, though its jobs are
kept as history:

```js
await app.ota.toggleRollout({
  rollout_id: rollout.rollout_id,
  state: "STOPPED",
});
```

Delete (DRAFT rollouts only) and list (paginated, newest first):

```js
await app.ota.deleteRollout({ rollout_id: rollout.rollout_id });

const { rollouts, page } = await app.ota.rolloutList({ page: 1 });
// page → { count, has_more }
```

### Targeting

`target` is required on `createRollout` — there is no implicit fleet-wide
rollout, a whole-org deploy must say so explicitly. `target.type` is one of
`"devices"`, `"logical_group"`, `"hierarchy_group"`, or `"all"`, and each type
takes its own fields:

```js
// 1. Specific devices — device_ids is a non-empty array of device ids
target: {
  type: "devices",
  device_ids: ["dev_abc", "dev_def"],
}

// 2. A logical (tag-based) group — group_id is required
target: {
  type: "logical_group",
  group_id: "grp_sensors_floor1",
}

// 3. A hierarchy (path-based) group — group_id is required
target: {
  type: "hierarchy_group",
  group_id: "grp_building_a",
}

// 4. The whole org
target: {
  type: "all",
}
```

Any target type also accepts an optional `exclude` — an array of device ids to
carve out of the resolved set. This is most useful with `all` or a group, e.g.
"every device except the two on the bench":

```js
target: {
  type: "all",
  exclude: ["dev_bench_1", "dev_bench_2"],
}
```

Targets are resolved twice: once as a preview when you create the DRAFT
(`device_count`), and authoritatively at activation against the fleet as it
exists then. A device added to a matching group after activation is not pulled
into an already-active rollout.

### Retry & install-later

Re-arm terminal jobs (`FAILED`, `ROLLED_BACK`, `VETOED` → `PENDING`). A re-armed
job keeps its original FIFO position. Omit `phases` to retry all three:

```js
const { retried } = await app.ota.retryRollout({
  rollout_id: rollout.rollout_id,
  phases: ["FAILED", "VETOED"],
});
```

For a `DOWNLOAD_ONLY` rollout: once devices have staged the image, nudge them to
install now (INSTALL_ONLY — no re-download):

```js
const { installing } = await app.ota.installRollout({
  rollout_id: rollout.rollout_id,
});
```

### Jobs & live phase updates

A job is one device's copy of a rollout. List jobs, inspect a single device's
history, or subscribe to every device's phase transition live instead of
polling:

```js
const { jobs, page } = await app.ota.jobsList({
  rollout_id: rollout.rollout_id,
  page: 1,
});

const { history } = await app.ota.jobHistory({
  rollout_id: rollout.rollout_id,
  device_id: "<id>",
});
```

Subscribe to live phase transitions for the whole org. Only one live
subscription is active at a time:

```js
const stop = app.ota.onJobPhaseUpdate((u) => {
  const err = u.error ? ` (err: ${u.error})` : "";
  console.log(`device=${u.device_id} -> ${u.phase}${err}`);
  // phases: PENDING → DOWNLOADING → DOWNLOADED → INSTALLING → INSTALLED
  //         (terminal failures: FAILED / ROLLED_BACK / VETOED)
});

// Stop it with either the returned unsubscribe...
stop();

// ...or the manager method
app.ota.offJobPhaseUpdate();
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
