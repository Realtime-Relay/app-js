# Ephemeral Alerting Engine

Client-side alert engine for the RelayX AppSDK. Evaluates breach conditions locally against streaming IoT data (telemetry, commands, events) and drives a state machine that fires, resolves, acknowledges, and mutes alerts — all over NATS.

---

## Table of Contents

1. [Architecture](#architecture)
2. [File Structure](#file-structure)
3. [Dual-Mode Operation](#dual-mode-operation)
4. [Data Flow](#data-flow)
5. [NATS Topics Deep Dive](#nats-topics-deep-dive)
6. [Rolling State](#rolling-state)
7. [State Machine](#state-machine)
8. [Duration, Recovery & Cooldown](#duration-recovery--cooldown)
9. [KV Lock (Single-Owner Enforcement)](#kv-lock-single-owner-enforcement)
10. [RPC Handlers](#rpc-handlers)
11. [Mute Behavior](#mute-behavior)
12. [Callbacks](#callbacks)
13. [Payload Shapes](#payload-shapes)
14. [Error Handling](#error-handling)
15. [Public API](#public-api)
16. [Examples](#examples)

---

## Architecture

```
                         EphemeralEngine (index.js)
                        ┌──────────────────────────┐
                        │  setEvaluator(fn)         │
                        │  listen(callbacks)         │
                        │  stop()                    │
                        │  ack() / ackAll()          │
                        └────────┬─────────┬────────┘
                  evaluator set? │         │ no evaluator?
                                 ▼         ▼
                   ┌──────────────┐   ┌──────────────┐
                   │ EphemeralOwner│   │EphemeralListener│
                   │  (owner.js)  │   │ (listener.js)│
                   └──────┬───────┘   └──────┬───────┘
                          │                  │
              ┌───────────┼───────┐          │
              ▼           ▼       ▼          ▼
         Data Topic    RPCs    KV Lock   Alert Events
         (JetStream)  (sub)   (bucket)   (JetStream)

              shared.js — constants, subject builders, helpers
```

The engine is split into four files:

| File          | Responsibility                                                                             |
| ------------- | ------------------------------------------------------------------------------------------ |
| `index.js`    | Orchestrator. Routes to owner or listener based on whether an evaluator is set.            |
| `owner.js`    | Subscribes to data, runs evaluator, drives state machine, handles RPCs, manages KV lock.   |
| `listener.js` | Subscribes to alert event stream, routes fire/resolved/ack/ack_all to callbacks.           |
| `shared.js`   | Subject maps, index constants, codec, payload builders, mute check, notification dispatch. |

---

## File Structure

```
src/ephemeral_alerting/
  index.js      — EphemeralEngine class (public entry point)
  owner.js      — EphemeralOwner class
  listener.js   — EphemeralListener class
  shared.js     — Shared constants and utilities
  README.md     — This file
```

---

## Dual-Mode Operation

A single alert rule can be consumed by many SDK instances. Exactly one runs as the **owner** (the evaluator); all others are **listeners**.

### Owner Mode

Activated when `setEvaluator(fn)` is called before `listen()`.

1. Acquires a KV lock (`ephemeral_owner_{rule_id}`) — throws if another owner is active.
2. Starts a 15-second heartbeat to refresh the lock.
3. Creates a JetStream consumer on the data topic (telemetry/command/event).
4. Subscribes to RPC subjects for remote ack/ackAll/mute from listeners.
5. On each incoming message: decodes msgpack, updates rolling state, runs evaluator, advances state machine.
6. Publishes fire/resolved events to the alert listen subject for listeners to consume.

### Listener Mode

Activated when `listen()` is called without a prior `setEvaluator()`.

1. Creates a JetStream consumer on `{orgID}.{env}.alerts.listen.{rule_id}.*`.
2. Routes incoming events (`fire`, `resolved`, `ack`, `ack_all`) to the corresponding callback.
3. Converts numeric timestamps to ISO strings for fire/resolved events.
4. Does **not** maintain rolling state (getter returns `{}`).

---

## Data Flow

### Owner

```
Device  ──telemetry──▶  NATS JetStream
                              │
                     ┌────────▼────────┐
                     │  Data Consumer   │
                     │  (msgpack decode)│
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐
                     │ Update Rolling   │
                     │ State            │
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐
                     │ Run Evaluator    │
                     │ fn(rollingState) │
                     │ → true/false     │
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐
                     │ State Machine    │
                     │ (fire/resolve)   │
                     └────────┬────────┘
                              │
               ┌──────────────┼──────────────┐
               ▼              ▼              ▼
          Publish event   Notify backend   Call local
          (for listeners) (dispatch)       callback
```

### Listener

```
Owner publishes ──▶ {orgID}.{env}.alerts.listen.{rule_id}.fire
                    {orgID}.{env}.alerts.listen.{rule_id}.resolved
                    {orgID}.{env}.alerts.listen.{rule_id}.ack
                    {orgID}.{env}.alerts.listen.{rule_id}.ack_all
                              │
                     ┌────────▼────────┐
                     │ Alert Consumer   │
                     │ (msgpack decode) │
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐
                     │ Route to         │
                     │ callback by      │
                     │ last token       │
                     └─────────────────┘
```

---

## NATS Topics Deep Dive

The ephemeral alerting engine uses four categories of NATS topics. Understanding these is critical for debugging and for setting up the backend stream/account imports correctly.

### 1. Data Topics (Owner Mode — JetStream Consumer)

These are the raw IoT data streams the owner subscribes to. The `config.topic` object in the alert rule determines which subject is built.

| Source    | Subject Pattern                                     | Example                                    |
| --------- | --------------------------------------------------- | ------------------------------------------ |
| TELEMETRY | `{orgID}.{env}.telemetry.{device_id}.{metric}`      | `abc123.test.telemetry.dev456.temperature` |
| COMMAND   | `{orgID}.{env}.command.queue.{device_id}.{command}` | `abc123.test.command.queue.dev456.reboot`  |
| EVENT     | `{orgID}.{env}.events.{device_id}.{event}`          | `abc123.test.events.dev456.door_opened`    |

**Wildcards:**

- `device_ident: '*'` → subscribes to all devices (NATS `*` wildcard in the device position)
- `last_token: '*'` → subscribes to all metrics/commands/events for the device (NATS `*` wildcard in the last position)

Both can be combined: `device_ident: '*', last_token: '*'` subscribes to every metric from every device.

**Subject construction** is handled by `buildDataSubject()` in `shared.js`:

```js
// shared.js
const SUBJECT_MAP = {
  TELEMETRY: (orgID, env, deviceId, lastToken) =>
    `${orgID}.${env}.telemetry.${deviceId}.${lastToken}`,
  COMMAND: (orgID, env, deviceId, lastToken) =>
    `${orgID}.${env}.command.queue.${deviceId}.${lastToken}`,
  EVENT: (orgID, env, deviceId, lastToken) =>
    `${orgID}.${env}.events.${deviceId}.${lastToken}`,
};
```

When `device_ident` is not `'*'`, it is resolved to the backend device ID via `ctx.device.resolveDeviceId()`.

**Consumer configuration:**

```js
{
    name: `eph_alert_${crypto.randomUUID()}`,  // unique per instance
    filter_subjects: subject,                   // built subject from above
    replay_policy: ReplayPolicy.Instant,
    delivery_policy: DeliverPolicy.New,         // only new messages
    ack_policy: AckPolicy.Explicit,
    opt_start_time: new Date(),                 // from now
}
```

The consumer is created on the stream `{orgID}_stream`. This stream must have subjects matching the data topic pattern in its subject bindings — otherwise the consumer creation will TIMEOUT.

**Token index extraction:**

When a data message arrives, the engine extracts `device_id` and `last_token` (metric/command/event name) by position in the subject:

| Source    | Subject Tokens                         | device_id index | last_token index |
| --------- | -------------------------------------- | --------------- | ---------------- |
| TELEMETRY | `orgID.env.telemetry.deviceId.metric`  | 3               | 4                |
| COMMAND   | `orgID.env.command.queue.deviceId.cmd` | 4               | 5                |
| EVENT     | `orgID.env.events.deviceId.event`      | 3               | 4                |

These indices are defined in `SUBJECT_DEVICE_INDEX` and `SUBJECT_LAST_TOKEN_INDEX` in `shared.js`.

**Encoding:** All data topic messages are **msgpack** encoded. The engine decodes with `msgpackDecode(msg.data)`.

---

### 2. Alert Event Topics (Owner publishes, Listener subscribes — JetStream)

When the owner's state machine fires, resolves, or processes an ack, it publishes an event to a well-known subject. Listeners subscribe to these via a wildcard JetStream consumer.

**Subject pattern:**

```
{orgID}.{env}.alerts.listen.{rule_id}.{event_type}
```

**Event types:**

| Event Type | Published When                                      | Publisher                             |
| ---------- | --------------------------------------------------- | ------------------------------------- |
| `fire`     | Breach held >= duration (or re-fire after cooldown) | Owner state machine                   |
| `resolved` | Clear held >= recovery_duration                     | Owner state machine                   |
| `ack`      | Single-device ack received (via RPC or local call)  | Owner RPC handler / `ack()` method    |
| `ack_all`  | All-device ack received (via RPC or local call)     | Owner RPC handler / `ackAll()` method |

**Listener's consumer configuration:**

```js
{
    name: `appjs_ephemeral_listen_${ruleId}_${crypto.randomUUID()}`,
    filter_subjects: `${orgID}.${env}.alerts.listen.${ruleId}.*`,
    replay_policy: 'instant',
    delivery_policy: 'new',
    ack_policy: 'explicit',
    opt_start_time: new Date(),
}
```

The listener routes each message to the correct callback by extracting the last token from the subject and mapping it:

```js
const callbackMap = {
  fire: "onFire",
  resolved: "onResolved",
  ack: "onAck",
  ack_all: "onAckAll",
};
```

For `fire` and `resolved` events, the listener converts the numeric `timestamp` field to an ISO string before calling the callback.

**Encoding:** Alert event messages are **msgpack** encoded (published with `msgpackEncode`, decoded with `msgpackDecode`).

---

### 3. RPC Topics (Owner subscribes via `natsClient.subscribe`)

Listeners send ack/ackAll/mute commands to the owner via NATS request/reply. The owner subscribes to a wildcard subject and routes by last token.

**Subject pattern:**

```
{orgID}.{env}.alerts.custom.{rule_id}.*
```

**RPC routes:**

| Last Token | Purpose                           | Request Body                            | Response Body                                                     |
| ---------- | --------------------------------- | --------------------------------------- | ----------------------------------------------------------------- |
| `ack`      | Acknowledge for a specific device | `{ device_id, acked_by, ack_notes }`    | `{ status: 'ACK_SUCCESS' }` or `{ status: 'ACK_FAILED', reason }` |
| `ack_all`  | Acknowledge for all devices       | `{ acked_by, ack_notes }`               | `{ status: 'ACK_SUCCESS' }` or `{ status: 'ACK_FAILED', reason }` |
| `mute`     | Mute or unmute the alert          | `{ mute_config: { type, mute_till? } }` | `{ status: 'MUTE_SUCCESS' }`                                      |

**Important:** These are **not** JetStream consumers. They use plain `natsClient.subscribe()` for low-latency request/reply.

**Encoding:** RPC request bodies are **msgpack** encoded. RPC responses are **JSON** encoded (via `JSONCodec`).

---

### 4. Backend Endpoints (NATS request/reply)

These subjects target backend microservices via NATS request/reply (not JetStream).

| Purpose                | Subject                                   | Encoding | Timeout |
| ---------------------- | ----------------------------------------- | -------- | ------- |
| Create ephemeral alert | `api.iot.alerts.{orgID}.create_ephemeral` | JSON     | 10s     |
| Sync mute config       | `api.iot.alerts.{orgID}.mute`             | JSON     | 10s     |
| Dispatch notifications | `api.iot.notification.{orgID}.dispatch`   | JSON     | 10s     |

The mute sync and notification dispatch are fire-and-forget — failures are swallowed and do not block the alerting pipeline.

---

### Encoding Summary

| Context                        | Encoding | Encode                    | Decode                         |
| ------------------------------ | -------- | ------------------------- | ------------------------------ |
| Data topic messages            | msgpack  | (device/backend)          | `msgpackDecode(msg.data)`      |
| Alert event publishes          | msgpack  | `msgpackEncode(payload)`  | `msgpackDecode(msg.data)`      |
| RPC requests (ack/ackAll/mute) | msgpack  | `msgpackEncode(payload)`  | `msgpackDecode(msg.data)`      |
| RPC responses                  | JSON     | `JSONCodec().encode(obj)` | (requester decodes)            |
| Backend request/reply          | JSON     | `JSONCodec().encode(obj)` | `JSONCodec().decode(msg.data)` |

---

### Topic Flow Diagram

```
                          ┌─────────────────┐
                          │   IoT Devices    │
                          └────────┬────────┘
                                   │ publish telemetry/commands/events
                                   ▼
                  ┌────────────────────────────────┐
                  │        NATS JetStream           │
                  │  Stream: {orgID}_stream          │
                  └──┬──────────────────────────┬──┘
                     │                          │
          ┌──────────▼──────────┐    ┌──────────▼──────────┐
          │   Data Consumer     │    │   Alert Consumer     │
          │   (Owner only)      │    │   (Listener only)    │
          │                     │    │                      │
          │ {orgID}.{env}.      │    │ {orgID}.{env}.       │
          │  telemetry.*.*      │    │  alerts.listen.      │
          │  command.queue.*.*  │    │  {rule_id}.*         │
          │  events.*.*         │    │                      │
          └──────────┬──────────┘    └──────────┬──────────┘
                     │                          │
                     ▼                          ▼
              ┌──────────┐              ┌──────────────┐
              │  Owner    │──publishes──▶│  Listeners   │
              │          │  fire/       │  (N clients) │
              │          │  resolved/   │              │
              │          │  ack/ack_all │              │
              └────┬─────┘              └──────┬───────┘
                   │                           │
                   │◄──── RPC (ack/mute) ──────┘
                   │     {orgID}.{env}.alerts.custom.{rule_id}.*
                   │
                   ▼
          ┌────────────────┐
          │  Backend APIs   │
          │  (NATS req/rep) │
          │  - mute sync    │
          │  - notifications│
          └────────────────┘
```

---

## Rolling State

The owner accumulates a rolling state object from incoming data. It is keyed by **device ident** (resolved from device ID via cache) and **metric/command/event name** (extracted from the NATS subject's last token).

### Shape by Source Type

**TELEMETRY:**

```js
{
  "sensor-01": {
    "temperature": { value: 90.5, timestamp: 1711234567890 },
    "humidity":    { value: 75.2, timestamp: 1711234567891 }
  },
  "sensor-02": {
    "temperature": { value: 22.1, timestamp: 1711234567892 }
  }
}
```

**COMMAND:**

```js
{
  "sensor-01": {
    "reboot": { /* full command data object */ }
  }
}
```

**EVENT:**

```js
{
  "sensor-01": {
    "door_opened": { /* full event data object */ }
  }
}
```

### How It Works

Each data message arrives on a subject like:

```
{orgID}.{env}.telemetry.{device_id}.{metric_name}
```

The engine extracts `device_id` and `metric_name` using the positional indices from `SUBJECT_DEVICE_INDEX` and `SUBJECT_LAST_TOKEN_INDEX` (see [NATS Topics Deep Dive](#1-data-topics-owner-mode--jetstream-consumer) above).

The `device_id` is then reverse-mapped to its human-readable `device_ident` via the device cache. If not found, the raw ID is used as the key.

Using `last_token: "*"` in the alert config subscribes to all metrics for a device, causing the rolling state to accumulate every metric received.

---

## State Machine

### States

| State          | Meaning                                                        |
| -------------- | -------------------------------------------------------------- |
| `normal`       | No active breach. Baseline state.                              |
| `alerting`     | Breach held long enough to fire. May re-fire on cooldown.      |
| `acknowledged` | Operator acknowledged the alert. Stays until clear + recovery. |

### Transition Table

```
┌─────────────┬───────────────────────────────┬──────────────┬───────────────┬──────────────┐
│ Current     │ Condition                     │ Duration Met │ Next State    │ Action       │
├─────────────┼───────────────────────────────┼──────────────┼───────────────┼──────────────┤
│ normal      │ breach, held < duration       │ No           │ normal        │ track        │
│ normal      │ breach, held >= duration      │ Yes          │ alerting      │ FIRE         │
│ alerting    │ breach, cooldown elapsed      │ -            │ alerting      │ Re-FIRE      │
│ alerting    │ breach, cooldown not elapsed  │ -            │ alerting      │ (silent)     │
│ alerting    │ clear, held < recovery        │ -            │ alerting      │ track        │
│ alerting    │ clear, held >= recovery       │ -            │ normal        │ RESOLVED     │
│ alerting    │ ack received                  │ -            │ acknowledged  │ publish ack  │
│ acknowledged│ breach (any)                  │ -            │ acknowledged  │ (silent)     │
│ acknowledged│ clear, held < recovery        │ -            │ acknowledged  │ track        │
│ acknowledged│ clear, held >= recovery       │ -            │ normal        │ RESOLVED     │
└─────────────┴───────────────────────────────┴──────────────┴───────────────┴──────────────┘
```

### Visual

```
                    breach held
          ┌──────── >= duration ────────┐
          │                             ▼
       normal ◄──── clear held ──── alerting ◄──┐
                    >= recovery        │  │      │
                         │             │  │      │
                         │         ack │  │ re-fire
                         │             ▼  │ (cooldown)
                         └────────  acknowledged
                       clear held
                       >= recovery
```

### Internal State Object

```js
{
  status: 'normal',           // 'normal' | 'alerting' | 'acknowledged'
  last_evaluated_at: null,    // Unix ms — null until first evaluation
  breached_since: null,       // Unix ms — when current breach streak started
  clear_since: null,          // Unix ms — when current clear streak started
  last_fired: 0,              // Unix ms — last time a fire event was published
  acked_by: null,             // string — who acknowledged
  acked_at: null,             // Unix ms — when acknowledged
  ack_notes: null,            // string | null — ack notes
}
```

---

## Duration, Recovery & Cooldown

All three are specified in the alert config in **seconds** and converted to milliseconds internally.

| Parameter    | Config Key                 | Purpose                                                                                 |
| ------------ | -------------------------- | --------------------------------------------------------------------------------------- |
| **Duration** | `config.duration`          | How long (seconds) the evaluator must continuously return `true` before the first FIRE. |
| **Recovery** | `config.recovery_duration` | How long (seconds) the evaluator must continuously return `false` before RESOLVED.      |
| **Cooldown** | `config.cooldown`          | Minimum gap (seconds) between re-fires while still in `alerting` state.                 |

### Staleness Check

If the gap between the current evaluation and `last_evaluated_at` exceeds `duration`, both `breached_since` and `clear_since` are reset. This prevents a stale streak from carrying over after a long gap in data.

---

## KV Lock (Single-Owner Enforcement)

Only one owner can run per alert rule across all SDK instances. This is enforced via a NATS KV entry.

| Property      | Value                                                   |
| ------------- | ------------------------------------------------------- |
| **Bucket**    | `{orgID}` (shared org bucket)                           |
| **Key**       | `ephemeral_owner_{rule_id}`                             |
| **Value**     | `{ "started_at": <unix_ms>, "expires_at": <unix_ms> }`  |
| **TTL**       | 30 seconds (stored in `expires_at`, not bucket-level)   |
| **Heartbeat** | Every 15 seconds, refreshes `expires_at` to `now + 30s` |

### Acquisition Flow

```
1. kv.get(key)
   ├── exists & expires_at > now  →  return false (locked)
   ├── exists & expires_at <= now →  kv.delete(key), continue
   └── KEY_NOT_FOUND             →  continue

2. kv.create(key, value)
   ├── success                   →  return true (acquired)
   ├── KEY_EXISTS                →  return false (race lost)
   └── other error               →  return true (proceed without lock)
```

### Release (on stop)

1. Clear heartbeat interval.
2. Delete KV key.

If KV is unavailable (`ctx.kvBucket` is null), locking is skipped entirely and the engine proceeds.

---

## RPC Handlers

The owner subscribes to a single wildcard NATS subject for incoming RPCs from listeners:

```
{orgID}.{env}.alerts.custom.{rule_id}.*
```

Routing is by last token:

| Last Token | Handler           | Description                              |
| ---------- | ----------------- | ---------------------------------------- |
| `ack`      | `handleAckRPC`    | Acknowledge alert for a specific device. |
| `ack_all`  | `handleAckAllRPC` | Acknowledge alert for all devices.       |
| `mute`     | `handleMuteRPC`   | Mute or unmute the alert.                |

### RPC Flow (ack/ack_all)

1. Decode msgpack from `msg.data`.
2. If `state.status !== 'alerting'` → respond `{ status: 'ACK_FAILED', reason: 'not in alerting state' }`.
3. Update local state to `acknowledged`.
4. Publish ack event to listen subject (for other listeners).
5. Call local `onAck`/`onAckAll` callback.
6. Respond `{ status: 'ACK_SUCCESS' }`.

### RPC Flow (mute)

1. Decode msgpack from `msg.data`.
2. If `mute_config.type === 'CLEAR'` or `null` → unmute (`rule.alert_mute_config = null`).
3. Otherwise → set `rule.alert_mute_config = mute_config`.
4. Fire-and-forget sync to backend at `api.iot.alerts.{orgID}.mute`.
5. Respond `{ status: 'MUTE_SUCCESS' }`.

---

## Mute Behavior

Mute is checked at the start of every evaluation cycle. If muted, the entire cycle is skipped — no state changes, no callbacks.

| Mute Type        | Behavior                          |
| ---------------- | --------------------------------- |
| `FOREVER`        | Always skip evaluation.           |
| `TIME_BASED`     | Skip if `Date.now() < mute_till`. |
| `CLEAR` / `null` | Not muted. Evaluate normally.     |

Mute state lives on `rule.alert_mute_config` and is synced to the backend asynchronously. Sync failures do not block the local mute operation.

---

## Callbacks

Passed to `listen(callbacks)`:

| Callback           | Mode             | Trigger                                 |
| ------------------ | ---------------- | --------------------------------------- |
| `onFire(data)`     | Owner & Listener | Alert breached for >= duration          |
| `onResolved(data)` | Owner & Listener | Alert cleared for >= recovery_duration  |
| `onAck(data)`      | Owner & Listener | Single-device acknowledgement           |
| `onAckAll(data)`   | Owner & Listener | All-device acknowledgement              |
| `onError(err)`     | Owner only       | Evaluator threw or returned non-boolean |

All callbacks are optional. Missing callbacks are silently skipped.

---

## Payload Shapes

### Fire / Resolved

```js
{
  alert: {
    id: "rule_abc123",
    name: "high_temp_alert",
    type: "TELEMETRY",          // source type
    config: { /* full rule config */ }
  },
  rolling_state: {
    "sensor-01": {
      "temperature": { value: 91.2, timestamp: 1711234567890 }
    }
  },
  timestamp: 1711234567890      // Unix ms (owner), ISO string (listener)
}
```

### Ack

```js
{
  status: "acknowledged",
  device_id: "sensor-01",       // present on single-device ack
  ack: {
    acked_by: "operator_jane",
    ack_notes: "Investigating",  // string or null
    acked_at: 1711234567890      // Unix ms
  }
}
```

### Ack All

```js
{
  status: "acknowledged",
  ack: {
    acked_by: "operator_jane",
    ack_notes: "Shift change",
    acked_at: 1711234567890
  }
}
```

---

## Error Handling

| Scenario                      | Behavior                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------- |
| Evaluator throws an exception | Caught. `onError(err)` called. State unchanged. Engine continues.                     |
| Evaluator returns non-boolean | `onError(new Error("Evaluator must return a boolean, got <type>"))`. State unchanged. |
| Notification dispatch fails   | Swallowed. Does not block fire/resolve.                                               |
| Mute backend sync fails       | Swallowed. Local mute still applies.                                                  |
| KV lock heartbeat fails       | Swallowed. Lock may be stolen by another instance.                                    |
| KV unavailable                | Lock skipped entirely. Engine proceeds without single-owner guarantee.                |

---

## Public API

The `EphemeralEngine` is not imported directly. It is managed by the `AlertManager` and exposed through the alert object returned by `app.alert.createEphemeral()` or `app.alert.get()`.

### Alert Object Methods

```js
// Set evaluator (must be called BEFORE listen to enter owner mode)
alert.setEvaluator((rollingState) => {
  // return true for breach, false for clear
  return rollingState["sensor-01"]?.temperature?.value > 85;
});

// Start the engine
await alert.listen({
  onFire: (data) => {},
  onResolved: (data) => {},
  onAck: (data) => {},
  onAckAll: (data) => {},
  onError: (err) => {}, // owner mode only
});

// Stop the engine (releases lock, deletes consumers, resets state)
await alert.stop();
```

### AlertManager Methods

```js
// Create an ephemeral alert rule on the backend
const alert = await app.alert.createEphemeral({
  name: "my_alert",
  description: "High temperature alert",
  config: {
    topic: {
      source: "TELEMETRY", // 'TELEMETRY' | 'COMMAND' | 'EVENT'
      device_ident: "s-3", // device ident or '*' for all
      last_token: "*", // metric name or '*' for all
    },
    duration: 5, // seconds
    recovery_duration: 10, // seconds
    cooldown: 10, // seconds (optional, default 0)
  },
  notification_channel: [], // notification channel IDs
});

// Retrieve existing alert
const alert = await app.alert.get("my_alert");

// Ack from listener (sends RPC to owner)
await app.alert.ack({
  alert_id: alert.id,
  device_id: "s-3",
  acked_by: "operator",
  ack_notes: "Looking into it",
});

// Ack all from listener
await app.alert.ackAll({
  alert_id: alert.id,
  acked_by: "operator",
});

// Mute
await app.alert.mute({
  id: alert.id,
  mute_config: { type: "FOREVER" },
});

await app.alert.mute({
  id: alert.id,
  mute_config: {
    type: "TIME_BASED",
    mute_till: new Date(Date.now() + 60000).toISOString(),
  },
});

// Unmute
await app.alert.unmute(alert.id);
```

### EphemeralEngine Getters

```js
engine.state; // { status, last_evaluated_at, breached_since, ... }
engine.rollingState; // { "sensor-01": { "temperature": { value, timestamp } } }
engine.mode; // 'owner' | 'listener' | null
```

---

## Examples

Working examples are in `examples/ephemeral-alerts/`. They demonstrate the full owner/listener workflow with a multi-metric breach condition.

### Running the Examples

Both examples require `RELAY_API_KEY` and `RELAY_SECRET` environment variables.

```bash
# Terminal 1 — start the owner (evaluator)
RELAY_API_KEY=your_key RELAY_SECRET=your_secret node examples/ephemeral-alerts/owner.js

# Terminal 2 — start the listener (event consumer + interactive CLI)
RELAY_API_KEY=your_key RELAY_SECRET=your_secret node examples/ephemeral-alerts/listener.js
```

The owner must be started first — it creates the alert rule on the backend. The listener fetches the existing rule by name.

---

### `owner.js` — Owner Mode with Multi-Metric Evaluator

This example creates (or fetches) an ephemeral alert named `ephemeral_multi_metric` that monitors device `s-3` for a compound breach condition: **temperature > 85 AND humidity > 70**.

**What it does:**

1. **Connects** to RelayX and registers a connection state listener.
2. **Creates or fetches** the alert rule — uses `app.alert.get()` first, falls back to `app.alert.createEphemeral()`.
3. **Sets the evaluator** via `setEvaluator()` — this makes this instance the **owner**. The evaluator receives the full rolling state and checks both metrics:
   ```js
   alert.setEvaluator((data) => {
     const device = data["s-3"];
     if (!device) return false;
     const temp = device.temperature?.value;
     const humidity = device.humidity?.value;
     if (temp == null || humidity == null) return false;
     return temp > 85 && humidity > 70;
   });
   ```
4. **Starts listening** with callbacks for `onFire`, `onResolved`, `onAck`, `onAckAll`, and `onError`.
5. **Shuts down gracefully** on `SIGINT` — calls `alert.stop()` (which releases the KV lock, deletes the JetStream consumer, and drains RPC subscriptions) then disconnects.

**Alert config:**

| Field                | Value         | Meaning                                                                 |
| -------------------- | ------------- | ----------------------------------------------------------------------- |
| `topic.source`       | `'TELEMETRY'` | Subscribe to the telemetry JetStream subject                            |
| `topic.device_ident` | `'s-3'`       | Single device                                                           |
| `topic.last_token`   | `'*'`         | All metrics — rolling state accumulates `temperature`, `humidity`, etc. |
| `duration`           | `5`           | Breach must hold for 5 seconds before firing                            |
| `recovery_duration`  | `10`          | Clear must hold for 10 seconds before resolving                         |
| `cooldown`           | `10`          | Minimum 10 seconds between re-fires                                     |

**Rolling state shape** (what the evaluator receives):

```js
{
  "s-3": {
    "temperature": { value: 90.15, timestamp: 1711234567890 },
    "humidity":    { value: 72.3,  timestamp: 1711234567891 }
  }
}
```

The `*` wildcard in `last_token` is key — it makes the data consumer subscribe to `{orgID}.{env}.telemetry.{device_id}.*`, so every metric published for this device updates the rolling state under its own key.

---

### `listener.js` — Listener Mode with Interactive CLI

This example connects as a **listener** (no evaluator) to the same alert rule. It receives fire/resolved/ack events published by the owner and provides an interactive terminal for sending ack/mute commands.

**What it does:**

1. **Connects** to RelayX.
2. **Fetches the alert** by name — exits with an error if the owner hasn't created it yet.
3. **Starts listening** without calling `setEvaluator()` — this makes it a **listener**. It subscribes to the alert event JetStream subject `{orgID}.{env}.alerts.listen.{rule_id}.*` and routes incoming events to callbacks.
4. **Presents an interactive CLI** using Node's `readline`:

   | Command | Action                 | NATS Path                                                                              |
   | ------- | ---------------------- | -------------------------------------------------------------------------------------- |
   | `1`     | Ack (single device)    | `app.alert.ack()` → NATS request to `{orgID}.{env}.alerts.custom.{rule_id}.ack`        |
   | `2`     | Ack all                | `app.alert.ackAll()` → NATS request to `{orgID}.{env}.alerts.custom.{rule_id}.ack_all` |
   | `3`     | Mute (FOREVER)         | `app.alert.mute()` → NATS request to `{orgID}.{env}.alerts.custom.{rule_id}.mute`      |
   | `4`     | Mute (TIME_BASED, 60s) | `app.alert.mute()` → same subject, with `mute_till`                                    |
   | `5`     | Unmute                 | `app.alert.unmute()` → same subject, with `type: 'CLEAR'`                              |
   | `q`     | Quit                   | Stops the engine and disconnects                                                       |

5. **Shuts down gracefully** on `q` or when the readline interface closes.

**How ack/mute reaches the owner:**

When the listener calls `app.alert.ack()`, the `AlertManager` sends a NATS request to the RPC subject. The owner's RPC subscription picks it up, updates the state machine, publishes an ack event to the listen subject, and responds. The listener then receives the ack event through its JetStream consumer and fires the `onAck` callback.

```
Listener                    NATS                         Owner
   │                         │                            │
   │── ack request ─────────▶│                            │
   │   (alerts.custom.       │── deliver to subscriber ──▶│
   │    {rule_id}.ack)       │                            │── update state
   │                         │                            │── publish ack event
   │                         │◀── ack event ──────────────│   (alerts.listen.
   │◀── JetStream deliver ──│    {rule_id}.ack)          │
   │                         │                            │
   │── onAck callback fires  │◀── respond ACK_SUCCESS ───│
   │                         │                            │
```

---

### Testing the Full Flow

1. Start `owner.js` in one terminal.
2. Start `listener.js` in another terminal.
3. Send telemetry data for device `s-3` with both `temperature` and `humidity` metrics (via a device simulator or the SDK's `app.command.send()`).
4. Watch the owner log evaluator output as data arrives.
5. When both temperature > 85 and humidity > 70 hold for 5+ seconds, the owner fires and both terminals show the `[FIRE]` event.
6. In the listener terminal, press `1` to ack — both terminals show the `[ACK]` event.
7. When the condition clears for 10+ seconds, both terminals show `[RESOLVED]`.
