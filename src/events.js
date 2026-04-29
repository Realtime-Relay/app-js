import { decode as msgpackDecode } from "@msgpack/msgpack";
import {
  validateEventName,
  validateFunction,
  validateConnected,
  validateIdent,
  validateNonEmptyArray,
  validateISO8601,
  validateStartBeforeEnd,
} from "./validation.js";
import { streamHistory } from "./utils.js";

export class EventManager {
  #ctx;
  #consumers = new Map(); // event_name -> consumer
  #callbacks = new Map(); // event_name -> callback

  constructor(ctx) {
    this.#ctx = ctx;
  }

  /**
   * Stream live events for one or more devices.
   *
   * params:
   *   name          string   required — event name
   *   device_ident  string|string[]  required:
   *                   - "*"        → all devices in org (wildcard subject; reverse-resolved ident in callback)
   *                   - [ident]    → single device (concrete subject)
   *                   - [a, b, …]  → list of devices (wildcard subject + in-callback filter)
   *                   - []         → throws
   *   callback      function (payload) — called with { [device_ident]: data }
   */
  async stream(params) {
    validateConnected(this.#ctx.connected);
    validateEventName(params.name);
    validateFunction(params.callback, "callback");

    if (params.device_ident === undefined || params.device_ident === null) {
      throw new Error("device_ident is required");
    }

    // deviceFilter: Map<device_id, ident> — present except in "*" wildcard mode
    // subjectDevice: concrete device_id when filtering to exactly one device, else "*"
    let deviceFilter = null;
    let subjectDevice = "*";
    let wildcard = false;

    if (typeof params.device_ident === "string") {
      if (params.device_ident !== "*") {
        throw new Error(
          'device_ident as a string must be "*". Use an array for specific devices.',
        );
      }
      wildcard = true;
      // Warm the device cache so the callback can reverse-resolve device_id → ident.
      try {
        await this.#ctx.device.list();
      } catch {
        // Best-effort — callback falls back to device_id as the key on miss.
      }
    } else if (Array.isArray(params.device_ident)) {
      if (params.device_ident.length === 0) {
        throw new Error("device_ident array cannot be empty");
      }
      for (const ident of params.device_ident) {
        validateIdent(ident, "device_ident");
      }
      deviceFilter = new Map();
      for (const ident of params.device_ident) {
        const id = await this.#ctx.device.resolveDeviceId(ident);
        deviceFilter.set(id, ident);
      }
      if (deviceFilter.size === 1) {
        subjectDevice = [...deviceFilter.keys()][0];
      }
    } else {
      throw new Error(
        'device_ident must be "*" or a non-empty array of device idents',
      );
    }

    if (this.#consumers.has(params.name)) {
      return false;
    }

    const subject = `${this.#ctx.orgID}.${this.#ctx.env}.events.${subjectDevice}.${params.name}`;

    const consumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `appjs_events_${params.name}_${crypto.randomUUID()}`,
        filter_subjects: subject,
        replay_policy: "instant",
        opt_start_time: new Date(),
        ack_policy: "explicit",
        delivery_policy: "new",
      },
    );

    this.#consumers.set(params.name, consumer);
    this.#callbacks.set(params.name, params.callback);

    const reverseLookup = (deviceId) => {
      for (const [ident, dev] of this.#ctx.device.cache) {
        if (dev?.id === deviceId) return ident;
      }
      return null;
    };

    await consumer.consume({
      callback: async (msg) => {
        msg.working();
        const data = msgpackDecode(msg.data);
        msg.ack();

        // subject = <org>.<env>.events.<device_id>.<name>
        const tokens = msg.subject.split(".");
        const deviceId = tokens[3];

        let ident;
        if (deviceFilter) {
          ident = deviceFilter.get(deviceId);
          if (!ident) return; // device not in this subscription's filter
        } else if (wildcard) {
          ident = reverseLookup(deviceId) ?? deviceId;
        }

        const cb = this.#callbacks.get(params.name);
        if (cb) cb({ [ident]: data });
      },
    });

    return true;
  }

  async off(params) {
    validateEventName(params.name);

    const consumer = this.#consumers.get(params.name);

    if (consumer) {
      await consumer.delete();
      this.#consumers.delete(params.name);
      this.#callbacks.delete(params.name);
    }
  }

  // ─── History ─────────────────────────────────────────────

  /**
   * Fetch historical events over the streaming protocol.
   *
   * params:
   *   device_ident  string   required
   *   event_names   string[] required
   *   start, end    ISO8601  required
   *   interval, aggregate_fn  optional
   *   onFrame       function? live frame callback
   *
   * Returns: { <event_name>: [{value, timestamp}, ...] }
   */
  async history(params) {
    validateConnected(this.#ctx.connected);
    validateIdent(params.device_ident, "device_ident");
    validateNonEmptyArray(params.event_names, "event_names");
    for (const name of params.event_names) {
      validateEventName(name);
    }
    validateISO8601(params.start, "start");
    validateISO8601(params.end, "end");
    validateStartBeforeEnd(params.start, params.end);

    if (params.onFrame !== undefined) {
      validateFunction(params.onFrame, "onFrame");
    }

    const deviceId = await this.#ctx.device.resolveDeviceId(
      params.device_ident,
    );

    const payload = {
      device_id: deviceId,
      env: this.#ctx.env,
      event_names: params.event_names,
      start: params.start,
      end: params.end,
    };
    if (params.interval) payload.interval = params.interval;
    if (params.aggregate_fn) payload.aggregate_fn = params.aggregate_fn;

    const result = await streamHistory(
      this.#ctx,
      `api.iot.db.${this.#ctx.orgID}.event.history`,
      payload,
      { onFrame: params.onFrame },
    );

    if (result.error) {
      throw new Error(
        `Event history failed: ${result.errorMessage ?? result.status}`,
      );
    }

    const events = {};
    for (const name of params.event_names) {
      events[name] = [];
    }

    for (const frame of result.frames) {
      if (!frame.data) continue;
      for (const [name, point] of Object.entries(frame.data)) {
        if (!events[name]) events[name] = [];
        events[name].push({ value: point.value, timestamp: point.timestamp });
      }
    }

    return events;
  }

  async deleteAllConsumers() {
    for (const [, consumer] of this.#consumers) {
      await consumer.delete();
    }

    this.#consumers.clear();
    this.#callbacks.clear();
  }
}
