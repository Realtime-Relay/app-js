import { JSONCodec } from "nats.ws";
import {
  decode as msgpackDecode,
  encode as msgpackEncode,
} from "@msgpack/msgpack";
import {
  validateIdent,
  validateFunction,
  validateConnected,
  validateArray,
  validateISO8601,
  validateStartBeforeEnd,
  validateNonEmptyArray,
} from "./validation.js";
import { httpHistory } from "./utils.js";

export class TelemetryManager {
  #ctx;
  #consumers = new Map(); // key: device_ident -> Array<{ consumer, metrics: Set|null, callback }>
  #codec = JSONCodec();

  constructor(ctx) {
    this.#ctx = ctx;
  }

  // ─── Streaming ───────────────────────────────────────────

  async stream(params) {
    validateConnected(this.#ctx.connected);
    validateIdent(params.device_ident, "device_ident");
    validateFunction(params.callback, "callback");

    let metricsFilter = null;

    if (typeof params.metric === "string") {
      if (params.metric !== "*") {
        throw new Error(
          'metric as a string must be "*". Use an array for specific metrics.',
        );
      }
    } else if (Array.isArray(params.metric)) {
      validateNonEmptyArray(params.metric, "metric");

      for (const m of params.metric) {
        await this.#validateMetric(params.device_ident, m);
      }

      metricsFilter = new Set(params.metric);
    } else {
      throw new Error(
        'metric must be "*" or a non-empty array of metric names',
      );
    }

    const deviceId = await this.#ctx.device.resolveDeviceId(
      params.device_ident,
    );

    const subject = `${this.#ctx.orgID}.${this.#ctx.env}.telemetry.${deviceId}.*`;

    const consumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `appjs_telemetry_${params.device_ident}_${crypto.randomUUID()}`,
        filter_subjects: subject,
        replay_policy: "instant",
        opt_start_time: new Date(),
        ack_policy: "explicit",
        delivery_policy: "new",
      },
    );

    const entry = {
      consumer,
      metrics: metricsFilter,
      callback: params.callback,
    };

    if (!this.#consumers.has(params.device_ident)) {
      this.#consumers.set(params.device_ident, []);
    }

    this.#consumers.get(params.device_ident).push(entry);

    await consumer.consume({
      callback: async (msg) => {
        msg.working();
        const data = msgpackDecode(msg.data);
        msg.ack();

        var tokens = msg.subject.split(".");
        var metric = tokens[tokens.length - 1];

        if (entry.metrics !== null && !entry.metrics.has(metric)) {
          return;
        }

        params.callback({
          metric: metric,
          data: data,
        });
      },
    });
  }

  async off(params) {
    validateIdent(params.device_ident, "device_ident");

    const subs = this.#consumers.get(params.device_ident);
    if (!subs || subs.length === 0) return;

    if (params.metric !== undefined) {
      validateArray(params.metric, "metric");

      for (let i = subs.length - 1; i >= 0; i--) {
        const sub = subs[i];

        // Skip wildcard ("*") subscriptions — metrics is null
        if (sub.metrics === null) continue;

        for (const m of params.metric) {
          sub.metrics.delete(m);
        }

        if (sub.metrics.size === 0) {
          await sub.consumer.delete();
          subs.splice(i, 1);
        }
      }

      if (subs.length === 0) {
        this.#consumers.delete(params.device_ident);
      }
    } else {
      for (const sub of subs) {
        await sub.consumer.delete();
      }
      this.#consumers.delete(params.device_ident);
    }
  }

  // ─── History ─────────────────────────────────────────────

  /**
   * Fetch historical telemetry from the influx-db-service.
   *
   * params:
   *   device_ident   string  required
   *   fields         string[] required (must exist on device schema)
   *   start, end     ISO8601 required
   *   interval       string?  optional Flux duration ("30s", "5m", "1h")
   *   aggregate_fn   string?  optional, paired with interval
   *                            (mean|min|max|sum|count|first|last|median|stddev)
   *
   * Returns: { <metric>: [{value, timestamp}, ...] } — aggregated from all frames.
   */
  async history(params) {
    validateConnected(this.#ctx.connected);
    validateIdent(params.device_ident, "device_ident");
    validateNonEmptyArray(params.fields, "fields");

    await this.#validateFields(params.device_ident, params.fields);

    validateISO8601(params.start, "start");
    validateISO8601(params.end, "end");
    validateStartBeforeEnd(params.start, params.end);

    const deviceId = await this.#ctx.device.resolveDeviceId(
      params.device_ident,
    );

    const payload = {
      device_id: deviceId,
      env: this.#ctx.env,
      start: params.start,
      end: params.end,
      fields: params.fields,
      last_value: false,
    };
    if (params.interval) payload.interval = params.interval;
    if (params.aggregate_fn) payload.aggregate_fn = params.aggregate_fn;

    const result = await httpHistory(
      this.#ctx,
      "/iot/db/telemetry/history",
      payload,
    );

    if (result.error) {
      throw new Error(
        `Telemetry history failed: ${result.errorMessage ?? result.status}`,
      );
    }

    // Aggregate frames into the legacy { metric: [...] } shape.
    // REST frames are the raw row: { <metric>: { value, timestamp } }.
    const telemetry = {};
    for (const field of params.fields) {
      telemetry[field] = [];
    }

    for (const frame of result.frames) {
      for (const [metric, point] of Object.entries(frame)) {
        if (!telemetry[metric]) telemetry[metric] = [];

        telemetry[metric].push({
          value: point.value,
          timestamp: point.timestamp,
        });
      }
    }

    return telemetry;
  }

  /**
   * Latest reading per metric over a time window. Returns
   *   { <metric>: { value, timestamp } }
   * The server returns a single frame containing one entry per metric.
   */
  async latest(params) {
    validateConnected(this.#ctx.connected);
    validateIdent(params.device_ident, "device_ident");
    validateNonEmptyArray(params.fields, "fields");

    await this.#validateFields(params.device_ident, params.fields);

    validateISO8601(params.start, "start");
    validateISO8601(params.end, "end");
    validateStartBeforeEnd(params.start, params.end);

    const deviceId = await this.#ctx.device.resolveDeviceId(
      params.device_ident,
    );

    const result = await httpHistory(
      this.#ctx,
      "/iot/db/telemetry/history",
      {
        device_id: deviceId,
        env: this.#ctx.env,
        start: params.start,
        end: params.end,
        fields: params.fields,
        last_value: true,
      },
    );

    if (result.error) {
      throw new Error(
        `Telemetry latest failed: ${result.errorMessage ?? result.status}`,
      );
    }

    // last_value mode emits exactly one frame containing all metrics' last
    // readings (or zero frames if there's no data at all). REST frames are the
    // raw row: { <metric>: { value, timestamp } }.
    const latest = {};
    const onlyFrame = result.frames[0];
    if (onlyFrame) {
      for (const [metric, point] of Object.entries(onlyFrame)) {
        latest[metric] = { value: point.value, timestamp: point.timestamp };
      }
    }
    return latest;
  }

  // ─── Validation ──────────────────────────────────────────

  async #validateFields(deviceIdent, fields) {
    let device = this.#ctx.device.cache.get(deviceIdent);

    if (!device || !device.schema) {
      device = await this.#ctx.device.get({ ident: deviceIdent });
    }

    if (!device || !device.schema) {
      throw new Error(`Device ${deviceIdent} not found or has no schema`);
    }

    const invalid = fields.filter((f) => !(f in device.schema));

    if (invalid.length > 0) {
      throw new Error(
        `fields contain invalid metrics: ${invalid.join(", ")}. Valid keys: ${Object.keys(device.schema).join(", ")}`,
      );
    }
  }

  async #validateMetric(deviceIdent, metric) {
    if (metric === "*") return;

    // Check cache first, fallback to NATS request
    let device = this.#ctx.device.cache.get(deviceIdent);

    if (!device || !device.schema) {
      device = await this.#ctx.device.get({ ident: deviceIdent });
    }

    if (!device || !device.schema) {
      throw new Error(`Device ${deviceIdent} not found or has no schema`);
    }

    if (!(metric in device.schema)) {
      throw new Error(
        `metric "${metric}" is not a valid key in device schema. Valid keys: ${Object.keys(device.schema).join(", ")}`,
      );
    }
  }

  // ─── Cleanup ─────────────────────────────────────────────

  async deleteAllConsumers() {
    for (const [deviceIdent, subs] of this.#consumers) {
      for (const sub of subs) {
        await sub.consumer.delete();
      }
    }

    this.#consumers.clear();
  }
}
