import { decode as msgpackDecode } from "@msgpack/msgpack";
import {
  validateIdent,
  validateFunction,
  validateConnected,
  validateNonEmptyArray,
  validateISO8601,
  validateStartBeforeEnd,
} from "./validation.js";
import { streamHistory } from "./utils.js";

const VALID_LEVELS = ["info", "warn", "error"];

export class LogManager {
  #ctx;
  #consumers = new Map(); // device_ident -> Array<{consumer, levels: Set|null, callback}>

  constructor(ctx) {
    this.#ctx = ctx;
  }

  // ─── Real-time streaming ─────────────────────────────────

  /**
   * Stream live device logs as they arrive. Subscribes via JetStream so
   * messages buffered offline by the device are not lost on reconnect.
   *
   * params:
   *   device_ident  string   required
   *   levels        string[] | "*"  default "*" — subset of info/warn/error
   *   callback      function (entry) — called with { level, data, timestamp }
   */
  async stream(params) {
    validateConnected(this.#ctx.connected);
    validateIdent(params.device_ident, "device_ident");
    validateFunction(params.callback, "callback");

    let levelFilter = null;

    if (typeof params.levels === "string") {
      if (params.levels !== "*") {
        throw new Error(
          'levels as a string must be "*". Use an array for specific levels.',
        );
      }
    } else if (Array.isArray(params.levels)) {
      validateNonEmptyArray(params.levels, "levels");
      const invalid = params.levels.filter((l) => !VALID_LEVELS.includes(l));
      if (invalid.length > 0) {
        throw new Error(
          `levels contains invalid values: ${invalid.join(", ")}. Valid: ${VALID_LEVELS.join(", ")}`,
        );
      }
      levelFilter = new Set(params.levels);
    } else if (params.levels === undefined) {
      // Default — subscribe to all
    } else {
      throw new Error(
        'levels must be "*", undefined, or a non-empty array of "info"|"warn"|"error"',
      );
    }

    const deviceId = await this.#ctx.device.resolveDeviceId(
      params.device_ident,
    );

    const subject = `${this.#ctx.orgID}.${this.#ctx.env}.logs.${deviceId}.*`;

    const consumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `appjs_logs_${params.device_ident}_${crypto.randomUUID()}`,
        filter_subjects: subject,
        replay_policy: "instant",
        opt_start_time: new Date(),
        ack_policy: "explicit",
        delivery_policy: "new",
      },
    );

    const entry = {
      consumer,
      levels: levelFilter,
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

        const tokens = msg.subject.split(".");
        const level = tokens[tokens.length - 1];

        if (entry.levels !== null && !entry.levels.has(level)) {
          return;
        }

        params.callback({
          level,
          data: data.data,
          timestamp: data.timestamp,
        });
      },
    });
  }

  async off(params) {
    validateIdent(params.device_ident, "device_ident");

    const subs = this.#consumers.get(params.device_ident);
    if (!subs || subs.length === 0) return;

    for (const sub of subs) {
      await sub.consumer.delete();
    }
    this.#consumers.delete(params.device_ident);
  }

  // ─── History ─────────────────────────────────────────────

  /**
   * Fetch historical device logs over the streaming protocol.
   *
   * params:
   *   device_ident  string   required
   *   levels        string[]?  subset of info/warn/error (default: all three)
   *   start, end    ISO8601  required
   *   interval, aggregate_fn  optional bucketing (count is the only
   *                            sensible aggregate on log strings)
   *   onFrame       function?  live frame callback
   *
   * Returns: { <level>: [{value, timestamp}, ...] }
   */
  async history(params) {
    validateConnected(this.#ctx.connected);
    validateIdent(params.device_ident, "device_ident");
    validateISO8601(params.start, "start");
    validateISO8601(params.end, "end");
    validateStartBeforeEnd(params.start, params.end);

    if (params.levels !== undefined) {
      validateNonEmptyArray(params.levels, "levels");
      const invalid = params.levels.filter((l) => !VALID_LEVELS.includes(l));
      if (invalid.length > 0) {
        throw new Error(
          `levels contains invalid values: ${invalid.join(", ")}. Valid: ${VALID_LEVELS.join(", ")}`,
        );
      }
    }

    if (params.onFrame !== undefined) {
      validateFunction(params.onFrame, "onFrame");
    }

    const deviceId = await this.#ctx.device.resolveDeviceId(
      params.device_ident,
    );

    const payload = {
      device_id: deviceId,
      env: this.#ctx.env,
      start: params.start,
      end: params.end,
    };
    if (params.levels) payload.levels = params.levels;
    if (params.interval) payload.interval = params.interval;
    if (params.aggregate_fn) payload.aggregate_fn = params.aggregate_fn;

    const result = await streamHistory(
      this.#ctx,
      `api.iot.db.${this.#ctx.orgID}.log.history`,
      payload,
      { onFrame: params.onFrame },
    );

    if (result.error) {
      throw new Error(
        `Log history failed: ${result.errorMessage ?? result.status}`,
      );
    }

    const logs = {};
    for (const lvl of params.levels ?? VALID_LEVELS) {
      logs[lvl] = [];
    }

    for (const frame of result.frames) {
      if (!frame.data) continue;
      for (const [level, point] of Object.entries(frame.data)) {
        if (!logs[level]) logs[level] = [];
        logs[level].push({ value: point.value, timestamp: point.timestamp });
      }
    }

    return logs;
  }

  // ─── Cleanup ─────────────────────────────────────────────

  async deleteAllConsumers() {
    for (const [, subs] of this.#consumers) {
      for (const sub of subs) {
        await sub.consumer.delete();
      }
    }
    this.#consumers.clear();
  }
}
