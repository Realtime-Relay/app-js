import { JSONCodec } from "nats.ws";
import {
  decode as msgpackDecode,
  encode as msgpackEncode,
} from "@msgpack/msgpack";
import {
  validateIdent,
  validateConnected,
  validateFunction,
  validatePositiveNumber,
  validateNonEmptyArray,
  validateISO8601,
  validateStartBeforeEnd,
} from "./validation.js";
import { EphemeralEngine } from "./ephemeral_alerting/index.js";
import { streamHistory } from "./utils.js";

const VALID_SOURCES = ["TELEMETRY", "COMMAND", "EVENT"];
const VALID_RULE_TYPES = ["DEVICE", "RULE"];
const VALID_ALERT_STATES = ["fire", "resolved", "ack"];

export class AlertManager {
  #ctx;
  #codec = JSONCodec();
  #listenConsumers = new Map(); // ruleId -> consumer[]
  #ephemeralEngines = new Map(); // ruleId -> EphemeralEngine
  #alertMetadata = new Map(); // alertId -> { type, ... }

  constructor(ctx) {
    this.#ctx = ctx;
  }

  #subject(op) {
    return `api.iot.alerts.${this.#ctx.orgID}.${op}`;
  }

  async #request(op, payload) {
    const res = await this.#ctx.natsClient.request(
      this.#subject(op),
      this.#codec.encode(payload),
      { timeout: 20000 },
    );

    return res.json();
  }

  // ─── Wrap non-ephemeral alert ────────────────────────────

  #wrapAlert(data) {
    if (!data) return data;

    const alert = { ...data };

    // Track metadata
    if (data.id) {
      this.#alertMetadata.set(data.id, { type: data.type || "THRESHOLD" });
    }

    alert.listen = async (callbacks) => {
      await this.#listen(data, callbacks);
    };

    alert.setEvaluator = (fn) => {
      throw new Error("setEvaluator is only allowed for EPHEMERAL alerts");
    };

    return alert;
  }

  // ─── Wrap ephemeral alert ────────────────────────────────

  #wrapEphemeralAlert(data) {
    if (!data) return data;

    const alert = { ...data };

    // Track metadata
    if (data.id) {
      this.#alertMetadata.set(data.id, { type: "EPHEMERAL" });
    }

    const engine = new EphemeralEngine(this.#ctx, data);
    this.#ephemeralEngines.set(data.id, engine);

    alert.setEvaluator = (fn) => {
      validateFunction(fn, "evaluator");
      engine.setEvaluator(fn);
    };

    alert.listen = async (callbacks) => {
      validateConnected(this.#ctx.connected);
      await engine.listen(callbacks);
    };

    alert.stop = async () => {
      await engine.stop();
    };

    return alert;
  }

  // ─── CRUD ────────────────────────────────────────────────

  async create(config) {
    validateConnected(this.#ctx.connected);
    validateIdent(config.name, "name");

    if (config.type === "EPHEMERAL") {
      throw new Error("Use createEphemeral() for EPHEMERAL alerts");
    }

    if (!["THRESHOLD", "RATE_CHANGE"].includes(config.type)) {
      throw new Error("type must be THRESHOLD or RATE_CHANGE");
    }

    const payload = {
      name: config.name,
      description: config.description,
      type: config.type,
      metric: config.metric,
      config: config.config,
      notification_channel: config.notification_channel || [],
      alert_mute_config: config.alert_mute_config,
      env: this.#ctx.env,
    };

    const res = await this.#request("create", payload);

    if (res.data) return this.#wrapAlert(res.data);
    return null;
  }

  async createEphemeral(config) {
    validateConnected(this.#ctx.connected);
    validateIdent(config.name, "name");

    if (!config.config) throw new Error("config is required");
    if (!config.config.topic) throw new Error("config.topic is required");

    if (!VALID_SOURCES.includes(config.config.topic.source)) {
      throw new Error(
        "config.topic.source must be TELEMETRY, COMMAND, or EVENT",
      );
    }

    if (!config.config.topic.device_ident)
      throw new Error("config.topic.device_ident is required");
    if (!config.config.topic.last_token)
      throw new Error("config.topic.last_token is required");

    validatePositiveNumber(config.config.duration, "config.duration");
    validatePositiveNumber(
      config.config.recovery_duration,
      "config.recovery_duration",
    );

    if (
      config.config.recovery_eval_type !== undefined &&
      !["VALUE", "TIMER"].includes(config.config.recovery_eval_type)
    ) {
      throw new Error('config.recovery_eval_type must be "VALUE" or "TIMER"');
    }

    const payload = {
      name: config.name,
      description: config.description,
      config: config.config,
      notification_channel: config.notification_channel || [],
      type: "EPHEMERAL",
      env: this.#ctx.env,
    };

    const res = await this.#request("create_ephemeral", payload);

    if (res.data) return this.#wrapEphemeralAlert(res.data);
    return null;
  }

  async update(config) {
    validateConnected(this.#ctx.connected);

    if (!config.id) throw new Error("id is required");

    const res = await this.#request("update", {
      ...config,
      env: this.#ctx.env,
    });

    if (res.data) return this.#wrapAlert(res.data);
    return res;
  }

  async updateEphemeral(config) {
    validateConnected(this.#ctx.connected);

    if (!config.id) throw new Error("id is required");

    if (
      config.config?.recovery_eval_type !== undefined &&
      !["VALUE", "TIMER"].includes(config.config.recovery_eval_type)
    ) {
      throw new Error('config.recovery_eval_type must be "VALUE" or "TIMER"');
    }

    const res = await this.#request("update_ephemeral", {
      ...config,
      type: "EPHEMERAL",
      env: this.#ctx.env,
    });

    if (res.data) return this.#wrapEphemeralAlert(res.data);
    return null;
  }

  async delete(alertId) {
    validateConnected(this.#ctx.connected);

    if (!alertId) throw new Error("id is required");

    // Clean up engine if exists
    const engine = this.#ephemeralEngines.get(alertId);
    if (engine) {
      await engine.stop();
      this.#ephemeralEngines.delete(alertId);
    }
    this.#alertMetadata.delete(alertId);

    const res = await this.#request("delete", { id: alertId });

    return res.status === "ALERT_DELETE_SUCCESS";
  }

  async list() {
    validateConnected(this.#ctx.connected);

    const res = await this.#request("list", {});
    const alerts = res.data || [];

    // Track metadata for all listed alerts
    for (const a of alerts) {
      if (a.id) {
        this.#alertMetadata.set(a.id, { type: a.type || "THRESHOLD" });
      }
    }

    return alerts;
  }

  async get(alertName) {
    validateConnected(this.#ctx.connected);
    validateIdent(alertName, "alertName");

    const res = await this.#request("get", { name: alertName });

    if (!res.data) return null;

    // Track metadata
    if (res.data.id) {
      this.#alertMetadata.set(res.data.id, {
        type: res.data.type || "THRESHOLD",
      });
    }

    // Return correct wrapper based on type
    if (res.data.type === "EPHEMERAL") {
      return this.#wrapEphemeralAlert(res.data);
    }

    return this.#wrapAlert(res.data);
  }

  // ─── History ──────────────────────────────────────────────

  /**
   * Fetch alert event history (fire / resolved / ack) over the streaming
   * protocol. Returns an event timeline ordered by timestamp.
   *
   * params:
   *   rule_type      "DEVICE" | "RULE"  required
   *   device_ident   string?  required when rule_type=DEVICE
   *   rule_id        string?  required when rule_type=RULE
   *   rule_states    string[]?  default ["fire","resolved","ack"]
   *   incident_id    string?  optional, filter to one incident
   *   start, end     ISO8601 required
   *   interval       string?  paired with aggregate_fn for bucketing
   *   aggregate_fn   "count"? — only "count" allowed for alerts
   *   onFrame        function? live frame callback
   *
   * Returns: { events: [{state, value, timestamp, incident_id}, ...] }
   */
  async history(params) {
    validateConnected(this.#ctx.connected);

    if (!params.rule_type) throw new Error("rule_type is required");
    if (!VALID_RULE_TYPES.includes(params.rule_type)) {
      throw new Error("rule_type must be DEVICE or RULE");
    }
    if (params.rule_type === "DEVICE" && !params.device_ident) {
      throw new Error("device_ident is required for rule_type DEVICE");
    }
    if (params.rule_type === "RULE" && !params.rule_id) {
      throw new Error("rule_id is required for rule_type RULE");
    }

    if (params.rule_states) {
      validateNonEmptyArray(params.rule_states, "rule_states");
      const invalid = params.rule_states.filter(
        (s) => !VALID_ALERT_STATES.includes(s),
      );
      if (invalid.length > 0) {
        throw new Error(
          `rule_states contains invalid values: ${invalid.join(", ")}. Valid values: ${VALID_ALERT_STATES.join(", ")}`,
        );
      }
    }

    if (params.aggregate_fn && params.aggregate_fn !== "count") {
      throw new Error("aggregate_fn for alerts must be 'count'");
    }

    validateISO8601(params.start, "start");
    validateISO8601(params.end, "end");
    validateStartBeforeEnd(params.start, params.end);

    if (params.onFrame !== undefined) {
      validateFunction(params.onFrame, "onFrame");
    }

    const payload = {
      rule_type: params.rule_type,
      env: this.#ctx.env,
      rule_states: params.rule_states || ["fire", "resolved", "ack"],
      start: params.start,
      end: params.end,
    };

    if (params.device_ident) {
      payload.device_id = await this.#ctx.device.resolveDeviceId(
        params.device_ident,
      );
    }
    if (params.rule_type === "RULE") payload.rule_id = params.rule_id;
    if (params.incident_id) payload.incident_id = params.incident_id;
    if (params.interval) payload.interval = params.interval;
    if (params.aggregate_fn) payload.aggregate_fn = params.aggregate_fn;

    const result = await streamHistory(
      this.#ctx,
      `api.iot.db.${this.#ctx.orgID}.alerts.history`,
      payload,
      { onFrame: params.onFrame },
    );

    if (result.error) {
      throw new Error(
        `Alert history failed: ${result.errorMessage ?? result.status}`,
      );
    }

    // Each frame: { last, data: { <state>: { value, timestamp, incident_id } } }
    // Flatten into a chronological event list.
    const events = [];
    for (const frame of result.frames) {
      if (!frame.data) continue;
      for (const [state, point] of Object.entries(frame.data)) {
        events.push({
          state,
          value: point.value,
          timestamp: point.timestamp,
          incident_id: point.incident_id ?? null,
        });
      }
    }

    return { events };
  }

  // ─── Ack / AckAll ────────────────────────────────────────

  /**
   * Acknowledge the current incident for an alert.
   *
   *   - Backend alerts (THRESHOLD / RATE_CHANGE): incidents are scoped to
   *     (rule, device). `device_id` REQUIRED — picks which device's incident.
   *   - Ephemeral alerts: state is rule-scoped (one evaluator per rule), but
   *     the ack is still tagged with `device_id` for audit/query purposes —
   *     so the ack row is visible in `rule_type: "DEVICE"` history queries.
   *     `device_id` REQUIRED.
   */
  async ack(params) {
    validateConnected(this.#ctx.connected);

    if (!params.alert_id) throw new Error("alert_id is required");
    if (!params.acked_by) throw new Error("acked_by is required");
    if (!params.device_id) throw new Error("device_id is required");

    // Local ephemeral owner — in-process ack. State machine stays rule-scoped;
    // device_id is carried into the published audit event.
    const engine = this.#ephemeralEngines.get(params.alert_id);
    if (engine && engine.mode === "owner") {
      return engine.ack(params.device_id, params.acked_by, params.ack_notes);
    }

    // Listener-side ephemeral — RPC to the remote owner.
    const meta = this.#alertMetadata.get(params.alert_id);
    if (meta?.type === "EPHEMERAL") {
      const subject = `${this.#ctx.orgID}.${this.#ctx.env}.alerts.custom.${params.alert_id}.ack`;

      const res = await this.#ctx.natsClient.request(
        subject,
        msgpackEncode({
          device_id: params.device_id,
          acked_by: params.acked_by,
          ack_notes: params.ack_notes,
        }),
        { timeout: 10000 },
      );

      const data = res.json();
      return data.status === "ACK_SUCCESS";
    }

    // Backend (non-ephemeral) — per-(rule, device) incident.
    const res = await this.#request("ack", {
      device_id: params.device_id,
      rule_id: params.alert_id,
      acked_by: params.acked_by,
      env: this.#ctx.env,
      ack_notes: params.ack_notes,
    });

    return res.status === "ALERT_ACK_SUCCESS";
  }

  // ─── Mute / Unmute ───────────────────────────────────────

  async mute(params) {
    validateConnected(this.#ctx.connected);

    if (!params.id) throw new Error("id is required");
    if (!params.mute_config) throw new Error("mute_config is required");

    if (!["FOREVER", "TIME_BASED"].includes(params.mute_config.type)) {
      throw new Error("mute_config.type must be FOREVER or TIME_BASED");
    }

    if (
      params.mute_config.type === "TIME_BASED" &&
      !params.mute_config.mute_till
    ) {
      throw new Error("mute_till is required for TIME_BASED mute");
    }

    // Ephemeral — RPC to owner (single 'mute' endpoint handles both mute/unmute)
    const meta = this.#alertMetadata.get(params.id);
    if (meta?.type === "EPHEMERAL") {
      const subject = `${this.#ctx.orgID}.${this.#ctx.env}.alerts.custom.${params.id}.mute`;

      const res = await this.#ctx.natsClient.request(
        subject,
        msgpackEncode({ mute_config: params.mute_config }),
        { timeout: 10000 },
      );

      return res.json();
    }

    const payload = {
      rule_id: params.id,
      type: params.mute_config.type,
    };

    if (params.mute_config.type === "TIME_BASED") {
      payload.mute_till = params.mute_config.mute_till;
    }

    return this.#request("mute", payload);
  }

  async unmute(id) {
    validateConnected(this.#ctx.connected);

    if (!id) throw new Error("id is required");

    // Ephemeral — RPC to owner via same 'mute' endpoint with type=CLEAR
    const meta = this.#alertMetadata.get(id);
    if (meta?.type === "EPHEMERAL") {
      const subject = `${this.#ctx.orgID}.${this.#ctx.env}.alerts.custom.${id}.mute`;

      const res = await this.#ctx.natsClient.request(
        subject,
        msgpackEncode({ mute_config: { type: "CLEAR" } }),
        { timeout: 10000 },
      );

      return res.json();
    }

    return this.#request("mute", { rule_id: id, type: "CLEAR" });
  }

  // ─── Listen (non-ephemeral) ──────────────────────────────

  async #listen(rule, callbacks) {
    validateConnected(this.#ctx.connected);

    const ruleId = rule.id;
    const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${ruleId}.*`;

    const callbackMap = {
      fire: callbacks.onFire,
      resolved: callbacks.onResolved,
      ack: callbacks.onAck,
    };

    const consumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `appjs_alert_listen_${ruleId}_${crypto.randomUUID()}`,
        filter_subjects: subject,
        replay_policy: "instant",
        opt_start_time: new Date(),
        ack_policy: "explicit",
        delivery_policy: "new",
      },
    );

    await consumer.consume({
      callback: async (msg) => {
        msg.working();
        const data = msgpackDecode(msg.data);
        msg.ack();

        const tokens = msg.subject.split(".");
        const eventType = tokens[tokens.length - 1];
        const cb = callbackMap[eventType];

        if (cb) cb(this.#transformTimestamp(data));
      },
    });

    this.#listenConsumers.set(ruleId, [consumer]);
  }

  #transformTimestamp(data) {
    const transformed = { ...data };

    if (typeof transformed.timestamp === "number") {
      transformed.timestamp = new Date(transformed.timestamp).toISOString();
    }

    return transformed;
  }

  // ─── Cleanup ─────────────────────────────────────────────

  async deleteAllConsumers() {
    for (const [, consumers] of this.#listenConsumers) {
      for (const consumer of consumers) {
        await consumer.delete();
      }
    }
    this.#listenConsumers.clear();

    for (const [, engine] of this.#ephemeralEngines) {
      await engine.stop();
    }
    this.#ephemeralEngines.clear();
  }
}
