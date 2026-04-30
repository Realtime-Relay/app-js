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
const VALID_RULE_TYPES = ["DEVICE", "RULE", "ORG"];
const VALID_ALERT_STATES = ["fire", "resolved", "ack"];

export class AlertManager {
  #ctx;
  #codec = JSONCodec();
  #listenConsumers = new Map(); // ruleId -> consumer[]
  #streamConsumers = new Map(); // token -> consumer[] (from .stream() — one consumer per filter_subject)
  #ephemeralEngines = new Map(); // ruleId -> EphemeralEngine
  #alertMetadata = new Map(); // alertId -> { type, rule }
  #refreshing = null;          // de-dupes concurrent list() refreshes triggered by getById()

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
      this.#alertMetadata.set(data.id, { type: data.type || "THRESHOLD", rule: data });
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
      this.#alertMetadata.set(data.id, { type: "EPHEMERAL", rule: data });
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
        this.#alertMetadata.set(a.id, { type: a.type || "THRESHOLD", rule: a });
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
        rule: res.data,
      });
    }

    // Return correct wrapper based on type
    if (res.data.type === "EPHEMERAL") {
      return this.#wrapEphemeralAlert(res.data);
    }

    return this.#wrapAlert(res.data);
  }

  // ─── Cache lookup by ID ──────────────────────────────────

  /**
   * Returns the cached rule for `ruleId`, or `null` if not present.
   * Synchronous — no network. Useful inside tight callbacks (e.g. an alert
   * event listener) where the cache has already been warmed via `list()`.
   */
  getCachedById(ruleId) {
    return this.#alertMetadata.get(ruleId)?.rule ?? null;
  }

  /**
   * Returns the rule for `ruleId`. On a cache miss, refreshes the cache
   * via `list()` once (concurrent calls share the in-flight refresh), then
   * tries again. Returns `null` if the rule still cannot be found.
   *
   * Use this in alert-event listeners that subscribe to a wildcard subject
   * — when an event for a previously-unseen rule arrives, the cache will
   * self-heal on first access.
   */
  async getById(ruleId) {
    validateConnected(this.#ctx.connected);
    if (!ruleId) throw new Error("ruleId is required");

    const hit = this.#alertMetadata.get(ruleId);
    if (hit?.rule) return hit.rule;

    if (!this.#refreshing) {
      this.#refreshing = this.list().finally(() => {
        this.#refreshing = null;
      });
    }
    await this.#refreshing;

    return this.#alertMetadata.get(ruleId)?.rule ?? null;
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
      throw new Error("rule_type must be DEVICE, RULE, or ORG");
    }
    // Companion-field rules per rule_type:
    //   DEVICE → require device_ident (single) or device_idents (array)
    //   RULE   → require rule_id; device_ident(s) optional (narrows)
    //   ORG    → no required companion; device_idents and rule_id both optional
    if (params.rule_type === "DEVICE") {
      const hasSingle = !!params.device_ident;
      const hasArray =
        Array.isArray(params.device_idents) && params.device_idents.length > 0;
      if (!hasSingle && !hasArray) {
        throw new Error(
          "device_ident or device_idents is required for rule_type DEVICE",
        );
      }
    }
    if (params.rule_type === "RULE" && !params.rule_id) {
      throw new Error("rule_id is required for rule_type RULE");
    }
    if (params.device_idents !== undefined) {
      if (
        !Array.isArray(params.device_idents) ||
        params.device_idents.length === 0
      ) {
        throw new Error("device_idents must be a non-empty array of strings");
      }
      for (const ident of params.device_idents) {
        validateIdent(ident, "device_idents[]");
      }
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

    // Resolve idents to ids in parallel. Both `device_ident` (single) and
    // `device_idents` (array) are accepted; the array form is forwarded to
    // the backend as `device_ids`. Single-ident callers continue to receive
    // the legacy `device_id` field for backwards compat.
    if (params.device_ident) {
      payload.device_id = await this.#ctx.device.resolveDeviceId(
        params.device_ident,
      );
    }
    if (Array.isArray(params.device_idents) && params.device_idents.length > 0) {
      payload.device_ids = await Promise.all(
        params.device_idents.map((ident) =>
          this.#ctx.device.resolveDeviceId(ident),
        ),
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

    // Each frame: { last, data: { <state>: { value, timestamp, incident_id, rule_id, device_id } } }
    // Flatten into a chronological event list. rule_id and device_id come from
    // the row tags (added in the iterator) and are populated for org-wide
    // queries where the caller doesn't already know which rule/device emitted.
    const events = [];
    for (const frame of result.frames) {
      if (!frame.data) continue;
      for (const [state, point] of Object.entries(frame.data)) {
        events.push({
          state,
          value: point.value,
          timestamp: point.timestamp,
          incident_id: point.incident_id ?? null,
          rule_id: point.rule_id ?? null,
          device_id: point.device_id ?? null,
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

  // ─── Global stream (all rules, with optional filters) ────

  /**
   * Subscribe to alert lifecycle events (fire / resolved / ack) across ALL
   * rules in the org, with optional client-side filters. To stop, call the
   * returned `off()`.
   *
   * Two JetStream consumers are created per call — one per filter_subject —
   * so each consumer carries a single subject filter that the org's
   * permission policy can match cleanly:
   *   - import.{orgID}.{env}.alerts.listen.*.*   (backend-emitted events)
   *   - {orgID}.{env}.alerts.listen.*.*           (ephemeral owner-emitted events)
   * The two streams share one in-process callback path, so consumers see a
   * single merged event feed.
   *
   * Filters are AND-combined. All optional. If a filter is omitted, that
   * dimension is unrestricted.
   *
   *   filters: {
   *     ruleIds:       string[]   — match by rule id (extracted from subject)
   *     deviceIdents:  string[]   — match against payload `device_id`,
   *                                  resolved from idents via the device cache
   *     groupIds:      string[]   — match `rule.config.scope.value` when
   *                                  `scope.type` is LOGICAL_GROUP or HEIRARCHY
   *   }
   *
   * The callback receives a uniform AlertStreamEvent for every state:
   *   {
   *     state:         "fire" | "resolved" | "ack",
   *     rule_id:       string,
   *     rule_name:     string,
   *     rule_type:     "THRESHOLD" | "RATE_CHANGE" | "EPHEMERAL",
   *     device_id:     string,                          // "" on resolved
   *     device_ident:  string | undefined,              // reverse-resolved via device cache
   *     incident_id:   string | null,
   *     timestamp:     number,                          // unix ms
   *     rolling_state?: any,                            // present on fire/resolved (post-migration)
   *     last_value?:   any,                             // present on fire/resolved (pre-migration backend)
   *     ack?:          { acked_by, acked_at, ack_notes } // present on ack
   *   }
   *
   * @returns {Promise<{ off: () => Promise<void> }>}
   */
  async stream({ filters = {}, callback } = {}) {
    validateConnected(this.#ctx.connected);
    validateFunction(callback, "callback");

    if (filters.ruleIds !== undefined && !Array.isArray(filters.ruleIds)) {
      throw new Error("filters.ruleIds must be an array");
    }
    if (
      filters.deviceIdents !== undefined &&
      !Array.isArray(filters.deviceIdents)
    ) {
      throw new Error("filters.deviceIdents must be an array");
    }
    if (filters.groupIds !== undefined && !Array.isArray(filters.groupIds)) {
      throw new Error("filters.groupIds must be an array");
    }

    const ruleIdSet =
      filters.ruleIds && filters.ruleIds.length > 0
        ? new Set(filters.ruleIds)
        : null;
    const groupIdSet =
      filters.groupIds && filters.groupIds.length > 0
        ? new Set(filters.groupIds)
        : null;

    // Resolve device idents up-front so we can match payload `device_id`
    // server-side IDs against caller-provided idents.
    let deviceIdSet = null;
    if (filters.deviceIdents && filters.deviceIdents.length > 0) {
      deviceIdSet = new Set();
      for (const ident of filters.deviceIdents) {
        validateIdent(ident, "filters.deviceIdents[]");
        try {
          const id = await this.#ctx.device.resolveDeviceId(ident);
          deviceIdSet.add(id);
        } catch {
          // Unfound idents silently drop; nothing to match against.
        }
      }
    }

    const orgID = this.#ctx.orgID;
    const env = this.#ctx.env;

    // Two separate filter_subject (singular) values, one consumer each, so
    // each consumer's permission scope matches a single concrete subject.
    const filterSubjects = [
      `import.${orgID}.${env}.alerts.listen.*.*`,
      `${orgID}.${env}.alerts.listen.*.*`,
    ];

    const token = `appjs_alert_stream_${crypto.randomUUID()}`;

    // Reverse device_id → ident lookup using the SDK's device cache.
    const reverseIdent = (deviceId) => {
      if (!deviceId) return undefined;
      for (const [ident, dev] of this.#ctx.device.cache) {
        if (dev?.id === deviceId) return ident;
      }
      return undefined;
    };

    const handleMessage = async (msg) => {
      msg.working();
      try {
        const data = msgpackDecode(msg.data);
        msg.ack();

        const tokens = msg.subject.split(".");
        const ruleId = tokens[tokens.length - 2];
        const state = tokens[tokens.length - 1];

        if (!VALID_ALERT_STATES.includes(state)) return;

        // Rule filter (cheap subject-based check first).
        if (ruleIdSet && !ruleIdSet.has(ruleId)) return;

        // Look up rule (cache hit normally; auto-refreshes on miss).
        const rule = await this.getById(ruleId);
        if (!rule) return;

        // Group filter — only meaningful for LOGICAL_GROUP / HEIRARCHY scopes.
        if (groupIdSet) {
          const scope = rule.config?.scope;
          if (!scope) return;
          if (scope.type !== "LOGICAL_GROUP" && scope.type !== "HEIRARCHY")
            return;
          if (!groupIdSet.has(scope.value)) return;
        }

        // Device filter — payload's device_id is server-side. resolved
        // events have device_id="" since they aren't tied to one device.
        const eventDeviceId = data.device_id ?? "";
        if (deviceIdSet) {
          if (!eventDeviceId || !deviceIdSet.has(eventDeviceId)) return;
        }

        const deviceIdent = reverseIdent(eventDeviceId);

        // Build the uniform event payload (snake_case to match SDK convention).
        const event = {
          state,
          rule_id: ruleId,
          rule_name: rule.name,
          rule_type: rule.type || "THRESHOLD",
          device_id: eventDeviceId,
          device_ident: deviceIdent,
          incident_id: data.incident_id ?? null,
          timestamp:
            typeof data.timestamp === "number"
              ? data.timestamp
              : (data.ack?.acked_at ?? Date.now()),
        };

        if (state === "ack" && data.ack) {
          event.ack = {
            acked_by: data.ack.acked_by,
            acked_at: data.ack.acked_at,
            ack_notes: data.ack.ack_notes ?? null,
          };
        } else if (data.rolling_state) {
          // Spec shape (ephemeral + post-migration backend).
          event.rolling_state = data.rolling_state;
        } else if (data.last_value !== undefined) {
          // Backwards compat: pre-migration backend events publish
          // last_value: { value, field_name } instead of rolling_state.
          event.last_value = data.last_value;
        }

        callback(event);
      } catch (err) {
        // A bad message must not blow up either consumer.
        // eslint-disable-next-line no-console
        console.error("[alert.stream] callback error", err);
      }
    };

    // Create the two consumers in parallel.
    const consumers = await Promise.all(
      filterSubjects.map((subject, i) =>
        this.#ctx.jetstream.consumers.get(`${orgID}_stream`, {
          name: `${token}_${i}`,
          filter_subjects: subject,
          replay_policy: "instant",
          opt_start_time: new Date(),
          ack_policy: "explicit",
          delivery_policy: "new",
        }),
      ),
    );

    // Start consuming on both. consume() doesn't resolve, so don't await.
    for (const consumer of consumers) {
      consumer.consume({ callback: handleMessage });
    }

    this.#streamConsumers.set(token, consumers);

    return {
      off: async () => {
        const list = this.#streamConsumers.get(token);
        if (!list) return;
        this.#streamConsumers.delete(token);
        await Promise.all(
          list.map(async (c) => {
            try {
              await c.delete();
            } catch {
              /* swallow — best-effort cleanup */
            }
          }),
        );
      },
    };
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

    for (const [, consumers] of this.#streamConsumers) {
      for (const consumer of consumers) {
        try {
          await consumer.delete();
        } catch {
          /* swallow — best-effort cleanup */
        }
      }
    }
    this.#streamConsumers.clear();

    for (const [, engine] of this.#ephemeralEngines) {
      await engine.stop();
    }
    this.#ephemeralEngines.clear();
  }
}
