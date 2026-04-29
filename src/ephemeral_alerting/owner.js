import {
  decode as msgpackDecode,
  encode as msgpackEncode,
} from "@msgpack/msgpack";
import { AckPolicy, ReplayPolicy } from "nats.ws";
import { DeliverPolicy } from "@nats-io/jetstream";
import {
  SUBJECT_DEVICE_INDEX,
  SUBJECT_LAST_TOKEN_INDEX,
  codec,
  buildDataSubject,
  resolveIdentFromId,
  buildAlertPayload,
  isMuted,
  createFreshState,
  dispatchNotifications,
  publishEvent,
} from "./shared.js";

export class EphemeralOwner {
  #ctx;
  #rule;
  #evaluator;
  #callbacks;
  #dataConsumer = null;
  #rpcSubscriptions = [];
  #rollingState = {};
  #kvBucket = null;
  #heartbeatInterval = null;
  #recoveryInterval = null;
  #lastDataAt = null;
  // Single rule-global state. Ephemeral alerts run with ONE owner and ONE
  // user-supplied evaluator that judges the entire rolling state — the
  // incident is scoped to the rule, not to any specific device.
  #state;
  #running = true;

  constructor(ctx, rule, evaluator, callbacks) {
    this.#ctx = ctx;
    this.#rule = rule;
    this.#evaluator = evaluator;
    this.#callbacks = callbacks;
    this.#state = createFreshState();
  }

  async start() {
    const lockAcquired = await this.#acquireLock();
    if (!lockAcquired) {
      this.#running = false;
      throw new Error("Evaluator already active for this rule");
    }

    this.#startHeartbeat();
    this.#startRecoveryCheck();
    await this.#subscribeDataTopic();
    await this.#subscribeRPCs();
  }

  async stop() {
    this.#running = false;

    if (this.#recoveryInterval) {
      clearInterval(this.#recoveryInterval);
      this.#recoveryInterval = null;
    }

    if (this.#dataConsumer) {
      await this.#dataConsumer.delete();
      this.#dataConsumer = null;
    }

    for (const sub of this.#rpcSubscriptions) {
      try {
        await sub.drain();
      } catch (err) {
        this.#ctx.logger.error("Failed to drain RPC subscription", err);
      }
    }
    this.#rpcSubscriptions = [];

    await this.#releaseLock();

    this.#rollingState = {};
    this.#state = createFreshState();
  }

  // ─── Local Ack/AckAll ────────────────────────────────────

  // Local ack — targets the rule's current incident. State machine is
  // rule-scoped, but the audit event we publish carries `device_id` so it's
  // tagged correctly in Influx and shows up in `rule_type: "DEVICE"` history
  // queries.
  async ack(deviceId, ackedBy, ackNotes = null) {
    if (this.#state.status !== "alerting") return false;

    this.#state.status = "acknowledged";
    this.#state.acked_by = ackedBy;
    this.#state.acked_at = Date.now();
    this.#state.ack_notes = ackNotes;

    const payload = {
      status: "acknowledged",
      device_id: deviceId,
      incident_id: this.#state.incident_id,
      ack: {
        acked_by: ackedBy,
        ack_notes: ackNotes,
        acked_at: this.#state.acked_at,
      },
    };

    await publishEvent(this.#ctx, this.#rule, "ack", payload);

    if (this.#callbacks.onAck) {
      this.#callbacks.onAck(payload);
    }

    return true;
  }

  // ─── Data Subscription ───────────────────────────────────

  async #subscribeDataTopic() {
    const subject = await buildDataSubject(this.#ctx, this.#rule);

    this.#dataConsumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `eph_alert_${crypto.randomUUID()}`,
        filter_subjects: subject,
        replay_policy: ReplayPolicy.Instant,
        opt_start_time: new Date(),
        ack_policy: AckPolicy.Explicit,
        delivery_policy: DeliverPolicy.New,
      },
    );

    await this.#dataConsumer.consume({
      callback: async (msg) => {
        msg.working();
        const data = msgpackDecode(msg.data);
        msg.ack();
        this.#lastDataAt = Date.now();
        this.#updateRollingState(msg.subject, data);
        await this.#evaluate(msg.subject, data);
      },
    });
  }

  // ─── RPC Subscription ────────────────────────────────────

  async #subscribeRPCs() {
    const ruleId = this.#rule.id;
    const subject = `${this.#ctx.orgID}.${this.#ctx.env}.alerts.custom.${ruleId}.*`;

    const sub = this.#ctx.natsClient.subscribe(subject);
    this.#rpcSubscriptions.push(sub);

    (async () => {
      for await (const msg of sub) {
        try {
          const tokens = msg.subject.split(".");
          const lastToken = tokens[tokens.length - 1];

          switch (lastToken) {
            case "ack":
              this.#handleAckRPC(msg);
              break;
            case "mute":
              this.#handleMuteRPC(msg);
              break;
            default:
              break;
          }
        } catch (err) {
          this.#ctx.logger.error("Error processing RPC message", err);
        }
      }
    })();
  }

  // RPC handler for remote ack. State machine is rule-scoped; we forward the
  // listener-supplied device_id into the audit event so the ack row gets
  // tagged correctly in Influx.
  #handleAckRPC(msg) {
    const data = msgpackDecode(msg.data);

    if (this.#state.status !== "alerting") {
      msg.respond(
        codec.encode({ status: "ACK_FAILED", reason: "not in alerting state" }),
      );
      return;
    }

    this.#state.status = "acknowledged";
    this.#state.acked_by = data.acked_by;
    this.#state.acked_at = Date.now();
    this.#state.ack_notes = data.ack_notes || null;

    const payload = {
      status: "acknowledged",
      device_id: data.device_id,
      incident_id: this.#state.incident_id,
      ack: {
        acked_by: data.acked_by,
        ack_notes: data.ack_notes || null,
        acked_at: this.#state.acked_at,
      },
    };

    publishEvent(this.#ctx, this.#rule, "ack", payload);
    if (this.#callbacks.onAck) this.#callbacks.onAck(payload);
    msg.respond(codec.encode({ status: "ACK_SUCCESS" }));
  }

  #handleMuteRPC(msg) {
    const data = msgpackDecode(msg.data);
    const muteConfig = data.mute_config;

    if (muteConfig?.type === "CLEAR" || muteConfig === null) {
      this.#rule.alert_mute_config = null;
    } else {
      this.#rule.alert_mute_config = muteConfig;
    }

    this.#syncMuteToBackend();
    msg.respond(codec.encode({ status: "MUTE_SUCCESS" }));
  }

  async #syncMuteToBackend() {
    try {
      const muteConfig = this.#rule.alert_mute_config;
      const payload = muteConfig
        ? {
            rule_id: this.#rule.id,
            type: muteConfig.type,
            mute_till: muteConfig.mute_till,
          }
        : { rule_id: this.#rule.id, type: "CLEAR" };

      await this.#ctx.natsClient.request(
        `api.iot.alerts.${this.#ctx.orgID}.mute`,
        codec.encode(payload),
        { timeout: 10000 },
      );
    } catch (err) {
      // Sync failure should not block mute operation
      this.#ctx.logger.error("Failed to sync mute to backend", err);
    }
  }

  // ─── Rolling State ───────────────────────────────────────

  #updateRollingState(subject, data) {
    const tokens = subject.split(".");
    const source = this.#rule.config.topic.source;

    const deviceIdIdx = SUBJECT_DEVICE_INDEX[source];
    const lastTokenIdx = SUBJECT_LAST_TOKEN_INDEX[source];

    const deviceId = tokens[deviceIdIdx];
    const lastToken = tokens[lastTokenIdx];

    const ident = resolveIdentFromId(this.#ctx, deviceId) || deviceId;

    if (!this.#rollingState[ident]) {
      this.#rollingState[ident] = {};
    }

    if (source === "TELEMETRY") {
      this.#rollingState[ident][lastToken] = {
        value: data.value,
        timestamp: data.timestamp || Date.now(),
      };
    } else {
      this.#rollingState[ident][lastToken] = data;
    }
  }

  // ─── State Machine ───────────────────────────────────────

  async #evaluate(subject, data) {
    if (!this.#running || !this.#evaluator) return;

    const tokens = subject.split(".");
    const source = this.#rule.config.topic.source;

    const deviceIdIdx = SUBJECT_DEVICE_INDEX[source];

    const deviceId = tokens[deviceIdIdx];

    const now = Date.now();
    const durationMs = (this.#rule.config.duration || 0) * 1000;
    const recoveryMs = (this.#rule.config.recovery_duration || 0) * 1000;
    const cooldownMs = (this.#rule.config.cooldown || 0) * 1000;

    if (isMuted(this.#rule)) return;

    // Check staleness
    if (this.#state.last_evaluated_at !== null) {
      const gap = now - this.#state.last_evaluated_at;
      if (gap > durationMs && durationMs > 0) {
        this.#state.breached_since = null;
        this.#state.clear_since = null;
      }
    }

    let breached;
    try {
      breached = this.#evaluator(this.#rollingState);
    } catch (err) {
      if (this.#callbacks.onError) this.#callbacks.onError(err);
      return;
    }

    if (typeof breached !== "boolean") {
      if (this.#callbacks.onError) {
        this.#callbacks.onError(
          new Error(`Evaluator must return a boolean, got ${typeof breached}`),
        );
      }
      return;
    }

    if (breached) {
      this.#state.clear_since = null;

      if (this.#state.breached_since === null) {
        this.#state.breached_since = now;
      }

      const heldFor = now - this.#state.breached_since;

      if (this.#state.status === "normal" && heldFor >= durationMs) {
        // Open a new incident on every normal → alerting transition.
        this.#state.status = "alerting";
        this.#state.last_fired = now;
        this.#state.incident_id = crypto.randomUUID();

        await this.#publishFire(now, deviceId);

        await dispatchNotifications(this.#ctx, this.#rule, {
          alert: {
            id: this.#rule.id,
            name: this.#rule.name,
            config: this.#rule.config,
          },
          device_id: deviceId,
          incident_id: this.#state.incident_id,
          last_value: this.#rollingState,
          timestamp: Date.now(),
        });

        if (this.#callbacks.onFire) {
          this.#callbacks.onFire(
            buildAlertPayload(
              this.#rule,
              this.#rollingState,
              now,
              deviceId,
              this.#state.incident_id,
            ),
          );
        }
      } else if (
        this.#state.status === "alerting" &&
        now - this.#state.last_fired >= cooldownMs
      ) {
        this.#state.last_fired = now;

        await this.#publishFire(now, deviceId);

        await dispatchNotifications(this.#ctx, this.#rule, {
          alert: {
            id: this.#rule.id,
            name: this.#rule.name,
            config: this.#rule.config,
          },
          device_id: deviceId,
          incident_id: this.#state.incident_id,
          last_value: this.#rollingState,
          timestamp: Date.now(),
        });

        if (this.#callbacks.onFire) {
          this.#callbacks.onFire(
            buildAlertPayload(
              this.#rule,
              this.#rollingState,
              now,
              deviceId,
              this.#state.incident_id,
            ),
          );
        }
      } else if (
        this.#state.status === "acknowledged" &&
        now - this.#state.last_fired >= cooldownMs
      ) {
        // Audit-only fire — incident is acked, suppress notification AND
        // skip the onFire callback. The export event still goes out so the
        // server-side audit log shows the breach is ongoing.
        this.#state.last_fired = now;
        await this.#publishFire(now, deviceId);
      }
    } else {
      this.#state.breached_since = null;

      if (this.#state.clear_since === null) {
        this.#state.clear_since = now;
      }

      const clearedFor = now - this.#state.clear_since;

      if (
        (this.#state.status === "alerting" ||
          this.#state.status === "acknowledged") &&
        clearedFor >= recoveryMs
      ) {
        // Capture closing incident_id BEFORE clearing so the resolved
        // event ties to the correct incident.
        const closingIncidentId = this.#state.incident_id;

        await this.#publishResolved(now, deviceId, closingIncidentId);

        await dispatchNotifications(this.#ctx, this.#rule, {
          alert: {
            id: this.#rule.id,
            name: this.#rule.name,
            config: this.#rule.config,
          },
          device_id: "",
          incident_id: closingIncidentId,
          last_value: this.#rollingState,
          timestamp: Date.now(),
        });

        if (this.#callbacks.onResolved) {
          this.#callbacks.onResolved(
            buildAlertPayload(
              this.#rule,
              this.#rollingState,
              now,
              deviceId,
              closingIncidentId,
            ),
          );
        }

        this.#state.status = "normal";
        this.#state.acked_by = null;
        this.#state.acked_at = null;
        this.#state.ack_notes = null;
        this.#state.breached_since = null;
        this.#state.clear_since = null;
        this.#state.incident_id = null;
      }
    }

    this.#state.last_evaluated_at = now;
  }

  async #publishFire(timestamp, deviceId) {
    await publishEvent(
      this.#ctx,
      this.#rule,
      "fire",
      buildAlertPayload(
        this.#rule,
        this.#rollingState,
        timestamp,
        deviceId,
        this.#state.incident_id,
      ),
    );
  }

  async #publishResolved(timestamp, deviceId, incidentId) {
    await publishEvent(
      this.#ctx,
      this.#rule,
      "resolved",
      buildAlertPayload(
        this.#rule,
        this.#rollingState,
        timestamp,
        deviceId,
        incidentId ?? this.#state.incident_id,
      ),
    );
  }

  // ─── KV Lock ─────────────────────────────────────────────

  async #acquireLock() {
    const kv = this.#ctx.kvBucket;
    if (!kv) return true;

    this.#kvBucket = kv;
    const key = `ephemeral_owner_${this.#rule.id}`;

    const now = Date.now();
    const ownerId = crypto.randomUUID();
    const lockValue = JSON.stringify({
      owner_id: ownerId,
      started_at: now,
      expires_at: now + 30000,
    });

    // Check if lock exists and is active
    try {
      const entry = await kv.get(key);
      const str = entry?.string();

      if (str && str.length > 0) {
        const lockData = JSON.parse(str);

        if (lockData.expires_at && now > lockData.expires_at) {
          // Expired — take over with CAS
          await kv.update(key, lockValue, entry.revision);
          return true;
        } else {
          return false;
        }
      }
    } catch (err) {
      // Key not found
    }

    // No active lock — write and verify
    try {
      await kv.put(key, lockValue);

      // Re-read to verify we own it (handles race with another writer)
      const verify = await kv.get(key);
      const verifyData = JSON.parse(verify.string());

      if (verifyData.owner_id === ownerId) {
        return true;
      }

      return false;
    } catch (err) {
      return false;
    }
  }

  #startHeartbeat() {
    if (!this.#kvBucket) return;

    const key = `ephemeral_owner_${this.#rule.id}`;

    this.#heartbeatInterval = setInterval(async () => {
      try {
        const value = JSON.stringify({
          started_at: Date.now(),
          expires_at: Date.now() + 30000,
        });
        await this.#kvBucket.put(key, value);
      } catch (err) {
        // Heartbeat failure — lock may be lost
        this.#ctx.logger.error("Heartbeat tick failed", err);
      }
    }, 15000);
  }

  #startRecoveryCheck() {
    const recoveryEvalType = this.#rule.config?.recovery_eval_type || "VALUE";
    if (recoveryEvalType !== "TIMER") return;

    const recoveryS = this.#rule.config?.recovery_duration || 0;
    const recoveryMs = recoveryS * 1000;

    const tickMs =
      (recoveryS > 0 ? Math.max(1, Math.min(30, recoveryS / 2)) : 5) * 1000;

    this.#recoveryInterval = setInterval(async () => {
      if (!this.#running) return;

      if (
        this.#state.status !== "alerting" &&
        this.#state.status !== "acknowledged"
      )
        return;

      if (this.#lastDataAt === null) return;

      const now = Date.now();
      const silenceMs = now - this.#lastDataAt;

      if (silenceMs >= recoveryMs) {
        const closingIncidentId = this.#state.incident_id;
        const resolvedPayload = buildAlertPayload(
          this.#rule,
          this.#rollingState,
          now,
          "",
          closingIncidentId,
        );
        await publishEvent(this.#ctx, this.#rule, "resolved", resolvedPayload);

        await dispatchNotifications(this.#ctx, this.#rule, {
          alert: {
            id: this.#rule.id,
            name: this.#rule.name,
            config: this.#rule.config,
          },
          device_id: "",
          incident_id: closingIncidentId,
          last_value: this.#rollingState,
          timestamp: now,
        });

        if (this.#callbacks.onResolved) {
          this.#callbacks.onResolved(resolvedPayload);
        }

        this.#state.status = "normal";
        this.#state.acked_by = null;
        this.#state.acked_at = null;
        this.#state.ack_notes = null;
        this.#state.breached_since = null;
        this.#state.clear_since = null;
        this.#state.incident_id = null;
      }
    }, tickMs);
  }

  async #releaseLock() {
    if (this.#heartbeatInterval) {
      clearInterval(this.#heartbeatInterval);
      this.#heartbeatInterval = null;
    }

    if (this.#kvBucket) {
      try {
        await this.#kvBucket.purge(`ephemeral_owner_${this.#rule.id}`);
      } catch (err) {
        // Ignore — key may not exist
        this.#ctx.logger.error("Failed to release lock", err);
      }
      this.#kvBucket = null;
    }
  }

  // ─── Getters ─────────────────────────────────────────────

  get state() {
    return { ...this.#state };
  }
  get rollingState() {
    return { ...this.#rollingState };
  }
}
