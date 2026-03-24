import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack';
import { JSONCodec } from 'nats.ws';

const SUBJECT_MAP = {
    TELEMETRY: (orgID, env, deviceId, lastToken) => `${orgID}.${env}.telemetry.${deviceId}.${lastToken}`,
    COMMAND: (orgID, env, deviceId, lastToken) => `${orgID}.${env}.command.queue.${deviceId}.${lastToken}`,
    EVENT: (orgID, env, deviceId, lastToken) => `${orgID}.${env}.events.${deviceId}.${lastToken}`,
};

// Positions of device_id and last_token in each subject type
const SUBJECT_DEVICE_INDEX = { TELEMETRY: 2, COMMAND: 3, EVENT: 2 };
const SUBJECT_LAST_TOKEN_INDEX = { TELEMETRY: 3, COMMAND: 4, EVENT: 3 };

/**
 * Client-side ephemeral alert engine with dual-mode operation.
 *
 * Owner mode (evaluator set): subscribes to data, evaluates, fires/resolves, handles RPCs
 * Listener mode (no evaluator): subscribes to alert events, delegates ack to owner via RPC
 */
export class EphemeralEngine {
    #ctx;
    #rule;
    #evaluator = null;
    #callbacks = {};
    #dataConsumer = null;
    #alertConsumer = null;       // listener mode: single wildcard consumer
    #rpcSubscriptions = [];      // owner mode: natsClient.subscribe() subs
    #codec = JSONCodec();
    #running = false;
    #mode = null;                // 'owner' | 'listener'
    #rollingState = {};
    #kvBucket = null;
    #heartbeatInterval = null;
    #state = null;

    constructor(ctx, rule) {
        this.#ctx = ctx;
        this.#rule = rule;
    }

    setEvaluator(fn) {
        if (typeof fn !== 'function') throw new Error('evaluator must be a function');
        this.#evaluator = fn;
    }

    async listen(callbacks) {
        if (this.#running) return;
        this.#callbacks = callbacks || {};
        this.#state = this.#createFreshState();
        this.#rollingState = {};
        this.#running = true;

        if (this.#evaluator) {
            this.#mode = 'owner';
            await this.#startOwnerMode();
        } else {
            this.#mode = 'listener';
            await this.#startListenerMode();
        }
    }

    async stop() {
        this.#running = false;

        // Clean up data consumer (owner mode)
        if (this.#dataConsumer) {
            await this.#dataConsumer.delete();
            this.#dataConsumer = null;
        }

        // Clean up alert consumer (listener mode)
        if (this.#alertConsumer) {
            await this.#alertConsumer.delete();
            this.#alertConsumer = null;
        }

        // Clean up RPC subscriptions (owner mode)
        for (const sub of this.#rpcSubscriptions) {
            try { await sub.drain(); } catch { /* ignore */ }
        }
        this.#rpcSubscriptions = [];

        // Release KV lock
        await this.#releaseLock();

        // Reset state
        this.#rollingState = {};
        this.#state = this.#createFreshState();
        this.#mode = null;
    }

    // ─── Owner Mode ──────────────────────────────────────────

    async #startOwnerMode() {
        // Acquire lock first — throws if already held
        const lockAcquired = await this.#acquireLock();
        if (!lockAcquired) {
            this.#running = false;
            this.#mode = null;
            throw new Error('Evaluator already active for this rule');
        }
        this.#startHeartbeat();
        await this.#subscribeDataTopic();
        await this.#subscribeRPCs();
    }

    async #subscribeDataTopic() {
        const subject = await this.#buildDataSubject();

        this.#dataConsumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_ephemeral_data_${this.#rule.id}_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        await this.#dataConsumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();
                this.#updateRollingState(msg.subject, data);
                await this.#evaluate();
            },
        });
    }

    async #subscribeRPCs() {
        const ruleId = this.#rule.id;
        const subject = `${this.#ctx.orgID}.${this.#ctx.env}.alerts.custom.${ruleId}.*`;

        const sub = this.#ctx.natsClient.subscribe(subject);
        this.#rpcSubscriptions.push(sub);

        (async () => {
            for await (const msg of sub) {
                try {
                    const tokens = msg.subject.split('.');
                    const lastToken = tokens[tokens.length - 1];

                    switch (lastToken) {
                        case 'ack': this.#handleAckRPC(msg); break;
                        case 'ack_all': this.#handleAckAllRPC(msg); break;
                        case 'mute': this.#handleMuteRPC(msg); break;
                        default: break; // Ignore unknown tokens
                    }
                } catch { /* ignore */ }
            }
        })();
    }

    #handleAckRPC(msg) {
        const data = msg.json ? msg.json() : JSON.parse(new TextDecoder().decode(msg.data));

        if (this.#state.status !== 'alerting') {
            msg.respond(this.#codec.encode({ status: 'ACK_FAILED', reason: 'not in alerting state' }));
            return;
        }

        this.#state.status = 'acknowledged';
        this.#state.acked_by = data.acked_by;
        this.#state.acked_at = Date.now();
        this.#state.ack_notes = data.ack_notes || null;

        const payload = {
            status: 'acknowledged',
            device_id: data.device_id,
            ack: { acked_by: data.acked_by, ack_notes: data.ack_notes || null, acked_at: this.#state.acked_at },
        };

        this.#ctx.publishOrBuffer(
            `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.ack`,
            msgpackEncode(payload)
        );

        if (this.#callbacks.onAck) this.#callbacks.onAck(payload);
        msg.respond(this.#codec.encode({ status: 'ACK_SUCCESS' }));
    }

    #handleAckAllRPC(msg) {
        const data = msg.json ? msg.json() : JSON.parse(new TextDecoder().decode(msg.data));

        if (this.#state.status !== 'alerting') {
            msg.respond(this.#codec.encode({ status: 'ACK_FAILED', reason: 'not in alerting state' }));
            return;
        }

        this.#state.status = 'acknowledged';
        this.#state.acked_by = data.acked_by;
        this.#state.acked_at = Date.now();
        this.#state.ack_notes = data.ack_notes || null;

        const payload = {
            status: 'acknowledged',
            ack: { acked_by: data.acked_by, ack_notes: data.ack_notes || null, acked_at: this.#state.acked_at },
        };

        this.#ctx.publishOrBuffer(
            `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.ack_all`,
            msgpackEncode(payload)
        );

        if (this.#callbacks.onAckAll) this.#callbacks.onAckAll(payload);
        msg.respond(this.#codec.encode({ status: 'ACK_SUCCESS' }));
    }

    #handleMuteRPC(msg) {
        const data = msg.json ? msg.json() : JSON.parse(new TextDecoder().decode(msg.data));
        const muteConfig = data.mute_config;

        if (muteConfig?.type === 'CLEAR' || muteConfig === null) {
            // Unmute
            this.#rule.alert_mute_config = null;
        } else {
            // Mute (FOREVER or TIME_BASED)
            this.#rule.alert_mute_config = muteConfig;
        }

        // Also sync to backend
        this.#syncMuteToBackend();

        msg.respond(this.#codec.encode({ status: 'MUTE_SUCCESS' }));
    }

    async #syncMuteToBackend() {
        try {
            const muteConfig = this.#rule.alert_mute_config;
            const payload = muteConfig
                ? { rule_id: this.#rule.id, type: muteConfig.type, mute_till: muteConfig.mute_till }
                : { rule_id: this.#rule.id, type: 'CLEAR' };

            await this.#ctx.natsClient.request(
                `api.iot.alerts.${this.#ctx.orgID}.mute`,
                this.#codec.encode(payload),
                { timeout: 10000 }
            );
        } catch {
            // Sync failure should not block mute operation
        }
    }

    // ─── Listener Mode ───────────────────────────────────────

    async #startListenerMode() {
        const ruleId = this.#rule.id;
        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${ruleId}.*`;

        const callbackMap = { fire: 'onFire', resolved: 'onResolved', ack: 'onAck', ack_all: 'onAckAll' };

        this.#alertConsumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_ephemeral_listen_${ruleId}_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        await this.#alertConsumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();

                const tokens = msg.subject.split('.');
                const lastToken = tokens[tokens.length - 1];
                const cbName = callbackMap[lastToken];
                if (cbName && this.#callbacks[cbName]) {
                    // Transform timestamp for fire/resolved
                    if ((lastToken === 'fire' || lastToken === 'resolved') && typeof data.timestamp === 'number') {
                        data.timestamp = new Date(data.timestamp).toISOString();
                    }
                    this.#callbacks[cbName](data);
                }
            },
        });
    }

    // ─── Subject Construction ────────────────────────────────

    async #buildDataSubject() {
        const topic = this.#rule.config.topic;
        const builder = SUBJECT_MAP[topic.source];
        if (!builder) throw new Error(`Unknown topic source: ${topic.source}`);

        let deviceId = topic.device_ident;
        if (deviceId !== '*') {
            deviceId = await this.#ctx.device.resolveDeviceId(topic.device_ident);
        }

        return builder(this.#ctx.orgID, this.#ctx.env, deviceId, topic.last_token);
    }

    // ─── Rolling State ───────────────────────────────────────

    #updateRollingState(subject, data) {
        const tokens = subject.split('.');
        const source = this.#rule.config.topic.source;

        const deviceIdIdx = SUBJECT_DEVICE_INDEX[source];
        const lastTokenIdx = SUBJECT_LAST_TOKEN_INDEX[source];

        const deviceId = tokens[deviceIdIdx];
        const lastToken = tokens[lastTokenIdx];

        // Reverse-map device_id to ident
        const ident = this.#resolveIdentFromId(deviceId) || deviceId;

        if (!this.#rollingState[ident]) {
            this.#rollingState[ident] = {};
        }

        if (source === 'TELEMETRY') {
            this.#rollingState[ident][lastToken] = {
                value: data.value,
                timestamp: data.timestamp || Date.now(),
            };
        } else {
            // COMMAND or EVENT — store full data
            this.#rollingState[ident][lastToken] = data;
        }
    }

    #resolveIdentFromId(deviceId) {
        if (!this.#ctx.device?.cache) return null;
        for (const [ident, device] of this.#ctx.device.cache) {
            if (device.id === deviceId) return ident;
        }
        return null;
    }

    // ─── State Machine ───────────────────────────────────────

    async #evaluate() {
        if (!this.#running || !this.#evaluator) return;

        const now = Date.now();
        const durationMs = (this.#rule.config.duration || 0) * 1000;
        const recoveryMs = (this.#rule.config.recovery_duration || 0) * 1000;
        const cooldownMs = (this.#rule.config.cooldown || 0) * 1000;

        if (this.#isMuted()) return;

        // Check staleness
        if (this.#state.last_evaluated_at !== null) {
            const gap = now - this.#state.last_evaluated_at;
            if (gap > durationMs && durationMs > 0) {
                this.#state.breached_since = null;
                this.#state.clear_since = null;
            }
        }

        // Run evaluator with full rolling state — catch crashes to avoid killing the engine
        let breached;
        try {
            breached = this.#evaluator(this.#rollingState);
        } catch (err) {
            if (this.#callbacks.onError) this.#callbacks.onError(err);
            return;
        }

        if (typeof breached !== 'boolean') {
            if (this.#callbacks.onError) {
                this.#callbacks.onError(new Error(`Evaluator must return a boolean, got ${typeof breached}`));
            }
            return;
        }

        if (breached) {
            this.#state.clear_since = null;

            if (this.#state.breached_since === null) {
                this.#state.breached_since = now;
            }

            const heldFor = now - this.#state.breached_since;

            if (this.#state.status === 'normal' && heldFor >= durationMs) {
                this.#state.status = 'alerting';
                this.#state.last_fired = now;
                await this.#publishFire(now);
                await this.#dispatchNotifications();
                if (this.#callbacks.onFire) {
                    this.#callbacks.onFire(this.#buildAlertPayload(now));
                }
            } else if (this.#state.status === 'alerting' && (now - this.#state.last_fired) >= cooldownMs) {
                this.#state.last_fired = now;
                await this.#publishFire(now);
                await this.#dispatchNotifications();
                if (this.#callbacks.onFire) {
                    this.#callbacks.onFire(this.#buildAlertPayload(now));
                }
            }
            // acknowledged → silent
        } else {
            this.#state.breached_since = null;

            if (this.#state.clear_since === null) {
                this.#state.clear_since = now;
            }

            const clearedFor = now - this.#state.clear_since;

            if ((this.#state.status === 'alerting' || this.#state.status === 'acknowledged') && clearedFor >= recoveryMs) {
                await this.#publishResolved(now);
                await this.#dispatchNotifications();
                if (this.#callbacks.onResolved) {
                    this.#callbacks.onResolved(this.#buildAlertPayload(now));
                }
                this.#state.status = 'normal';
                this.#state.acked_by = null;
                this.#state.acked_at = null;
                this.#state.ack_notes = null;
                this.#state.breached_since = null;
                this.#state.clear_since = null;
            }
        }

        this.#state.last_evaluated_at = now;
    }

    // ─── Mute Check ──────────────────────────────────────────

    #isMuted() {
        const muteConfig = this.#rule.alert_mute_config;
        if (!muteConfig) return false;
        if (muteConfig.type === 'FOREVER') return true;
        if (muteConfig.type === 'TIME_BASED') {
            if (muteConfig.mute_till && Date.now() < muteConfig.mute_till) return true;
        }
        return false;
    }

    // ─── Payload Builders ────────────────────────────────────

    #buildAlertPayload(timestamp) {
        return {
            rule: {
                name: this.#rule.name,
                type: this.#rule.config.topic.source,
                config: this.#rule.config,
            },
            rolling_state: { ...this.#rollingState },
            timestamp,
        };
    }

    async #publishFire(timestamp) {
        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.fire`;
        await this.#ctx.publishOrBuffer(subject, msgpackEncode(this.#buildAlertPayload(timestamp)));
    }

    async #publishResolved(timestamp) {
        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.resolved`;
        await this.#ctx.publishOrBuffer(subject, msgpackEncode(this.#buildAlertPayload(timestamp)));
    }

    // ─── Notification Dispatch ───────────────────────────────

    async #dispatchNotifications() {
        const channels = this.#rule.notification_channel;
        if (!channels || channels.length === 0) return;

        try {
            await this.#ctx.natsClient.request(
                `api.iot.notification.${this.#ctx.orgID}.dispatch`,
                this.#codec.encode(channels),
                { timeout: 10000 }
            );
        } catch {
            // Notification dispatch failure should not block alerting
        }
    }

    // ─── KV Lock ─────────────────────────────────────────────

    /**
     * Acquires KV lock. Returns true if acquired, false if already held.
     * Uses expires_at in the value for TTL since the bucket is shared (orgID).
     */
    async #acquireLock() {
        const kv = this.#ctx.kvBucket;
        if (!kv) return true; // No KV available — skip locking, proceed

        this.#kvBucket = kv;
        const key = `ephemeral_owner_${this.#rule.id}`;

        // Check if lock already exists and is still valid
        try {
            const entry = await kv.get(key);
            const lockData = entry.json();

            // Check if lock has expired (30s TTL)
            if (lockData.expires_at && Date.now() > lockData.expires_at) {
                // Lock expired — delete stale key and proceed to acquire
                await kv.delete(key);
            } else {
                // Lock is still valid — another owner is active
                return false;
            }
        } catch (err) {
            // KEY_NOT_FOUND is expected — lock is free
            if (err.code !== 'KEY_NOT_FOUND' && !err.message?.includes('not found')) {
                return true; // Unexpected error — proceed without lock
            }
        }

        // Lock is free — acquire it
        try {
            const value = JSON.stringify({
                started_at: Date.now(),
                expires_at: Date.now() + 30000, // 30s TTL
            });
            await kv.create(key, value);
            return true;
        } catch (err) {
            if (err.code === 'KEY_EXISTS' || err.message?.includes('already exists')) {
                return false;
            }
            return true; // Other errors — proceed without lock
        }
    }

    #startHeartbeat() {
        if (!this.#kvBucket) return;

        const key = `ephemeral_owner_${this.#rule.id}`;

        this.#heartbeatInterval = setInterval(async () => {
            try {
                const value = JSON.stringify({
                    started_at: Date.now(),
                    expires_at: Date.now() + 30000, // Refresh 30s TTL
                });
                await this.#kvBucket.put(key, value);
            } catch {
                // Heartbeat failure — lock may be lost
            }
        }, 15000);
    }

    async #releaseLock() {
        if (this.#heartbeatInterval) {
            clearInterval(this.#heartbeatInterval);
            this.#heartbeatInterval = null;
        }

        if (this.#kvBucket) {
            try {
                await this.#kvBucket.delete(`ephemeral_owner_${this.#rule.id}`);
            } catch {
                // Ignore — key may not exist
            }
            this.#kvBucket = null;
        }
    }

    // ─── Local Ack/AckAll (Owner direct calls) ───────────────

    async ack(ackedBy, ackNotes = null) {
        if (this.#state.status !== 'alerting') return false;

        this.#state.status = 'acknowledged';
        this.#state.acked_by = ackedBy;
        this.#state.acked_at = Date.now();
        this.#state.ack_notes = ackNotes;

        const payload = {
            status: 'acknowledged',
            ack: { acked_by: ackedBy, ack_notes: ackNotes, acked_at: this.#state.acked_at },
        };

        await this.#ctx.publishOrBuffer(
            `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.ack`,
            msgpackEncode(payload)
        );

        if (this.#callbacks.onAck) this.#callbacks.onAck(payload);
        return true;
    }

    async ackAll(ackedBy, ackNotes = null) {
        if (this.#state.status !== 'alerting') return false;

        this.#state.status = 'acknowledged';
        this.#state.acked_by = ackedBy;
        this.#state.acked_at = Date.now();
        this.#state.ack_notes = ackNotes;

        const payload = {
            status: 'acknowledged',
            ack: { acked_by: ackedBy, ack_notes: ackNotes, acked_at: this.#state.acked_at },
        };

        await this.#ctx.publishOrBuffer(
            `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.ack_all`,
            msgpackEncode(payload)
        );

        if (this.#callbacks.onAckAll) this.#callbacks.onAckAll(payload);
        return true;
    }

    // ─── Helpers ─────────────────────────────────────────────

    #createFreshState() {
        return {
            status: 'normal',
            last_evaluated_at: null,
            clear_since: null,
            breached_since: null,
            last_fired: 0,
            acked_by: null,
            acked_at: null,
            ack_notes: null,
        };
    }

    get state() {
        return { ...this.#state };
    }

    get rollingState() {
        return { ...this.#rollingState };
    }

    get mode() {
        return this.#mode;
    }
}
