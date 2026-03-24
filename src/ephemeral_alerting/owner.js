import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack';
import { AckPolicy, ReplayPolicy } from 'nats.ws';
import { DeliverPolicy } from "@nats-io/jetstream";
import {
    SUBJECT_DEVICE_INDEX, SUBJECT_LAST_TOKEN_INDEX,
    codec, buildDataSubject, resolveIdentFromId, buildAlertPayload,
    isMuted, createFreshState, dispatchNotifications, publishEvent,
} from './shared.js';

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
            throw new Error('Evaluator already active for this rule');
        }

        this.#startHeartbeat();
        await this.#subscribeDataTopic();
        await this.#subscribeRPCs();
    }

    async stop() {
        this.#running = false;

        if (this.#dataConsumer) {
            await this.#dataConsumer.delete();
            this.#dataConsumer = null;
        }

        for (const sub of this.#rpcSubscriptions) {
            try { await sub.drain(); } catch { /* ignore */ }
        }
        this.#rpcSubscriptions = [];

        await this.#releaseLock();

        this.#rollingState = {};
        this.#state = createFreshState();
    }

    // ─── Local Ack/AckAll ────────────────────────────────────

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

        await publishEvent(this.#ctx, this.#rule, 'ack', payload);
        
        if (this.#callbacks.onAck){
            this.#callbacks.onAck(payload);
        }

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

        await publishEvent(this.#ctx, this.#rule, 'ack_all', payload);
        
        if (this.#callbacks.onAckAll){
            this.#callbacks.onAckAll(payload);
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
                delivery_policy: DeliverPolicy.New
            }
        );

        await this.#dataConsumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();
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
                    const tokens = msg.subject.split('.');
                    const lastToken = tokens[tokens.length - 1];

                    switch (lastToken) {
                        case 'ack': this.#handleAckRPC(msg); break;
                        case 'ack_all': this.#handleAckAllRPC(msg); break;
                        case 'mute': this.#handleMuteRPC(msg); break;
                        default: break;
                    }
                } catch { /* ignore */ }
            }
        })();
    }

    #handleAckRPC(msg) {
        const data = msgpackDecode(msg.data)

        if (this.#state.status !== 'alerting') {
            msg.respond(codec.encode({ status: 'ACK_FAILED', reason: 'not in alerting state' }));
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

        publishEvent(this.#ctx, this.#rule, 'ack', payload);
        if (this.#callbacks.onAck) this.#callbacks.onAck(payload);
        msg.respond(codec.encode({ status: 'ACK_SUCCESS' }));
    }

    #handleAckAllRPC(msg) {
        const data = msgpackDecode(msg.data)

        if (this.#state.status !== 'alerting') {
            msg.respond(codec.encode({ status: 'ACK_FAILED', reason: 'not in alerting state' }));
            return;
        }

        this.#state.status = 'acknowledged';
        this.#state.acked_by = data.acked_by;
        this.#state.acked_at = Date.now();
        this.#state.ack_notes = data.ack_notes || null;

        const payload = {
            status: 'acknowledged',
            ack: { 
                acked_by: data.acked_by,
                ack_notes: data.ack_notes || null,
                acked_at: this.#state.acked_at
            }
        };

        publishEvent(this.#ctx, this.#rule, 'ack_all', payload);
        
        if (this.#callbacks.onAckAll){
            this.#callbacks.onAckAll(payload);
        }
        
        msg.respond(codec.encode({ status: 'ACK_SUCCESS' }));
    }

    #handleMuteRPC(msg) {
        const data = msgpackDecode(msg.data)
        const muteConfig = data.mute_config;

        if (muteConfig?.type === 'CLEAR' || muteConfig === null) {
            this.#rule.alert_mute_config = null;
        } else {
            this.#rule.alert_mute_config = muteConfig;
        }

        this.#syncMuteToBackend();
        msg.respond(codec.encode({ status: 'MUTE_SUCCESS' }));
    }

    async #syncMuteToBackend() {
        try {
            const muteConfig = this.#rule.alert_mute_config;
            const payload = muteConfig
                ? { rule_id: this.#rule.id, type: muteConfig.type, mute_till: muteConfig.mute_till }
                : { rule_id: this.#rule.id, type: 'CLEAR' };

            await this.#ctx.natsClient.request(
                `api.iot.alerts.${this.#ctx.orgID}.mute`,
                codec.encode(payload),
                { timeout: 10000 }
            );
        } catch {
            // Sync failure should not block mute operation
        }
    }

    // ─── Rolling State ───────────────────────────────────────

    #updateRollingState(subject, data) {
        const tokens = subject.split('.');
        const source = this.#rule.config.topic.source;

        const deviceIdIdx = SUBJECT_DEVICE_INDEX[source];
        const lastTokenIdx = SUBJECT_LAST_TOKEN_INDEX[source];

        const deviceId = tokens[deviceIdIdx];
        const lastToken = tokens[lastTokenIdx];

        const ident = resolveIdentFromId(this.#ctx, deviceId) || deviceId;

        if (!this.#rollingState[ident]) {
            this.#rollingState[ident] = {};
        }

        if (source === 'TELEMETRY') {
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

        const tokens = subject.split('.');
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
                
                await this.#publishFire(now, deviceId);
                
                await dispatchNotifications(this.#ctx, this.#rule, {
                    alert: {
                        id: this.#rule.id,
                        name: this.#rule.name,
                        config: this.#rule.config
                    },
                    device_id: deviceId,
                    last_value: this.#rollingState,
                    timestamp: Date.now()
                });
                
                if (this.#callbacks.onFire) {
                    this.#callbacks.onFire(buildAlertPayload(this.#rule, this.#rollingState, now, deviceId));
                }
            } else if (this.#state.status === 'alerting' && (now - this.#state.last_fired) >= cooldownMs) {
                this.#state.last_fired = now;
                
                await this.#publishFire(now, deviceId);
                
                await dispatchNotifications(this.#ctx, this.#rule, {
                    alert: {
                        id: this.#rule.id,
                        name: this.#rule.name,
                        config: this.#rule.config
                    },
                    device_id: deviceId,
                    last_value: this.#rollingState,
                    timestamp: Date.now()
                });
                
                if (this.#callbacks.onFire) {
                    this.#callbacks.onFire(buildAlertPayload(this.#rule, this.#rollingState, now, deviceId));
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
                await this.#publishResolved(now, deviceId);
                
                await dispatchNotifications(this.#ctx, this.#rule, {
                    alert: {
                        id: this.#rule.id,
                        name: this.#rule.name,
                        config: this.#rule.config
                    },
                    device_id: "",
                    last_value: this.#rollingState,
                    timestamp: Date.now()
                });
                
                if (this.#callbacks.onResolved) {
                    this.#callbacks.onResolved(buildAlertPayload(this.#rule, this.#rollingState, now, deviceId));
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

    async #publishFire(timestamp, deviceId) {
        await publishEvent(this.#ctx, this.#rule, 'fire', buildAlertPayload(this.#rule, this.#rollingState, timestamp, deviceId));
    }

    async #publishResolved(timestamp, deviceId) {
        await publishEvent(this.#ctx, this.#rule, 'resolved', buildAlertPayload(this.#rule, this.#rollingState, timestamp, deviceId));
    }

    // ─── KV Lock ─────────────────────────────────────────────

    async #acquireLock() {
        const kv = this.#ctx.kvBucket;
        if (!kv) return true;

        this.#kvBucket = kv;
        const key = `ephemeral_owner_${this.#rule.id}`;

        try {
            const entry = await kv.get(key);
            const lockData = entry.json();

            if (lockData.expires_at && Date.now() > lockData.expires_at) {
                await kv.delete(key);
            } else {
                return false;
            }
        } catch (err) {
            if (err.code !== 'KEY_NOT_FOUND' && !err.message?.includes('not found')) {
                return true;
            }
        }

        try {
            const value = JSON.stringify({
                started_at: Date.now(),
                expires_at: Date.now() + 30000,
            });
            await kv.create(key, value);
            return true;
        } catch (err) {
            if (err.code === 'KEY_EXISTS' || err.message?.includes('already exists')) {
                return false;
            }
            return true;
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

    // ─── Getters ─────────────────────────────────────────────

    get state() { return { ...this.#state }; }
    get rollingState() { return { ...this.#rollingState }; }
}
