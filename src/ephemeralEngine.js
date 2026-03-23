import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack';
import { JSONCodec } from 'nats.ws';

/**
 * Client-side ephemeral alert evaluation engine.
 * Mirrors the backend alerting_manager/threshold_eval.js state machine.
 */
export class EphemeralEngine {
    #ctx;
    #rule;
    #evaluator;
    #callbacks = {};
    #telemetryConsumer = null;
    #alertConsumers = [];
    #codec = JSONCodec();
    #running = false;

    // State machine
    #state = null;

    constructor(ctx, rule, evaluator) {
        this.#ctx = ctx;
        this.#rule = rule;
        this.#evaluator = evaluator;
    }

    setEvaluator(fn) {
        this.#evaluator = fn;
    }

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

    async start(callbacks) {
        if (this.#running) return;
        this.#callbacks = callbacks;
        this.#state = this.#createFreshState();
        this.#running = true;

        // TODO: Load state from NATS KV when backend endpoint is ready
        // const kvState = await this.#loadStateFromKV();
        // if (kvState) this.#state = kvState;

        // Subscribe to telemetry for the device + metric
        await this.#subscribeTelemetry();

        // Subscribe to alert topics for remote events (ack, ack_all from other instances)
        await this.#subscribeAlertTopics();
    }

    async stop() {
        this.#running = false;
        if (this.#telemetryConsumer) {
            await this.#telemetryConsumer.delete();
            this.#telemetryConsumer = null;
        }
        for (const consumer of this.#alertConsumers) {
            await consumer.delete();
        }
        this.#alertConsumers = [];
    }

    async #subscribeTelemetry() {
        const deviceId = this.#rule.config.scope.value;
        // Resolve device_id to ident for telemetry subject
        let deviceIdent = null;
        for (const [ident, device] of this.#ctx.device.cache) {
            if (device.id === deviceId) {
                deviceIdent = ident;
                break;
            }
        }
        if (!deviceIdent) {
            throw new Error(`Cannot resolve device_id ${deviceId} to ident for telemetry subscription`);
        }

        const subject = `${this.#ctx.orgID}.${this.#ctx.env}.telemetry.${deviceIdent}.${this.#rule.metric}`;

        this.#telemetryConsumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_ephemeral_${this.#rule.id}_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        await this.#telemetryConsumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();
                await this.#evaluate(data);
            },
        });
    }

    async #subscribeAlertTopics() {
        const ruleId = this.#rule.id;
        const baseSubject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${ruleId}`;

        const events = ['fire', 'resolved', 'ack', 'ack_all'];
        const callbackMap = {
            fire: 'onFire',
            resolved: 'onResolved',
            ack: 'onAck',
            ack_all: 'onAckAll',
        };

        for (const event of events) {
            const subject = `${baseSubject}.${event}`;

            const consumer = await this.#ctx.jetstream.consumers.get(
                `${this.#ctx.orgID}_stream`,
                {
                    name: `appjs_ephemeral_listen_${ruleId}_${event}_${crypto.randomUUID()}`,
                    filter_subjects: subject,
                    replay_policy: 'instant',
                    opt_start_time: new Date(),
                    ack_policy: 'explicit',
                    delivery_policy: 'new',
                }
            );

            this.#alertConsumers.push(consumer);

            await consumer.consume({
                callback: async (msg) => {
                    msg.working();
                    const data = msgpackDecode(msg.data);
                    msg.ack();

                    // Handle remote ack/ack_all by updating local state
                    if (event === 'ack' || event === 'ack_all') {
                        if (this.#state.status === 'alerting') {
                            this.#state.status = 'acknowledged';
                            this.#state.acked_by = data.ack?.acked_by || null;
                            this.#state.acked_at = data.ack?.acked_at || Date.now();
                            this.#state.ack_notes = data.ack?.ack_notes || null;
                        }
                    }

                    const cbName = callbackMap[event];
                    if (this.#callbacks[cbName]) {
                        this.#callbacks[cbName](data);
                    }
                },
            });
        }
    }

    async #evaluate(telemetryData) {
        if (!this.#running || !this.#evaluator) return;

        const now = Date.now();
        const durationMs = (this.#rule.config.duration || 0) * 1000;
        const recoveryMs = (this.#rule.config.recovery_duration || 0) * 1000;
        const cooldownMs = (this.#rule.config.cooldown || 0) * 1000;

        // Check mute status
        if (this.#isMuted()) return;

        // Check staleness
        if (this.#state.last_evaluated_at !== null) {
            const gap = now - this.#state.last_evaluated_at;
            if (gap > durationMs && durationMs > 0) {
                this.#state.breached_since = null;
                this.#state.clear_since = null;
            }
        }

        // Run evaluator
        const breached = this.#evaluator(telemetryData);

        if (breached) {
            this.#state.clear_since = null;

            if (this.#state.breached_since === null) {
                this.#state.breached_since = now;
            }

            const heldFor = now - this.#state.breached_since;

            if (this.#state.status === 'normal' && heldFor >= durationMs) {
                // FIRE
                this.#state.status = 'alerting';
                this.#state.last_fired = now;
                await this.#publishFire(telemetryData, now);
                await this.#dispatchNotifications(telemetryData, now);
                if (this.#callbacks.onFire) {
                    this.#callbacks.onFire(this.#buildAlertPayload(telemetryData, now));
                }
            } else if (this.#state.status === 'alerting' && (now - this.#state.last_fired) >= cooldownMs) {
                // Re-FIRE (cooldown cycle)
                this.#state.last_fired = now;
                await this.#publishFire(telemetryData, now);
                await this.#dispatchNotifications(telemetryData, now);
                if (this.#callbacks.onFire) {
                    this.#callbacks.onFire(this.#buildAlertPayload(telemetryData, now));
                }
            }
            // If acknowledged, do nothing (silent)
        } else {
            // Clear
            this.#state.breached_since = null;

            if (this.#state.clear_since === null) {
                this.#state.clear_since = now;
            }

            const clearedFor = now - this.#state.clear_since;

            if ((this.#state.status === 'alerting' || this.#state.status === 'acknowledged') && clearedFor >= recoveryMs) {
                // RESOLVED
                await this.#publishResolved(telemetryData, now);
                await this.#dispatchNotifications(telemetryData, now);
                if (this.#callbacks.onResolved) {
                    this.#callbacks.onResolved(this.#buildAlertPayload(telemetryData, now));
                }

                // Reset state
                this.#state.status = 'normal';
                this.#state.acked_by = null;
                this.#state.acked_at = null;
                this.#state.ack_notes = null;
                this.#state.breached_since = null;
                this.#state.clear_since = null;
            }
        }

        this.#state.last_evaluated_at = now;

        // TODO: Persist state to NATS KV when backend endpoint is ready
        // await this.#saveStateToKV();
    }

    #isMuted() {
        const muteConfig = this.#rule.alert_mute_config;
        if (!muteConfig) return false;

        if (muteConfig.type === 'FOREVER') return true;
        if (muteConfig.type === 'TIME_BASED') {
            const muteTill = muteConfig.mute_till;
            if (muteTill && Date.now() < muteTill) return true;
        }
        return false;
    }

    #buildAlertPayload(telemetryData, timestamp) {
        return {
            rule: {
                name: this.#rule.name,
                type: 'DEVICE',
                type_value: this.#rule.config.scope.value,
            },
            device_id: this.#rule.config.scope.value,
            last_value: {
                value: telemetryData.value,
                field_name: this.#rule.metric,
            },
            timestamp,
        };
    }

    async #publishFire(telemetryData, timestamp) {
        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.fire`;
        const payload = this.#buildAlertPayload(telemetryData, timestamp);
        await this.#ctx.jetstream.publish(subject, msgpackEncode(payload));
    }

    async #publishResolved(telemetryData, timestamp) {
        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.resolved`;
        const payload = this.#buildAlertPayload(telemetryData, timestamp);
        await this.#ctx.jetstream.publish(subject, msgpackEncode(payload));
    }

    async #dispatchNotifications(telemetryData, timestamp) {
        const channels = this.#rule.notification_channel;
        if (!channels || channels.length === 0) return;

        const alertData = this.#buildAlertPayload(telemetryData, timestamp);

        for (const notifId of channels) {
            await this.#ctx.natsClient.publish(
                'notif_dispatcher.notification.send',
                this.#codec.encode({
                    notif_id: notifId,
                    org_id: this.#ctx.orgID,
                    alert_data: alertData,
                })
            );
        }
    }

    /**
     * Handle local ack for ephemeral alerts
     */
    async ack(ackedBy, ackNotes = null) {
        if (this.#state.status !== 'alerting') return false;

        this.#state.status = 'acknowledged';
        this.#state.acked_by = ackedBy;
        this.#state.acked_at = Date.now();
        this.#state.ack_notes = ackNotes;

        // Resolve device_id to ident
        let deviceIdent = null;
        for (const [ident, device] of this.#ctx.device.cache) {
            if (device.id === this.#rule.config.scope.value) {
                deviceIdent = ident;
                break;
            }
        }

        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.ack`;
        await this.#ctx.jetstream.publish(subject, msgpackEncode({
            status: 'acknowledged',
            device_ident: deviceIdent,
            ack: {
                acked_by: ackedBy,
                ack_notes: ackNotes,
                acked_at: this.#state.acked_at,
            },
        }));

        if (this.#callbacks.onAck) {
            this.#callbacks.onAck({
                status: 'acknowledged',
                device_ident: deviceIdent,
                ack: {
                    acked_by: ackedBy,
                    ack_notes: ackNotes,
                    acked_at: this.#state.acked_at,
                },
            });
        }

        return true;
    }

    async ackAll(ackedBy, ackNotes = null) {
        if (this.#state.status !== 'alerting') return false;

        this.#state.status = 'acknowledged';
        this.#state.acked_by = ackedBy;
        this.#state.acked_at = Date.now();
        this.#state.ack_notes = ackNotes;

        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${this.#rule.id}.ack_all`;
        await this.#ctx.jetstream.publish(subject, msgpackEncode({
            status: 'acknowledged',
            ack: {
                acked_by: ackedBy,
                ack_notes: ackNotes,
                acked_at: this.#state.acked_at,
            },
        }));

        if (this.#callbacks.onAckAll) {
            this.#callbacks.onAckAll({
                status: 'acknowledged',
                ack: {
                    acked_by: ackedBy,
                    ack_notes: ackNotes,
                    acked_at: this.#state.acked_at,
                },
            });
        }

        return true;
    }

    // Expose state for testing
    get state() {
        return { ...this.#state };
    }
}
