import { JSONCodec } from 'nats.ws';
import { decode as msgpackDecode } from '@msgpack/msgpack';
import { validateIdent, validateConnected, validateFunction } from './validation.js';
import { EphemeralEngine } from './ephemeralEngine.js';

export class AlertManager {
    #ctx;
    #codec = JSONCodec();
    #listenConsumers = new Map(); // ruleId -> consumer[]
    #ephemeralEngines = new Map(); // ruleId -> EphemeralEngine

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
            { timeout: 5000 }
        );
        return res.json();
    }

    #wrapAlert(data, evaluator = null) {
        if (!data) return data;
        const alert = { ...data };
        let storedEvaluator = evaluator;

        alert.listen = async (callbacks) => {
            await this.#listen(data, callbacks, storedEvaluator);
        };

        alert.setEvaluator = (fn) => {
            if (data.type !== 'EPHEMERAL') {
                throw new Error('setEvaluator is only allowed for EPHEMERAL alerts');
            }
            validateFunction(fn, 'evaluator');
            storedEvaluator = fn;

            // Update running engine if exists
            const engine = this.#ephemeralEngines.get(data.id);
            if (engine) {
                engine.setEvaluator(fn);
            }
        };

        return alert;
    }

    async create(config) {
        validateConnected(this.#ctx.connected);
        validateIdent(config.name, 'name');

        if (!['THRESHOLD', 'RATE_CHANGE', 'EPHEMERAL'].includes(config.type)) {
            throw new Error('type must be THRESHOLD, RATE_CHANGE, or EPHEMERAL');
        }

        if (config.type === 'EPHEMERAL') {
            if (config.config?.scope?.type !== 'DEVICE') {
                throw new Error('EPHEMERAL alerts require scope.type = "DEVICE"');
            }
            if (config.evaluator && typeof config.evaluator !== 'function') {
                throw new Error('evaluator must be a function');
            }
        }

        // Build request payload (exclude evaluator)
        const payload = {
            name: config.name,
            description: config.description,
            type: config.type,
            metric: config.metric,
            config: config.config,
            notification_channel: config.notification_channel || [],
            alert_mute_config: config.alert_mute_config,
        };

        const res = await this.#request('create', payload);

        if (res.status === 'ALERT_CREATE_SUCCESS') {
            return this.#wrapAlert(res.data, config.evaluator || null);
        }

        return res;
    }

    async update(config) {
        validateConnected(this.#ctx.connected);
        if (!config.id) throw new Error('id is required');

        const res = await this.#request('update', config);

        if (res.status === 'ALERT_UPDATE_SUCCESS') {
            return this.#wrapAlert(res.data);
        }

        return res;
    }

    async delete(alertName) {
        validateConnected(this.#ctx.connected);
        if (!alertName) throw new Error('alertName is required');

        const res = await this.#request('delete', { id: alertName });
        return res.status === 'ALERT_DELETE_SUCCESS';
    }

    async list() {
        validateConnected(this.#ctx.connected);
        const res = await this.#request('list', {});
        return res.data || [];
    }

    async get(alertName) {
        validateConnected(this.#ctx.connected);
        validateIdent(alertName, 'alertName');

        const res = await this.#request('get', { name: alertName });

        if (res.status === 'ALERT_GET_SUCCESS') {
            return this.#wrapAlert(res.data);
        }

        return res;
    }

    async ack(params) {
        validateConnected(this.#ctx.connected);
        if (!params.alert) throw new Error('alert is required');
        if (!params.acked_by) throw new Error('acked_by is required');

        // Check if this is an ephemeral alert with a running engine
        const engine = this.#ephemeralEngines.get(params.alert.id);
        if (engine) {
            return engine.ack(params.acked_by, params.ack_notes);
        }

        const res = await this.#request('ack', {
            device_id: params.alert.config?.scope?.value,
            rule_id: params.alert.id,
            acked_by: params.acked_by,
            env: this.#ctx.env,
            ack_notes: params.ack_notes,
        });

        return res.status === 'ALERT_ACK_SUCCESS';
    }

    async ackAll(params) {
        validateConnected(this.#ctx.connected);
        if (!params.alert) throw new Error('alert is required');
        if (!params.acked_by) throw new Error('acked_by is required');

        const engine = this.#ephemeralEngines.get(params.alert.id);
        if (engine) {
            return engine.ackAll(params.acked_by, params.ack_notes);
        }

        const res = await this.#request('ack_all', {
            rule_id: params.alert.id,
            acked_by: params.acked_by,
            env: this.#ctx.env,
            ack_notes: params.ack_notes,
        });

        return res.status === 'ALERT_ACK_SUCCESS';
    }

    async mute(params) {
        validateConnected(this.#ctx.connected);
        if (!params.id) throw new Error('id is required');
        if (!params.mute_config) throw new Error('mute_config is required');
        if (!['FOREVER', 'TIME_BASED'].includes(params.mute_config.type)) {
            throw new Error('mute_config.type must be FOREVER or TIME_BASED');
        }

        const payload = {
            rule_id: params.id,
            type: params.mute_config.type,
        };
        if (params.mute_config.type === 'TIME_BASED') {
            if (!params.mute_config.mute_till) {
                throw new Error('mute_till is required for TIME_BASED mute');
            }
            payload.mute_till = params.mute_config.mute_till;
        }

        return this.#request('mute', payload);
    }

    async unmute(id) {
        validateConnected(this.#ctx.connected);
        if (!id) throw new Error('id is required');

        return this.#request('mute', { rule_id: id, type: 'CLEAR' });
    }

    async #listen(rule, callbacks, evaluator) {
        validateConnected(this.#ctx.connected);

        if (rule.type === 'EPHEMERAL') {
            // Start ephemeral engine
            const engine = new EphemeralEngine(this.#ctx, rule, evaluator);
            this.#ephemeralEngines.set(rule.id, engine);
            await engine.start(callbacks);
            return;
        }

        // Non-ephemeral: subscribe to JetStream alert topics
        const ruleId = rule.id;
        const baseSubject = `import.${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${ruleId}`;

        const events = [
            { suffix: 'fire', callback: callbacks.onFire },
            { suffix: 'resolved', callback: callbacks.onResolved },
            { suffix: 'ack', callback: callbacks.onAck },
            { suffix: 'ack_all', callback: callbacks.onAckAll },
        ];

        const consumers = [];

        for (const { suffix, callback } of events) {
            if (!callback) continue;

            const subject = `${baseSubject}.${suffix}`;

            const consumer = await this.#ctx.jetstream.consumers.get(
                `${this.#ctx.orgID}_stream`,
                {
                    name: `appjs_alert_listen_${ruleId}_${suffix}_${crypto.randomUUID()}`,
                    filter_subjects: subject,
                    replay_policy: 'instant',
                    opt_start_time: new Date(),
                    ack_policy: 'explicit',
                    delivery_policy: 'new',
                }
            );

            consumers.push(consumer);

            await consumer.consume({
                callback: async (msg) => {
                    msg.working();
                    const data = msgpackDecode(msg.data);
                    msg.ack();
                    callback(data);
                },
            });
        }

        this.#listenConsumers.set(ruleId, consumers);
    }

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
