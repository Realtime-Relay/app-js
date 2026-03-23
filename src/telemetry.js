import { JSONCodec } from 'nats.ws';
import { decode as msgpackDecode } from '@msgpack/msgpack';
import { validateIdent, validateFunction, validateConnected, validateArray, validateISO8601, validateStartBeforeEnd, validateNonEmptyArray } from './validation.js';

export class TelemetryManager {
    #ctx;
    #consumers = new Map(); // key: `${device_ident}:${metric}` -> consumer
    #codec = JSONCodec();

    constructor(ctx) {
        this.#ctx = ctx;
    }

    async stream(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.device_ident, 'device_ident');
        if (params.metric == null || params.metric === '') {
            throw new Error('metric is required');
        }
        validateFunction(params.callback, 'callback');

        const key = `${params.device_ident}:${params.metric}`;
        if (this.#consumers.has(key)) {
            return false;
        }

        const subject = `${this.#ctx.orgID}.${this.#ctx.env}.telemetry.${params.device_ident}.${params.metric}`;

        const consumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_telemetry_${params.device_ident}_${params.metric}_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        this.#consumers.set(key, consumer);

        await consumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();
                params.callback(data);
            },
        });

        return true;
    }

    async off(params) {
        validateIdent(params.device_ident, 'device_ident');

        if (params.metric !== undefined) {
            validateArray(params.metric, 'metric');
            for (const m of params.metric) {
                const key = `${params.device_ident}:${m}`;
                const consumer = this.#consumers.get(key);
                if (consumer) {
                    await consumer.delete();
                    this.#consumers.delete(key);
                }
            }
        } else {
            // Kill all consumers for this device
            for (const [key, consumer] of this.#consumers) {
                if (key.startsWith(`${params.device_ident}:`)) {
                    await consumer.delete();
                    this.#consumers.delete(key);
                }
            }
        }
    }

    async history(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.device_ident, 'device_ident');
        validateNonEmptyArray(params.fields, 'fields');
        validateISO8601(params.start, 'start');
        validateISO8601(params.end, 'end');
        validateStartBeforeEnd(params.start, params.end);

        const deviceId = await this.#ctx.device.resolveDeviceId(params.device_ident);

        const res = await this.#ctx.natsClient.request(
            `api.iot.db.${this.#ctx.orgID}.telemetry.history`,
            this.#codec.encode({
                device_id: deviceId,
                env: this.#ctx.env,
                start: params.start,
                end: params.end,
                fields: params.fields,
            }),
            { timeout: 5000 }
        );

        return res.json();
    }

    async deleteAllConsumers() {
        for (const [key, consumer] of this.#consumers) {
            await consumer.delete();
        }
        this.#consumers.clear();
    }
}
