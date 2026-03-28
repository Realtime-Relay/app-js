import { JSONCodec } from 'nats.ws';
import { decode as msgpackDecode } from '@msgpack/msgpack';
import { validateIdent, validateTelemetryMetric, validateFunction, validateConnected, validateArray, validateISO8601, validateStartBeforeEnd, validateNonEmptyArray } from './validation.js';


export class TelemetryManager {

    #ctx;
    #consumers = new Map(); // key: `${device_ident}:${metric}` -> consumer
    #codec = JSONCodec();

    constructor(ctx) {
        this.#ctx = ctx;
    }

    // ─── Streaming ───────────────────────────────────────────

    async stream(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.device_ident, 'device_ident');

        validateTelemetryMetric(params.metric);
        validateFunction(params.callback, 'callback');

        // Validate metric against device schema
        await this.#validateMetric(params.device_ident, params.metric);

        const key = `${params.device_ident}:${params.metric}`;

        if (this.#consumers.has(key)) {
            return false;
        }

        const deviceId = await this.#ctx.device.resolveDeviceId(params.device_ident);

        const subject = `${this.#ctx.orgID}.${this.#ctx.env}.telemetry.${deviceId}.${params.metric}`;

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

                var tokens = msg.subject.split(".")
                var metric = msg.subject.split(".")[tokens.length - 1]

                params.callback({
                    metric: metric,
                    data: data
                });
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

    // ─── History ─────────────────────────────────────────────

    async history(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.device_ident, 'device_ident');
        validateNonEmptyArray(params.fields, 'fields');

        await this.#validateFields(params.device_ident, params.fields);

        validateISO8601(params.start, 'start');
        validateISO8601(params.end, 'end');
        validateStartBeforeEnd(params.start, params.end);

        const deviceId = await this.#ctx.device.resolveDeviceId(params.device_ident);

        var res = null;

        try{
            res = await this.#ctx.natsClient.request(
                `api.iot.db.${this.#ctx.orgID}.telemetry.history`,
                this.#codec.encode({
                    device_id: deviceId,
                    env: this.#ctx.env,
                    start: params.start,
                    end: params.end,
                    fields: params.fields,
                    last_value: false
                }),
                { timeout: 20000 }
            );

            res = msgpackDecode(res.data)
        }catch(err){
            this.#ctx.logger.error("Telemetry history request failed", err)

            throw new Error("Telemetry history request timed-out")
        }

        var data = res.status == "TELEMETRY_FETCH_SUCCESS" ? res.data : []

        return data
    }

    async latest(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.device_ident, 'device_ident');
        validateNonEmptyArray(params.fields, 'fields');

        await this.#validateFields(params.device_ident, params.fields);

        validateISO8601(params.start, 'start');
        validateISO8601(params.end, 'end');
        validateStartBeforeEnd(params.start, params.end);

        const deviceId = await this.#ctx.device.resolveDeviceId(params.device_ident);

        var res = null;

        try{
            res = await this.#ctx.natsClient.request(
                `api.iot.db.${this.#ctx.orgID}.telemetry.history`,
                this.#codec.encode({
                    device_id: deviceId,
                    env: this.#ctx.env,
                    start: params.start,
                    end: params.end,
                    fields: params.fields,
                    last_value: true
                }),
                { timeout: 20000 }
            );

            res = msgpackDecode(res.data)
        }catch(err){
            this.#ctx.logger.error("Telemetry history request failed", err)

            throw new Error("Telemetry history request timed-out")
        }

        var latestValues = {};

        if(data.status == "TELEMETRY_FETCH_SUCCESS"){
            data = data.data;

            for(let key of Object.keys(data)){
                latestValues[key] = data[key][0];
            }
        }

        return latestValues;
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
            throw new Error(`fields contain invalid metrics: ${invalid.join(', ')}. Valid keys: ${Object.keys(device.schema).join(', ')}`);
        }
    }

    async #validateMetric(deviceIdent, metric) {
        if (metric === '*') return;

        // Check cache first, fallback to NATS request
        let device = this.#ctx.device.cache.get(deviceIdent);

        if (!device || !device.schema) {
            device = await this.#ctx.device.get({ ident: deviceIdent });
        }

        if (!device || !device.schema) {
            throw new Error(`Device ${deviceIdent} not found or has no schema`);
        }

        if (!(metric in device.schema)) {
            throw new Error(`metric "${metric}" is not a valid key in device schema. Valid keys: ${Object.keys(device.schema).join(', ')}`);
        }
    }

    // ─── Cleanup ─────────────────────────────────────────────

    async deleteAllConsumers() {
        for (const [key, consumer] of this.#consumers) {
            await consumer.delete();
        }

        this.#consumers.clear();
    }
}
