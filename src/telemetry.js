import { JSONCodec } from 'nats.ws';
import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack';
import { validateIdent,
    validateFunction, validateConnected, validateArray, validateISO8601, validateStartBeforeEnd, validateNonEmptyArray } from './validation.js';

export class TelemetryManager {

    #ctx;
    #consumers = new Map(); // key: device_ident -> Array<{ consumer, metrics: Set|null, callback }>
    #codec = JSONCodec();

    constructor(ctx) {
        this.#ctx = ctx;
    }

    // ─── Streaming ───────────────────────────────────────────

    async stream(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.device_ident, 'device_ident');
        validateFunction(params.callback, 'callback');

        let metricsFilter = null;

        if (typeof params.metric === 'string') {
            if (params.metric !== '*') {
                throw new Error('metric as a string must be "*". Use an array for specific metrics.');
            }
        } else if (Array.isArray(params.metric)) {
            validateNonEmptyArray(params.metric, 'metric');

            for (const m of params.metric) {
                await this.#validateMetric(params.device_ident, m);
            }

            metricsFilter = new Set(params.metric);
        } else {
            throw new Error('metric must be "*" or a non-empty array of metric names');
        }

        const deviceId = await this.#ctx.device.resolveDeviceId(params.device_ident);

        const subject = `${this.#ctx.orgID}.${this.#ctx.env}.telemetry.${deviceId}.*`;

        const consumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_telemetry_${params.device_ident}_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        const entry = { consumer, metrics: metricsFilter, callback: params.callback };

        if (!this.#consumers.has(params.device_ident)) {
            this.#consumers.set(params.device_ident, []);
        }

        this.#consumers.get(params.device_ident).push(entry);

        await consumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();

                var tokens = msg.subject.split(".");
                var metric = tokens[tokens.length - 1];

                if (entry.metrics !== null && !entry.metrics.has(metric)) {
                    return;
                }

                params.callback({
                    metric: metric,
                    data: data
                });
            },
        });
    }

    async off(params) {
        validateIdent(params.device_ident, 'device_ident');

        const subs = this.#consumers.get(params.device_ident);
        if (!subs || subs.length === 0) return;

        if (params.metric !== undefined) {
            validateArray(params.metric, 'metric');

            for (let i = subs.length - 1; i >= 0; i--) {
                const sub = subs[i];

                // Skip wildcard ("*") subscriptions — metrics is null
                if (sub.metrics === null) continue;

                for (const m of params.metric) {
                    sub.metrics.delete(m);
                }

                if (sub.metrics.size === 0) {
                    await sub.consumer.delete();
                    subs.splice(i, 1);
                }
            }

            if (subs.length === 0) {
                this.#consumers.delete(params.device_ident);
            }
        } else {
            for (const sub of subs) {
                await sub.consumer.delete();
            }
            this.#consumers.delete(params.device_ident);
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

        var startCursor = params.start
        var telemetry = {};

        for(let field of params.fields){
            telemetry[field] = []
        }

        while(true){
            try{
                res = await this.#ctx.natsClient.request(
                    `api.iot.db.${this.#ctx.orgID}.telemetry.history`,
                    this.#codec.encode({
                        device_id: deviceId,
                        env: this.#ctx.env,
                        start: startCursor,
                        end: params.end,
                        fields: params.fields,
                        last_value: false
                    }),
                    { timeout: 20000 }
                );

                res = msgpackDecode(res.data)

                if(res.status == "TELEMETRY_FETCH_SUCCESS"){
                    var data = res.data;

                    var hasMore = data.has_more

                    var telemetryPage = data.data;

                    for(let metric of Object.keys(telemetryPage)){
                        telemetry[metric] = telemetry[metric].concat(telemetryPage[metric])
                    }

                    if(hasMore){
                        startCursor = data.cursor;

                        continue;
                    }else{
                        // We got all the data, we're done

                        break;
                    }
                }else{
                    // We weren't able to fetch tlm

                    break;
                }
            }catch(err){
                console.log(err)
                this.#ctx.logger.error("Telemetry history request failed", err)

                throw new Error("Telemetry history request timed-out")
            }
        }

        return telemetry
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

            console.log(res)
        }catch(err){
            this.#ctx.logger.error("Telemetry history request failed", err)

            throw new Error("Telemetry history request timed-out")
        }

        var latestValues = {};

        if(res.status == "TELEMETRY_FETCH_SUCCESS"){
            var data = res.data.data;

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
        for (const [deviceIdent, subs] of this.#consumers) {
            for (const sub of subs) {
                await sub.consumer.delete();
            }
        }

        this.#consumers.clear();
    }
}
