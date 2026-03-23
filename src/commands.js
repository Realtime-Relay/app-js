import { JSONCodec } from 'nats.ws';
import { encode as msgpackEncode } from '@msgpack/msgpack';
import { validateIdent, validateNonEmptyArray, validateConnected, validateISO8601, validateObject } from './validation.js';

export class CommandManager {
    #ctx;
    #codec = JSONCodec();

    constructor(ctx) {
        this.#ctx = ctx;
    }

    async send(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.name, 'name');
        validateNonEmptyArray(params.device_ident, 'device_ident');
        if (params.data == null) {
            throw new Error('data is required');
        }

        // Validate each ident
        for (const ident of params.device_ident) {
            validateIdent(ident, 'device_ident[]');
        }

        // Resolve idents to device IDs
        const deviceIds = await this.#ctx.device.resolveDeviceIds(params.device_ident);

        let allSuccess = true;
        for (const deviceId of deviceIds) {
            const subject = `${this.#ctx.orgID}.${this.#ctx.env}.command.queue.${deviceId}.${params.name}`;
            const payload = msgpackEncode({
                value: params.data,
                timestamp: Date.now(),
            });

            const ack = await this.#ctx.jetstream.publish(subject, payload);
            if (ack == null) {
                allSuccess = false;
            }
        }

        return allSuccess;
    }

    async history(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.name, 'name');
        validateNonEmptyArray(params.device_idents, 'device_idents');
        validateISO8601(params.start, 'start');

        const end = params.end || new Date().toISOString();

        const deviceIds = await this.#ctx.device.resolveDeviceIds(params.device_idents);

        const res = await this.#ctx.natsClient.request(
            `api.iot.db.${this.#ctx.orgID}.command.history`,
            this.#codec.encode({
                device_ids: deviceIds,
                env: this.#ctx.env,
                command_name: params.name,
                start: params.start,
                end: end,
            }),
            { timeout: 5000 }
        );

        return res.json();
    }
}
