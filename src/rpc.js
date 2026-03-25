import { JSONCodec } from 'nats.ws';
import { validateIdent, validateRpcName, validateConnected, validatePositiveNumber } from './validation.js';


export class RPCManager {

    #ctx;
    #codec = JSONCodec();

    constructor(ctx) {
        this.#ctx = ctx;
    }

    async call(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.device_ident, 'device_ident');
        validateRpcName(params.name);

        if (params.timeout != null) {
            validatePositiveNumber(params.timeout, 'timeout');
        }

        if (params.data == null) {
            throw new Error('data is required');
        }

        const timeoutMs = params.timeout ? params.timeout * 1000 : 10000; // default 10s

        const deviceId = await this.#ctx.device.resolveDeviceId(params.device_ident);
        const subject = `${this.#ctx.orgID}.${this.#ctx.env}.command.rpc.${deviceId}.${params.name}`;

        const res = await this.#ctx.natsClient.request(
            subject,
            this.#codec.encode(params.data),
            { timeout: timeoutMs }
        );

        return res.json();
    }
}
