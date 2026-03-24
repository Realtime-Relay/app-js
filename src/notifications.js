import { JSONCodec } from 'nats.ws';
import { validateIdent, validateObject, validateConnected } from './validation.js';

export class NotificationManager {
    #ctx;
    #codec = JSONCodec();

    constructor(ctx) {
        this.#ctx = ctx;
    }

    #subject(op) {
        return `api.iot.notification.${this.#ctx.orgID}.${op}`;
    }

    async #request(op, payload) {
        const res = await this.#ctx.natsClient.request(
            this.#subject(op),
            this.#codec.encode(payload),
            { timeout: 20000 }
        );
        return res.json();
    }

    async create(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.name, 'name');
        if (params.type !== 'WEBHOOK' && params.type !== 'EMAIL') {
            throw new Error('type must be "WEBHOOK" or "EMAIL"');
        }
        validateObject(params.config, 'config');

        if (params.type === 'WEBHOOK') {
            if (!params.config.endpoint) throw new Error('config.endpoint is required for WEBHOOK');
        }
        if (params.type === 'EMAIL') {
            if (!Array.isArray(params.config.recipients)) throw new Error('config.recipients is required for EMAIL');
            if (!params.config.subject) throw new Error('config.subject is required for EMAIL');
            if (!params.config.template) throw new Error('config.template is required for EMAIL');
        }

        return this.#request('create', {
            name: params.name,
            type: params.type,
            config: params.config,
        });
    }

    async update(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.name, 'name');
        if (params.type !== 'WEBHOOK' && params.type !== 'EMAIL') {
            throw new Error('type must be "WEBHOOK" or "EMAIL"');
        }
        validateObject(params.config, 'config');

        return this.#request('update', {
            name: params.name,
            type: params.type,
            config: params.config,
        });
    }

    async delete(notifId) {
        validateConnected(this.#ctx.connected);
        if (!notifId) throw new Error('notif_id is required');

        const res = await this.#request('delete', { id: notifId });
        return res.status === 'NOTIFICATION_DELETE_SUCCESS';
    }

    async list() {
        validateConnected(this.#ctx.connected);
        const res = await this.#request('list', {});
        return res.data || [];
    }

    async get(notifId) {
        validateConnected(this.#ctx.connected);
        if (!notifId) throw new Error('notif_id is required');
        return this.#request('get', { id: notifId });
    }
}
