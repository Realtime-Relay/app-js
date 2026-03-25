import { JSONCodec } from 'nats.ws';
import { validateIdent, validateObject, validateConnected } from './validation.js';

const CACHE_TTL = 2 * 60 * 60 * 1000; // 2 hours in ms


export class DeviceManager {

    #ctx;
    #cache = new Map();     // ident -> device object
    #cacheTimestamp = 0;
    #codec = JSONCodec();

    constructor(ctx) {
        this.#ctx = ctx;
    }

    get cache() {
        return this.#cache;
    }

    #isCacheValid() {
        return this.#cache.size > 0 && (Date.now() - this.#cacheTimestamp) < CACHE_TTL;
    }

    #refreshCacheTimestamp() {
        this.#cacheTimestamp = Date.now();
    }

    #subject(op) {
        return `api.iot.devices.${this.#ctx.orgID}.${op}`;
    }

    async #request(op, payload) {
        const res = await this.#ctx.natsClient.request(
            this.#subject(op),
            this.#codec.encode(payload),
            { timeout: 20000 }
        );

        return res.json();
    }

    // ─── Resolution ──────────────────────────────────────────

    async resolveDeviceId(ident) {
        // If cache has this specific ident, use it (regardless of global TTL)
        const cached = this.#cache.get(ident);
        if (cached && cached.id) {
            return cached.id;
        }

        // Fallback to NATS request
        const device = await this.get({ ident });
        if (device && device.id) {
            return device.id;
        }

        throw new Error(`Device not found: ${ident}`);
    }

    async resolveDeviceIds(idents) {
        const ids = [];

        for (const ident of idents) {
            try {
                const id = await this.resolveDeviceId(ident);
                ids.push(id);
            } catch (err) {
                // Skip unfound devices
                this.#ctx.logger.error('Failed to resolve device ID', err);
            }
        }

        return ids;
    }

    // ─── CRUD ────────────────────────────────────────────────

    async create(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.ident, 'ident');
        validateObject(params.schema, 'schema');
        validateObject(params.config, 'config');

        const res = await this.#request('create', {
            ident: params.ident,
            env: this.#ctx.env,
            schema: params.schema,
            config: params.config,
        });

        if (res.status === 'DEVICE_CREATE_SUCCESS') {
            this.#cache.set(params.ident, res.data);
            return res.data;
        }

        return null;
    }

    async update(params) {
        validateConnected(this.#ctx.connected);

        if (!params.id) throw new Error('id is required');

        const payload = { id: params.id };
        if (params.ident) payload.ident = params.ident;
        if (params.schema) payload.schema = params.schema;
        if (params.config) payload.config = params.config;

        const res = await this.#request('update', payload);

        if (res.status === 'DEVICE_UPDATE_SUCCESS') {
            const device = res.data.device;
            if (device?.ident) {
                this.#cache.set(device.ident, device);
            }
            return res.data.device;
        }

        return null;
    }

    async delete(ident) {
        validateConnected(this.#ctx.connected);
        validateIdent(ident, 'ident');

        const res = await this.#request('delete', { ident });

        if (res.status === 'DEVICE_DELETE_SUCCESS') {
            this.#cache.delete(ident);
        }

        return res.status === 'DEVICE_DELETE_SUCCESS';
    }

    async list() {
        validateConnected(this.#ctx.connected);

        const res = await this.#request('list', {});

        if (res.status === 'DEVICE_FETCH_SUCCESS') {
            this.#cache.clear();
            for (const device of res.data.devices) {
                this.#cache.set(device.ident, device);
            }
            this.#refreshCacheTimestamp();
        }

        return res.data?.devices || [];
    }

    async get(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.ident, 'ident');

        // Check cache first
        const cached = this.#cache.get(params.ident);
        if (cached && this.#isCacheValid()) {
            return cached;
        }

        const res = await this.#request('get', { ident: params.ident });

        if (res.status === 'DEVICE_GET_SUCCESS') {
            this.#cache.set(params.ident, res.data);
            return res.data;
        }

        return res;
    }

    clearCache() {
        this.#cache.clear();
        this.#cacheTimestamp = 0;
    }
}
