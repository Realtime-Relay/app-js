import { JSONCodec } from 'nats.ws';
import { decode as msgpackDecode } from '@msgpack/msgpack';
import { validateIdent, validateArray, validateFunction, validateConnected } from './validation.js';

export class LogicalGroupManager {
    #ctx;
    #codec = JSONCodec();
    #streamConsumers = new Map(); // groupID -> consumer

    constructor(ctx) {
        this.#ctx = ctx;
    }

    #subject(op) {
        return `api.iot.cohort.${this.#ctx.orgID}.logical.${op}`;
    }

    async #request(op, payload) {
        const res = await this.#ctx.natsClient.request(
            this.#subject(op),
            this.#codec.encode(payload),
            { timeout: 20000 }
        );
        return res.json();
    }

    #wrapGroup(data) {
        if (!data) return data;
        const group = { ...data };
        group.stream = (params) => this.#streamGroup(data.id, params);
        return group;
    }

    async create(params) {
        validateConnected(this.#ctx.connected);
        validateIdent(params.name, 'name');
        validateArray(params.tags, 'tags');
        validateArray(params.device_idents, 'device_idents');

        const deviceIds = await this.#ctx.device.resolveDeviceIds(params.device_idents);

        const res = await this.#request('create', {
            device_ids: deviceIds,
            name: params.name,
            tags: params.tags,
        });

        if (res.data) {
            return this.#wrapGroup(res.data);
        }
        return res.data;
    }

    async update(params) {
        validateConnected(this.#ctx.connected);
        if (!params.id) throw new Error('id is required');

        const payload = { id: params.id };

        if (params.devices) {
            payload.devices = {
                add: params.devices.add ? await this.#ctx.device.resolveDeviceIds(params.devices.add) : [],
                remove: params.devices.remove ? await this.#ctx.device.resolveDeviceIds(params.devices.remove) : [],
            };
        }

        if (params.tags) {
            payload.tags = {
                add: params.tags.add || [],
                remove: params.tags.remove || [],
            };
        }

        const res = await this.#request('update', payload);
        if (res.data) {
            return this.#wrapGroup(res.data);
        }
        return res.data;
    }

    async delete(groupId) {
        validateConnected(this.#ctx.connected);
        if (!groupId) throw new Error('group_id is required');

        const res = await this.#request('delete', { id: groupId });
        return res.status === 'LOGICAL_GROUP_DELETE_SUCCESS';
    }

    async list() {
        validateConnected(this.#ctx.connected);
        const res = await this.#request('list', {});
        return res.data || [];
    }

    async get(groupId) {
        validateConnected(this.#ctx.connected);
        if (!groupId) throw new Error('group_id is required');

        const res = await this.#request('get', { id: groupId });
        if (res.status === 'LOGICAL_GROUP_GET_SUCCESS') {
            res.data.id = groupId
            return this.#wrapGroup(res.data);
        }
        return res;
    }

    async listDevices(groupId) {
        validateConnected(this.#ctx.connected);
        if (!groupId) throw new Error('group_id is required');

        const res = await this.#ctx.natsClient.request(
            `api.iot.cohort.${this.#ctx.orgID}.logical.device.list`,
            this.#codec.encode({ id: groupId }),
            { timeout: 20000 }
        );
        const data = res.json();
        return data.data || [];
    }

    async #streamGroup(groupId, params) {
        validateConnected(this.#ctx.connected);
        validateFunction(params.callback, 'callback');

        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.group.listen.${groupId}`;

        const consumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_logical_group_${groupId}_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        this.#streamConsumers.set(groupId, consumer);

        const filterIdents = params.device_idents || null;

        await consumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();

                if (filterIdents && !filterIdents.includes(data.ident)) {
                    return;
                }

                params.callback(data);
            },
        });
    }

    async deleteAllConsumers() {
        for (const [, consumer] of this.#streamConsumers) {
            await consumer.delete();
        }
        this.#streamConsumers.clear();
    }
}
