import { decode as msgpackDecode } from '@msgpack/msgpack';
import { validateEventName, validateFunction, validateConnected } from './validation.js';


export class EventManager {

    #ctx;
    #consumers = new Map(); // event_name -> consumer
    #callbacks = new Map(); // event_name -> callback

    constructor(ctx) {
        this.#ctx = ctx;
    }

    async stream(params) {
        validateConnected(this.#ctx.connected);
        validateEventName(params.name);
        validateFunction(params.callback, 'callback');

        if (this.#consumers.has(params.name)) {
            return false;
        }

        const subject = `${this.#ctx.orgID}.${this.#ctx.env}.events.*.${params.name}`;

        const consumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_events_${params.name}_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        this.#consumers.set(params.name, consumer);
        this.#callbacks.set(params.name, params.callback);

        await consumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();

                const cb = this.#callbacks.get(params.name);
                if (cb) cb(data);
            },
        });

        return true;
    }

    async off(params) {
        validateEventName(params.name);

        const consumer = this.#consumers.get(params.name);

        if (consumer) {
            await consumer.delete();
            this.#consumers.delete(params.name);
            this.#callbacks.delete(params.name);
        }
    }

    async deleteAllConsumers() {
        for (const [, consumer] of this.#consumers) {
            await consumer.delete();
        }

        this.#consumers.clear();
        this.#callbacks.clear();
    }
}
