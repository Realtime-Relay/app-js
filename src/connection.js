import { connect, JSONCodec, Events, DebugEvents, credsAuthenticator } from 'nats.ws';
import { jetstream } from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { decode as msgpackDecode } from '@msgpack/msgpack';
import { buildCredentials } from './utils.js';
import { validateFunction } from './validation.js';

const SERVERS = [
    'wss://api.relay-x.io:4421',
    'wss://api.relay-x.io:4422',
    'wss://api.relay-x.io:4423',
];


export class ConnectionManager {

    #ctx;
    #listenerCallback = null;
    #presenceCallback = null;
    #presenceConsumer = null;
    #connectCalled = false;
    #isReconnecting = false;

    constructor(ctx) {
        this.#ctx = ctx;
    }

    listeners(callback) {
        validateFunction(callback, 'callback');
        this.#listenerCallback = callback;
    }

    async presence(callback) {
        validateFunction(callback, 'callback');

        if (!this.#ctx.connected) {
            throw new Error('Not connected. Call app.connect() first.');
        }

        this.#presenceCallback = callback;
        await this.#startPresenceConsumer();
    }

    async connect() {
        if (this.#connectCalled) return;

        const credsFile = buildCredentials(this.#ctx.apiKey, this.#ctx.secret);
        const encodedCreds = new TextEncoder().encode(credsFile);
        const credsAuth = credsAuthenticator(encodedCreds);

        try {
            this.#ctx.natsClient = await connect({
                servers: SERVERS,
                noEcho: true,
                reconnect: true,
                maxReconnectAttempts: 1200,
                reconnectTimeWait: 500,
                maxPingOut: 2,
                pingInterval: 5000,
                authenticator: credsAuth,
                token: this.#ctx.apiKey,
            });

            this.#ctx.jetstream = await jetstream(this.#ctx.natsClient);

            // Initialize KV bucket for ephemeral alert locks
            try {
                const kvm = new Kvm(this.#ctx.jetstream);
                this.#ctx.kvBucket = await kvm.open(this.#ctx.orgID);
            } catch {
                // KV not available — ephemeral locking will be skipped
                this.#ctx.kvBucket = null;
            }

            this.#ctx.connected = true;
            this.#connectCalled = true;

            // Populate device cache
            await this.#ctx.device.list();

            this.#emitEvent('connected');
        } catch (err) {
            this.#ctx.connected = false;
            this.#emitEvent('auth_failed');
            throw err;
        }

        // Monitor connection status
        this.#monitorStatus();

        // Handle connection close
        this.#ctx.natsClient.closed().then(() => {
            this.#ctx.connected = false;
            this.#connectCalled = false;
            this.#isReconnecting = false;
            this.#emitEvent('disconnected');
        });
    }

    async disconnect() {
        if (!this.#ctx.natsClient) return;

        // Clean up presence consumer
        if (this.#presenceConsumer) {
            await this.#presenceConsumer.delete();
            this.#presenceConsumer = null;
        }

        this.#ctx.connected = false;
        this.#connectCalled = false;
        this.#ctx.device.clearCache();
        this.#ctx.offlineBuffer.length = 0;

        await this.#ctx.natsClient.close();
    }

    // ─── Presence Consumer ───────────────────────────────────

    async #startPresenceConsumer() {
        if (this.#presenceConsumer) {
            await this.#presenceConsumer.delete();
        }

        const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.presence.*`;

        this.#presenceConsumer = await this.#ctx.jetstream.consumers.get(
            `${this.#ctx.orgID}_stream`,
            {
                name: `appjs_presence_${crypto.randomUUID()}`,
                filter_subjects: subject,
                replay_policy: 'instant',
                opt_start_time: new Date(),
                ack_policy: 'explicit',
                delivery_policy: 'new',
            }
        );

        await this.#presenceConsumer.consume({
            callback: async (msg) => {
                msg.working();
                const data = msgpackDecode(msg.data);
                msg.ack();

                if (this.#presenceCallback) {
                    this.#presenceCallback(data);
                }
            },
        });
    }

    // ─── Connection Monitoring ───────────────────────────────

    async #monitorStatus() {
        (async () => {
            for await (const s of this.#ctx.natsClient.status()) {
                switch (s.type) {
                    case Events.Disconnect:
                        this.#ctx.connected = false;
                        break;

                    case Events.Reconnect:
                        this.#ctx.connected = true;
                        this.#isReconnecting = false;
                        this.#flushOfflineBuffer();
                        this.#emitEvent('reconnected');
                        break;

                    case DebugEvents.Reconnecting:
                        if (!this.#isReconnecting) {
                            this.#isReconnecting = true;
                            this.#emitEvent('reconnecting');
                        }
                        break;

                    case Events.Error:
                        if (s.data === 'NATS_PROTOCOL_ERR') {
                            this.#emitEvent('auth_failed');
                            await this.#ctx.natsClient.close();
                        }
                        break;
                }
            }
        })().catch(() => {});
    }

    // ─── Offline Buffer ──────────────────────────────────────

    async #flushOfflineBuffer() {
        const messages = this.#ctx.offlineBuffer.splice(0);

        for (const { subject, payload } of messages) {
            try {
                await this.#ctx.jetstream.publish(subject, payload);
            } catch {
                // If publish fails during flush, discard — connection may drop again
            }
        }
    }

    #emitEvent(event) {
        if (this.#listenerCallback) {
            this.#listenerCallback(event);
        }
    }
}
