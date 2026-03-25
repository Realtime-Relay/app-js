import { ConnectionManager } from './connection.js';
import { TelemetryManager } from './telemetry.js';
import { CommandManager } from './commands.js';
import { RPCManager } from './rpc.js';
import { DeviceManager } from './device.js';
import { EventManager } from './events.js';
import { AlertManager } from './alerts.js';
import { LogicalGroupManager } from './logicalGroup.js';
import { HeirarchyGroupManager } from './heirarchyGroup.js';
import { NotificationManager } from './notifications.js';
import { decode } from 'nats-jwt';


export class RelayApp {

    constructor(config) {
        if (config == null || typeof config !== 'object') {
            throw new Error('config must be an object');
        }

        if (!config.api_key) {
            throw new Error('api_key is required');
        }

        if (!config.secret) {
            throw new Error('secret is required');
        }

        if (config.mode !== 'production' && config.mode !== 'test') {
            throw new Error('mode must be "production" or "test"');
        }

        const debug = config.debug ?? false;
        if (typeof debug !== 'boolean') {
            throw new Error('debug must be a boolean');
        }

        // Decode orgID from api_key using nats-jwt decode()
        let orgID;

        try {
            const decoded = decode(config.api_key);
            orgID = decoded.nats.org_data.org_id;
        } catch (e) {
            throw new Error('Invalid api_key: could not decode JWT to extract orgID');
        }

        if (!orgID) {
            throw new Error('Could not extract orgID from api_key');
        }

        // Shared context object — mutated by managers
        const ctx = {
            apiKey: config.api_key,
            secret: config.secret,
            orgID,
            env: config.mode,
            natsClient: null,
            jetstream: null,
            connected: false,
            offlineBuffer: [],

            _debug: debug,

            logger: {
                error(msg, err) {
                    if (ctx._debug) {
                        console.error(`[relay-sdk] ERROR: ${msg}`);
                        if (err) console.error(err);
                    }
                },
                warn(msg) {
                    if (ctx._debug) console.warn(`[relay-sdk] WARN: ${msg}`);
                },
                info(msg) {
                    if (ctx._debug) console.log(`[relay-sdk] INFO: ${msg}`);
                },
                debug(msg) {
                    if (ctx._debug) console.log(`[relay-sdk] DEBUG: ${msg}`);
                },
            },

            /**
             * Publish via JetStream if connected, otherwise buffer for later.
             * Returns ack if published, null if buffered.
             */
            async publishOrBuffer(subject, payload) {
                if (ctx.connected && ctx.jetstream) {
                    try {
                        return await ctx.jetstream.publish(subject, payload);
                    } catch (err) {
                        // Publish failed (TIMEOUT, disconnect race) — buffer it
                        ctx.offlineBuffer.push({ subject, payload });
                        return null;
                    }
                }

                ctx.offlineBuffer.push({ subject, payload });
                return null;
            },
        };

        // Initialize managers
        this.device = new DeviceManager(ctx);
        ctx.device = this.device;

        this.connection = new ConnectionManager(ctx);
        this.telemetry = new TelemetryManager(ctx);
        this.command = new CommandManager(ctx);
        this.rpc = new RPCManager(ctx);
        this.events = new EventManager(ctx);
        this.alert = new AlertManager(ctx);
        this.logicalGroup = new LogicalGroupManager(ctx);
        this.heirarchyGroup = new HeirarchyGroupManager(ctx);
        this.notification = new NotificationManager(ctx);

        this._ctx = ctx; // Exposed for testing only
    }

    setDebug(enabled) {
        if (typeof enabled !== 'boolean') {
            throw new Error('debug must be a boolean');
        }

        this._ctx._debug = enabled;
    }

    async connect() {
        return this.connection.connect();
    }

    async disconnect() {
        return this.connection.disconnect();
    }
}

export default RelayApp;
