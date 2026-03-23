/**
 * ============================================================
 * CONNECTION SPEC — RelayApp Core + Connection Management
 * ============================================================
 *
 * Covers: RelayApp constructor, connect, disconnect,
 *         connection.listeners, connection.presence
 */

// ─────────────────────────────────────────────────────────────
// RelayApp(config)
// ─────────────────────────────────────────────────────────────

/**
 * @class RelayApp
 * @description Class that creates a new RelayApp instance via `new RelayApp(config)`.
 *              Validates config, extracts orgID from api_key, and
 *              instantiates all sub-manager classes.
 *
 * @param {Object} config
 * @param {string} config.api_key       - Required. NATS JWT credential.
 * @param {string} config.secret        - Required. NATS NKEY seed.
 * @param {string} config.mode          - Required. "production" | "test".
 *                                         Used as `env` in NATS subjects.
 *
 * @throws {Error} If api_key is null/undefined/empty
 * @throws {Error} If secret is null/undefined/empty
 * @throws {Error} If mode is not "production" or "test"
 * @throws {Error} If config is not an object
 *
 * @behavior
 * - Extracts orgID by decoding api_key via `decode()` from `nats-jwt` library
 * - Stores mode as `env` for use in all NATS subject construction
 * - Instantiates sub-managers as properties (NATS not live until connect()):
 *     app.connection      → ConnectionManager
 *     app.telemetry       → TelemetryManager
 *     app.command          → CommandManager
 *     app.rpc              → RPCManager
 *     app.device           → DeviceManager
 *     app.events           → EventManager
 *     app.alert            → AlertManager
 *     app.logicalGroup     → LogicalGroupManager
 *     app.heirarchyGroup   → HeirarchyGroupManager
 *     app.notification     → NotificationManager
 * - Each manager receives shared refs: natsClient, jetstream, orgID, env, deviceCache
 * - No network calls are made in the constructor
 *
 * @returns {RelayApp} Instance with all sub-managers attached
 *
 * @example
 * const app = new RelayApp({
 *     api_key: "<nats_jwt>",
 *     secret: "<nkey_seed>",
 *     mode: "production"
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.connect()
// ─────────────────────────────────────────────────────────────

/**
 * @method connect
 * @description Establishes NATS WebSocket connection, initializes JetStream,
 *              populates device cache, and starts status monitoring.
 *              Idempotent — calling connect() when already connected is a no-op.
 *
 * @nats_servers ["wss://api.relay-x.io:4421", "wss://api.relay-x.io:4422", "wss://api.relay-x.io:4423"]
 *
 * @throws {Error} On connection failure (auth_failed, network error)
 *
 * @behavior
 * - Builds NATS creds file from api_key (JWT) and secret (NKEY seed):
 *     -----BEGIN NATS USER JWT-----
 *     <api_key>
 *     ------END NATS USER JWT------
 *     ...
 *     -----BEGIN USER NKEY SEED-----
 *     <secret>
 *     ------END USER NKEY SEED------
 * - Connects via nats.ws connect() with:
 *     servers: <server_array>
 *     noEcho: true
 *     reconnect: true
 *     maxReconnectAttempts: 1200
 *     reconnectTimeWait: 1000
 *     authenticator: credsAuthenticator(creds)
 *     token: api_key
 * - Initializes JetStream client via jetstream(natsClient)
 * - Populates device cache via app.device.list()
 * - Starts monitoring natsClient.status() for connection events
 * - On success: fires "connected" event via connection.listeners
 * - On failure: fires "auth_failed" event via connection.listeners
 * - Sets up natsClient.closed() handler for disconnect detection
 *
 * @returns {Promise<void>}
 *
 * @example
 * await app.connect()
 */

// ─────────────────────────────────────────────────────────────
// app.disconnect()
// ─────────────────────────────────────────────────────────────

/**
 * @method disconnect
 * @description Gracefully closes the NATS connection and cleans up all resources.
 *
 * @behavior
 * - Deletes all active JetStream consumers (telemetry, events, presence, alerts, group streams)
 * - Clears device cache
 * - Closes NATS connection via natsClient.close()
 * - Fires "disconnected" event via connection.listeners
 * - Resets internal connected state
 * - If natsClient is null, logs warning and returns
 *
 * @returns {Promise<void>}
 *
 * @example
 * await app.disconnect()
 */

// ─────────────────────────────────────────────────────────────
// app.connection.listeners(callback)
// ─────────────────────────────────────────────────────────────

/**
 * @method connection.listeners
 * @description Registers a callback for connection lifecycle events.
 *
 * @param {Function} callback - Required. Called with event name string.
 *
 * @throws {Error} If callback is not a function
 *
 * @events
 * - "connected"         — Fired on successful initial connection
 * - "disconnected"      — Fired when connection is closed (clean or unexpected)
 * - "reconnecting"      — Fired when client is attempting to reconnect
 * - "reconnected"       — Fired when client successfully reconnects
 * - "auth_failed"       — Fired when authentication fails during connect
 * - "reconnect_failed"  — Fired when max reconnect attempts exhausted
 *
 * @behavior
 * - Maps from NATS client status events:
 *     Events.Disconnect      → "disconnected"
 *     Events.Reconnect       → "reconnected"
 *     DebugEvents.Reconnecting → "reconnecting"
 *     Events.Error (NATS_PROTOCOL_ERR) → "auth_failed"
 *     maxReconnectAttempts exhausted → "reconnect_failed"
 * - Only one listener callback at a time. Calling again replaces previous.
 *
 * @returns {void}
 *
 * @example
 * app.connection.listeners((event) => {
 *     console.log("Connection event:", event)
 *     // "connected" | "disconnected" | "reconnecting" |
 *     // "reconnected" | "auth_failed" | "reconnect_failed"
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.connection.presence(callback)
// ─────────────────────────────────────────────────────────────

/**
 * @method connection.presence
 * @description Subscribes to device presence events (connect/disconnect).
 *
 * @param {Function} callback - Required. Called with presence data object.
 *
 * @throws {Error} If callback is not a function
 * @throws {Error} If not connected
 *
 * @nats_subject import.{orgID}.{env}.presence.*
 * @nats_type jetstream_consumer
 * @encoding msgpack (decode on receive)
 *
 * @callback_payload
 * {
 *     device_ident: string,                    // Device identifier
 *     event: "connected" | "disconnected",     // Presence event type
 *     data: object                             // User-defined JSON, passthrough (no processing)
 * }
 *
 * @behavior
 * - Creates a JetStream consumer on the presence subject
 * - Wildcard `*` matches any device_id in the subject
 * - Decodes msgpack payload, invokes callback with decoded data
 * - The `data` field is passed through to the user without any processing
 * - Only one presence callback at a time. Calling again replaces previous.
 * - Consumer is cleaned up on disconnect()
 *
 * @returns {void}
 *
 * @example
 * app.connection.presence((data) => {
 *     console.log(`Device ${data.device_ident} ${data.event}`)
 *     // data.data contains user-defined payload from the device
 * })
 */
