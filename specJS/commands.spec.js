/**
 * ============================================================
 * COMMANDS SPEC — Command Send + History
 * ============================================================
 *
 * Covers: app.command.send, app.command.history
 */

// ─────────────────────────────────────────────────────────────
// app.command.send({ name, device_ident, data })
// ─────────────────────────────────────────────────────────────

/**
 * @method command.send
 * @description Sends a command to one or more devices via JetStream publish.
 *              Resolves device idents to device IDs and publishes individually
 *              to each device's command queue subject.
 *
 * @param {Object} params
 * @param {string}   params.name          - Required. Command name.
 *                                           Validated: [a-zA-Z0-9_-]+
 * @param {string[]} params.device_ident  - Required. Array of device identifiers.
 * @param {Object}   params.data          - Required. Arbitrary command payload (user-defined).
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If device_ident is not a non-empty array
 * @throws {Error} If any device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If data is null/undefined
 *
 * @nats_subject {orgID}.{env}.command.queue.<device_id>.<command_name>
 * @nats_type publish (JetStream publish per device, or buffered if offline)
 * @encoding msgpack
 *
 * @publish_payload (per device)
 * {
 *     value: object,         // The user-provided data object
 *     timestamp: number      // Date.now() — unix ms timestamp
 * }
 *
 * @offline_behavior
 * - Uses ctx.publishOrBuffer() instead of direct jetstream.publish()
 * - If connected: publishes immediately via JetStream
 * - If disconnected: buffers the message in ctx.offlineBuffer[]
 * - Buffered messages are flushed automatically on reconnect
 * - Does NOT throw when disconnected — commands are queued
 *
 * @behavior
 * - Resolves each device_ident to device_id via device cache
 *   (check local cache first, fallback to app.device.get() request)
 * - Unfound devices are SKIPPED (no publish), marked with error in result
 * - For each found device: msgpack encode payload, publish or buffer
 * - Returns a per-ident result map:
 *     { sent: true }                              — ack received (online publish succeeded)
 *     { sent: false, buffered: true }             — queued for later (offline OR publish failed/TIMEOUT)
 *     { sent: false, error: "Device not found" }  — ident could not be resolved
 *
 * @returns {Promise<Object.<string, { sent: boolean, buffered?: boolean, error?: string }>>}
 *          Map of ident → send result
 *
 * @example
 * var result = await app.command.send({
 *     name: "reboot",
 *     device_ident: ["sensor_01", "sensor_99"],
 *     data: { force: true, delay: 5 }
 * })
 * // result === {
 * //   "sensor_01": { sent: true },
 * //   "sensor_99": { sent: false, error: "Device not found" }
 * // }
 */

// ─────────────────────────────────────────────────────────────
// app.command.history({ name, device_idents, start, end })
// ─────────────────────────────────────────────────────────────

/**
 * @method command.history
 * @description Fetches historical command records for specific devices
 *              within a time range. Sends a single NATS request with all
 *              resolved device_ids.
 *
 * @param {Object} params
 * @param {string}   params.name           - Required. Command name.
 *                                            Validated: [a-zA-Z0-9_-]+
 * @param {string[]} params.device_idents  - Required. Array of device identifiers.
 * @param {string}   params.start          - Required. ISO8601 datetime string.
 * @param {string}   [params.end]          - Optional. ISO8601 datetime string.
 *                                            Defaults to new Date().toISOString() if not specified.
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If device_idents is not a non-empty array
 * @throws {Error} If start is not a valid ISO8601 string
 * @throws {Error} If end is provided and not a valid ISO8601 string
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.db.{orgID}.command.history
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     device_ids: string[],     // Only found device IDs (unfound are excluded)
 *     env: string,              // From app mode
 *     command_name: string,     // The command name
 *     start: string,            // ISO8601 datetime
 *     end: string               // ISO8601 datetime (defaults to now())
 * }
 *
 * @response_payload
 * // Success (backend returns data keyed by device_id):
 * {
 *     status: "COMMAND_FETCH_SUCCESS",
 *     data: { "<device_id>": array }
 * }
 * // Failure:
 * {
 *     status: "COMMAND_FETCH_FAILURE",
 *     data: { msg: string[] }
 * }
 *
 * @behavior
 * - Resolves device_idents to device_ids via device cache
 * - Unfound idents are SKIPPED from the request, marked with error in result
 * - Sends a single NATS request with only found device_ids
 * - If all idents are unfound, skips the request entirely
 * - Remaps backend response keys from device_id → ident
 * - If end is not provided, defaults to current time: new Date().toISOString()
 * - Returns a per-ident map:
 *     { "<ident>": [ ...records ] }                  — found device with history
 *     { "<ident>": { error: "Device not found" } }   — ident could not be resolved
 * - Throws on transport/timeout error
 *
 * @returns {Promise<Object.<string, (array|{ error: string })>>}
 *          Map of ident → history records or error
 *
 * @example
 * var history = await app.command.history({
 *     name: "reboot",
 *     device_idents: ["sensor_01", "sensor_99"],
 *     start: "2026-01-01T00:00:00.000Z",
 *     end: "2026-01-02T00:00:00.000Z"
 * })
 * // history === {
 * //   "sensor_01": [ { value: {...}, time: "..." }, ... ],
 * //   "sensor_99": { error: "Device not found" }
 * // }
 */
