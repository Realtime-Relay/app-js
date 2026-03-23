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
 * @throws {Error} If not connected
 * @throws {Error} If device_ident resolution fails (device not found)
 *
 * @nats_subject {orgID}.{env}.command.queue.<device_id>.<command_name>
 * @nats_type publish (JetStream publish per device)
 * @encoding msgpack
 *
 * @publish_payload (per device)
 * {
 *     value: object,         // The user-provided data object
 *     timestamp: number      // Date.now() — unix ms timestamp
 * }
 *
 * @behavior
 * - Resolves each device_ident to device_id via device cache
 *   (check local cache first, fallback to app.device.get() request)
 * - Loops through resolved device IDs
 * - For each device: msgpack encode payload, JetStream publish to command queue subject
 * - JetStream publish returns an ack object
 * - Returns true if ack != null (command was sent successfully)
 * - If any device ident cannot be resolved, throw error for that device
 *
 * @returns {Promise<boolean>} true if all commands published successfully (all acks received)
 *
 * @example
 * var sent = await app.command.send({
 *     name: "reboot",
 *     device_ident: ["sensor_01", "sensor_02"],
 *     data: { force: true, delay: 5 }
 * })
 * // sent === true if all devices received the command
 */

// ─────────────────────────────────────────────────────────────
// app.command.history({ name, device_idents, start, end })
// ─────────────────────────────────────────────────────────────

/**
 * @method command.history
 * @description Fetches historical command records for specific devices
 *              within a time range.
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
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.db.{orgID}.command.history
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     device_ids: string[],     // Resolved from device_idents via device cache
 *     env: string,              // From app mode
 *     command_name: string,     // The command name
 *     start: string,            // ISO8601 datetime
 *     end: string               // ISO8601 datetime (defaults to now())
 * }
 *
 * @response_payload
 * // Success:
 * {
 *     status: "COMMAND_FETCH_SUCCESS",
 *     data: object              // Command history records
 * }
 * // Failure:
 * {
 *     status: "COMMAND_FETCH_FAILURE",
 *     data: { msg: string[] }
 * }
 *
 * @behavior
 * - Resolves device_idents to device_ids via device cache
 * - If end is not provided, defaults to current time: new Date().toISOString()
 * - Returns data on success
 * - Returns failure response on business logic error
 * - Throws on transport/timeout error
 *
 * @returns {Promise<object>} Command history data
 *
 * @example
 * var history = await app.command.history({
 *     name: "reboot",
 *     device_idents: ["sensor_01", "sensor_02"],
 *     start: "2026-01-01T00:00:00Z",
 *     end: "2026-01-02T00:00:00Z"
 * })
 */
