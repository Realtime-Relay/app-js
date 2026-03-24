/**
 * ============================================================
 * TELEMETRY SPEC — Telemetry Streaming + History
 * ============================================================
 *
 * Covers: app.telemetry.stream, app.telemetry.off, app.telemetry.history, app.telemetry.latest
 */

// ─────────────────────────────────────────────────────────────
// app.telemetry.stream({ device_ident, metric, callback })
// ─────────────────────────────────────────────────────────────

/**
 * @method telemetry.stream
 * @description Subscribes to a real-time telemetry stream for a specific
 *              device and metric via JetStream consumer.
 *
 * @param {Object} params
 * @param {string} params.device_ident  - Required. Device identifier.
 *                                         Validated: [a-zA-Z0-9_-]+
 * @param {string} params.metric        - Required. Metric name or "*" for all metrics.
 * @param {Function} params.callback    - Required. Called with each telemetry data point.
 *
 * @throws {Error} If device_ident is null/undefined/empty
 * @throws {Error} If device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If metric is null/undefined/empty
 * @throws {Error} If metric is not "*" and not a key in device.schema
 * @throws {Error} If callback is not a function
 * @throws {Error} If not connected
 *
 * @nats_subject {orgID}.{env}.telemetry.<device_id>.<metric>
 * @nats_type jetstream_consumer
 * @encoding msgpack (decode on receive)
 *
 * @callback_payload
 * {
 *     metric: string,       // Metric name (e.g., "temperature")
 *     value: any,           // Metric value
 *     timestamp: number     // Unix timestamp (ms)
 * }
 *
 * @metric_validation
 * - Only two forms of metric are allowed:
 *   1. "*" — subscribe to all metrics for the device
 *   2. A specific metric name that MUST be a key in device.schema
 * - If metric is not "*" and is not found in device.schema, throw Error
 *   with message: 'metric "<name>" is not a valid key in device schema'
 *
 * @behavior
 * - Validates metric against device.schema (fetched via device.get())
 * - Resolves device_ident → device_id via device cache (local first, fallback to device.get())
 * - Creates a JetStream consumer for the specific device_id + metric subject
 * - If metric is "*", subscribes to all metrics for the device
 * - Decodes msgpack payload on each message, invokes callback
 * - One consumer per device_ident + metric combination (keyed by ident for user-facing tracking)
 * - If already subscribed to same device_ident + metric, return false (no duplicate)
 * - Consumer name format: appjs_telemetry_{device_ident}_{metric}_{uuid}
 *
 * @returns {boolean} true if subscription created, false if already exists
 *
 * @example
 * app.telemetry.stream({
 *     device_ident: "sensor_01",
 *     metric: "temperature",
 *     callback: (data) => {
 *         console.log(`${data.metric}: ${data.value} at ${data.timestamp}`)
 *     }
 * })
 *
 * // Subscribe to all metrics
 * app.telemetry.stream({
 *     device_ident: "sensor_01",
 *     metric: "*",
 *     callback: (data) => { ... }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.telemetry.off({ device_ident, metric? })
// ─────────────────────────────────────────────────────────────

/**
 * @method telemetry.off
 * @description Unsubscribes from telemetry streams for a device.
 *              Can target all metrics or specific ones.
 *
 * @param {Object} params
 * @param {string}   params.device_ident  - Required. Device identifier.
 * @param {string[]} [params.metric]      - Optional. Array of metric names to unsubscribe.
 *                                           If omitted, unsubscribes ALL metrics for the device.
 *
 * @throws {Error} If device_ident is null/undefined/empty
 * @throws {Error} If device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If metric is provided but not an array
 *
 * @behavior
 * - If metric is omitted: delete ALL JetStream consumers for device_ident
 *   (loop through all active consumers matching this device)
 * - If metric is provided: loop through array, delete consumer for each
 *   specific {orgID}.{env}.telemetry.<device_ident>.<metric>
 * - Removes consumer from internal consumer tracking map
 *
 * @returns {void}
 *
 * @example
 * // Unsubscribe from all metrics for a device
 * app.telemetry.off({ device_ident: "sensor_01" })
 *
 * // Unsubscribe from specific metrics only
 * app.telemetry.off({
 *     device_ident: "sensor_01",
 *     metric: ["temperature", "humidity"]
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.telemetry.history({ device_ident, fields, start, end })
// ─────────────────────────────────────────────────────────────

/**
 * @method telemetry.history
 * @description Fetches historical telemetry data for a device within a time range.
 *
 * @param {Object} params
 * @param {string}   params.device_ident  - Required. Device identifier.
 *                                           Validated: [a-zA-Z0-9_-]+
 * @param {string[]} params.fields        - Required. Array of metric field names
 *                                           (e.g., ["temperature", "humidity"]).
 * @param {string}   params.start         - Required. ISO8601 datetime string.
 * @param {string}   params.end           - Required. ISO8601 datetime string.
 *
 * @throws {Error} If device_ident is null/undefined/empty
 * @throws {Error} If fields is not a non-empty array
 * @throws {Error} If any field in fields is not a key in device.schema
 * @throws {Error} If start or end is not a valid ISO8601 string
 * @throws {Error} If start >= end (start must be before end)
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.db.{orgID}.telemetry.history
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     device_id: string,     // Resolved from device_ident via device cache
 *     env: string,           // From app mode ("production" | "test")
 *     start: string,         // ISO8601 datetime
 *     end: string,           // ISO8601 datetime
 *     fields: string[]       // Metric field names
 * }
 *
 * @response_payload
 * // Success:
 * {
 *     status: "TELEMETRY_FETCH_SUCCESS",
 *     data: object            // Telemetry history records
 * }
 * // Failure:
 * {
 *     status: "TELEMETRY_FETCH_FAILURE",
 *     data: { msg: string[] }
 * }
 *
 * @behavior
 * - Resolves device_ident to device_id via device cache (check local first, fallback to device.get())
 * - Validates start < end before making request
 * - Returns data on success
 * - Returns failure response on business logic error
 * - Throws on transport/timeout error
 *
 * @returns {Promise<object>} Telemetry history data
 *
 * @example
 * var history = await app.telemetry.history({
 *     device_ident: "sensor_01",
 *     fields: ["temperature", "humidity"],
 *     start: "2026-01-01T00:00:00.000Z",
 *     end: "2026-01-02T00:00:00.000Z"
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.telemetry.latest({ device_ident, fields })
// ─────────────────────────────────────────────────────────────

/**
 * @method telemetry.latest
 * @description Convenience method that fetches telemetry history for
 *              the last 24 hours (now - 1 day → now).
 *
 * @param {Object} params
 * @param {string}   params.device_ident  - Required. Device identifier.
 *                                           Validated: [a-zA-Z0-9_-]+
 * @param {string[]} params.fields        - Required. Array of metric field names
 *                                           (e.g., ["temperature", "humidity"]).
 *
 * @throws {Error} If device_ident is null/undefined/empty
 * @throws {Error} If fields is not a non-empty array
 * @throws {Error} If any field in fields is not a key in device.schema
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.db.{orgID}.telemetry.history
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     device_id: string,     // Resolved from device_ident via device cache
 *     env: string,           // From app mode ("production" | "test")
 *     start: string,         // now() - 24 hours, ISO8601
 *     end: string,           // now(), ISO8601
 *     fields: string[]       // Metric field names (each must be a key in device.schema)
 * }
 *
 * @behavior
 * - Computes start = now() - 24h, end = now()
 * - Resolves device_ident → device_id via device cache
 * - Sends same request as history() with auto-computed time range
 * - Returns the raw response from the backend
 *
 * @returns {Promise<object>} Telemetry history data for last 24 hours
 *
 * @example
 * var latest = await app.telemetry.latest({
 *     device_ident: "sensor_01",
 *     fields: ["temperature", "humidity"]
 * })
 */
