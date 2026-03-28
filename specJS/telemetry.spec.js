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
 *              device via a wildcard JetStream consumer. Supports subscribing
 *              to all metrics or a specific set with client-side filtering.
 *              Multiple independent subscriptions per device are allowed.
 *
 * @param {Object} params
 * @param {string}          params.device_ident  - Required. Device identifier.
 *                                                  Validated: [a-zA-Z0-9_-]+
 * @param {string|string[]} params.metric        - Required.
 *                                                  "*" (string) — subscribe to all metrics.
 *                                                  string[] — specific metric names to filter on.
 *                                                  Non-"*" strings are rejected.
 * @param {Function}        params.callback      - Required. Called with each telemetry data point.
 *
 * @throws {Error} If device_ident is null/undefined/empty
 * @throws {Error} If device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If metric is a non-"*" string
 * @throws {Error} If metric is not a string or array
 * @throws {Error} If metric is an empty array
 * @throws {Error} If any metric in array is not a key in device.schema
 * @throws {Error} If callback is not a function
 * @throws {Error} If not connected
 *
 * @nats_subject {orgID}.{env}.telemetry.<device_id>.*
 * @nats_type jetstream_consumer
 * @encoding msgpack (decode on receive)
 *
 * @callback_payload
 * {
 *     metric: string,       // Metric name extracted from last token of NATS subject
 *     data: object          // Raw msgpack-decoded payload from the message
 * }
 *
 * @metric_validation
 * - metric accepts two forms:
 *   1. "*" (string) — subscribe to all metrics, no client-side filter
 *   2. string[] — each entry MUST be a key in device.schema
 * - Non-"*" strings throw: 'metric as a string must be "*". Use an array for specific metrics.'
 * - Invalid schema keys throw: 'metric "<name>" is not a valid key in device schema'
 *
 * @consumer_tracking
 * - Internal map: Map<device_ident, Array<{ consumer, metrics: Set|null, callback }>>
 * - Each stream() call creates a new independent subscription (no dedup)
 * - metrics is null for "*" (all metrics), Set for specific metrics
 * - Multiple subscriptions for the same device are allowed with different callbacks/metrics
 *
 * @behavior
 * - If metric is string[]: validates each metric against device.schema
 * - Resolves device_ident → device_id via device cache (local first, fallback to device.get())
 * - Always creates a wildcard JetStream consumer: {orgID}.{env}.telemetry.{deviceId}.*
 * - Client-side filtering: if metrics is a Set, only invokes callback when the
 *   extracted metric name is in the Set. If metrics is null ("*"), all messages pass.
 * - Consumer name format: appjs_telemetry_{device_ident}_{uuid}
 *
 * @returns {void}
 *
 * @example
 * // Subscribe to specific metrics
 * app.telemetry.stream({
 *     device_ident: "sensor_01",
 *     metric: ["temperature", "humidity"],
 *     callback: (data) => {
 *         console.log(`${data.metric}:`, data.data)
 *     }
 * })
 *
 * // Subscribe to all metrics
 * app.telemetry.stream({
 *     device_ident: "sensor_01",
 *     metric: "*",
 *     callback: (data) => {
 *         console.log(`${data.metric}:`, data.data)
 *     }
 * })
 *
 * // Multiple independent subscriptions for same device
 * app.telemetry.stream({
 *     device_ident: "sensor_01",
 *     metric: ["temperature"],
 *     callback: tempHandler
 * })
 * app.telemetry.stream({
 *     device_ident: "sensor_01",
 *     metric: ["humidity"],
 *     callback: humidityHandler
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.telemetry.off({ device_ident, metric? })
// ─────────────────────────────────────────────────────────────

/**
 * @method telemetry.off
 * @description Unsubscribes from telemetry streams for a device.
 *              Can target all subscriptions or remove specific metrics from
 *              filtered subscriptions.
 *
 * @param {Object} params
 * @param {string}   params.device_ident  - Required. Device identifier.
 * @param {string[]} [params.metric]      - Optional. Array of metric names to unsubscribe.
 *                                           If omitted, unsubscribes ALL subscriptions for the device.
 *
 * @throws {Error} If device_ident is null/undefined/empty
 * @throws {Error} If device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If metric is provided but not an array
 *
 * @behavior
 * - If metric is omitted: delete ALL JetStream consumers for device_ident
 *   (all subscriptions including "*" subscriptions), remove map entry.
 * - If metric is provided: iterate all subscriptions for the device.
 *   - Wildcard subscriptions (metrics = null, i.e. "*") are SKIPPED — unaffected.
 *   - For filtered subscriptions: remove each specified metric from the Set.
 *   - If a subscription's metric Set becomes empty, delete its consumer.
 *   - If all subscriptions are removed, clean up the map entry.
 * - No-op if device has no active subscriptions.
 *
 * @returns {void}
 *
 * @example
 * // Unsubscribe all subscriptions for a device (including "*" subscriptions)
 * app.telemetry.off({ device_ident: "sensor_01" })
 *
 * // Remove specific metrics from filtered subscriptions (leaves "*" subscriptions intact)
 * app.telemetry.off({
 *     device_ident: "sensor_01",
 *     metric: ["temperature"]
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
 * @encoding JSONCodec (request) / msgpack (response)
 *
 * @request_payload
 * {
 *     device_id: string,     // Resolved from device_ident via device cache
 *     env: string,           // From app mode ("production" | "test")
 *     start: string,         // ISO8601 datetime (cursor on subsequent pages)
 *     end: string,           // ISO8601 datetime
 *     fields: string[],      // Metric field names
 *     last_value: false      // Always false for history()
 * }
 *
 * @response_payload (msgpack decoded)
 * // Success (paginated):
 * {
 *     status: "TELEMETRY_FETCH_SUCCESS",
 *     data: {
 *         has_more: boolean,                      // true if more pages exist
 *         cursor: string,                         // ISO8601 cursor for next page (if has_more)
 *         data: { "<metric>": array }             // telemetry records per metric for this page
 *     }
 * }
 * // Failure:
 * {
 *     status: "TELEMETRY_FETCH_FAILURE",
 *     data: { msg: string[] }
 * }
 *
 * @pagination
 * - Uses a while(true) loop with cursor-based pagination
 * - First request uses params.start as the start cursor
 * - If response has has_more=true, sets startCursor = data.cursor and loops
 * - If response has has_more=false or status is not SUCCESS, breaks
 * - Accumulates records across pages: telemetry[metric].concat(telemetryPage[metric])
 * - Pre-initializes telemetry object with empty arrays for all requested fields
 * - Request timeout: 20000ms
 *
 * @behavior
 * - Resolves device_ident to device_id via device cache (check local first, fallback to device.get())
 * - Validates start < end before making request
 * - On non-success status: breaks loop (returns whatever has been accumulated)
 * - On transport/timeout error: throws Error("Telemetry history request timed-out")
 *
 * @returns {Promise<Object.<string, array>>} Object keyed by metric name → array of records
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
 * @description Fetches the latest (most recent) telemetry value for each
 *              requested field within a caller-specified time range.
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
 * @encoding JSONCodec (request) / msgpack (response)
 *
 * @request_payload
 * {
 *     device_id: string,     // Resolved from device_ident via device cache
 *     env: string,           // From app mode ("production" | "test")
 *     start: string,         // ISO8601 datetime (caller-provided)
 *     end: string,           // ISO8601 datetime (caller-provided)
 *     fields: string[],      // Metric field names (each must be a key in device.schema)
 *     last_value: true       // Always true for latest() — tells backend to return only most recent value
 * }
 *
 * @response_payload (msgpack decoded)
 * // Success:
 * {
 *     status: "TELEMETRY_FETCH_SUCCESS",
 *     data: {
 *         data: { "<metric>": [<single_record>] }  // Array with one element per metric
 *     }
 * }
 *
 * @behavior
 * - Validates start < end before making request
 * - Resolves device_ident → device_id via device cache
 * - Sends a single request (no pagination) with last_value: true
 * - On TELEMETRY_FETCH_SUCCESS: extracts first element of each metric array
 *   → returns { "<metric>": <single_record> } (flat object)
 * - On non-success status: returns empty object {}
 * - On transport/timeout error: throws Error("Telemetry history request timed-out")
 * - Request timeout: 20000ms
 *
 * @returns {Promise<Object.<string, object>>} Object keyed by metric name → single latest record
 *
 * @example
 * var latest = await app.telemetry.latest({
 *     device_ident: "sensor_01",
 *     fields: ["temperature", "humidity"],
 *     start: "2026-03-27T00:00:00.000Z",
 *     end: "2026-03-28T00:00:00.000Z"
 * })
 * // latest === { temperature: { value: 25.3, time: "..." }, humidity: { value: 60, time: "..." } }
 */
