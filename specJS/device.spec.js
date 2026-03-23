/**
 * ============================================================
 * DEVICE SPEC — Device CRUD, List, Get + Cache
 * ============================================================
 *
 * Covers: app.device.create, app.device.update, app.device.delete,
 *         app.device.list, app.device.get, device cache behavior
 */

// ─────────────────────────────────────────────────────────────
// DEVICE CACHE BEHAVIOR
// ─────────────────────────────────────────────────────────────

/**
 * @description Internal device cache for ident-to-device mapping.
 *
 * @behavior
 * - Populated on app.connect() via app.device.list()
 * - Stores full device objects keyed by ident
 * - TTL: 2 hours from last population/refresh
 * - On TTL expiry: lazy-reload on next access (next .get() or ident resolution triggers device.list())
 * - On device.create() success: immediately add new device to cache
 * - On device.update() success: immediately update cached device
 * - On device.delete() success: immediately remove device from cache
 * - Used by other managers (command, rpc, telemetry history, groups) to resolve ident → device_id
 * - Resolution flow: check cache → if miss or TTL expired → NATS request to device.get
 *
 * @cache_structure
 * Map<string, object>  // key: device_ident, value: full device object (includes id, ident, schema, config, etc.)
 *
 * @ttl 7200000 (2 hours in milliseconds)
 */

// ─────────────────────────────────────────────────────────────
// app.device.create({ ident, schema, config })
// ─────────────────────────────────────────────────────────────

/**
 * @method device.create
 * @description Creates a new device. Subject to org limit checks on the backend.
 *
 * @param {Object} params
 * @param {string} params.ident   - Required. Unique device identifier.
 *                                   Validated: [a-zA-Z0-9_-]+
 * @param {Object} params.schema  - Required. Device schema (must be valid JSON).
 * @param {Object} params.config  - Required. Device config (must be valid JSON).
 *
 * @throws {Error} If ident is null/undefined/empty
 * @throws {Error} If ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If schema is not a valid JSON object
 * @throws {Error} If config is not a valid JSON object
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.devices.{orgID}.create
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     ident: string,      // Device identifier
 *     env: string,        // From app mode ("production" | "test")
 *     schema: object,     // Device schema
 *     config: object      // Device config
 * }
 *
 * @response_payload
 * // Success:
 * {
 *     status: "DEVICE_CREATE_SUCCESS",
 *     data: {
 *         id: string,         // Generated device ID
 *         org_id: string,
 *         ident: string,
 *         env: string,
 *         schema: object,
 *         config: object,
 *         creds: object       // Device NATS credentials
 *     }
 * }
 * // Failure:
 * {
 *     status: "DEVICE_CREATE_FAILURE",
 *     data: {
 *         msg: string[],
 *         code?: "DEVICE_CREATE_LIMIT_REACHED",
 *         data?: { limit: number, current_count: number }
 *     }
 * }
 *
 * @behavior
 * - Sends request to backend
 * - On success: adds device to local cache, returns device data
 * - On failure: returns failure response (does not throw)
 * - Throws only on transport/timeout errors
 *
 * @returns {Promise<object>} Device object on success, failure response on business error
 *
 * @example
 * var device = await app.device.create({
 *     ident: "sensor_01",
 *     schema: { temperature: "float", humidity: "float" },
 *     config: { report_interval: 30 }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.device.update({ ident, schema, config })
// ─────────────────────────────────────────────────────────────

/**
 * @method device.update
 * @description Updates an existing device's schema and/or config.
 *
 * @param {Object} params
 * @param {string} params.ident    - Required. Device identifier to update.
 *                                    Validated: [a-zA-Z0-9_-]+
 * @param {Object} [params.schema] - Optional. New device schema (must be valid JSON).
 * @param {Object} [params.config] - Optional. New device config (must be valid JSON).
 *
 * @throws {Error} If ident is null/undefined/empty
 * @throws {Error} If ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If neither schema nor config is provided
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.devices.{orgID}.update
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     id: string,          // Device ID (resolved from ident via cache)
 *     ident: string,       // Optional — only if renaming
 *     schema: object,      // Optional — new schema
 *     config: object       // Optional — new config
 * }
 *
 * @response_payload
 * // Success:
 * {
 *     status: "DEVICE_UPDATE_SUCCESS",
 *     data: {
 *         org_id: string,
 *         device: {
 *             id: string,
 *             ident: string,
 *             schema: object,
 *             config: object
 *         }
 *     }
 * }
 * // Failure:
 * {
 *     status: "DEVICE_UPDATE_FAILURE",
 *     data: {
 *         msg: string,
 *         code?: "DEVICE_IDENT_EXISTS"
 *     }
 * }
 *
 * @behavior
 * - Resolves ident to device ID via cache before sending request
 * - On success: updates device in local cache, returns device data
 * - On failure: returns failure response
 * - Throws on transport/timeout errors
 *
 * @returns {Promise<object>} Updated device object on success
 *
 * @example
 * var device = await app.device.update({
 *     ident: "sensor_01",
 *     config: { report_interval: 60 }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.device.delete(ident)
// ─────────────────────────────────────────────────────────────

/**
 * @method device.delete
 * @description Deletes a device by its identifier.
 *
 * @param {string} ident - Required. Device identifier.
 *                          Validated: [a-zA-Z0-9_-]+
 *
 * @throws {Error} If ident is null/undefined/empty
 * @throws {Error} If ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.devices.{orgID}.delete
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     ident: string       // Device identifier
 * }
 *
 * @response_payload
 * // Success:
 * {
 *     status: "DEVICE_DELETE_SUCCESS",
 *     data: {
 *         org_id: string,
 *         device_id: string
 *     }
 * }
 * // Failure:
 * {
 *     status: "DEVICE_DELETE_FAILURE",
 *     data: {
 *         msg: string,
 *         code?: "INVALID_DEVICE_ID"
 *     }
 * }
 *
 * @behavior
 * - On success: removes device from local cache
 * - On failure: returns failure response
 * - Throws on transport/timeout errors
 *
 * @returns {Promise<boolean>} true on successful deletion
 *
 * @example
 * var deleted = await app.device.delete("sensor_01")
 */

// ─────────────────────────────────────────────────────────────
// app.device.list()
// ─────────────────────────────────────────────────────────────

/**
 * @method device.list
 * @description Fetches all devices for the org. Also refreshes the local device cache.
 *
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.devices.{orgID}.list
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {} (empty object)
 *
 * @response_payload
 * {
 *     status: "DEVICE_FETCH_SUCCESS",
 *     data: {
 *         org_id: string,
 *         devices: object[]    // Array of device objects
 *     }
 * }
 *
 * @behavior
 * - Sends request to backend
 * - Replaces entire local device cache with response data
 * - Resets cache TTL to 2 hours from now
 * - Called automatically during app.connect()
 *
 * @returns {Promise<object[]>} Array of device objects
 *
 * @example
 * var devices = await app.device.list()
 */

// ─────────────────────────────────────────────────────────────
// app.device.get({ ident })
// ─────────────────────────────────────────────────────────────

/**
 * @method device.get
 * @description Gets a single device by identifier.
 *              Checks local cache first, falls back to NATS request.
 *
 * @param {Object} params
 * @param {string} params.ident - Required. Device identifier.
 *                                 Validated: [a-zA-Z0-9_-]+
 *
 * @throws {Error} If ident is null/undefined/empty
 * @throws {Error} If ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.devices.{orgID}.get
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     ident: string       // Device identifier
 * }
 *
 * @response_payload
 * // Success:
 * {
 *     status: "DEVICE_GET_SUCCESS",
 *     data: object        // Full device object
 * }
 * // Failure:
 * {
 *     status: "DEVICE_GET_FAILURE",
 *     data: { msg: string }
 * }
 *
 * @behavior
 * - Step 1: Check local cache for ident
 * - Step 2: If cache hit AND TTL is valid → return cached device (no network call)
 * - Step 3: If cache miss OR TTL expired → send NATS request to backend
 * - Step 4: On success, update cache with response and return device
 * - Step 5: On failure, return failure response
 *
 * @returns {Promise<object>} Device object
 *
 * @example
 * var device = await app.device.get({ ident: "sensor_01" })
 * // device = { id: "...", ident: "sensor_01", schema: {...}, config: {...}, ... }
 */
