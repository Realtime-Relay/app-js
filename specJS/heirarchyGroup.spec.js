/**
 * ============================================================
 * HEIRARCHY GROUP SPEC — Hierarchy Group CRUD, Device List, Stream
 * ============================================================
 *
 * Covers: app.heirarchyGroup.create, update, delete, list, get,
 *         listDevices, heirarchyGroup.stream
 */

// ─────────────────────────────────────────────────────────────
// app.heirarchyGroup.create({ name, heirarchy, device_idents })
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.create
 * @description Creates a new hierarchy group with a hierarchy path and devices.
 *
 * @param {Object} params
 * @param {string}   params.name           - Required. Group name.
 *                                            Validated: [a-zA-Z0-9_.-]+
 * @param {string}   params.heirarchy      - Required. Hierarchy path (dot-separated tokens).
 *                                            Validated: [a-zA-Z0-9_.-]+
 *                                            Example: "building_a.floor_1"
 * @param {string[]} params.device_idents  - Required. Array of device identifiers.
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_.-]+)
 * @throws {Error} If heirarchy is null/undefined/empty
 * @throws {Error} If heirarchy fails validation ([a-zA-Z0-9_.-]+)
 * @throws {Error} If device_idents is not an array
 * @throws {Error} If any device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.heirarchy.create
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     device_ids: string[],   // Resolved from device_idents via device cache
 *     name: string,
 *     heirarchy: string       // Dot-separated hierarchy path
 * }
 *
 * @response_payload
 * { status: "HEIRARCHY_GROUP_CREATE_SUCCESS" }
 *
 * @behavior
 * - Resolves device_idents to device_ids via device cache (skips unfound)
 * - If res.data exists, wraps with .stream() and returns
 * - If res.data is absent, returns undefined
 *
 * @returns {Promise<HeirarchyGroupObject|undefined>} Group object with .stream() if data returned, else undefined
 *
 * @example
 * var group = await app.heirarchyGroup.create({
 *     name: "building_a_floor_1",
 *     heirarchy: "building_a.floor_1",
 *     device_idents: ["sensor_01", "sensor_02"]
 * })
 * // group.stream({ callback: (data) => ... })
 */

// ─────────────────────────────────────────────────────────────
// app.heirarchyGroup.update({ id, name, heirarchy, devices })
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.update
 * @description Updates a hierarchy group's name, hierarchy path, and/or device membership.
 *
 * @param {Object} params
 * @param {string}   params.id                  - Required. Group ID.
 * @param {string}   [params.name]              - Optional. New group name.
 * @param {string}   [params.heirarchy]         - Optional. New hierarchy path.
 *                                                Validated: [a-zA-Z0-9_.-]+
 * @param {Object}   [params.devices]           - Optional. Device changes.
 * @param {string[]} [params.devices.add]       - Device idents to add.
 * @param {string[]} [params.devices.remove]    - Device idents to remove.
 *
 * @throws {Error} If id is null/undefined/empty
 * @throws {Error} If heirarchy fails validation ([a-zA-Z0-9_.-]+) when provided
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.heirarchy.update
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     id: string,
 *     devices: {
 *         add: string[],       // Resolved device_ids
 *         remove: string[]     // Resolved device_ids
 *     },
 *     heirarchy: string        // Updated hierarchy path
 * }
 *
 * @response_payload
 * // Success:
 * { status: "HIERARCHY_GROUP_UPDATE_SUCCESS" }
 * // Failure:
 * { status: "HIERARCHY_GROUP_UPDATE_FAILURE", data: { msg: string[] } }
 *
 * @behavior
 * - Resolves device idents in add/remove arrays to device_ids via cache (skips unfound)
 * - If res.data exists, wraps with .stream() and returns
 * - If res.data is absent, returns undefined
 *
 * @returns {Promise<HeirarchyGroupObject|undefined>} Updated group object with .stream() if data returned, else undefined
 *
 * @example
 * var group = await app.heirarchyGroup.update({
 *     id: "<group_id>",
 *     heirarchy: "building_a.floor_2",
 *     devices: { add: ["sensor_05"], remove: ["sensor_01"] }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.heirarchyGroup.delete(group_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.delete
 * @description Deletes a hierarchy group by ID.
 *
 * @param {string} group_id - Required. Group ID.
 *
 * @throws {Error} If group_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.heirarchy.delete
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * { status: "HEIRARCHY_GROUP_DELETE_SUCCESS" }
 *
 * @returns {Promise<boolean>} true on successful deletion
 *
 * @example
 * var deleted = await app.heirarchyGroup.delete("<group_id>")
 */

// ─────────────────────────────────────────────────────────────
// app.heirarchyGroup.list()
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.list
 * @description Lists all hierarchy groups for the org.
 *
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.heirarchy.list
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {} (empty object)
 *
 * @response_payload
 * { status: "HEIRARCHY_GROUP_LIST_SUCCESS", data: object[] }
 *
 * @returns {Promise<object[]>} Array of hierarchy group objects
 *
 * @example
 * var groups = await app.heirarchyGroup.list()
 */

// ─────────────────────────────────────────────────────────────
// app.heirarchyGroup.get(group_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.get
 * @description Gets a single hierarchy group by ID.
 *              Returns a group object with .stream() method.
 *
 * @param {string} group_id - Required. Group ID.
 *
 * @throws {Error} If group_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.heirarchy.get
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * // Success:
 * { status: "HEIRARCHY_GROUP_GET_SUCCESS", data: object }
 * // Failure:
 * { status: "HEIRARCHY_GROUP_GET_FAILURE", data: null }
 *
 * @behavior
 * - On HEIRARCHY_GROUP_GET_SUCCESS, injects `id = groupId` into res.data, wraps with .stream()
 * - On failure, returns raw response object
 *
 * @returns {Promise<HeirarchyGroupObject|object>} Wrapped group on success, raw response on failure
 *
 * @example
 * var group = await app.heirarchyGroup.get("<group_id>")
 */

// ─────────────────────────────────────────────────────────────
// app.heirarchyGroup.listDevices(group_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.listDevices
 * @description Lists all devices in a hierarchy group.
 *
 * @param {string} group_id - Required. Group ID.
 *
 * @throws {Error} If group_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.heirarchy.device.list
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * { status: "HEIRARCHY_DEVICE_LIST_SUCCESS", data: object[] }
 *
 * @returns {Promise<object[]>} Array of device objects
 *
 * @example
 * var devices = await app.heirarchyGroup.listDevices("<group_id>")
 */

// ─────────────────────────────────────────────────────────────
// heirarchyGroup.stream({ device_idents?, heirarchy?, metric?, metrics?, callback })
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.stream
 * @description Subscribes to the hierarchy group's real-time data stream.
 *              Metric and hierarchy are part of the NATS subject (server-side).
 *              Device idents and multi-metric arrays are filtered client-side.
 *
 * @param {Object} params
 * @param {string[]} [params.device_idents]  - Optional. Array of device identifiers
 *                                              to filter on (client-side).
 * @param {string}   [params.heirarchy]      - Optional. Hierarchy wildcard override
 *                                              for the NATS subject. If omitted, uses
 *                                              the group's stored hierarchy path.
 *                                              Validated: [a-zA-Z0-9_.*>-]+
 *                                              ">" must be the last token.
 * @param {string}   [params.metric]         - Optional. Must be "*" (all metrics).
 *                                              Mutually exclusive with `metrics`.
 * @param {string[]} [params.metrics]        - Optional. Array of specific metric names.
 *                                              Mutually exclusive with `metric`.
 *                                              Each validated: [a-zA-Z0-9_-]+
 *                                              If single item: used directly in NATS subject.
 *                                              If multiple: subject uses "*", filter client-side.
 * @param {Function} params.callback         - Required. Called with each data point.
 *
 * @throws {Error} If callback is not a function
 * @throws {Error} If both metric and metrics are provided
 * @throws {Error} If heirarchy is provided and fails wildcard validation
 * @throws {Error} If ">" appears anywhere other than the last token
 * @throws {Error} If metrics contains invalid ident characters
 * @throws {Error} If not connected
 *
 * @nats_subject import.{orgID}.{env}.heirarchy.listen.{metric_token}.{heirarchy_token}
 * @nats_type jetstream_consumer
 * @encoding msgpack (decode on receive)
 *
 * @subject_construction
 * metric_token:
 *   - metric: "*"                     → "*"
 *   - metrics: ["temperature"]        → "temperature" (single → direct in subject)
 *   - metrics: ["temp", "humidity"]   → "*" (multiple → client-side filter)
 *   - neither provided                → "*" (default: all metrics)
 *
 * heirarchy_token:
 *   - params.heirarchy provided       → use params.heirarchy (wildcards allowed)
 *   - params.heirarchy omitted        → use group.heirarchy (from wrapped group)
 *
 * @callback_payload
 * {
 *     ident: string,        // Device identifier
 *     metric: string,       // Metric name
 *     value: any,           // Metric value
 *     timestamp: number     // Unix timestamp
 * }
 *
 * @behavior
 * - Constructs NATS subject with metric_token and heirarchy_token (server-side filtering)
 * - Creates a JetStream consumer on the constructed subject
 * - Decodes msgpack payload
 * - Client-side filtering (applied in order):
 *   1. If device_idents provided: check if decoded ident is in the array
 *   2. If metrics has multiple items: check if decoded metric is in the array
 * - Both client-side filters must pass for callback to be invoked (AND logic)
 * - Consumer is cleaned up on disconnect()
 *
 * @returns {void}
 *
 * @example
 * var group = await app.heirarchyGroup.get("<group_id>")
 *
 * // All metrics, all devices, group's stored hierarchy
 * group.stream({
 *     callback: (data) => {
 *         console.log(data)
 *     }
 * })
 *
 * // Specific metric, override hierarchy with wildcard
 * group.stream({
 *     metrics: ["temperature"],
 *     heirarchy: "campus.building_a.*",
 *     callback: (data) => {
 *         console.log(data)
 *     }
 * })
 *
 * // Multiple metrics (client-side filter), filter by device
 * group.stream({
 *     device_idents: ["sensor_01"],
 *     metrics: ["temperature", "humidity"],
 *     callback: (data) => {
 *         console.log(data)
 *     }
 * })
 *
 * // All metrics explicitly
 * group.stream({
 *     metric: "*",
 *     callback: (data) => {
 *         console.log(data)
 *     }
 * })
 */
