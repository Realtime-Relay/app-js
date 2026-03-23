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
 * - Resolves device_idents to device_ids via device cache
 * - Returns group object with .stream() method on success
 *
 * @returns {Promise<HeirarchyGroupObject>} Group object with .stream()
 *
 * @example
 * var group = await app.heirarchyGroup.create({
 *     name: "building_a_floor_1",
 *     heirarchy: "building_a.floor_1",
 *     device_idents: ["sensor_01", "sensor_02"]
 * })
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
 * - Resolves device idents in add/remove arrays to device_ids via cache
 *
 * @returns {Promise<HeirarchyGroupObject>} Updated group object with .stream()
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
 * @returns {Promise<HeirarchyGroupObject>} Group object with .stream()
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
// heirarchyGroup.stream({ device_idents?, heirarchy?, callback })
// ─────────────────────────────────────────────────────────────

/**
 * @method heirarchyGroup.stream
 * @description Subscribes to the hierarchy group's real-time data stream.
 *              Optionally filters by device identifiers and/or hierarchy
 *              wildcard pattern (both client-side).
 *
 * @param {Object} params
 * @param {string[]} [params.device_idents]  - Optional. Array of device identifiers
 *                                              to filter on. Only data from matching
 *                                              devices is passed to callback.
 * @param {string}   [params.heirarchy]      - Optional. Hierarchy wildcard pattern for
 *                                              client-side filtering.
 *                                              Validated: [a-zA-Z0-9_.*>-]+
 *                                              Supports NATS-style wildcards:
 *                                              "*" matches single token,
 *                                              ">" matches one or more tokens (must be last).
 *                                              Example: "building_a.*" or "building_a.>"
 * @param {Function} params.callback         - Required. Called with each data point.
 *
 * @throws {Error} If callback is not a function
 * @throws {Error} If heirarchy is provided and fails validation ([a-zA-Z0-9_.*>-]+)
 * @throws {Error} If not connected
 *
 * @nats_subject import.{orgID}.{env}.heirarchy.listen.{groupID}
 * @nats_type jetstream_consumer
 * @encoding msgpack (decode on receive)
 *
 * @callback_payload
 * {
 *     ident: string,                      // Device identifier
 *     data: { "<metric>": <value> }       // Metric key-value pairs
 * }
 *
 * @behavior
 * - Creates a JetStream consumer on the hierarchy group's listen subject
 * - Decodes msgpack payload
 * - Client-side filtering (applied in order):
 *   1. If device_idents provided: check if decoded ident is in the array
 *   2. If heirarchy provided: match the device's hierarchy path against
 *      the wildcard pattern using NATS topic pattern matching rules
 *      (same logic as Realtime class #topicPatternMatcher)
 * - Both filters must pass for callback to be invoked (AND logic)
 * - If no filters provided: invoke callback for all data
 * - Consumer is cleaned up on disconnect()
 *
 * @returns {void}
 *
 * @example
 * var group = await app.heirarchyGroup.get("<group_id>")
 *
 * // Filter by device and hierarchy
 * group.stream({
 *     device_idents: ["sensor_01"],
 *     heirarchy: "building_a.*",
 *     callback: (data) => {
 *         console.log(`Device ${data.ident}:`, data.data)
 *     }
 * })
 *
 * // Filter by hierarchy only
 * group.stream({
 *     heirarchy: "building_a.>",
 *     callback: (data) => {
 *         console.log(data)
 *     }
 * })
 *
 * // No filters — receive all
 * group.stream({
 *     callback: (data) => {
 *         console.log(data)
 *     }
 * })
 */
