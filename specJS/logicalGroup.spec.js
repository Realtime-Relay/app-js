/**
 * ============================================================
 * LOGICAL GROUP SPEC — Logical Group CRUD, Device List, Stream
 * ============================================================
 *
 * Covers: app.logicalGroup.create, update, delete, list, get,
 *         listDevices, logicalGroup.stream
 */

// ─────────────────────────────────────────────────────────────
// app.logicalGroup.create({ name, tags, device_idents })
// ─────────────────────────────────────────────────────────────

/**
 * @method logicalGroup.create
 * @description Creates a new logical group with devices and tags.
 *
 * @param {Object} params
 * @param {string}   params.name           - Required. Group name.
 *                                            Validated: [a-zA-Z0-9_-]+
 * @param {string[]} params.tags           - Required. Array of tag strings.
 * @param {string[]} params.device_idents  - Required. Array of device identifiers.
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If tags is not an array
 * @throws {Error} If device_idents is not an array
 * @throws {Error} If any device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.logical.create
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     device_ids: string[],   // Resolved from device_idents via device cache
 *     name: string,
 *     tags: string[]
 * }
 *
 * @response_payload
 * { status: "LOGICAL_GROUP_CREATE_SUCCESS" }
 *
 * @behavior
 * - Resolves device_idents to device_ids via device cache
 * - Returns group object with .stream() method on success
 *
 * @returns {Promise<LogicalGroupObject>} Group object with .stream()
 *
 * @example
 * var group = await app.logicalGroup.create({
 *     name: "floor_1_sensors",
 *     tags: ["floor_1", "temperature"],
 *     device_idents: ["sensor_01", "sensor_02", "sensor_03"]
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.logicalGroup.update({ id, name, tags, devices })
// ─────────────────────────────────────────────────────────────

/**
 * @method logicalGroup.update
 * @description Updates a logical group's name, tags, and/or device membership.
 *              Tags and devices use add/remove pattern.
 *
 * @param {Object} params
 * @param {string}   params.id                  - Required. Group ID.
 * @param {string}   [params.name]              - Optional. New group name.
 * @param {Object}   [params.tags]              - Optional. Tag changes.
 * @param {string[]} [params.tags.add]          - Tags to add.
 * @param {string[]} [params.tags.remove]       - Tags to remove.
 * @param {Object}   [params.devices]           - Optional. Device changes.
 * @param {string[]} [params.devices.add]       - Device idents to add.
 * @param {string[]} [params.devices.remove]    - Device idents to remove.
 *
 * @throws {Error} If id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.logical.update
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
 *     tags: {
 *         add: string[],
 *         remove: string[]
 *     }
 * }
 *
 * @response_payload
 * // Success:
 * { status: "LOGICAL_GROUP_UPDATE_SUCCESS" }
 * // Failure:
 * { status: "LOGICAL_GROUP_UPDATE_FAILURE", data: { msg: string[] } }
 *
 * @behavior
 * - Resolves device idents in add/remove arrays to device_ids via cache
 *
 * @returns {Promise<LogicalGroupObject>} Updated group object with .stream()
 *
 * @example
 * var group = await app.logicalGroup.update({
 *     id: "<group_id>",
 *     name: "floor_1_updated",
 *     tags: { add: ["humidity"], remove: ["floor_1"] },
 *     devices: { add: ["sensor_04"], remove: ["sensor_01"] }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.logicalGroup.delete(group_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method logicalGroup.delete
 * @description Deletes a logical group by ID.
 *
 * @param {string} group_id - Required. Group ID.
 *
 * @throws {Error} If group_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.logical.delete
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * // Success:
 * { status: "LOGICAL_GROUP_DELETE_SUCCESS" }
 * // Failure:
 * { status: "LOGICAL_GROUP_DELETE_FAILURE", data: { msg: string[] } }
 *
 * @returns {Promise<boolean>} true on successful deletion
 *
 * @example
 * var deleted = await app.logicalGroup.delete("<group_id>")
 */

// ─────────────────────────────────────────────────────────────
// app.logicalGroup.list()
// ─────────────────────────────────────────────────────────────

/**
 * @method logicalGroup.list
 * @description Lists all logical groups for the org.
 *
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.logical.list
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {} (empty object)
 *
 * @response_payload
 * // Success:
 * { status: "LOGICAL_GROUP_LIST_SUCCESS", data: object[] }
 * // Failure:
 * { status: "LOGICAL_GROUP_LIST_FAILURE", data: { msg: string } }
 *
 * @returns {Promise<object[]>} Array of logical group objects
 *
 * @example
 * var groups = await app.logicalGroup.list()
 */

// ─────────────────────────────────────────────────────────────
// app.logicalGroup.get(group_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method logicalGroup.get
 * @description Gets a single logical group by ID.
 *              Returns a group object with .stream() method.
 *
 * @param {string} group_id - Required. Group ID.
 *
 * @throws {Error} If group_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.logical.get
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * // Success:
 * { status: "LOGICAL_GROUP_GET_SUCCESS", data: object }
 * // Failure:
 * { status: "LOGICAL_GROUP_GET_FAILURE", data: null }
 *
 * @returns {Promise<LogicalGroupObject>} Group object with .stream()
 *
 * @example
 * var group = await app.logicalGroup.get("<group_id>")
 */

// ─────────────────────────────────────────────────────────────
// app.logicalGroup.listDevices(group_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method logicalGroup.listDevices
 * @description Lists all devices in a logical group.
 *
 * @param {string} group_id - Required. Group ID.
 *
 * @throws {Error} If group_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.cohort.{orgID}.logical.device.list
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * // Success:
 * { status: "LOGICAL_DEVICE_LIST_SUCCESS", data: object[] }
 * // Failure:
 * { status: "LOGICAL_DEVICE_LIST_FAILURE", data: { msg: string } }
 *
 * @returns {Promise<object[]>} Array of device objects
 *
 * @example
 * var devices = await app.logicalGroup.listDevices("<group_id>")
 */

// ─────────────────────────────────────────────────────────────
// logicalGroup.stream({ device_idents?, callback })
// ─────────────────────────────────────────────────────────────

/**
 * @method logicalGroup.stream
 * @description Subscribes to the group's real-time data stream.
 *              Optionally filters by device identifiers (client-side).
 *
 * @param {Object} params
 * @param {string[]} [params.device_idents]  - Optional. Array of device identifiers
 *                                              to filter on. If provided, only data
 *                                              from matching devices is passed to callback.
 * @param {Function} params.callback         - Required. Called with each data point.
 *
 * @throws {Error} If callback is not a function
 * @throws {Error} If not connected
 *
 * @nats_subject import.{orgID}.{env}.group.listen.{groupID}
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
 * - Creates a JetStream consumer on the group's listen subject
 * - Decodes msgpack payload
 * - If device_idents is provided: check if decoded ident is in the filter array
 *   → if match: invoke callback
 *   → if no match: skip (client-side filtering)
 * - If device_idents is omitted: invoke callback for all data
 * - Consumer is cleaned up on disconnect()
 *
 * @returns {void}
 *
 * @example
 * var group = await app.logicalGroup.get("<group_id>")
 *
 * group.stream({
 *     device_idents: ["sensor_01", "sensor_02"],
 *     callback: (data) => {
 *         console.log(`Device ${data.ident}:`, data.data)
 *         // { ident: "sensor_01", data: { temperature: 25.3, humidity: 60 } }
 *     }
 * })
 *
 * // Without filter — receive all devices
 * group.stream({
 *     callback: (data) => {
 *         console.log(data)
 *     }
 * })
 */
