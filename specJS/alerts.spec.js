/**
 * ============================================================
 * ALERTS SPEC — Alert CRUD, Ack, Mute, Listen + Ephemeral Engine
 * ============================================================
 *
 * Covers: app.alert.create, update, delete, list, get, ack, ackAll,
 *         mute, unmute, alert.listen, alert.setEvaluator,
 *         Ephemeral Alert Engine (client-side state machine)
 */

// ─────────────────────────────────────────────────────────────
// app.alert.create(config)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.create
 * @description Creates a new alert rule. Subject to org limit checks.
 *              For EPHEMERAL type, the evaluator function is stored locally
 *              and NOT sent to the backend.
 *
 * @param {Object} config
 * @param {string}   config.name                  - Required. Unique alert name.
 *                                                   Validated: [a-zA-Z0-9_-]+
 * @param {string}   [config.description]         - Optional. Alert description.
 * @param {string}   config.type                  - Required. "THRESHOLD" | "RATE_CHANGE" | "EPHEMERAL"
 * @param {string}   config.metric                - Required. Metric name to evaluate.
 * @param {Object}   config.config                - Required. Alert configuration.
 * @param {Object}   config.config.scope          - Required. Scope definition.
 * @param {string}   config.config.scope.type     - Required. "DEVICE" | "LOGICAL_GROUP" | "HEIRARCHY"
 *                                                   For EPHEMERAL: MUST be "DEVICE" only.
 * @param {string}   config.config.scope.value    - Required. Device ID for DEVICE,
 *                                                   group_id for LOGICAL_GROUP or HEIRARCHY.
 *                                                   For EPHEMERAL: MUST be a device_id.
 * @param {string}   [config.config.operator]     - Required for THRESHOLD/RATE_CHANGE.
 *                                                   ">" | ">=" | "==" | "<=" | "<"
 * @param {number}   [config.config.value]        - Required for THRESHOLD/RATE_CHANGE.
 *                                                   Threshold value to compare against.
 * @param {number}   config.config.duration        - Required. Duration in seconds the metric must
 *                                                   be in breach before firing alert.
 * @param {number}   config.config.recovery_duration - Required. Duration in seconds the metric must
 *                                                     be clear before resolving alert.
 * @param {number}   [config.config.cooldown]      - Optional. Cooldown in seconds before re-fire.
 *                                                   Defaults to 0.
 * @param {string[]} [config.notification_channel] - Optional. Array of notification IDs.
 *                                                   Defaults to [].
 * @param {Object}   [config.alert_mute_config]   - Optional. Initial mute configuration.
 * @param {Function} [config.evaluator]            - Required for EPHEMERAL only.
 *                                                   Client-side evaluator function.
 *                                                   NOT sent to backend. Stored locally.
 *                                                   Receives telemetry data, returns boolean.
 *                                                   true = breach, false = clear.
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If type is not one of THRESHOLD, RATE_CHANGE
 * @throws {Error} If type is EPHEMERAL (must use createEphemeral() instead)
 * @throws {Error} If type is THRESHOLD/RATE_CHANGE and operator/value are missing
 * @throws {Error} If duration or recovery_duration are missing or not positive numbers
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.create
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     name: string,
 *     description: string | undefined,
 *     type: "THRESHOLD" | "RATE_CHANGE" | "EPHEMERAL",
 *     metric: string,
 *     config: {
 *         scope: { type: string, value: string },
 *         operator: string | undefined,
 *         value: number | undefined,
 *         duration: number,
 *         recovery_duration: number,
 *         cooldown: number            // defaults to 0
 *     },
 *     notification_channel: string[],  // defaults to []
 *     alert_mute_config: object | undefined
 * }
 * // NOTE: evaluator function is NOT included in the request payload
 *
 * @response_payload
 * // Success:
 * {
 *     status: "ALERT_CREATE_SUCCESS",
 *     data: object                    // Full rule object (includes id, name, type, config, etc.)
 * }
 * // Failure:
 * {
 *     status: "ALERT_CREATE_FAILURE",
 *     data: {
 *         msg: string[],
 *         code?: "ALERT_CREATE_LIMIT_REACHED",
 *         data?: { limit: number, current_count: number }
 *     }
 * }
 *
 * @behavior
 * - Sends create request to backend (without evaluator)
 * - On success: returns an alert object with .listen() and .setEvaluator() methods
 * - If EPHEMERAL, stores evaluator function locally on the returned alert object
 * - On failure: returns failure response
 *
 * @returns {Promise<AlertObject>} Alert object with .listen() and .setEvaluator()
 *
 * @example
 * // THRESHOLD alert
 * var alert = await app.alert.create({
 *     name: "high_temp",
 *     type: "THRESHOLD",
 *     metric: "temperature",
 *     config: {
 *         scope: { type: "DEVICE", value: "<device_id>" },
 *         operator: ">",
 *         value: 85,
 *         duration: 300,
 *         recovery_duration: 120,
 *         cooldown: 600
 *     },
 *     notification_channel: ["notif_webhook_01"]
 * })
 *
 * // EPHEMERAL alert
 * var alert = await app.alert.create({
 *     name: "custom_check",
 *     type: "EPHEMERAL",
 *     metric: "cpu_usage",
 *     config: {
 *         scope: { type: "DEVICE", value: "<device_id>" },
 *         duration: 60,
 *         recovery_duration: 30,
 *         cooldown: 120
 *     },
 *     evaluator: (data) => {
 *         return data.value > 90
 *     }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.update(config)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.update
 * @description Updates an existing alert rule.
 *
 * @param {Object} config
 * @param {string}   config.id                     - Required. Alert rule ID.
 * @param {string}   [config.name]                 - Optional. New name.
 * @param {string}   [config.description]          - Optional. New description.
 * @param {string}   [config.type]                 - Optional. New type.
 * @param {string}   [config.metric]               - Optional. New metric.
 * @param {Object}   [config.config]               - Optional. New config object.
 * @param {Object}   [config.alert_mute_config]    - Optional. New mute config.
 * @param {string[]} [config.notification_channel]  - Optional. New notification channels.
 *
 * @throws {Error} If id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.update
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     id: string,
 *     name?: string,
 *     description?: string,
 *     type?: string,
 *     metric?: string,
 *     config?: object,
 *     alert_mute_config?: object,
 *     notification_channel?: string[]
 * }
 *
 * @response_payload
 * // Success:
 * { status: "ALERT_UPDATE_SUCCESS", data: object }
 * // Failure:
 * { status: "ALERT_UPDATE_FAILURE", data: { msg: string } }
 *
 * @returns {Promise<AlertObject>} Updated alert object with .listen() and .setEvaluator()
 *
 * @example
 * var alert = await app.alert.update({
 *     id: "<alert_id>",
 *     config: { ...newConfig }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.delete(alertId)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.delete
 * @description Deletes an alert rule by ID.
 *
 * @param {string} alertId - Required. Alert rule ID.
 *
 * @throws {Error} If id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.delete
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * // Success:
 * { status: "ALERT_DELETE_SUCCESS", data: { id: string } }
 * // Failure:
 * { status: "ALERT_DELETE_FAILURE", data: { msg: string[] } }
 *
 * @returns {Promise<boolean>} true on successful deletion
 *
 * @example
 * var deleted = await app.alert.delete("67e1a2b3c4d5e6f7a8b9c0d1")
 */

// ─────────────────────────────────────────────────────────────
// app.alert.list()
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.list
 * @description Lists all alert rules for the org.
 *
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.list
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {} (empty object)
 *
 * @response_payload
 * { status: "ALERT_LIST_SUCCESS", data: object[] }
 *
 * @returns {Promise<object[]>} Array of alert rule objects
 *
 * @example
 * var alerts = await app.alert.list()
 */

// ─────────────────────────────────────────────────────────────
// app.alert.get(alertName)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.get
 * @description Gets a single alert rule by name.
 *              Returns an alert object with .listen() and .setEvaluator() methods.
 *
 * @param {string} alertName - Required. Alert name.
 *                              Validated: [a-zA-Z0-9_-]+
 *
 * @throws {Error} If alertName is null/undefined/empty
 * @throws {Error} If alertName fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.get
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { name: string }
 *
 * @response_payload
 * // Success:
 * { status: "ALERT_GET_SUCCESS", data: object }
 * // Failure:
 * { status: "ALERT_GET_FAILURE", data: { msg: string[] } }
 *
 * @returns {Promise<AlertObject|null>} Alert object with .listen() and .setEvaluator(),
 *          or null if not found
 *
 * @example
 * var alert = await app.alert.get("high_temp")
 * if (!alert) console.log("Alert not found")
 */

// ─────────────────────────────────────────────────────────────
// app.alert.ack({ device_id, alert_id, acked_by, ack_notes? })
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.ack
 * @description Acknowledges an alert for a specific device.
 *
 * @routing
 * - Non-ephemeral: sends to backend via api.iot.alerts.{orgID}.ack
 * - Ephemeral + local owner engine: calls engine.ack() directly
 * - Ephemeral + no local engine: sends NATS request to {orgID}.{env}.alerts.custom.{alert_id}.ack
 *
 * Alert type is tracked via internal #alertMetadata Map (populated by create/createEphemeral/get/list).
 *
 * @param {Object} params
 * @param {string} params.device_id  - Required. Device ID to ack alert for.
 * @param {string} params.alert_id   - Required. Alert rule ID.
 * @param {string} params.acked_by   - Required. Identifier of who is acknowledging.
 * @param {string} [params.ack_notes] - Optional. Notes for the acknowledgment.
 *
 * @throws {Error} If device_id is null/undefined/empty
 * @throws {Error} If alert_id is null/undefined/empty
 * @throws {Error} If acked_by is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.ack
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     device_id: string,     // Device ID
 *     rule_id: string,       // Alert rule ID
 *     acked_by: string,      // Who is acking
 *     env: string,           // From app mode
 *     ack_notes: string | undefined
 * }
 *
 * @response_payload
 * { status: "ALERT_ACK_SUCCESS" | "ALERT_ACK_FAILURE" }
 *
 * @backend_side_effect
 * Backend publishes to: export.{orgID}.{env}.alerts.listen.{rule_id}.ack
 * Encoding: msgpack
 * Payload:
 * {
 *     status: string,            // "acknowledged"
 *     device_ident: string,      // Device identifier
 *     ack: {
 *         acked_by: string,
 *         ack_notes: string | null,
 *         acked_at: number       // Unix ms timestamp
 *     }
 * }
 *
 * @returns {Promise<boolean>} true if ack successful
 *
 * @example
 * var ack = await app.alert.ack({
 *     device_id: "69bffcb28cc30a4f716936bc",
 *     alert_id: "rule_1",
 *     acked_by: "operator_jane",
 *     ack_notes: "Investigating cooling system"
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.ackAll({ alert_id, acked_by, ack_notes? })
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.ackAll
 * @description Acknowledges an alert across all devices for a rule.
 *
 * @routing (same as ack)
 * - Non-ephemeral: sends to backend via api.iot.alerts.{orgID}.ack_all
 * - Ephemeral + local owner engine: calls engine.ackAll() directly
 * - Ephemeral + no local engine: sends NATS request to {orgID}.{env}.alerts.custom.{alert_id}.ack_all
 *
 * @param {Object} params
 * @param {string} params.alert_id  - Required. Alert rule ID.
 * @param {string} params.acked_by  - Required. Identifier of who is acknowledging.
 * @param {string} [params.ack_notes] - Optional. Notes for the acknowledgment.
 *
 * @throws {Error} If alert_id is null/undefined/empty
 * @throws {Error} If acked_by is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.ack_all
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     rule_id: string,
 *     acked_by: string,
 *     env: string,
 *     ack_notes: string | undefined
 * }
 *
 * @response_payload
 * { status: "ALERT_ACK_SUCCESS" | "ALERT_ACK_FAILURE" }
 *
 * @backend_side_effect
 * Backend publishes to: export.{orgID}.{env}.alerts.listen.{rule_id}.ack_all
 * Encoding: msgpack
 * Payload:
 * {
 *     status: string,            // "acknowledged"
 *     ack: {
 *         acked_by: string,
 *         ack_notes: string | null,
 *         acked_at: number       // Unix ms timestamp
 *     }
 * }
 * // NOTE: No device_ident field in ack_all (acknowledges all devices for the rule)
 *
 * @returns {Promise<boolean>} true if ack_all successful
 *
 * @example
 * var ackAll = await app.alert.ackAll({
 *     alert_id: "rule_1",
 *     acked_by: "operator_jane",
 *     ack_notes: "Bulk ack for maintenance window"
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.mute({ id, mute_config })
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.mute
 * @description Mutes an alert rule. Muted alerts are not evaluated.
 *
 * @routing
 * - Non-ephemeral: sends to backend via api.iot.alerts.{orgID}.mute
 * - Ephemeral: sends NATS request to {orgID}.{env}.alerts.custom.{rule_id}.mute
 *
 * @param {Object} params
 * @param {string} params.id                    - Required. Alert rule ID.
 * @param {Object} params.mute_config           - Required. Mute configuration.
 * @param {string} params.mute_config.type      - Required. "FOREVER" | "TIME_BASED"
 * @param {string} [params.mute_config.mute_till] - Required for TIME_BASED.
 *                                                  ISO8601 timestamp UTC.
 *
 * @throws {Error} If id is null/undefined/empty
 * @throws {Error} If mute_config.type is not FOREVER or TIME_BASED
 * @throws {Error} If type is TIME_BASED and mute_till is missing
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.mute
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     rule_id: string,
 *     type: "FOREVER" | "TIME_BASED",
 *     mute_till: number | undefined    // Unix timestamp, required for TIME_BASED
 * }
 *
 * @response_payload
 * // Success:
 * { status: "ALERT_RULE_MUTE_SUCCESS", data: object }
 * // Failure:
 * { status: "ALERT_RULE_MUTE_FAILURE", data: { msg: string[] } }
 *
 * @returns {Promise<object>} Updated rule with mute config
 *
 * @example
 * await app.alert.mute({
 *     id: "<alert_id>",
 *     mute_config: {
 *         type: "TIME_BASED",
 *         mute_till: "2026-03-25T00:00:00Z"
 *     }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.unmute(id)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.unmute
 * @description Unmutes an alert rule. Uses the same NATS subject as mute
 *              but with type = "CLEAR".
 *
 * @routing
 * - Non-ephemeral: sends to backend via api.iot.alerts.{orgID}.mute (type=CLEAR)
 * - Ephemeral: sends NATS request to {orgID}.{env}.alerts.custom.{rule_id}.unmute
 *
 * @param {string} id - Required. Alert rule ID.
 *
 * @throws {Error} If id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.alerts.{orgID}.mute
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     rule_id: string,
 *     type: "CLEAR"
 * }
 *
 * @response_payload
 * // Success:
 * { status: "ALERT_RULE_MUTE_SUCCESS", data: object }
 * // Failure:
 * { status: "ALERT_RULE_MUTE_FAILURE", data: { msg: string[] } }
 *
 * @returns {Promise<object>} Updated rule with cleared mute config
 *
 * @example
 * await app.alert.unmute("<alert_id>")
 */

// ─────────────────────────────────────────────────────────────
// alert.listen({ onFire, onResolved, onAck, onAckAll })
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.listen
 * @description Subscribes to alert lifecycle events for a specific rule.
 *              For non-ephemeral (THRESHOLD, RATE_CHANGE): pure passthrough
 *              from backend JetStream events.
 *              For EPHEMERAL: starts the client-side alert engine
 *              AND subscribes to JetStream topics for remote ack/ack_all.
 *
 * @param {Object} callbacks
 * @param {Function} [callbacks.onFire]      - Called when alert fires.
 * @param {Function} [callbacks.onResolved]  - Called when alert resolves.
 * @param {Function} [callbacks.onAck]       - Called when alert is acknowledged.
 * @param {Function} [callbacks.onAckAll]    - Called when all alerts for rule are acknowledged.
 * @param {Function} [callbacks.onError]     - (Ephemeral owner mode only) Called when the evaluator
 *                                              function throws. Receives the error object.
 *                                              The evaluation cycle is skipped and state is unchanged.
 *                                              Local only — not published over NATS.
 *
 * @throws {Error} If alert object is invalid
 * @throws {Error} If not connected
 *
 * @nats_subject import.{orgID}.{env}.alerts.listen.<rule_id>.*
 * @nats_type jetstream_consumer (single wildcard consumer)
 *
 * The last token of the subject determines the event type:
 * - fire       → onFire
 * - resolved   → onResolved
 * - ack        → onAck
 * - ack_all    → onAckAll
 * Messages with unrecognised last tokens are silently ignored.
 * @encoding msgpack (decode on receive)
 *
 * @callback_payloads
 *
 * onFire / onResolved (after SDK transformation):
 * {
 *     alert: {
 *         id: string,            // Rule ID
 *         name: string,          // Rule name
 *         type: string,          // Scope type (DEVICE, LOGICAL_GROUP, HEIRARCHY)
 *         type_value: string     // Scope value
 *     },
 *     device_id: string,         // Device ID
 *     last_value: {
 *         value: any,            // Current metric value
 *         field_name: string     // Metric name
 *     },
 *     timestamp: string          // ISO8601 datetime string (converted from unix ms)
 * }
 *
 * onAck:
 * {
 *     status: string,            // "acknowledged"
 *     device_ident: string,
 *     ack: {
 *         acked_by: string,
 *         ack_notes: string | null,
 *         acked_at: number       // Unix ms timestamp
 *     }
 * }
 *
 * onAckAll:
 * {
 *     status: string,            // "acknowledged"
 *     ack: {
 *         acked_by: string,
 *         ack_notes: string | null,
 *         acked_at: number
 *     }
 * }
 * // NOTE: No device_ident in onAckAll
 *
 * @behavior_non_ephemeral (THRESHOLD, RATE_CHANGE)
 * - Creates 1 JetStream consumer with wildcard subject (rule_id.*)
 * - Extracts last token from msg.subject to determine event type
 * - Decodes msgpack for each message
 * - Transforms payload before invoking callback:
 *   - Converts timestamp (unix ms) → ISO8601 string
 * - Invokes the matching callback (onFire/onResolved/onAck/onAckAll) with transformed data
 * - Ignores messages whose last token is not fire/resolved/ack/ack_all
 *
 * @behavior_ephemeral (see EPHEMERAL ALERT ENGINE section below)
 * - Subscribes to JetStream alert topics (same 4 consumers) for remote events
 * - ALSO starts the local ephemeral alert engine
 *
 * @returns {Promise<void>}
 *
 * @example
 * await alert.listen({
 *     onFire: (data) => {
 *         console.log(`ALERT FIRED: ${data.rule.name} on device ${data.device_id}`)
 *     },
 *     onResolved: (data) => {
 *         console.log(`ALERT RESOLVED: ${data.rule.name}`)
 *     },
 *     onAck: (data) => {
 *         console.log(`Alert acked by ${data.ack.acked_by}`)
 *     },
 *     onAckAll: (data) => {
 *         console.log(`All alerts acked by ${data.ack.acked_by}`)
 *     }
 * })
 */

// ─────────────────────────────────────────────────────────────
// alert.setEvaluator(fn)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.setEvaluator
 * @description Sets (or replaces) the client-side evaluator function for
 *              an EPHEMERAL alert. Only valid for type = EPHEMERAL.
 *
 * @param {Function} fn - Required. Evaluator function.
 *                         Receives the rolling state object.
 *                         Must return boolean: true = breach, false = clear.
 *
 * Rolling state shape depends on topic source:
 * TELEMETRY: { "<device_ident>": { "<metric>": { value, timestamp } } }
 * COMMAND:   { "<device_ident>": { "<command_name>": <command_data> } }
 * EVENT:     { "<device_ident>": { "<event_name>": <event_data> } }
 *
 * @throws {Error} If fn is not a function
 * @throws {Error} If alert type is not EPHEMERAL
 *
 * @behavior
 * - Stores the evaluator function on the alert object
 * - If .listen() is already active, the new evaluator takes effect immediately (hot-swap)
 * - Determines owner mode: calling setEvaluator() before listen() makes this instance the owner
 *
 * @returns {void}
 *
 * @example
 * var alert = await app.alert.createEphemeral({ ... })
 *
 * alert.setEvaluator((data) => {
 *     // data is the full rolling state — check any device/metric
 *     const cpu = data['sensor_01']?.cpu_usage?.value
 *     return cpu != null && cpu > 90
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.createEphemeral(params)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.createEphemeral
 * @description Creates an ephemeral alert rule. The evaluator runs client-side.
 *              Stored in the backend (visible via list/get).
 *
 * @param {Object} params
 * @param {string}   params.name                       - Required. Unique alert name. [a-zA-Z0-9_-]+
 * @param {string}   [params.description]              - Optional. Alert description.
 * @param {Object}   params.config                     - Required. Alert configuration.
 * @param {Object}   params.config.topic               - Required. Data source definition.
 * @param {string}   params.config.topic.source        - Required. "TELEMETRY" | "COMMAND" | "EVENT"
 * @param {string}   params.config.topic.device_ident  - Required. Device ident or "*".
 *                                                       If not "*", resolved to device_id before subject construction.
 * @param {string}   params.config.topic.last_token    - Required. Metric/command/event name or "*".
 * @param {number}   params.config.duration            - Required. Seconds breach must hold before FIRE.
 * @param {number}   params.config.recovery_duration   - Required. Seconds clear must hold before RESOLVED.
 * @param {number}   [params.config.cooldown]          - Optional. Seconds between re-fires. Defaults to 0.
 * @param {string[]} [params.notification_channel]     - Optional. Array of notification IDs. Defaults to [].
 *
 * @throws {Error} If name is null/undefined/empty or fails validation
 * @throws {Error} If config.topic.source is not TELEMETRY, COMMAND, or EVENT
 * @throws {Error} If config.duration or config.recovery_duration are missing/not positive
 * @throws {Error} If not connected
 *
 * @nats_subject api.iot.alerts.{orgID}.create_ephemeral
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     name: string,
 *     description: string | undefined,
 *     config: {
 *         topic: { source: string, device_ident: string, last_token: string },
 *         duration: number,
 *         recovery_duration: number,
 *         cooldown: number
 *     },
 *     notification_channel: string[]
 * }
 *
 * @response_payload
 * // Success:
 * { status: "EPHEMERAL_CREATE_SUCCESS", data: object }
 * // Failure:
 * { status: "EPHEMERAL_CREATE_FAILURE", data: { msg: string[] } }
 *
 * @returns {Promise<EphemeralAlertObject|null>} Wrapped alert with:
 *   - .listen({ onFire, onResolved, onAck, onAckAll }) — starts engine
 *   - .setEvaluator(fn) — sets client-side evaluator
 *   - .stop() — stops engine, resets state
 *   - All data fields from backend response
 *
 * @example
 * var alert = await app.alert.createEphemeral({
 *     name: 'high_cpu_custom',
 *     config: {
 *         topic: { source: 'TELEMETRY', device_ident: 's-3', last_token: 'cpu_usage' },
 *         duration: 30,
 *         recovery_duration: 15,
 *         cooldown: 60,
 *     },
 *     notification_channel: ['notif_1'],
 * })
 *
 * alert.setEvaluator((data) => {
 *     return data['s-3']?.cpu_usage?.value > 90
 * })
 *
 * await alert.listen({
 *     onFire: (d) => console.log('FIRE', d),
 *     onResolved: (d) => console.log('RESOLVED', d),
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.updateEphemeral(params)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.updateEphemeral
 * @description Updates an existing ephemeral alert rule.
 *
 * @param {Object} params
 * @param {string}   params.id                         - Required. Alert rule ID.
 * @param {string}   [params.name]                     - Optional. New name.
 * @param {string}   [params.description]              - Optional. New description.
 * @param {Object}   [params.config]                   - Optional. Partial config update.
 * @param {string[]} [params.notification_channel]     - Optional. New notification channels.
 *
 * @throws {Error} If id is null/undefined/empty
 * @throws {Error} If not connected
 *
 * @nats_subject api.iot.alerts.{orgID}.update_ephemeral
 * @nats_type request
 * @encoding JSONCodec
 *
 * @returns {Promise<EphemeralAlertObject|null>} Wrapped alert or null on failure
 */

// ═════════════════════════════════════════════════════════════
// EPHEMERAL ALERT ENGINE (Client-Side)
// ═════════════════════════════════════════════════════════════

/**
 * @description
 * The ephemeral alert engine runs entirely client-side in the SDK.
 * It supports multiple data sources (TELEMETRY, COMMAND, EVENT) and
 * operates in one of two modes depending on whether an evaluator is set.
 *
 * ─── TWO MODES ───
 *
 * MODE 1: OWNER (evaluator set via .setEvaluator() before .listen())
 * - Acquires NATS KV lock for single-owner enforcement
 * - Subscribes to data topic (JetStream consumer)
 * - Maintains rolling state object, passes to evaluator on each message
 * - State machine fires/resolves locally → calls callbacks AND publishes events
 * - Subscribes to 4 RPC subjects via natsClient.subscribe() for remote ack/ackAll/mute/unmute
 *
 * MODE 2: LISTENER (no evaluator set)
 * - Subscribes to import.{orgID}.{env}.alerts.listen.{rule_id}.* (JetStream)
 * - Routes events to callbacks by last token (same as non-ephemeral listen)
 * - For ack/ackAll: sends NATS request (RPC) to owner via custom RPC subjects
 *
 * ─── DATA TOPIC CONSTRUCTION ───
 *
 * Built from rule.config.topic:
 * | Source    | Subject                                                      |
 * |-----------|--------------------------------------------------------------|
 * | TELEMETRY | {orgID}.{env}.telemetry.{device_id|*}.{last_token}          |
 * | COMMAND   | {orgID}.{env}.command.queue.{device_id|*}.{last_token}      |
 * | EVENT     | {orgID}.{env}.events.{device_id|*}.{last_token}             |
 *
 * If device_ident !== "*": resolved to device_id via ctx.device.resolveDeviceId()
 *
 * ─── ROLLING STATE ───
 *
 * The engine accumulates latest values per device per metric/command/event.
 * Keys are device idents (reverse-mapped from device IDs via cache).
 *
 * TELEMETRY: { "sensor_01": { "temperature": { value, timestamp }, "humidity": { value, timestamp } } }
 * COMMAND:   { "sensor_01": { "reboot": <command_data> } }
 * EVENT:     { "sensor_01": { "door_opened": <event_data> } }
 *
 * On each message: update rolling state → pass full state to evaluator(rollingState) → boolean
 *
 * ─── NATS KV LOCK (Owner Mode Only) ───
 *
 * Package: @nats-io/kv
 * Bucket: {orgID} (existing shared org bucket, opened via new Kvm(jetstream).open(orgID) in ConnectionManager.connect())
 * Key: ephemeral_owner_{rule_id}
 * Value: { started_at, expires_at } (JSON)
 *
 * TTL is stored as `expires_at` in the value (bucket is shared, no bucket-level TTL).
 * expires_at = Date.now() + 30000 (30 seconds from creation/refresh).
 *
 * Heartbeat: owner re-puts key every 15 seconds, refreshing expires_at.
 *
 * On .listen() with evaluator:
 * 1. Check if key exists via kv.get()
 * 2. If exists AND expires_at > now → throw "Evaluator already active for this rule"
 * 3. If exists AND expires_at <= now → delete stale key, proceed
 * 4. If not exists → proceed
 * 5. Acquire via kv.create() (put-if-absent) with { client_id, started_at, expires_at }
 * 6. Start heartbeat interval (15s)
 *
 * On .stop():
 * - Delete key from KV
 * - Clear heartbeat interval
 *
 * ─── RPC SUBSCRIPTION (Owner Mode — natsClient.subscribe) ───
 *
 * Single wildcard subscription: {orgID}.{env}.alerts.custom.{rule_id}.*
 *
 * Routes by last token:
 * - "ack"      → handle ack
 * - "ack_all"  → handle ack_all
 * - "mute"     → handle mute/unmute (mute_config.type = "CLEAR" for unmute)
 *
 * Each RPC handler:
 * 1. Decode JSON payload from msg.data
 * 2. Update local state
 * 3. Call local callback (onAck/onAckAll)
 * 4. Publish event to import.{orgID}.{env}.alerts.listen.{rule_id}.{event}
 * 5. For mute: also sync to backend via api.iot.alerts.{orgID}.mute
 * 6. Reply via msg.respond() with { status: "ACK_SUCCESS"|"MUTE_SUCCESS" } or { status: "ACK_FAILED", reason }
 *
 * ─── OFFLINE BUFFER ───
 *
 * All JetStream publishes (fire, resolved) go through ctx.publishOrBuffer().
 * If disconnected, messages are queued and flushed on reconnect.
 *
 * ─── NOTIFICATION DISPATCH ───
 *
 * On FIRE or RESOLVED, if rule.notification_channel has entries:
 * Send NATS request to: api.iot.notification.{orgID}.dispatch
 * Encoding: JSONCodec
 * Payload: notification_channel (string[])
 *
 * ─── STATE MACHINE ───
 *
 * States: "normal" | "alerting" | "acknowledged"
 *
 * State object:
 * {
 *     status: "normal",
 *     last_evaluated_at: null,
 *     clear_since: null,
 *     breached_since: null,
 *     last_fired: 0,
 *     acked_by: null,
 *     acked_at: null,
 *     ack_notes: null
 * }
 *
 * ─── EVALUATION FLOW (per incoming data message) ───
 *
 * 1. CHECK MUTE:
 *    - FOREVER → skip evaluation
 *    - TIME_BASED and mute_till > now → skip
 *    - Otherwise → proceed
 *
 * 2. UPDATE ROLLING STATE: Extract device ident (reverse-map from ID) and
 *    last token from NATS subject. Update rollingState[ident][token] = decoded data.
 *
 * 3. RUN EVALUATOR: evaluator(rollingState) → boolean
 *    - true = breach
 *    - false = clear
 *    - If evaluator throws: call onError(err), skip evaluation cycle, state unchanged
 *    - If evaluator returns non-boolean: call onError(new Error("Evaluator must return a boolean, got <type>")), skip cycle
 *
 * 4. CHECK STALENESS: If gap > duration*1000ms → reset breached_since, clear_since
 *
 * 5. STATE TRANSITIONS: (same as before)
 *
 *    BREACHED:
 *    a. Set breached_since if null, clear clear_since
 *    b. If heldFor < duration → no action
 *    c. If normal AND heldFor >= duration → FIRE, transition to alerting
 *    d. If alerting AND cooldown elapsed → Re-FIRE
 *    e. If acknowledged → silent
 *
 *    CLEAR:
 *    a. Set clear_since if null, clear breached_since
 *    b. If clearedFor < recovery_duration → no action
 *    c. If alerting|acknowledged AND clearedFor >= recovery_duration → RESOLVED, transition to normal
 *
 * 6. UPDATE last_evaluated_at
 *
 * ─── On FIRE ───
 * 1. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.fire (msgpack)
 * 2. Dispatch notifications via api.iot.notification.{orgID}.dispatch
 * 3. Update state: status=alerting, last_fired=now
 * 4. Invoke onFire callback
 *
 * ─── On RESOLVED ───
 * 1. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.resolved (msgpack)
 * 2. Dispatch notifications
 * 3. Update state: status=normal, clear acked_by/acked_at/ack_notes/breached_since
 * 4. Invoke onResolved callback
 *
 * ─── On ACK (via RPC from listener, or local call) ───
 * 1. Update state: status=acknowledged, set acked_by/acked_at
 * 2. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.ack (msgpack)
 * 3. Invoke onAck callback
 * 4. Reply to RPC with success (if from RPC)
 *
 * ─── On ACK_ALL (same flow) ───
 * 1. Update state: status=acknowledged
 * 2. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.ack_all (msgpack)
 * 3. Invoke onAckAll callback
 * 4. Reply to RPC with success
 *
 * ─── stop() ───
 * 1. Delete data consumer
 * 2. Unsubscribe RPC subscriptions (drain)
 * 3. Delete alert consumers (listener mode)
 * 4. Release KV lock + clear heartbeat
 * 5. Reset rolling state to {}
 * 6. Reset state machine to normal
 *
 * ─── STATE TRANSITION TABLE ───
 *
 * | Current       | Condition                     | Duration Met?  | Next State     | Action               |
 * |---------------|-------------------------------|----------------|----------------|----------------------|
 * | normal        | breach, held < duration        | No             | normal         | Track breached_since |
 * | normal        | breach, held >= duration       | Yes            | alerting       | FIRE                 |
 * | alerting      | breach, cooldown elapsed       | -              | alerting       | Re-FIRE              |
 * | alerting      | breach, cooldown not elapsed   | -              | alerting       | Silent               |
 * | alerting      | acked                          | -              | acknowledged   | Silent               |
 * | alerting      | clear, held < recovery         | -              | alerting       | Track clear_since    |
 * | alerting      | clear, held >= recovery        | -              | normal         | RESOLVED             |
 * | acknowledged  | breach                         | -              | acknowledged   | Silent               |
 * | acknowledged  | clear, held < recovery         | -              | acknowledged   | Track clear_since    |
 * | acknowledged  | clear, held >= recovery        | -              | normal         | RESOLVED             |
 */
