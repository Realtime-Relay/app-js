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
 * @throws {Error} If type is not one of THRESHOLD, RATE_CHANGE, EPHEMERAL
 * @throws {Error} If type is EPHEMERAL and scope.type is not "DEVICE"
 * @throws {Error} If type is EPHEMERAL and evaluator is not a function
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
// app.alert.delete(alertName)
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.delete
 * @description Deletes an alert rule by ID.
 *
 * @param {string} alertName - Required. Alert name or ID.
 *
 * @throws {Error} If alertName is null/undefined/empty
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
 * var deleted = await app.alert.delete("high_temp")
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
 * @returns {Promise<AlertObject>} Alert object with .listen() and .setEvaluator()
 *
 * @example
 * var alert = await app.alert.get("high_temp")
 */

// ─────────────────────────────────────────────────────────────
// app.alert.ack({ alert, acked_by })
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.ack
 * @description Acknowledges an alert for a specific device.
 *
 * @param {Object} params
 * @param {Object} params.alert     - Required. Alert object (from get/create/update).
 * @param {string} params.acked_by  - Required. Identifier of who is acknowledging.
 *
 * @throws {Error} If alert is null/undefined
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
 *     device_id: string,     // From alert context
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
 *     alert: alertObj,
 *     acked_by: "operator_jane"
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.ackAll({ alert, acked_by })
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.ackAll
 * @description Acknowledges an alert across all devices for a rule.
 *
 * @param {Object} params
 * @param {Object} params.alert     - Required. Alert object.
 * @param {string} params.acked_by  - Required. Identifier of who is acknowledging.
 *
 * @throws {Error} If alert is null/undefined
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
 *     alert: alertObj,
 *     acked_by: "operator_jane"
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.alert.mute({ id, mute_config })
// ─────────────────────────────────────────────────────────────

/**
 * @method alert.mute
 * @description Mutes an alert rule. Muted alerts are not evaluated.
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
 *
 * @throws {Error} If alert object is invalid
 * @throws {Error} If not connected
 *
 * @nats_subjects (JetStream consumers — ALL alert types subscribe to these):
 * - import.{orgID}.{env}.alerts.listen.<rule_id>.fire       → onFire
 * - import.{orgID}.{env}.alerts.listen.<rule_id>.resolved   → onResolved
 * - import.{orgID}.{env}.alerts.listen.<rule_id>.ack        → onAck
 * - import.{orgID}.{env}.alerts.listen.<rule_id>.ack_all    → onAckAll
 * @encoding msgpack (decode on receive)
 *
 * @callback_payloads
 *
 * onFire / onResolved:
 * {
 *     rule: {
 *         name: string,          // Rule name
 *         type: string,          // Scope type (DEVICE, LOGICAL_GROUP, HEIRARCHY)
 *         type_value: string     // Scope value
 *     },
 *     device_id: string,
 *     last_value: {
 *         value: any,            // Current metric value
 *         field_name: string     // Metric name
 *     },
 *     timestamp: number          // Unix ms timestamp
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
 * - Creates 4 JetStream consumers (one per event type)
 * - Decodes msgpack for each message
 * - Invokes the appropriate callback with decoded data
 * - Pure passthrough — no local processing
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
 *                         Receives telemetry data object.
 *                         Must return boolean: true = breach, false = clear.
 *
 * @throws {Error} If fn is not a function
 * @throws {Error} If alert type is not EPHEMERAL
 *
 * @behavior
 * - Stores the evaluator function on the alert object
 * - If .listen() is already active, the new evaluator takes effect immediately
 * - The evaluator is called on each telemetry data point with the decoded data
 *
 * @returns {void}
 *
 * @example
 * var alertEphemeral = await app.alert.get("custom_check")
 *
 * alertEphemeral.setEvaluator((data) => {
 *     // Custom evaluation logic
 *     return data.value > 90 && data.value < 100
 * })
 */

// ═════════════════════════════════════════════════════════════
// EPHEMERAL ALERT ENGINE (Client-Side)
// ═════════════════════════════════════════════════════════════

/**
 * @description
 * The ephemeral alert engine runs entirely client-side in the SDK.
 * It mirrors the backend alerting engine (threshold_eval.js) but uses
 * a user-provided evaluator function instead of operator-based comparison.
 *
 * The engine is activated when .listen() is called on an EPHEMERAL alert
 * that has an evaluator set (via create config or .setEvaluator()).
 *
 * ─── TELEMETRY SUBSCRIPTION ───
 *
 * On .listen() for EPHEMERAL:
 * 1. Subscribe to telemetry: {orgID}.{env}.telemetry.<device_ident>.<metric>
 *    - device_ident resolved from rule's config.scope.value (device_id → ident via cache)
 *    - metric from rule.metric
 *    - Encoding: msgpack decode
 *
 * 2. Subscribe to JetStream alert topics (4 consumers) for remote events:
 *    - import.{orgID}.{env}.alerts.listen.<rule_id>.fire
 *    - import.{orgID}.{env}.alerts.listen.<rule_id>.resolved
 *    - import.{orgID}.{env}.alerts.listen.<rule_id>.ack
 *    - import.{orgID}.{env}.alerts.listen.<rule_id>.ack_all
 *    These handle ack/ack_all from OTHER SDK instances across the network.
 *
 * ─── STATE MACHINE ───
 *
 * States: "normal" | "alerting" | "acknowledged"
 *
 * State object (persisted to NATS KV):
 * {
 *     status: "normal",              // Current state
 *     last_evaluated_at: null,       // Unix ms timestamp of last evaluation
 *     clear_since: null,             // Unix ms timestamp when clear started
 *     breached_since: null,          // Unix ms timestamp when breach started
 *     last_fired: 0,                 // Unix ms timestamp of last fire notification
 *     acked_by: null,                // Who acknowledged
 *     acked_at: null,                // When acknowledged (unix ms)
 *     ack_notes: null                // Notes from acknowledgment
 * }
 *
 * KV Key: {ruleID}_{deviceID}
 * KV Bucket: TBD (pending backend endpoint — see Backend Tasks)
 *
 * ─── EVALUATION FLOW (per telemetry data point) ───
 *
 * 1. CHECK MUTE: Query rule's alert_mute_config (cached from backend).
 *    - If type = "FOREVER" → skip evaluation
 *    - If type = "TIME_BASED" and mute_till > now → skip evaluation
 *    - Otherwise → proceed
 *
 * 2. RUN EVALUATOR: Call user's evaluator(data) → returns boolean
 *    - true = metric is in breach
 *    - false = metric is clear
 *
 * 3. CHECK STALENESS: If gap between current timestamp and last_evaluated_at
 *    is greater than (duration * 1000) ms → reset breached_since and clear_since to null.
 *    This prevents stale state from triggering old alerts.
 *
 * 4. APPLY STATE TRANSITIONS:
 *
 *    IF BREACHED (evaluator returned true):
 *
 *    a. If breached_since is null → set breached_since = now, clear clear_since
 *    b. heldFor = now - breached_since
 *    c. If heldFor < duration (seconds * 1000 ms) → no action (still counting)
 *    d. If status == "normal" AND heldFor >= duration:
 *       → Transition to "alerting"
 *       → FIRE (see On FIRE below)
 *    e. If status == "alerting" AND (now - last_fired) >= cooldown (seconds * 1000 ms):
 *       → Re-FIRE (cooldown cycle notification)
 *    f. If status == "acknowledged":
 *       → Do nothing (silent, no re-fires while acknowledged)
 *
 *    IF CLEAR (evaluator returned false):
 *
 *    a. If clear_since is null → set clear_since = now, clear breached_since
 *    b. clearedFor = now - clear_since
 *    c. If clearedFor < recovery_duration (seconds * 1000 ms) → no action
 *    d. If status == "alerting" OR status == "acknowledged"
 *       AND clearedFor >= recovery_duration:
 *       → RESOLVE (see On RESOLVED below)
 *       → Transition to "normal"
 *       → Clear: acked_by, acked_at, ack_notes, breached_since
 *
 * 5. UPDATE: Set last_evaluated_at = now. Persist state to NATS KV.
 *
 * ─── On FIRE ───
 *
 * 1. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.fire
 *    Encoding: msgpack
 *    Payload:
 *    {
 *        rule: {
 *            name: <rule.name>,
 *            type: "DEVICE",
 *            type_value: <device_id>
 *        },
 *        device_id: <device_id>,
 *        last_value: {
 *            value: <current_value>,
 *            field_name: <rule.metric>
 *        },
 *        timestamp: Date.now()
 *    }
 *
 * 2. Dispatch notifications (if notification_channel has entries):
 *    For each notif_id in rule.notification_channel:
 *    Publish to: notif_dispatcher.notification.send
 *    Encoding: JSONCodec
 *    Payload:
 *    {
 *        notif_id: <notif_id>,
 *        org_id: <orgID>,
 *        alert_data: {
 *            rule: { name, type: "DEVICE", type_value: <device_id> },
 *            device_id: <device_id>,
 *            last_value: { value: <current_value>, field_name: <rule.metric> },
 *            timestamp: Date.now()
 *        }
 *    }
 *
 * 3. Update state: status = "alerting", last_fired = Date.now()
 *
 * 4. Invoke local onFire callback if registered
 *
 * 5. Persist state to NATS KV
 *
 * ─── On RESOLVED ───
 *
 * 1. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.resolved
 *    Encoding: msgpack
 *    Payload: same structure as FIRE payload
 *
 * 2. Dispatch notifications (same flow as FIRE)
 *
 * 3. Update state: status = "normal", clear acked_by/acked_at/ack_notes/breached_since
 *
 * 4. Invoke local onResolved callback if registered
 *
 * 5. Persist state to NATS KV
 *
 * ─── On ACK (from local SDK call — app.alert.ack) ───
 *
 * For ephemeral alerts, when ack is called locally:
 * 1. Update local state: status = "acknowledged", set acked_by, acked_at = Date.now()
 * 2. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.ack
 *    Encoding: msgpack
 *    Payload:
 *    {
 *        status: "acknowledged",
 *        device_ident: <device_ident>,
 *        ack: { acked_by, ack_notes: null, acked_at: Date.now() }
 *    }
 * 3. Invoke local onAck callback if registered
 * 4. Persist state to NATS KV
 *
 * ─── On ACK (from remote — received via JetStream subscription) ───
 *
 * When an ack event arrives from another SDK instance:
 * 1. Update local state: status = "acknowledged", set acked_by, acked_at from payload
 * 2. Invoke local onAck callback if registered
 * 3. Persist state to NATS KV
 *
 * ─── On ACK_ALL (local or remote — same flow) ───
 *
 * 1. Update local state for the device: status = "acknowledged"
 * 2. Publish to import.{orgID}.{env}.alerts.listen.{rule_id}.ack_all (if local)
 *    Payload: { status: "acknowledged", ack: { acked_by, ack_notes, acked_at } }
 * 3. Invoke local onAckAll callback if registered
 * 4. Persist state to NATS KV
 *
 * ─── STATE PERSISTENCE ───
 *
 * - On connect: load state from NATS KV if exists, otherwise create fresh state
 * - On every state change: persist to NATS KV
 * - KV key: {ruleID}_{deviceID}
 * - KV bucket/endpoint: TBD — pending backend creation
 *   (skeleton code should prepare for KV get/put operations)
 *
 * ─── STATE TRANSITION TABLE ───
 *
 * | Current       | Condition                     | Duration Met?  | Next State     | Action                     |
 * |---------------|-------------------------------|----------------|----------------|----------------------------|
 * | normal        | breach, held < duration        | No             | normal         | Track breached_since       |
 * | normal        | breach, held >= duration       | Yes            | alerting       | FIRE                       |
 * | alerting      | breach, cooldown elapsed       | -              | alerting       | Re-FIRE                    |
 * | alerting      | breach, cooldown not elapsed   | -              | alerting       | No action (silent)         |
 * | alerting      | acked                          | -              | acknowledged   | Silent                     |
 * | alerting      | clear, held < recovery         | -              | alerting       | Track clear_since          |
 * | alerting      | clear, held >= recovery        | -              | normal         | RESOLVED                   |
 * | acknowledged  | breach                         | -              | acknowledged   | No action (silent)         |
 * | acknowledged  | clear, held < recovery         | -              | acknowledged   | Track clear_since          |
 * | acknowledged  | clear, held >= recovery        | -              | normal         | RESOLVED                   |
 */
