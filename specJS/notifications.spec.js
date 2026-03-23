/**
 * ============================================================
 * NOTIFICATIONS SPEC — Notification Channel CRUD
 * ============================================================
 *
 * Covers: app.notification.create, update, delete, list, get
 *
 * NOTE: Only EMAIL and WEBHOOK types can be created via the SDK.
 */

// ─────────────────────────────────────────────────────────────
// app.notification.create({ name, type, config })
// ─────────────────────────────────────────────────────────────

/**
 * @method notification.create
 * @description Creates a new notification channel (EMAIL or WEBHOOK).
 *
 * @param {Object} params
 * @param {string} params.name    - Required. Notification channel name.
 *                                   Validated: [a-zA-Z0-9_-]+
 * @param {string} params.type    - Required. "WEBHOOK" | "EMAIL"
 * @param {Object} params.config  - Required. Type-specific configuration.
 *
 * For type = "WEBHOOK":
 * @param {string}   params.config.endpoint        - Required. Webhook URL.
 * @param {Object}   params.config.headers          - Required. HTTP headers.
 *                                                    Key-value pairs.
 * @param {number[]} [params.config.retry_back_off] - Optional. Retry backoff
 *                                                    intervals in ms.
 *
 * For type = "EMAIL":
 * @param {string[]} params.config.recipients  - Required. Array of email addresses.
 * @param {string}   params.config.subject     - Required. Email subject.
 * @param {string}   params.config.template    - Required. Email template
 *                                               (Handlebars template with alert_data context).
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If type is not "WEBHOOK" or "EMAIL"
 * @throws {Error} If config is missing required fields for the type
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.notification.{orgID}.create
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * // WEBHOOK:
 * {
 *     name: string,
 *     type: "WEBHOOK",
 *     config: {
 *         endpoint: string,
 *         headers: object,
 *         retry_back_off: number[] | undefined
 *     }
 * }
 * // EMAIL:
 * {
 *     name: string,
 *     type: "EMAIL",
 *     config: {
 *         recipients: string[],
 *         subject: string,
 *         template: string
 *     }
 * }
 *
 * @response_payload
 * // Success:
 * {
 *     status: "NOTIFICATION_CREATE_SUCCESS",
 *     data: { name: string, type: string }
 * }
 * // Failure:
 * {
 *     status: "NOTIFICATION_CREATE_FAILURE",
 *     data: { msg: string[] }
 * }
 *
 * @returns {Promise<object>} Notification object on success
 *
 * @example
 * // Webhook
 * var notif = await app.notification.create({
 *     name: "ops_webhook",
 *     type: "WEBHOOK",
 *     config: {
 *         endpoint: "https://hooks.example.com/alerts",
 *         headers: { "Authorization": "Bearer <token>" }
 *     }
 * })
 *
 * // Email
 * var notif = await app.notification.create({
 *     name: "ops_email",
 *     type: "EMAIL",
 *     config: {
 *         recipients: ["ops@example.com", "alerts@example.com"],
 *         subject: "Alert: {{rule.name}}",
 *         template: "Device {{device_id}} triggered {{rule.name}} at {{timestamp}}"
 *     }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.notification.update({ name, type, config })
// ─────────────────────────────────────────────────────────────

/**
 * @method notification.update
 * @description Updates an existing notification channel.
 *              Must send the ENTIRE config — not partial updates.
 *              Backend throws if type and config have a mismatch.
 *
 * @param {Object} params
 * @param {string} params.name    - Required. Notification channel name.
 * @param {string} params.type    - Required. "WEBHOOK" | "EMAIL"
 * @param {Object} params.config  - Required. Complete config for the type.
 *                                   Same structure as create.
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If type is not "WEBHOOK" or "EMAIL"
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.notification.{orgID}.update
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {
 *     id: string,         // Notification ID
 *     name: string,
 *     type: string,
 *     config: object      // Complete config — same structure as create
 * }
 *
 * @response_payload
 * // Success:
 * { status: "NOTIFICATION_UPDATE_SUCCESS", data: object }
 * // Failure:
 * { status: "NOTIFICATION_UPDATE_FAILURE", data: { msg: string } }
 *
 * @behavior
 * - Must send the entire config, not partial fields
 * - Backend validates type/config match — mismatches produce errors
 *
 * @returns {Promise<object>} Updated notification object
 *
 * @example
 * var notif = await app.notification.update({
 *     name: "ops_webhook",
 *     type: "WEBHOOK",
 *     config: {
 *         endpoint: "https://hooks.example.com/v2/alerts",
 *         headers: { "Authorization": "Bearer <new_token>" }
 *     }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.notification.delete(notif_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method notification.delete
 * @description Deletes a notification channel by ID.
 *
 * @param {string} notif_id - Required. Notification ID.
 *
 * @throws {Error} If notif_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.notification.{orgID}.delete
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * { status: "NOTIFICATION_DELETE_SUCCESS" | "NOTIFICATION_DELETE_FAILURE" }
 *
 * @returns {Promise<boolean>} true on successful deletion
 *
 * @example
 * var deleted = await app.notification.delete("<notif_id>")
 */

// ─────────────────────────────────────────────────────────────
// app.notification.list()
// ─────────────────────────────────────────────────────────────

/**
 * @method notification.list
 * @description Lists all notification channels for the org.
 *
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.notification.{orgID}.list
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * {} (empty object)
 *
 * @response_payload
 * { status: "NOTIFICATION_LIST_SUCCESS", data: object[] }
 *
 * @returns {Promise<object[]>} Array of notification channel objects
 *
 * @example
 * var notifs = await app.notification.list()
 */

// ─────────────────────────────────────────────────────────────
// app.notification.get(notif_id)
// ─────────────────────────────────────────────────────────────

/**
 * @method notification.get
 * @description Gets a single notification channel by ID.
 *
 * @param {string} notif_id - Required. Notification ID.
 *
 * @throws {Error} If notif_id is null/undefined/empty
 * @throws {Error} If not connected
 * @throws {Error} On transport/timeout failure
 *
 * @nats_subject api.iot.notification.{orgID}.get
 * @nats_type request
 * @encoding JSONCodec
 *
 * @request_payload
 * { id: string }
 *
 * @response_payload
 * // Success:
 * { status: "NOTIFICATION_FETCH_SUCCESS", data: object }
 * // Failure:
 * { status: "NOTIFICATION_FETCH_FAILURE", data: { msg: string } }
 *
 * @returns {Promise<object>} Notification channel object
 *
 * @example
 * var notif = await app.notification.get("<notif_id>")
 */
