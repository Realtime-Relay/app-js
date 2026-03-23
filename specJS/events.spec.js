/**
 * ============================================================
 * EVENTS SPEC — Event Streaming
 * ============================================================
 *
 * Covers: app.events.stream, app.events.off
 */

// ─────────────────────────────────────────────────────────────
// app.events.stream({ name, callback })
// ─────────────────────────────────────────────────────────────

/**
 * @method events.stream
 * @description Subscribes to a named event stream across all devices.
 *              Uses a wildcard for device_id position in the subject.
 *
 * @param {Object} params
 * @param {string}   params.name      - Required. Event name.
 *                                       Validated: [a-zA-Z0-9_-]+
 * @param {Function} params.callback  - Required. Called with each event data.
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If callback is not a function
 * @throws {Error} If not connected
 *
 * @nats_subject {orgID}.{env}.events.*.<event_name>
 * @nats_type jetstream_consumer
 * @encoding msgpack (decode on receive)
 *
 * @callback_payload
 * Decoded msgpack data from the event. Structure is event-defined.
 *
 * @behavior
 * - Creates a JetStream consumer on {orgID}.{env}.events.*.<event_name>
 * - The wildcard `*` matches any device_id in the subject
 * - Decodes msgpack payload, invokes callback with decoded data
 * - One callback per event name — no duplicates
 * - If already subscribed to same event name, returns false
 * - Consumer name format: appjs_events_{event_name}_{uuid}
 * - Consumer is cleaned up on disconnect()
 *
 * @returns {boolean} true if subscription created, false if already exists
 *
 * @example
 * app.events.stream({
 *     name: "door_opened",
 *     callback: (data) => {
 *         console.log("Door event:", data)
 *     }
 * })
 */

// ─────────────────────────────────────────────────────────────
// app.events.off({ name })
// ─────────────────────────────────────────────────────────────

/**
 * @method events.off
 * @description Unsubscribes from a named event stream.
 *
 * @param {Object} params
 * @param {string} params.name - Required. Event name to unsubscribe from.
 *                                Validated: [a-zA-Z0-9_-]+
 *
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_-]+)
 *
 * @behavior
 * - Deletes the JetStream consumer for the event name
 * - Removes callback from internal event map
 * - If no subscription exists for the event name, no-op
 *
 * @returns {void}
 *
 * @example
 * app.events.off({ name: "door_opened" })
 */
