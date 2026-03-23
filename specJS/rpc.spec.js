/**
 * ============================================================
 * RPC SPEC — Remote Procedure Calls
 * ============================================================
 *
 * Covers: app.rpc.call
 */

// ─────────────────────────────────────────────────────────────
// app.rpc.call({ device_ident, name, timeout, data })
// ─────────────────────────────────────────────────────────────

/**
 * @method rpc.call
 * @description Sends a synchronous RPC request to a specific device and
 *              waits for a response within the specified timeout.
 *              Uses NATS request/reply pattern.
 *
 * @param {Object} params
 * @param {string} params.device_ident  - Required. Device identifier.
 *                                         Validated: [a-zA-Z0-9_-]+
 * @param {string} params.name          - Required. RPC method name.
 *                                         Validated: [a-zA-Z0-9_-]+
 * @param {number} params.timeout       - Required. Timeout in seconds.
 *                                         Must be > 0.
 * @param {Object} params.data          - Required. RPC payload (arbitrary JSON).
 *
 * @throws {Error} If device_ident is null/undefined/empty
 * @throws {Error} If device_ident fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If name is null/undefined/empty
 * @throws {Error} If name fails validation ([a-zA-Z0-9_-]+)
 * @throws {Error} If timeout is not a positive number
 * @throws {Error} If data is null/undefined
 * @throws {Error} If not connected
 * @throws {Error} On timeout (device offline or did not respond in time)
 * @throws {Error} On transport failure
 *
 * @nats_subject {orgID}.{env}.command.rpc.<device_id>.<rpc_name>
 * @nats_type request (NATS request/reply)
 * @encoding JSONCodec
 *
 * @request_payload
 * The user-provided `data` object, encoded via JSONCodec.
 *
 * @response_payload
 * Response from the device, decoded via JSONCodec.
 * Structure is device-defined (arbitrary).
 *
 * @behavior
 * - Resolves device_ident to device_id via device cache
 *   (check local cache first, fallback to app.device.get() request)
 * - Sends NATS request to the RPC subject with timeout (seconds * 1000 ms)
 * - Waits for response from device
 * - If device is offline or does not respond, NATS will timeout → throws error
 * - Returns the decoded response from the device
 *
 * @returns {Promise<object>} Response from the device
 *
 * @example
 * var response = await app.rpc.call({
 *     device_ident: "gateway_01",
 *     name: "get_diagnostics",
 *     timeout: 20,
 *     data: { include_logs: true }
 * })
 * // response = { cpu: 45, memory: 72, uptime: 86400, ... }
 */
