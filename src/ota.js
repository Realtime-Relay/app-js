import { JSONCodec } from "nats.ws";
import axios from "axios";
import { decode } from "nats-jwt";
import { validateConnected } from "./validation.js";

const TOKEN_SERVICE = "file_handler";
const REQUEST_TIMEOUT = 20000;

const DEFAULT_LIST_LIMIT = 50;
const MAX_LIST_LIMIT = 200;

const VALID_REQUEST_TYPES = ["DOWNLOAD_ONLY", "DOWNLOAD_INSTALL"];
const VALID_TARGET_TYPES = ["devices", "logical_group", "hierarchy_group", "all"];
const VALID_STATES = ["ACTIVE", "PAUSED", "STOPPED"];
const RETRYABLE_PHASES = ["FAILED", "ROLLED_BACK", "VETOED"];
const ROLLOUT_STATUSES = ["DRAFT", "ACTIVE", "PAUSED", "STOPPED"];
const JOB_PHASES = [
  "PENDING", "DOWNLOADING", "DOWNLOADED", "INSTALLING",
  "INSTALLED", "FAILED", "ROLLED_BACK", "VETOED",
];

/**
 * OTAManager — firmware artifact + rollout operations.
 *
 * Firmware uploads/deletes go over HTTP to the ota-file-handler (the service
 * that owns the firmware artifact lifecycle). The HTTP credentials come from
 * `accounts.user.get_http_token` (NATS) — call `app.ota.init()` once after
 * connect to obtain them. Firmware listing and all rollout operations are
 * plain NATS requests to the ota-engine (org-scoped subjects).
 *
 * Rollout lifecycle: DRAFT (intent, editable, deletable) -> ACTIVE (snapshot
 * + blast) <-> PAUSED -> STOPPED (terminal). Devices work their job queues in
 * FIFO order of rollout activation.
 */
export class OTAManager {
  #ctx;
  #codec = JSONCodec();

  #token = null;
  #httpUrl = null;

  #jobPhaseSub = null; // live subscription for onJobPhaseUpdate

  constructor(ctx) {
    this.#ctx = ctx;
  }

  // ─── init ────────────────────────────────────────────────

  /**
   * Exchanges the app's NATS credential for a short-lived HTTP bearer token
   * and the file-handler base URL. Must be called (once) before
   * firmwareUpload / firmwareDelete. Token lives ~2h; call init() again to
   * refresh.
   *
   * @throws {Error} if not connected or the exchange fails
   */
  async init() {
    validateConnected(this.#ctx.connected);

    let reply = null;
    try {
      const res = await this.#ctx.natsClient.request(
        "accounts.user.get_http_token",
        this.#codec.encode({ jwt: this.#ctx.apiKey, service: TOKEN_SERVICE }),
        { timeout: REQUEST_TIMEOUT },
      );
      reply = res.json();
    } catch (err) {
      throw new Error(`ota.init failed: ${err.message}`);
    }

    if (
      reply?.status !== "HTTP_TOKEN_SUCCESS" ||
      !reply?.data?.token ||
      !reply?.data?.http_url
    ) {
      const reason = reply?.msg || reply?.data?.msg || reply?.status || "unknown error";
      throw new Error(`ota.init failed: ${reason}`);
    }

    this.#token = reply.data.token;
    this.#httpUrl = reply.data.http_url.replace(/\/+$/, "");

    return true;
  }

  #requireInit() {
    if (!this.#token || !this.#httpUrl) {
      throw new Error("OTA not initialized. Call app.ota.init() first.");
    }
  }

  // ─── validation helpers ──────────────────────────────────

  #requireParams(params) {
    if (params == null || typeof params !== "object" || Array.isArray(params)) {
      throw new Error("params must be an object");
    }
  }

  #requireString(value, fieldName) {
    if (typeof value !== "string" || value.trim().length === 0) {
      throw new Error(`${fieldName} must be a non-empty string`);
    }
  }

  // page (1-based) + limit -> offset, shared by all paginated list methods.
  #pagination(params) {
    const { page = 1, limit = DEFAULT_LIST_LIMIT } = params;

    if (!Number.isInteger(page) || page < 1) {
      throw new Error("page must be a positive integer");
    }
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIST_LIMIT) {
      throw new Error(`limit must be an integer between 1 and ${MAX_LIST_LIMIT}`);
    }

    return { limit, offset: (page - 1) * limit };
  }

  #requireFile(file) {
    if (file == null) {
      throw new Error("file is required");
    }

    // Blob/File (browser) — has a size + arrayBuffer()
    const isBlobLike =
      typeof file.arrayBuffer === "function" && typeof file.size === "number";
    // Buffer / TypedArray / ArrayBuffer (node)
    const isBinary =
      file instanceof ArrayBuffer ||
      ArrayBuffer.isView(file); // covers Buffer + Uint8Array

    if (!isBlobLike && !isBinary) {
      throw new Error(
        "file must be a Buffer, Uint8Array, ArrayBuffer, Blob, or File",
      );
    }

    const size = isBlobLike ? file.size : (file.byteLength ?? 0);
    if (size === 0) {
      throw new Error("file is empty");
    }
  }

  // axios response → envelope check. Calls use validateStatus: () => true so
  // non-2xx responses land here (with their envelope body) instead of throwing.
  #parseHttpResponse(res, action) {
    const body = res.data;
    const httpOk = res.status >= 200 && res.status < 300;

    if (body == null || typeof body !== "object") {
      throw new Error(`${action} failed: HTTP ${res.status}`);
    }

    // Envelope: { status: <boolean>, data: <payload> }
    if (!httpOk || body.status !== true) {
      const reason = body?.data?.message || `HTTP ${res.status}`;
      const err = new Error(`${action} failed: ${reason}`);
      if (body?.data?.code) err.code = body.data.code;
      throw err;
    }

    return body.data;
  }

  // ─── firmware upload ─────────────────────────────────────

  /**
   * Uploads a firmware binary to the file-handler (streamed to S3 server-side).
   *
   * @param {Object} params
   * @param {string} params.name        - Display name (X-Firmware-Name)
   * @param {string} params.version     - Firmware version (unique per org)
   * @param {Buffer|Uint8Array|ArrayBuffer|Blob|File} params.file - Firmware bytes
   * @param {string} [params.file_name] - Filename or full path — the SDK sends
   *                                      only the basename (after the last "/")
   *                                      as X-File-Name; the server takes the
   *                                      extension from it. Defaults to
   *                                      file.name for File objects.
   * @returns {Promise<Object>} res.data: { firmware_id, name, version, sha256, size, key }
   */
  async firmwareUpload(params) {
    this.#requireInit();
    this.#requireParams(params);

    const { name, version, file } = params;

    this.#requireString(name, "name");
    this.#requireString(version, "version");
    this.#requireFile(file);

    let fileName = params.file_name ?? (typeof file?.name === "string" ? file.name : "");
    if (fileName != null && typeof fileName !== "string") {
      throw new Error("file_name must be a string");
    }
    // file_name may be a full path — X-File-Name is the basename (after last "/").
    if (fileName) {
      fileName = fileName.slice(fileName.lastIndexOf("/") + 1);
    }

    const url = `${this.#httpUrl}/iot/ota/firmware?version=${encodeURIComponent(version)}`;

    const headers = {
      Authorization: `Bearer ${this.#token}`,
      "Content-Type": "application/octet-stream",
      "X-Firmware-Name": name.trim(),
    };
    if (fileName) {
      headers["X-File-Name"] = fileName;
    }

    let res = null;
    try {
      res = await axios.request({
        method: "POST",
        url,
        headers,
        data: file,
        validateStatus: () => true, // envelope-driven error handling
        maxBodyLength: Infinity, // firmware can be up to 64MB
        maxContentLength: Infinity,
      });
    } catch (err) {
      // AggregateError (e.g. ECONNREFUSED) has an empty .message — prefer .code
      throw new Error(`firmware upload failed: ${err.code || err.message || "network error"}`);
    }

    return this.#parseHttpResponse(res, "firmware upload");
  }

  // ─── firmware delete ─────────────────────────────────────

  /**
   * Deletes a firmware artifact (refused server-side while it is referenced
   * by an ACTIVE or PAUSED rollout).
   *
   * @param {Object} params
   * @param {string} params.id - firmware_id
   * @returns {Promise<Object>} res.data: { firmware_id, deleted }
   */
  async firmwareDelete(params) {
    this.#requireInit();
    this.#requireParams(params);

    const { id } = params;
    this.#requireString(id, "id");

    const url = `${this.#httpUrl}/iot/ota/firmware/${encodeURIComponent(id.trim())}`;

    let res = null;
    try {
      res = await axios.request({
        method: "DELETE",
        url,
        headers: { Authorization: `Bearer ${this.#token}` },
        validateStatus: () => true, // envelope-driven error handling
      });
    } catch (err) {
      // AggregateError (e.g. ECONNREFUSED) has an empty .message — prefer .code
      throw new Error(`firmware delete failed: ${err.code || err.message || "network error"}`);
    }

    return this.#parseHttpResponse(res, "firmware delete");
  }

  // ─── firmware list ───────────────────────────────────────

  /**
   * Paginated firmware list (NATS → ota-engine). Newest first.
   *
   * @param {Object} [params]
   * @param {number} [params.page=1]   - 1-based page number
   * @param {number} [params.limit=50] - page size (max 200)
   * @returns {Promise<Object>} { firmwares: [...], page: { limit, offset, count, has_more, next_offset } }
   */
  async firmwareList(params = {}) {
    validateConnected(this.#ctx.connected);
    this.#requireParams(params);

    let { page = 1, limit = DEFAULT_LIST_LIMIT } = params;

    if (!Number.isInteger(page) || page < 1) {
      throw new Error("page must be a positive integer");
    }
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIST_LIMIT) {
      throw new Error(`limit must be an integer between 1 and ${MAX_LIST_LIMIT}`);
    }

    const offset = (page - 1) * limit;

    let reply = null;
    try {
      // org identity is carried in the subject (NATS permissions scope it)
      const res = await this.#ctx.natsClient.request(
        `api.iot.ota.${this.#ctx.orgID}.firmware.list`,
        this.#codec.encode({ limit, offset }),
        { timeout: REQUEST_TIMEOUT },
      );
      reply = res.json();
    } catch (err) {
      throw new Error(`firmware list failed: ${err.message}`);
    }

    if (reply?.status !== "FIRMWARE_LIST_SUCCESS") {
      const reason =
        reply?.data?.code ||
        (Array.isArray(reply?.data) ? reply.data.join(", ") : reply?.status) ||
        "unknown error";
      throw new Error(`firmware list failed: ${reason}`);
    }

    return reply.data;
  }

  // ─── rollout helpers ─────────────────────────────────────

  async #rolloutRequest(op, payload, successStatus, action) {
    validateConnected(this.#ctx.connected);

    let reply = null;
    try {
      // org identity is carried in the subject (NATS permissions scope it)
      const res = await this.#ctx.natsClient.request(
        `api.iot.ota.${this.#ctx.orgID}.rollout.${op}`,
        this.#codec.encode(payload),
        { timeout: REQUEST_TIMEOUT },
      );
      reply = res.json();
    } catch (err) {
      throw new Error(`${action} failed: ${err.code || err.message}`);
    }

    if (reply?.status !== successStatus) {
      const d = reply?.data;
      const reason =
        d?.code || (Array.isArray(d) ? d.join(", ") : reply?.status) || "unknown error";
      const e = new Error(`${action} failed: ${reason}`);
      if (d?.code) e.code = d.code;
      throw e;
    }

    return reply.data;
  }

  // target: { type: "devices"|"logical_group"|"hierarchy_group"|"all", ..., exclude? }
  #validateTarget(target) {
    if (target == null || typeof target !== "object" || Array.isArray(target)) {
      throw new Error("target must be an object");
    }

    if (!VALID_TARGET_TYPES.includes(target.type)) {
      throw new Error(`target.type must be one of: ${VALID_TARGET_TYPES.join(", ")}`);
    }

    if (target.type === "devices") {
      if (
        !Array.isArray(target.device_ids) ||
        target.device_ids.length === 0 ||
        target.device_ids.some((d) => typeof d !== "string" || d.length === 0)
      ) {
        throw new Error(
          'target.device_ids must be a non-empty string array for type "devices"',
        );
      }
    }

    if (target.type === "logical_group" || target.type === "hierarchy_group") {
      this.#requireString(target.group_id, "target.group_id");
    }

    if (target.exclude != null) {
      if (
        !Array.isArray(target.exclude) ||
        target.exclude.some((d) => typeof d !== "string" || d.length === 0)
      ) {
        throw new Error("target.exclude must be a string array");
      }
    }
  }

  // Validates the optional rollout fields shared by create/update. Returns an
  // object containing only the fields that were provided.
  #pickRolloutFields(params) {
    const out = {};

    if (params.request_type !== undefined) {
      if (!VALID_REQUEST_TYPES.includes(params.request_type)) {
        throw new Error(
          `request_type must be one of: ${VALID_REQUEST_TYPES.join(", ")}`,
        );
      }
      out.request_type = params.request_type;
    }

    if (params.target !== undefined && params.target !== null) {
      this.#validateTarget(params.target);
      out.target = params.target;
    }

    if (params.force_download !== undefined) {
      if (typeof params.force_download !== "boolean") {
        throw new Error("force_download must be a boolean");
      }
      out.force_download = params.force_download;
    }

    if (params.force_install !== undefined) {
      if (typeof params.force_install !== "boolean") {
        throw new Error("force_install must be a boolean");
      }
      out.force_install = params.force_install;
    }

    if (params.user_config !== undefined) {
      if (
        params.user_config == null ||
        typeof params.user_config !== "object" ||
        Array.isArray(params.user_config)
      ) {
        throw new Error("user_config must be an object");
      }
      out.user_config = params.user_config;
    }

    return out;
  }

  // created_by defaults to the api_key's id (decoded from the JWT).
  #derivedCreatedBy() {
    try {
      return decode(this.#ctx.apiKey)?.nats?.org_data?.api_key_id ?? null;
    } catch {
      return null;
    }
  }

  // ─── rollout create ──────────────────────────────────────

  /**
   * Creates a DRAFT rollout — pure intent, no jobs exist yet. The returned
   * device_count is a PREVIEW: the target is re-resolved and snapshotted into
   * per-device jobs at activation (toggleRollout to ACTIVE), against the
   * fleet as it exists then.
   *
   * @param {Object} params
   * @param {string} params.firmware_id      - Required.
   * @param {string} params.request_type     - Required. "DOWNLOAD_ONLY" | "DOWNLOAD_INSTALL".
   * @param {Object} params.target           - Required — no implicit
   *                                           fleet-wide: whole-org rollouts
   *                                           must say { type: "all" }. Shapes:
   *                                           { type:"devices", device_ids:[...] }
   *                                           { type:"logical_group"|"hierarchy_group", group_id }
   *                                           { type:"all" } — plus optional exclude:[device_ids]
   * @param {boolean} [params.force_download]
   * @param {boolean} [params.force_install]
   * @param {Object}  [params.user_config]
   * @param {string}  [params.created_by]    - Optional. Defaults to the api_key
   *                                           id; null when underivable.
   * @returns {Promise<Object>} { rollout_id, status: "DRAFT", device_count (preview) }
   */
  async createRollout(params) {
    this.#requireParams(params);
    this.#requireString(params.firmware_id, "firmware_id");

    if (params.request_type === undefined) {
      throw new Error("request_type is required");
    }

    if (params.target == null) {
      throw new Error(
        'target is required — pass { type: "all" } explicitly for a fleet-wide rollout',
      );
    }

    const fields = this.#pickRolloutFields(params);

    if (params.created_by !== undefined && params.created_by !== null) {
      this.#requireString(params.created_by, "created_by");
    }

    // Optional: explicit value → derived api_key id → null.
    const created_by = params.created_by ?? this.#derivedCreatedBy() ?? null;

    return await this.#rolloutRequest(
      "create",
      {
        firmware_id: params.firmware_id,
        created_by,
        env: this.#ctx.env, // device env for nudge subjects
        ...fields,
      },
      "ROLLOUT_CREATE_SUCCESS",
      "rollout create",
    );
  }

  // ─── rollout update ──────────────────────────────────────

  /**
   * Updates a DRAFT rollout (anything past DRAFT is immutable —
   * ROLLOUT_NOT_DRAFT). Changing the target returns a fresh preview count;
   * no jobs exist until activation.
   *
   * @param {Object} params
   * @param {string} params.rollout_id - Required.
   * @param {Object} [params.target]
   * @param {string} [params.request_type]
   * @param {boolean} [params.force_download]
   * @param {boolean} [params.force_install]
   * @param {Object} [params.user_config]
   * @returns {Promise<Object>} { rollout_id, device_count? (preview) }
   */
  async updateRollout(params) {
    this.#requireParams(params);
    this.#requireString(params.rollout_id, "rollout_id");

    const fields = this.#pickRolloutFields(params);

    if (Object.keys(fields).length === 0) {
      throw new Error(
        "at least one of target, request_type, force_download, force_install, user_config is required",
      );
    }

    return await this.#rolloutRequest(
      "update",
      { rollout_id: params.rollout_id, ...fields },
      "ROLLOUT_UPDATE_SUCCESS",
      "rollout update",
    );
  }

  // ─── rollout delete ──────────────────────────────────────

  /**
   * Deletes a DRAFT rollout. Activated rollouts are permanent history —
   * stop them instead (toggleRollout to STOPPED). Fails with
   * ROLLOUT_NOT_DRAFT otherwise.
   *
   * @param {Object} params
   * @param {string} params.rollout_id - Required.
   * @returns {Promise<Object>} { rollout_id }
   */
  async deleteRollout(params) {
    this.#requireParams(params);
    this.#requireString(params.rollout_id, "rollout_id");

    return await this.#rolloutRequest(
      "delete",
      { rollout_id: params.rollout_id },
      "ROLLOUT_DELETE_SUCCESS",
      "rollout delete",
    );
  }

  // ─── rollout state (activate / pause / resume / stop) ───

  /**
   * Drives the rollout lifecycle: DRAFT→ACTIVE (the snapshot moment — target
   * resolved into per-device jobs + blast), ACTIVE⇄PAUSED (pause freezes
   * PENDING jobs and blocks each device's FIFO queue head; resume serves the
   * same snapshot again), ACTIVE/PAUSED→STOPPED (terminal; jobs preserved as
   * history). Invalid moves fail with INVALID_TRANSITION.
   *
   * @param {Object} params
   * @param {string} params.rollout_id - Required.
   * @param {string} params.state      - Required. "ACTIVE" | "PAUSED" | "STOPPED".
   * @returns {Promise<Object>} { rollout_id, state, device_count? (on first activation) }
   */
  async toggleRollout(params) {
    this.#requireParams(params);
    this.#requireString(params.rollout_id, "rollout_id");

    if (!VALID_STATES.includes(params.state)) {
      throw new Error(`state must be one of: ${VALID_STATES.join(", ")}`);
    }

    return await this.#rolloutRequest(
      "state",
      { rollout_id: params.rollout_id, state: params.state },
      "ROLLOUT_STATE_SUCCESS",
      "rollout state",
    );
  }

  // ─── rollout retry (manual) ──────────────────────────────

  /**
   * Manually re-arms terminal jobs (FAILED / ROLLED_BACK / VETOED → PENDING,
   * attempts+1, history logged) and nudges the affected devices. The rollout
   * must be ACTIVE or PAUSED (ROLLOUT_NOT_LIVE otherwise). A re-armed job
   * keeps its original FIFO position (rollout activation order).
   *
   * @param {Object} params
   * @param {string}   params.rollout_id   - Required.
   * @param {string[]} [params.phases]     - Subset of FAILED|ROLLED_BACK|VETOED
   *                                         (default: all three).
   * @param {string[]} [params.device_ids] - Limit the retry to these devices.
   * @returns {Promise<Object>} { rollout_id, retried }
   */
  async retryRollout(params) {
    this.#requireParams(params);
    this.#requireString(params.rollout_id, "rollout_id");

    const payload = { rollout_id: params.rollout_id };

    if (params.phases !== undefined && params.phases !== null) {
      if (
        !Array.isArray(params.phases) ||
        params.phases.length === 0 ||
        params.phases.some((p) => !RETRYABLE_PHASES.includes(p))
      ) {
        throw new Error(`phases must be a subset of: ${RETRYABLE_PHASES.join(", ")}`);
      }
      payload.phases = params.phases;
    }

    if (params.device_ids !== undefined && params.device_ids !== null) {
      if (
        !Array.isArray(params.device_ids) ||
        params.device_ids.length === 0 ||
        params.device_ids.some((d) => typeof d !== "string" || d.length === 0)
      ) {
        throw new Error("device_ids must be a non-empty string array");
      }
      payload.device_ids = params.device_ids;
    }

    return await this.#rolloutRequest(
      "retry",
      payload,
      "ROLLOUT_RETRY_SUCCESS",
      "rollout retry",
    );
  }

  // ─── rollout install (install-later) ─────────────────────

  /**
   * Install-later: nudge devices whose job is DOWNLOADED (image staged via a
   * DOWNLOAD_ONLY rollout) to install it now, no re-download. Rollout must be
   * ACTIVE (else ROLLOUT_NOT_LIVE). Non-staged devices are ignored.
   *
   * @param {Object}   params
   * @param {string}   params.rollout_id   Required.
   * @param {string[]} [params.device_ids] Limit to these staged devices.
   * @returns {Promise<Object>} { rollout_id, installing }
   */
  async installRollout(params) {
    this.#requireParams(params);
    this.#requireString(params.rollout_id, "rollout_id");

    const payload = { rollout_id: params.rollout_id };

    if (params.device_ids !== undefined && params.device_ids !== null) {
      if (
        !Array.isArray(params.device_ids) ||
        params.device_ids.length === 0 ||
        params.device_ids.some((d) => typeof d !== "string" || d.length === 0)
      ) {
        throw new Error("device_ids must be a non-empty string array");
      }
      payload.device_ids = params.device_ids;
    }

    return await this.#rolloutRequest(
      "install",
      payload,
      "ROLLOUT_INSTALL_SUCCESS",
      "rollout install",
    );
  }

  // ─── rollout list ────────────────────────────────────────

  /**
   * Paginated rollout list (newest first). Optional status filter.
   *
   * @param {Object} [params]
   * @param {number} [params.page=1]
   * @param {number} [params.limit=50]  - max 200
   * @param {string} [params.status]    - "DRAFT" | "ACTIVE" | "PAUSED" | "STOPPED"
   * @returns {Promise<Object>} { rollouts: [...], page }
   */
  async rolloutList(params = {}) {
    this.#requireParams(params);

    if (params.status !== undefined && !ROLLOUT_STATUSES.includes(params.status)) {
      throw new Error(`status must be one of: ${ROLLOUT_STATUSES.join(", ")}`);
    }

    const { limit, offset } = this.#pagination(params);

    const payload = { limit, offset };
    if (params.status !== undefined) payload.status = params.status;

    return await this.#rolloutRequest(
      "list",
      payload,
      "ROLLOUT_LIST_SUCCESS",
      "rollout list",
    );
  }

  // ─── jobs list (per-device scoreboard) ───────────────────

  /**
   * Paginated jobs for one rollout + aggregate phase stats. Sorted by most
   * recent movement. History is never included — use jobHistory() for a
   * device's time log. Optional phase filter (pairs with retryRollout's
   * device_ids: list FAILED jobs, retry exactly those devices).
   *
   * @param {Object} params
   * @param {string} params.rollout_id - Required.
   * @param {number} [params.page=1]
   * @param {number} [params.limit=50] - max 200
   * @param {string} [params.phase]    - any job phase
   * @returns {Promise<Object>} { rollout_id, stats, jobs: [...], page }
   */
  async jobsList(params) {
    validateConnected(this.#ctx.connected);
    this.#requireParams(params);
    this.#requireString(params.rollout_id, "rollout_id");

    if (params.phase !== undefined && !JOB_PHASES.includes(params.phase)) {
      throw new Error(`phase must be one of: ${JOB_PHASES.join(", ")}`);
    }

    const { limit, offset } = this.#pagination(params);

    const payload = { rollout_id: params.rollout_id, limit, offset };
    if (params.phase !== undefined) payload.phase = params.phase;

    let reply = null;
    try {
      const res = await this.#ctx.natsClient.request(
        `api.iot.ota.${this.#ctx.orgID}.jobs.list`,
        this.#codec.encode(payload),
        { timeout: REQUEST_TIMEOUT },
      );
      reply = res.json();
    } catch (err) {
      throw new Error(`jobs list failed: ${err.code || err.message}`);
    }

    if (reply?.status !== "JOBS_LIST_SUCCESS") {
      const d = reply?.data;
      const reason =
        d?.code || (Array.isArray(d) ? d.join(", ") : reply?.status) || "unknown error";
      const e = new Error(`jobs list failed: ${reason}`);
      if (d?.code) e.code = d.code;
      throw e;
    }

    return reply.data;
  }

  // ─── job history (paginated time log of one job) ─────────

  /**
   * Paginated history of a single job — chronological (oldest first; it
   * renders as a timeline). page.total is exact.
   *
   * @param {Object} params
   * @param {string} params.job_id  - Required (from jobsList).
   * @param {number} [params.page=1]
   * @param {number} [params.limit=50] - max 200
   * @returns {Promise<Object>} { job_id, rollout_id, device_id, phase, attempts, history: [...], page }
   */
  async jobHistory(params) {
    validateConnected(this.#ctx.connected);
    this.#requireParams(params);
    this.#requireString(params.job_id, "job_id");

    const { limit, offset } = this.#pagination(params);

    let reply = null;
    try {
      const res = await this.#ctx.natsClient.request(
        `api.iot.ota.${this.#ctx.orgID}.jobs.history`,
        this.#codec.encode({ job_id: params.job_id, limit, offset }),
        { timeout: REQUEST_TIMEOUT },
      );
      reply = res.json();
    } catch (err) {
      throw new Error(`job history failed: ${err.code || err.message}`);
    }

    if (reply?.status !== "JOBS_HISTORY_SUCCESS") {
      const d = reply?.data;
      const reason =
        d?.code || (Array.isArray(d) ? d.join(", ") : reply?.status) || "unknown error";
      const e = new Error(`job history failed: ${reason}`);
      if (d?.code) e.code = d.code;
      throw e;
    }

    return reply.data;
  }

  // ─── live job phase updates ──────────────────────────────

  /**
   * Subscribe to live job phase updates for every device in the org/env. The
   * engine publishes each device transition to
   * import.<org>.<env>.ota.<device_id>.job_phase_update; this subscribes with a
   * '*' wildcard. Replaces any prior subscription. Returns an unsubscribe fn.
   *
   * @param {function} callback ({ rollout_id, device_id, phase, error, ts })
   * @returns {function} unsubscribe
   */
  onJobPhaseUpdate(callback) {
    validateConnected(this.#ctx.connected);
    if (typeof callback !== "function") {
      throw new Error("onJobPhaseUpdate requires a callback function");
    }

    this.offJobPhaseUpdate(); // one live subscription at a time

    const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.ota.*.job_phase_update`;
    this.#jobPhaseSub = this.#ctx.natsClient.subscribe(subject, {
      callback: (err, msg) => {
        if (err) return;
        let update;
        try {
          update = this.#codec.decode(msg.data);
        } catch {
          return;
        }
        callback(update);
      },
    });

    return () => this.offJobPhaseUpdate();
  }

  /** Stop the onJobPhaseUpdate() subscription. No-op if not subscribed. */
  offJobPhaseUpdate() {
    this.#jobPhaseSub?.unsubscribe();
    this.#jobPhaseSub = null;
  }
}
