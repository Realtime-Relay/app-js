import { JSONCodec } from "nats.ws";
import axios from "axios";
import { validateConnected } from "./validation.js";

const TOKEN_SERVICE = "file_handler";
const REQUEST_TIMEOUT = 20000;

const DEFAULT_LIST_LIMIT = 50;
const MAX_LIST_LIMIT = 200;

/**
 * OTAManager — firmware artifact + rollout operations.
 *
 * Firmware uploads/deletes go over HTTP to the ota-file-handler (the service
 * that owns the firmware artifact lifecycle). The HTTP credentials come from
 * `accounts.user.get_http_token` (NATS) — call `app.ota.init()` once after
 * connect to obtain them. Firmware listing is a plain NATS request to the
 * ota-engine.
 */
export class OTAManager {
  #ctx;
  #codec = JSONCodec();

  #token = null;
  #httpUrl = null;

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
   * @param {string} [params.file_name] - Original filename (X-File-Name —
   *                                      extension source). Defaults to
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
   * by an active rollout).
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
}
