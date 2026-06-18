import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("axios", () => ({
  default: { request: vi.fn() },
}));

import axios from "axios";
import { createMockContext } from "./setup.js";
import { OTAManager } from "../src/ota.js";

const TOKEN = "test_http_token_abc";
const HTTP_URL = "http://localhost:3200";

function makeCtx(responseOverrides = {}) {
  return createMockContext({
    apiKey: "test_api_key_jwt",
    responses: {
      "accounts.user.get_http_token": {
        status: "HTTP_TOKEN_SUCCESS",
        data: { token: TOKEN, http_url: HTTP_URL },
      },
      "api.iot.ota.test_org_123.firmware.list": {
        status: "FIRMWARE_LIST_SUCCESS",
        data: {
          firmwares: [
            {
              firmware_id: "fw_1",
              name: "Boiler fix",
              version: "1.0.0",
              sha256: "abc",
              size: 1024,
              ext: "bin",
            },
          ],
          page: { limit: 50, offset: 0, count: 1, has_more: false, next_offset: null },
        },
      },
      "api.iot.ota.test_org_123.rollout.create": {
        status: "ROLLOUT_CREATE_SUCCESS",
        data: { rollout_id: "ro_1", status: "DRAFT", device_count: 3 },
      },
      "api.iot.ota.test_org_123.rollout.update": {
        status: "ROLLOUT_UPDATE_SUCCESS",
        data: { rollout_id: "ro_1", device_count: 2 },
      },
      "api.iot.ota.test_org_123.rollout.delete": {
        status: "ROLLOUT_DELETE_SUCCESS",
        data: { rollout_id: "ro_1" },
      },
      "api.iot.ota.test_org_123.rollout.state": {
        status: "ROLLOUT_STATE_SUCCESS",
        data: { rollout_id: "ro_1", state: "ACTIVE", device_count: 3 },
      },
      "api.iot.ota.test_org_123.rollout.retry": {
        status: "ROLLOUT_RETRY_SUCCESS",
        data: { rollout_id: "ro_1", retried: 2 },
      },
      "api.iot.ota.test_org_123.rollout.list": {
        status: "ROLLOUT_LIST_SUCCESS",
        data: {
          rollouts: [
            { rollout_id: "ro_1", firmware_id: "fw_1", status: "ACTIVE", env: "production" },
            { rollout_id: "ro_2", firmware_id: "fw_1", status: "DRAFT", env: "production" },
          ],
          page: { limit: 50, offset: 0, count: 2, has_more: false, next_offset: null },
        },
      },
      "api.iot.ota.test_org_123.jobs.list": {
        status: "JOBS_LIST_SUCCESS",
        data: {
          rollout_id: "ro_1",
          stats: { INSTALLED: 8, FAILED: 2 },
          jobs: [
            { job_id: "j_1", device_id: "665f000000000000000000a1", phase: "FAILED", error: "sha mismatch", attempts: 1 },
          ],
          page: { limit: 50, offset: 0, count: 1, has_more: false, next_offset: null },
        },
      },
      "api.iot.ota.test_org_123.jobs.history": {
        status: "JOBS_HISTORY_SUCCESS",
        data: {
          job_id: "j_1",
          rollout_id: "ro_1",
          device_id: "665f000000000000000000a1",
          phase: "FAILED",
          attempts: 1,
          history: [
            { phase: "PENDING", ts: "2026-06-11T10:00:00.000Z", note: "activated" },
            { phase: "DOWNLOADING", ts: "2026-06-11T10:01:00.000Z" },
            { phase: "FAILED", ts: "2026-06-11T10:02:00.000Z", error: "sha mismatch" },
          ],
          page: { limit: 50, offset: 0, count: 3, total: 3, has_more: false, next_offset: null },
        },
      },
      ...responseOverrides,
    },
  });
}

function lastPayload(ctx, subject) {
  const call = ctx.natsClient.request.mock.calls.find((c) => c[0] === subject);
  return JSON.parse(new TextDecoder().decode(call[1]));
}

// axios mock resolving with the HTTP envelope { status: <boolean>, data: <...> }
function mockHttp(body, { httpStatus = 200 } = {}) {
  axios.request.mockResolvedValueOnce({ status: httpStatus, data: body });
  return axios.request;
}

async function initializedOta(ctx = makeCtx()) {
  const ota = new OTAManager(ctx);
  await ota.init();
  return { ota, ctx };
}

beforeEach(() => {
  axios.request.mockReset();
});

// ─── init ────────────────────────────────────────────────────

describe("ota.init", () => {
  it("exchanges the api_key for a token + http url", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    await expect(ota.init()).resolves.toBe(true);

    const call = ctx.natsClient.request.mock.calls.find(
      (c) => c[0] === "accounts.user.get_http_token",
    );
    expect(call).toBeTruthy();
    const payload = JSON.parse(new TextDecoder().decode(call[1]));
    expect(payload).toEqual({ jwt: "test_api_key_jwt", service: "file_handler" });
  });

  it("throws when not connected", async () => {
    const ctx = makeCtx();
    ctx.connected = false;
    const ota = new OTAManager(ctx);

    await expect(ota.init()).rejects.toThrow(/Not connected/);
  });

  it("throws when the backend rejects the exchange", async () => {
    const ctx = makeCtx({
      "accounts.user.get_http_token": {
        status: "HTTP_TOKEN_FAILURE",
        code: "INVALID_JWT",
        msg: "Invalid JWT",
      },
    });
    const ota = new OTAManager(ctx);

    await expect(ota.init()).rejects.toThrow(/ota.init failed: Invalid JWT/);
  });

  it("throws when the reply is missing token or url", async () => {
    const ctx = makeCtx({
      "accounts.user.get_http_token": {
        status: "HTTP_TOKEN_SUCCESS",
        data: { token: null, http_url: null },
      },
    });
    const ota = new OTAManager(ctx);

    await expect(ota.init()).rejects.toThrow(/ota.init failed/);
  });
});

// ─── firmwareUpload ──────────────────────────────────────────

describe("ota.firmwareUpload", () => {
  const FILE = new Uint8Array([0xe9, 0x01, 0x02, 0x03]);

  it("throws if init() has not been called", async () => {
    const ota = new OTAManager(makeCtx());
    await expect(
      ota.firmwareUpload({ name: "fw", version: "1.0.0", file: FILE }),
    ).rejects.toThrow(/Call app.ota.init\(\) first/);
  });

  it("validates params", async () => {
    const { ota } = await initializedOta();

    await expect(ota.firmwareUpload()).rejects.toThrow(/params must be an object/);
    await expect(
      ota.firmwareUpload({ version: "1.0.0", file: FILE }),
    ).rejects.toThrow(/name must be a non-empty string/);
    await expect(
      ota.firmwareUpload({ name: "fw", file: FILE }),
    ).rejects.toThrow(/version must be a non-empty string/);
    await expect(
      ota.firmwareUpload({ name: "fw", version: "1.0.0" }),
    ).rejects.toThrow(/file is required/);
    await expect(
      ota.firmwareUpload({ name: "fw", version: "1.0.0", file: "not-bytes" }),
    ).rejects.toThrow(/file must be/);
    await expect(
      ota.firmwareUpload({ name: "fw", version: "1.0.0", file: new Uint8Array(0) }),
    ).rejects.toThrow(/file is empty/);
  });

  it("POSTs the file with auth + name headers and returns res.data", async () => {
    const { ota } = await initializedOta();
    const data = {
      firmware_id: "fw_1",
      name: "Boiler fix",
      version: "1.0.0",
      sha256: "abc",
      size: 4,
      key: "staging/firmware/org/fw_1.bin",
    };
    const httpFn = mockHttp({ status: true, data });

    const out = await ota.firmwareUpload({
      name: "Boiler fix",
      version: "1.0.0",
      file: FILE,
      file_name: "app.bin",
    });

    expect(out).toEqual(data);

    const [cfg] = httpFn.mock.calls[0];
    expect(cfg.url).toBe(`${HTTP_URL}/iot/ota/firmware?version=1.0.0`);
    expect(cfg.method).toBe("POST");
    expect(cfg.data).toBe(FILE);
    expect(cfg.headers.Authorization).toBe(`Bearer ${TOKEN}`);
    expect(cfg.headers["Content-Type"]).toBe("application/octet-stream");
    expect(cfg.headers["X-Firmware-Name"]).toBe("Boiler fix");
    expect(cfg.headers["X-File-Name"]).toBe("app.bin");
    expect(cfg.maxBodyLength).toBe(Infinity);
  });

  it("extracts the basename when file_name is a full path", async () => {
    const { ota } = await initializedOta();
    const httpFn = mockHttp({ status: true, data: {} });

    await ota.firmwareUpload({
      name: "fw",
      version: "3.0.0",
      file: FILE,
      file_name: "/Users/arjun/Downloads/builds/firmware-v3.bin",
    });

    const [cfg] = httpFn.mock.calls[0];
    expect(cfg.headers["X-File-Name"]).toBe("firmware-v3.bin");
  });

  it("uses file.name as X-File-Name when file_name is omitted", async () => {
    const { ota } = await initializedOta();
    const httpFn = mockHttp({ status: true, data: {} });

    const file = new Uint8Array([1, 2, 3]);
    file.name = "from-file.bin"; // File objects carry .name

    await ota.firmwareUpload({ name: "fw", version: "2.0.0", file });

    const [cfg] = httpFn.mock.calls[0];
    expect(cfg.headers["X-File-Name"]).toBe("from-file.bin");
  });

  it("throws with the envelope message on failure", async () => {
    const { ota } = await initializedOta();
    mockHttp(
      {
        status: false,
        data: { message: "Firmware version already exists", code: "VERSION_EXISTS" },
      },
      { httpStatus: 409 },
    );

    const err = await ota
      .firmwareUpload({ name: "fw", version: "1.0.0", file: FILE })
      .catch((e) => e);

    expect(err).toBeInstanceOf(Error);
    expect(err.message).toMatch(/Firmware version already exists/);
    expect(err.code).toBe("VERSION_EXISTS");
  });

  it("throws on network failure", async () => {
    const { ota } = await initializedOta();
    axios.request.mockRejectedValueOnce(new Error("ECONNREFUSED"));

    await expect(
      ota.firmwareUpload({ name: "fw", version: "1.0.0", file: FILE }),
    ).rejects.toThrow(/firmware upload failed: ECONNREFUSED/);
  });
});

// ─── firmwareDelete ──────────────────────────────────────────

describe("ota.firmwareDelete", () => {
  it("throws if init() has not been called", async () => {
    const ota = new OTAManager(makeCtx());
    await expect(ota.firmwareDelete({ id: "fw_1" })).rejects.toThrow(
      /Call app.ota.init\(\) first/,
    );
  });

  it("validates params", async () => {
    const { ota } = await initializedOta();

    await expect(ota.firmwareDelete()).rejects.toThrow(/params must be an object/);
    await expect(ota.firmwareDelete({})).rejects.toThrow(
      /id must be a non-empty string/,
    );
    await expect(ota.firmwareDelete({ id: "  " })).rejects.toThrow(
      /id must be a non-empty string/,
    );
  });

  it("DELETEs the firmware and returns res.data", async () => {
    const { ota } = await initializedOta();
    const data = { firmware_id: "fw_1", deleted: true };
    const httpFn = mockHttp({ status: true, data });

    const out = await ota.firmwareDelete({ id: "fw_1" });

    expect(out).toEqual(data);

    const [cfg] = httpFn.mock.calls[0];
    expect(cfg.url).toBe(`${HTTP_URL}/iot/ota/firmware/fw_1`);
    expect(cfg.method).toBe("DELETE");
    expect(cfg.headers.Authorization).toBe(`Bearer ${TOKEN}`);
  });

  it("throws with the envelope message on failure", async () => {
    const { ota } = await initializedOta();
    mockHttp(
      {
        status: false,
        data: {
          message: "Firmware is used by an active rollout",
          code: "FIRMWARE_IN_ACTIVE_ROLLOUT",
        },
      },
      { httpStatus: 409 },
    );

    const err = await ota.firmwareDelete({ id: "fw_1" }).catch((e) => e);
    expect(err.message).toMatch(/active rollout/);
    expect(err.code).toBe("FIRMWARE_IN_ACTIVE_ROLLOUT");
  });
});

// ─── firmwareList ────────────────────────────────────────────

describe("ota.firmwareList", () => {
  it("requests the paginated list over NATS and returns data", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.firmwareList({ page: 1 });

    expect(out.firmwares).toHaveLength(1);
    expect(out.firmwares[0].firmware_id).toBe("fw_1");
    expect(out.page.has_more).toBe(false);

    const call = ctx.natsClient.request.mock.calls.find(
      (c) => c[0] === "api.iot.ota.test_org_123.firmware.list",
    );
    const payload = JSON.parse(new TextDecoder().decode(call[1]));
    expect(payload).toEqual({ limit: 50, offset: 0 });
  });

  it("maps page + limit to offset", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    await ota.firmwareList({ page: 3, limit: 20 });

    const call = ctx.natsClient.request.mock.calls.find(
      (c) => c[0] === "api.iot.ota.test_org_123.firmware.list",
    );
    const payload = JSON.parse(new TextDecoder().decode(call[1]));
    expect(payload).toEqual({ limit: 20, offset: 40 });
  });

  it("defaults to page 1 when called without params", async () => {
    const ota = new OTAManager(makeCtx());
    const out = await ota.firmwareList();
    expect(out.page.offset).toBe(0);
  });

  it("does not require init()", async () => {
    const ota = new OTAManager(makeCtx());
    await expect(ota.firmwareList({ page: 1 })).resolves.toBeTruthy();
  });

  it("validates page and limit", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(ota.firmwareList({ page: 0 })).rejects.toThrow(
      /page must be a positive integer/,
    );
    await expect(ota.firmwareList({ page: 1.5 })).rejects.toThrow(
      /page must be a positive integer/,
    );
    await expect(ota.firmwareList({ page: 1, limit: 0 })).rejects.toThrow(
      /limit must be an integer between 1 and 200/,
    );
    await expect(ota.firmwareList({ page: 1, limit: 500 })).rejects.toThrow(
      /limit must be an integer between 1 and 200/,
    );
  });

  it("throws when not connected", async () => {
    const ctx = makeCtx();
    ctx.connected = false;
    const ota = new OTAManager(ctx);

    await expect(ota.firmwareList({ page: 1 })).rejects.toThrow(/Not connected/);
  });

  it("throws on failure status", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.firmware.list": {
        status: "FIRMWARE_LIST_FAILURE",
        data: ["bad request"],
      },
    });
    const ota = new OTAManager(ctx);

    await expect(ota.firmwareList({ page: 1 })).rejects.toThrow(
      /firmware list failed: bad request/,
    );
  });
});

// ─── rollouts ────────────────────────────────────────────────

describe("ota.createRollout", () => {
  const BASE = {
    firmware_id: "fw_1",
    request_type: "DOWNLOAD_INSTALL",
    target: { type: "all" },
    created_by: "665f1a2b3c4d5e6f7a8b9c0d",
  };

  it("creates a DRAFT and returns the preview count", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.createRollout({
      ...BASE,
      target: {
        type: "devices",
        device_ids: ["665f000000000000000000a1", "665f000000000000000000a2"],
      },
      force_install: true,
      user_config: { apply: "app_gated" },
    });

    expect(out).toEqual({ rollout_id: "ro_1", status: "DRAFT", device_count: 3 });

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.create");
    expect(payload).toEqual({
      firmware_id: "fw_1",
      created_by: "665f1a2b3c4d5e6f7a8b9c0d",
      env: "production",
      request_type: "DOWNLOAD_INSTALL",
      target: {
        type: "devices",
        device_ids: ["665f000000000000000000a1", "665f000000000000000000a2"],
      },
      force_install: true,
      user_config: { apply: "app_gated" },
    });
  });

  it("requires an explicit target — no implicit fleet-wide", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(
      ota.createRollout({ ...BASE, target: undefined }),
    ).rejects.toThrow(/target is required/);
  });

  it("always sends env and an explicit target", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    await ota.createRollout(BASE);

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.create");
    expect(payload.target).toEqual({ type: "all" });
    expect(payload.env).toBe("production");
  });

  it("validates inputs", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(ota.createRollout()).rejects.toThrow(/params must be an object/);
    await expect(
      ota.createRollout({ ...BASE, firmware_id: undefined }),
    ).rejects.toThrow(/firmware_id must be a non-empty string/);
    await expect(
      ota.createRollout({ ...BASE, request_type: undefined }),
    ).rejects.toThrow(/request_type is required/);
    await expect(
      ota.createRollout({ ...BASE, request_type: "INSTALL" }),
    ).rejects.toThrow(/request_type must be one of/);
    await expect(
      ota.createRollout({ ...BASE, target: { type: "nope" } }),
    ).rejects.toThrow(/target.type must be one of/);
    await expect(
      ota.createRollout({ ...BASE, target: { type: "devices", device_ids: [] } }),
    ).rejects.toThrow(/device_ids must be a non-empty string array/);
    await expect(
      ota.createRollout({ ...BASE, target: { type: "logical_group" } }),
    ).rejects.toThrow(/target.group_id must be a non-empty string/);
    await expect(
      ota.createRollout({ ...BASE, target: { type: "all", exclude: [42] } }),
    ).rejects.toThrow(/target.exclude must be a string array/);
    await expect(
      ota.createRollout({ ...BASE, force_download: "yes" }),
    ).rejects.toThrow(/force_download must be a boolean/);
    await expect(
      ota.createRollout({ ...BASE, user_config: [] }),
    ).rejects.toThrow(/user_config must be an object/);
  });

  it("sends created_by: null when absent and underivable", async () => {
    // ctx apiKey is not a real JWT, so derivation fails → null (optional field)
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    await ota.createRollout({
      firmware_id: "fw_1",
      request_type: "DOWNLOAD_ONLY",
      target: { type: "all" },
    });

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.create");
    expect(payload.created_by).toBeNull();
  });

  it("throws with code on failure status", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.rollout.create": {
        status: "ROLLOUT_CREATE_FAILURE",
        data: { code: "FIRMWARE_NOT_READY" },
      },
    });
    const ota = new OTAManager(ctx);

    const err = await ota.createRollout(BASE).catch((e) => e);
    expect(err.message).toMatch(/rollout create failed: FIRMWARE_NOT_READY/);
    expect(err.code).toBe("FIRMWARE_NOT_READY");
  });
});

describe("ota.updateRollout", () => {
  it("updates draft fields and returns the fresh preview", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.updateRollout({
      rollout_id: "ro_1",
      request_type: "DOWNLOAD_ONLY",
      target: { type: "all", exclude: ["665f000000000000000000a3"] },
    });

    expect(out).toEqual({ rollout_id: "ro_1", device_count: 2 });

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.update");
    expect(payload).toEqual({
      rollout_id: "ro_1",
      request_type: "DOWNLOAD_ONLY",
      target: { type: "all", exclude: ["665f000000000000000000a3"] },
    });
  });

  it("requires rollout_id and at least one field", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(ota.updateRollout({})).rejects.toThrow(
      /rollout_id must be a non-empty string/,
    );
    await expect(ota.updateRollout({ rollout_id: "ro_1" })).rejects.toThrow(
      /at least one of/,
    );
  });

  it("throws ROLLOUT_NOT_DRAFT when the rollout left DRAFT", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.rollout.update": {
        status: "ROLLOUT_UPDATE_FAILURE",
        data: { code: "ROLLOUT_NOT_DRAFT" },
      },
    });
    const ota = new OTAManager(ctx);

    const err = await ota
      .updateRollout({ rollout_id: "ro_1", force_install: true })
      .catch((e) => e);
    expect(err.code).toBe("ROLLOUT_NOT_DRAFT");
  });
});

describe("ota.deleteRollout", () => {
  it("deletes a draft and returns data", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.deleteRollout({ rollout_id: "ro_1" });
    expect(out).toEqual({ rollout_id: "ro_1" });

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.delete");
    expect(payload).toEqual({ rollout_id: "ro_1" });
  });

  it("validates rollout_id", async () => {
    const ota = new OTAManager(makeCtx());
    await expect(ota.deleteRollout({})).rejects.toThrow(
      /rollout_id must be a non-empty string/,
    );
  });

  it("throws ROLLOUT_NOT_DRAFT for activated rollouts", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.rollout.delete": {
        status: "ROLLOUT_DELETE_FAILURE",
        data: { code: "ROLLOUT_NOT_DRAFT" },
      },
    });
    const ota = new OTAManager(ctx);

    const err = await ota.deleteRollout({ rollout_id: "ro_1" }).catch((e) => e);
    expect(err.code).toBe("ROLLOUT_NOT_DRAFT");
  });
});

describe("ota.toggleRollout", () => {
  it("activates a draft (snapshot + blast) and returns data", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.toggleRollout({ rollout_id: "ro_1", state: "ACTIVE" });
    expect(out).toEqual({ rollout_id: "ro_1", state: "ACTIVE", device_count: 3 });

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.state");
    expect(payload).toEqual({ rollout_id: "ro_1", state: "ACTIVE" });
  });

  it("pauses and stops via the same endpoint", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.rollout.state": {
        status: "ROLLOUT_STATE_SUCCESS",
        data: { rollout_id: "ro_1", state: "PAUSED" },
      },
    });
    const ota = new OTAManager(ctx);

    const out = await ota.toggleRollout({ rollout_id: "ro_1", state: "PAUSED" });
    expect(out.state).toBe("PAUSED");
  });

  it("validates the state value", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(
      ota.toggleRollout({ rollout_id: "ro_1", state: "DRAFT" }),
    ).rejects.toThrow(/state must be one of/);
    await expect(
      ota.toggleRollout({ rollout_id: "ro_1" }),
    ).rejects.toThrow(/state must be one of/);
  });

  it("throws INVALID_TRANSITION on illegal moves", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.rollout.state": {
        status: "ROLLOUT_STATE_FAILURE",
        data: { code: "INVALID_TRANSITION", from: "STOPPED" },
      },
    });
    const ota = new OTAManager(ctx);

    const err = await ota
      .toggleRollout({ rollout_id: "ro_1", state: "ACTIVE" })
      .catch((e) => e);
    expect(err.code).toBe("INVALID_TRANSITION");
  });

  it("throws when not connected", async () => {
    const ctx = makeCtx();
    ctx.connected = false;
    const ota = new OTAManager(ctx);

    await expect(
      ota.toggleRollout({ rollout_id: "ro_1", state: "ACTIVE" }),
    ).rejects.toThrow(/Not connected/);
  });
});

describe("ota.retryRollout", () => {
  it("re-arms terminal jobs and returns the count", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.retryRollout({
      rollout_id: "ro_1",
      phases: ["FAILED", "VETOED"],
      device_ids: ["665f000000000000000000a1"],
    });

    expect(out).toEqual({ rollout_id: "ro_1", retried: 2 });

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.retry");
    expect(payload).toEqual({
      rollout_id: "ro_1",
      phases: ["FAILED", "VETOED"],
      device_ids: ["665f000000000000000000a1"],
    });
  });

  it("defaults to all retryable phases when omitted", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    await ota.retryRollout({ rollout_id: "ro_1" });

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.retry");
    expect(payload).toEqual({ rollout_id: "ro_1" });
  });

  it("validates inputs", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(ota.retryRollout({})).rejects.toThrow(
      /rollout_id must be a non-empty string/,
    );
    await expect(
      ota.retryRollout({ rollout_id: "ro_1", phases: ["INSTALLED"] }),
    ).rejects.toThrow(/phases must be a subset of/);
    await expect(
      ota.retryRollout({ rollout_id: "ro_1", phases: [] }),
    ).rejects.toThrow(/phases must be a subset of/);
    await expect(
      ota.retryRollout({ rollout_id: "ro_1", device_ids: [""] }),
    ).rejects.toThrow(/device_ids must be a non-empty string array/);
  });

  it("throws ROLLOUT_NOT_LIVE for drafts and stopped rollouts", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.rollout.retry": {
        status: "ROLLOUT_RETRY_FAILURE",
        data: { code: "ROLLOUT_NOT_LIVE", from: "STOPPED" },
      },
    });
    const ota = new OTAManager(ctx);

    const err = await ota.retryRollout({ rollout_id: "ro_1" }).catch((e) => e);
    expect(err.code).toBe("ROLLOUT_NOT_LIVE");
  });
});

describe("ota.rolloutList", () => {
  it("lists rollouts with pagination and optional status filter", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.rolloutList({ page: 2, limit: 25, status: "ACTIVE" });
    expect(out.rollouts).toHaveLength(2);

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.list");
    expect(payload).toEqual({ limit: 25, offset: 25, status: "ACTIVE" });
  });

  it("defaults to page 1 / limit 50 / no filter", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    await ota.rolloutList();

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.rollout.list");
    expect(payload).toEqual({ limit: 50, offset: 0 });
  });

  it("validates status and pagination", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(ota.rolloutList({ status: "LIVE" })).rejects.toThrow(
      /status must be one of/,
    );
    await expect(ota.rolloutList({ page: 0 })).rejects.toThrow(
      /page must be a positive integer/,
    );
    await expect(ota.rolloutList({ limit: 999 })).rejects.toThrow(
      /limit must be an integer between 1 and 200/,
    );
  });
});

describe("ota.jobsList", () => {
  it("lists jobs + stats for a rollout, with phase filter", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.jobsList({ rollout_id: "ro_1", phase: "FAILED" });
    expect(out.stats).toEqual({ INSTALLED: 8, FAILED: 2 });
    expect(out.jobs[0].job_id).toBe("j_1");
    expect(out.jobs[0].history).toBeUndefined(); // never included

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.jobs.list");
    expect(payload).toEqual({
      rollout_id: "ro_1",
      phase: "FAILED",
      limit: 50,
      offset: 0,
    });
  });

  it("validates inputs", async () => {
    const ota = new OTAManager(makeCtx());

    await expect(ota.jobsList({})).rejects.toThrow(
      /rollout_id must be a non-empty string/,
    );
    await expect(
      ota.jobsList({ rollout_id: "ro_1", phase: "EXPLODED" }),
    ).rejects.toThrow(/phase must be one of/);
  });

  it("throws with code on failure", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.jobs.list": {
        status: "JOBS_LIST_FAILURE",
        data: { code: "ROLLOUT_NOT_FOUND" },
      },
    });
    const ota = new OTAManager(ctx);

    const err = await ota.jobsList({ rollout_id: "nope" }).catch((e) => e);
    expect(err.code).toBe("ROLLOUT_NOT_FOUND");
  });
});

describe("ota.jobHistory", () => {
  it("returns the paginated chronological time log", async () => {
    const ctx = makeCtx();
    const ota = new OTAManager(ctx);

    const out = await ota.jobHistory({ job_id: "j_1" });
    expect(out.history).toHaveLength(3);
    expect(out.history[0].note).toBe("activated");
    expect(out.page.total).toBe(3);

    const payload = lastPayload(ctx, "api.iot.ota.test_org_123.jobs.history");
    expect(payload).toEqual({ job_id: "j_1", limit: 50, offset: 0 });
  });

  it("validates job_id", async () => {
    const ota = new OTAManager(makeCtx());
    await expect(ota.jobHistory({})).rejects.toThrow(
      /job_id must be a non-empty string/,
    );
  });

  it("throws JOB_NOT_FOUND on failure", async () => {
    const ctx = makeCtx({
      "api.iot.ota.test_org_123.jobs.history": {
        status: "JOBS_HISTORY_FAILURE",
        data: { code: "JOB_NOT_FOUND" },
      },
    });
    const ota = new OTAManager(ctx);

    const err = await ota.jobHistory({ job_id: "nope" }).catch((e) => e);
    expect(err.code).toBe("JOB_NOT_FOUND");
  });
});
