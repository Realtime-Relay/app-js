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
      ...responseOverrides,
    },
  });
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
