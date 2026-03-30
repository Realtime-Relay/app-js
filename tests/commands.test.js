import { describe, it, expect, vi } from "vitest";
import { encode as msgpackEncode } from "@msgpack/msgpack";
import { createMockContext } from "./setup.js";
import { DeviceManager } from "../src/device.js";
import { CommandManager } from "../src/commands.js";

function makeCtx(overrides = {}) {
  const ctx = createMockContext({
    responses: {
      "api.iot.devices.test_org_123.get": {
        status: "DEVICE_GET_SUCCESS",
        data: { id: "dev_1", ident: "sensor_01" },
      },
      "api.iot.db.test_org_123.command.history": {
        status: "SUCCESS",
        data: [],
      },
      ...overrides.responses,
    },
    ...overrides,
  });
  const device = new DeviceManager(ctx);
  ctx.device = device;
  device.cache.set("sensor_01", { id: "dev_1", ident: "sensor_01" });
  device.cache.set("sensor_02", { id: "dev_2", ident: "sensor_02" });
  return ctx;
}

describe("CommandManager", () => {
  describe("send", () => {
    it("resolves idents to device_ids and publishes to each device", async () => {
      const ctx = makeCtx();
      const cm = new CommandManager(ctx);

      await cm.send({
        name: "reboot",
        device_ident: ["sensor_01", "sensor_02"],
        data: { force: true },
      });

      expect(ctx.jetstream.publish).toHaveBeenCalledTimes(2);
      expect(ctx.jetstream.publish).toHaveBeenCalledWith(
        "test_org_123.production.command.queue.dev_1.reboot",
        expect.anything(),
      );
      expect(ctx.jetstream.publish).toHaveBeenCalledWith(
        "test_org_123.production.command.queue.dev_2.reboot",
        expect.anything(),
      );
    });

    it("returns per-ident result with sent: true on success", async () => {
      const ctx = makeCtx();
      const cm = new CommandManager(ctx);

      const result = await cm.send({
        name: "reboot",
        device_ident: ["sensor_01"],
        data: { force: true },
      });

      expect(result).toEqual({ sensor_01: { sent: true } });
    });

    it("buffers and returns sent: false when publish throws (TIMEOUT)", async () => {
      const ctx = makeCtx();
      ctx.jetstream.publish = vi.fn(async () => {
        throw new Error("TIMEOUT");
      });
      const cm = new CommandManager(ctx);

      const result = await cm.send({
        name: "reboot",
        device_ident: ["sensor_01"],
        data: { force: true },
      });

      expect(result).toEqual({ sensor_01: { sent: false, buffered: true } });
      expect(ctx.offlineBuffer).toHaveLength(1);
    });

    it("skips unfound devices and marks them with error", async () => {
      const ctx = makeCtx({
        responses: {
          "api.iot.devices.test_org_123.get": {
            status: "DEVICE_NOT_FOUND",
            data: {},
          },
        },
      });
      // Only sensor_01 is in cache, sensor_99 is not
      ctx.device.cache.delete("sensor_02");
      const cm = new CommandManager(ctx);

      const result = await cm.send({
        name: "reboot",
        device_ident: ["sensor_01", "sensor_99"],
        data: { force: true },
      });

      expect(result.sensor_01).toEqual({ sent: true });
      expect(result.sensor_99).toEqual({
        sent: false,
        error: "Device not found",
      });
      // Only 1 publish call (for sensor_01)
      expect(ctx.jetstream.publish).toHaveBeenCalledTimes(1);
    });
  });

  describe("history", () => {
    it("sends single request with device_ids array", async () => {
      const ctx = makeCtx();
      ctx.natsClient.request = vi.fn(async (subject) => {
        if (subject === "api.iot.db.test_org_123.command.history") {
          return {
            data: msgpackEncode({
              status: "COMMAND_FETCH_SUCCESS",
              data: {
                has_more: false,
                cursor: null,
                data: { dev_1: [{ value: {}, timestamp: 1000 }], dev_2: [] },
              },
            }),
          };
        }
      });
      const cm = new CommandManager(ctx);

      const result = await cm.history({
        name: "reboot",
        device_idents: ["sensor_01", "sensor_02"],
        start: "2025-01-01T00:00:00.000Z",
        end: "2025-01-02T00:00:00.000Z",
      });

      const historyCalls = ctx.natsClient.request.mock.calls.filter(
        ([subj]) => subj === "api.iot.db.test_org_123.command.history",
      );
      expect(historyCalls).toHaveLength(1);
      expect(result).toBeDefined();
      expect(result.sensor_01).toHaveLength(1);
    });

    it("defaults end to now() if omitted", async () => {
      const ctx = makeCtx();
      ctx.natsClient.request = vi.fn(async (subject) => {
        if (subject === "api.iot.db.test_org_123.command.history") {
          return {
            data: msgpackEncode({
              status: "COMMAND_FETCH_SUCCESS",
              data: { has_more: false, cursor: null, data: { dev_1: [] } },
            }),
          };
        }
      });
      const cm = new CommandManager(ctx);

      await cm.history({
        name: "reboot",
        device_idents: ["sensor_01"],
        start: "2025-01-01T00:00:00.000Z",
      });

      expect(ctx.natsClient.request).toHaveBeenCalledWith(
        "api.iot.db.test_org_123.command.history",
        expect.anything(),
        { timeout: 20000 },
      );
    });
  });

  describe("validation", () => {
    it("throws on invalid command name", async () => {
      const ctx = makeCtx();
      const cm = new CommandManager(ctx);

      await expect(
        cm.send({ name: "bad name!", device_ident: ["sensor_01"], data: {} }),
      ).rejects.toThrow("invalid characters");
    });

    it("throws on empty device_ident array", async () => {
      const ctx = makeCtx();
      const cm = new CommandManager(ctx);

      await expect(
        cm.send({ name: "reboot", device_ident: [], data: {} }),
      ).rejects.toThrow("must be a non-empty array");
    });

    it("throws on missing data", async () => {
      const ctx = makeCtx();
      const cm = new CommandManager(ctx);

      await expect(
        cm.send({ name: "reboot", device_ident: ["sensor_01"] }),
      ).rejects.toThrow("data is required");
    });

    it("buffers commands when not connected", async () => {
      const ctx = makeCtx({ connected: false });
      const cm = new CommandManager(ctx);

      const result = await cm.send({
        name: "reboot",
        device_ident: ["sensor_01"],
        data: { force: true },
      });

      expect(result.sensor_01).toEqual({ sent: false, buffered: true });
      expect(ctx.offlineBuffer).toHaveLength(1);
      expect(ctx.offlineBuffer[0].subject).toBe(
        "test_org_123.production.command.queue.dev_1.reboot",
      );
    });
  });
});
