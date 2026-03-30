import { describe, it, expect, vi } from "vitest";
import { encode as msgpackEncode } from "@msgpack/msgpack";
import { createMockContext, createMockConsumer } from "./setup.js";
import { DeviceManager } from "../src/device.js";
import { TelemetryManager } from "../src/telemetry.js";

function makeCtx(overrides = {}) {
  const consumer = createMockConsumer();
  const ctx = createMockContext({
    consumer,
    responses: {
      "api.iot.devices.test_org_123.get": {
        status: "DEVICE_GET_SUCCESS",
        data: {
          id: "dev_1",
          ident: "sensor_01",
          schema: { temperature: "float", humidity: "float" },
        },
      },
    },
    ...overrides,
  });
  ctx.logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
  const device = new DeviceManager(ctx);
  ctx.device = device;
  device.cache.set("sensor_01", {
    id: "dev_1",
    ident: "sensor_01",
    schema: { temperature: "float", humidity: "float" },
  });
  device.cache.set("sensor_02", {
    id: "dev_2",
    ident: "sensor_02",
    schema: { pressure: "float" },
  });
  return { ctx, consumer };
}

describe("TelemetryManager", () => {
  describe("stream", () => {
    it('creates wildcard JetStream consumer with metric: "*"', async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: "*",
        callback: () => {},
      });

      expect(ctx.jetstream.consumers.get).toHaveBeenCalledWith(
        "test_org_123_stream",
        expect.objectContaining({
          filter_subjects: "test_org_123.production.telemetry.dev_1.*",
        }),
      );
    });

    it("creates wildcard JetStream consumer with metric array", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: () => {},
      });

      expect(ctx.jetstream.consumers.get).toHaveBeenCalledWith(
        "test_org_123_stream",
        expect.objectContaining({
          filter_subjects: "test_org_123.production.telemetry.dev_1.*",
        }),
      );
    });

    it("throws when metric is a non-* string", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "sensor_01",
          metric: "temperature",
          callback: () => {},
        }),
      ).rejects.toThrow('metric as a string must be "*"');
    });

    it("throws when metric is not string or array", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "sensor_01",
          metric: 123,
          callback: () => {},
        }),
      ).rejects.toThrow('metric must be "*" or a non-empty array');
    });

    it("throws when metric is an empty array", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "sensor_01",
          metric: [],
          callback: () => {},
        }),
      ).rejects.toThrow("non-empty array");
    });

    it("validates each metric in array against device schema", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "sensor_01",
          metric: ["temperature", "nonexistent"],
          callback: () => {},
        }),
      ).rejects.toThrow(
        'metric "nonexistent" is not a valid key in device schema',
      );
    });

    it("allows multiple independent subscriptions for same device", async () => {
      const consumer1 = createMockConsumer();
      const consumer2 = createMockConsumer();
      let callCount = 0;
      const ctx = createMockContext({
        responses: {
          "api.iot.devices.test_org_123.get": {
            status: "DEVICE_GET_SUCCESS",
            data: {
              id: "dev_1",
              ident: "sensor_01",
              schema: { temperature: "float", humidity: "float" },
            },
          },
        },
      });
      const consumers = [consumer1, consumer2];
      ctx.jetstream.consumers.get = vi.fn(
        async () => consumers[callCount++] || createMockConsumer(),
      );
      const device = new DeviceManager(ctx);
      ctx.device = device;
      device.cache.set("sensor_01", {
        id: "dev_1",
        ident: "sensor_01",
        schema: { temperature: "float", humidity: "float" },
      });

      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: () => {},
      });
      await tm.stream({
        device_ident: "sensor_01",
        metric: ["humidity"],
        callback: () => {},
      });

      expect(ctx.jetstream.consumers.get).toHaveBeenCalledTimes(2);
    });

    it("returns void (undefined)", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      const result = await tm.stream({
        device_ident: "sensor_01",
        metric: "*",
        callback: () => {},
      });

      expect(result).toBeUndefined();
    });
  });

  describe("stream client-side filtering", () => {
    it("invokes callback for matching metrics in filter set", async () => {
      const { ctx, consumer } = makeCtx();
      const tm = new TelemetryManager(ctx);
      const cb = vi.fn();

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: cb,
      });

      const consumeCallback = consumer.consume.mock.calls[0][0].callback;

      // Simulate a temperature message
      const { encode } = await import("@msgpack/msgpack");
      await consumeCallback({
        subject: "test_org_123.production.telemetry.dev_1.temperature",
        data: encode({ value: 25.3, timestamp: 1000 }),
        working: vi.fn(),
        ack: vi.fn(),
      });

      expect(cb).toHaveBeenCalledWith({
        metric: "temperature",
        data: { value: 25.3, timestamp: 1000 },
      });
    });

    it("skips callback for non-matching metrics in filter set", async () => {
      const { ctx, consumer } = makeCtx();
      const tm = new TelemetryManager(ctx);
      const cb = vi.fn();

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: cb,
      });

      const consumeCallback = consumer.consume.mock.calls[0][0].callback;

      const { encode } = await import("@msgpack/msgpack");
      await consumeCallback({
        subject: "test_org_123.production.telemetry.dev_1.humidity",
        data: encode({ value: 60, timestamp: 1000 }),
        working: vi.fn(),
        ack: vi.fn(),
      });

      expect(cb).not.toHaveBeenCalled();
    });

    it('invokes callback for all metrics when metric is "*"', async () => {
      const { ctx, consumer } = makeCtx();
      const tm = new TelemetryManager(ctx);
      const cb = vi.fn();

      await tm.stream({
        device_ident: "sensor_01",
        metric: "*",
        callback: cb,
      });

      const consumeCallback = consumer.consume.mock.calls[0][0].callback;

      const { encode } = await import("@msgpack/msgpack");
      await consumeCallback({
        subject: "test_org_123.production.telemetry.dev_1.humidity",
        data: encode({ value: 60, timestamp: 1000 }),
        working: vi.fn(),
        ack: vi.fn(),
      });

      expect(cb).toHaveBeenCalledWith({
        metric: "humidity",
        data: { value: 60, timestamp: 1000 },
      });
    });
  });

  describe("off", () => {
    it("without metric kills all consumers for device", async () => {
      const consumer1 = createMockConsumer();
      const consumer2 = createMockConsumer();
      let callCount = 0;
      const ctx = createMockContext({
        responses: {
          "api.iot.devices.test_org_123.get": {
            status: "DEVICE_GET_SUCCESS",
            data: {
              id: "dev_1",
              ident: "sensor_01",
              schema: { temperature: "float", humidity: "float" },
            },
          },
        },
      });
      const consumers = [consumer1, consumer2];
      ctx.jetstream.consumers.get = vi.fn(
        async () => consumers[callCount++] || createMockConsumer(),
      );
      const device = new DeviceManager(ctx);
      ctx.device = device;
      device.cache.set("sensor_01", {
        id: "dev_1",
        ident: "sensor_01",
        schema: { temperature: "float", humidity: "float" },
      });

      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: () => {},
      });
      await tm.stream({
        device_ident: "sensor_01",
        metric: ["humidity"],
        callback: () => {},
      });

      await tm.off({ device_ident: "sensor_01" });

      expect(consumer1.delete).toHaveBeenCalled();
      expect(consumer2.delete).toHaveBeenCalled();
    });

    it("with metric array removes metrics from filtered subscriptions", async () => {
      const consumer1 = createMockConsumer();
      let callCount = 0;
      const ctx = createMockContext({
        responses: {
          "api.iot.devices.test_org_123.get": {
            status: "DEVICE_GET_SUCCESS",
            data: {
              id: "dev_1",
              ident: "sensor_01",
              schema: { temperature: "float", humidity: "float" },
            },
          },
        },
      });
      ctx.jetstream.consumers.get = vi.fn(async () => consumer1);
      const device = new DeviceManager(ctx);
      ctx.device = device;
      device.cache.set("sensor_01", {
        id: "dev_1",
        ident: "sensor_01",
        schema: { temperature: "float", humidity: "float" },
      });

      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature", "humidity"],
        callback: () => {},
      });

      // Remove only temperature — subscription still has humidity
      await tm.off({ device_ident: "sensor_01", metric: ["temperature"] });

      expect(consumer1.delete).not.toHaveBeenCalled();
    });

    it("deletes consumer when all metrics removed from subscription", async () => {
      const consumer1 = createMockConsumer();
      const ctx = createMockContext({
        responses: {
          "api.iot.devices.test_org_123.get": {
            status: "DEVICE_GET_SUCCESS",
            data: {
              id: "dev_1",
              ident: "sensor_01",
              schema: { temperature: "float", humidity: "float" },
            },
          },
        },
      });
      ctx.jetstream.consumers.get = vi.fn(async () => consumer1);
      const device = new DeviceManager(ctx);
      ctx.device = device;
      device.cache.set("sensor_01", {
        id: "dev_1",
        ident: "sensor_01",
        schema: { temperature: "float", humidity: "float" },
      });

      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: () => {},
      });

      await tm.off({ device_ident: "sensor_01", metric: ["temperature"] });

      expect(consumer1.delete).toHaveBeenCalled();
    });

    it("leaves wildcard subscriptions untouched when off called with specific metrics", async () => {
      const wildcardConsumer = createMockConsumer();
      const filteredConsumer = createMockConsumer();
      let callCount = 0;
      const ctx = createMockContext({
        responses: {
          "api.iot.devices.test_org_123.get": {
            status: "DEVICE_GET_SUCCESS",
            data: {
              id: "dev_1",
              ident: "sensor_01",
              schema: { temperature: "float", humidity: "float" },
            },
          },
        },
      });
      const consumers = [wildcardConsumer, filteredConsumer];
      ctx.jetstream.consumers.get = vi.fn(
        async () => consumers[callCount++] || createMockConsumer(),
      );
      const device = new DeviceManager(ctx);
      ctx.device = device;
      device.cache.set("sensor_01", {
        id: "dev_1",
        ident: "sensor_01",
        schema: { temperature: "float", humidity: "float" },
      });

      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: "*",
        callback: () => {},
      });
      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: () => {},
      });

      await tm.off({ device_ident: "sensor_01", metric: ["temperature"] });

      expect(wildcardConsumer.delete).not.toHaveBeenCalled();
      expect(filteredConsumer.delete).toHaveBeenCalled();
    });

    it("removes metric across multiple subscriptions", async () => {
      const consumer1 = createMockConsumer();
      const consumer2 = createMockConsumer();
      let callCount = 0;
      const ctx = createMockContext({
        responses: {
          "api.iot.devices.test_org_123.get": {
            status: "DEVICE_GET_SUCCESS",
            data: {
              id: "dev_1",
              ident: "sensor_01",
              schema: { temperature: "float", humidity: "float" },
            },
          },
        },
      });
      const consumers = [consumer1, consumer2];
      ctx.jetstream.consumers.get = vi.fn(
        async () => consumers[callCount++] || createMockConsumer(),
      );
      const device = new DeviceManager(ctx);
      ctx.device = device;
      device.cache.set("sensor_01", {
        id: "dev_1",
        ident: "sensor_01",
        schema: { temperature: "float", humidity: "float" },
      });

      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature", "humidity"],
        callback: () => {},
      });
      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature"],
        callback: () => {},
      });

      await tm.off({ device_ident: "sensor_01", metric: ["temperature"] });

      // consumer1 still has humidity, consumer2 is empty → deleted
      expect(consumer1.delete).not.toHaveBeenCalled();
      expect(consumer2.delete).toHaveBeenCalled();
    });

    it("no-op when device has no subscriptions", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      // Should not throw
      await tm.off({ device_ident: "sensor_01" });
      await tm.off({ device_ident: "sensor_01", metric: ["temperature"] });
    });
  });

  describe("history", () => {
    it("sends correct request with resolved device_id", async () => {
      const { ctx } = makeCtx();
      // Override request to return msgpack-encoded paginated response
      ctx.natsClient.request = vi.fn(async (subject) => {
        if (subject === "api.iot.db.test_org_123.telemetry.history") {
          return {
            data: msgpackEncode({
              status: "TELEMETRY_FETCH_SUCCESS",
              data: {
                has_more: false,
                cursor: null,
                data: {
                  temperature: [{ value: 23.5, time: "2025-01-01T12:00:00Z" }],
                },
              },
            }),
          };
        }
      });
      const tm = new TelemetryManager(ctx);

      await tm.history({
        device_ident: "sensor_01",
        fields: ["temperature"],
        start: "2025-01-01T00:00:00.000Z",
        end: "2025-01-02T00:00:00.000Z",
      });

      expect(ctx.natsClient.request).toHaveBeenCalledWith(
        "api.iot.db.test_org_123.telemetry.history",
        expect.anything(),
        { timeout: 20000 },
      );
    });

    it("throws when fields contain invalid schema keys", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.history({
          device_ident: "sensor_01",
          fields: ["temperature", "nonexistent"],
          start: "2025-01-01T00:00:00.000Z",
          end: "2025-01-02T00:00:00.000Z",
        }),
      ).rejects.toThrow("fields contain invalid metrics: nonexistent");
    });

    it("validates start < end", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.history({
          device_ident: "sensor_01",
          fields: ["temperature"],
          start: "2025-01-02T00:00:00.000Z",
          end: "2025-01-01T00:00:00.000Z",
        }),
      ).rejects.toThrow("start must be before end");
    });
  });

  describe("validation", () => {
    it("throws on invalid ident", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "bad ident!",
          metric: "*",
          callback: () => {},
        }),
      ).rejects.toThrow("invalid characters");
    });

    it("throws on missing callback", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({ device_ident: "sensor_01", metric: "*" }),
      ).rejects.toThrow("callback is required");
    });

    it("throws when not connected", async () => {
      const { ctx } = makeCtx({ connected: false });
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "sensor_01",
          metric: "*",
          callback: () => {},
        }),
      ).rejects.toThrow("Not connected");
    });
  });

  describe("metric validation", () => {
    it("allows * as metric", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: "*",
        callback: () => {},
      });

      expect(ctx.jetstream.consumers.get).toHaveBeenCalled();
    });

    it("allows metric array with valid schema keys", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await tm.stream({
        device_ident: "sensor_01",
        metric: ["temperature", "humidity"],
        callback: () => {},
      });

      expect(ctx.jetstream.consumers.get).toHaveBeenCalled();
    });

    it("throws when metric array contains invalid schema key", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "sensor_01",
          metric: ["nonexistent"],
          callback: () => {},
        }),
      ).rejects.toThrow(
        'metric "nonexistent" is not a valid key in device schema',
      );
    });

    it("includes valid keys in error message", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.stream({
          device_ident: "sensor_01",
          metric: ["pressure"],
          callback: () => {},
        }),
      ).rejects.toThrow("temperature, humidity");
    });
  });

  describe("latest", () => {
    it("sends history request with last_value true", async () => {
      const { ctx } = makeCtx();
      ctx.natsClient.request = vi.fn(async (subject) => {
        if (subject === "api.iot.db.test_org_123.telemetry.history") {
          return {
            data: msgpackEncode({
              status: "TELEMETRY_FETCH_SUCCESS",
              data: {
                data: {
                  temperature: [{ value: 23.5, time: "2026-03-24T00:00:00Z" }],
                },
              },
            }),
          };
        }
      });
      const tm = new TelemetryManager(ctx);

      const result = await tm.latest({
        device_ident: "sensor_01",
        fields: ["temperature"],
        start: "2026-03-23T00:00:00.000Z",
        end: "2026-03-24T00:00:00.000Z",
      });

      expect(ctx.natsClient.request).toHaveBeenCalledWith(
        "api.iot.db.test_org_123.telemetry.history",
        expect.anything(),
        { timeout: 20000 },
      );
      expect(result).toEqual({
        temperature: { value: 23.5, time: "2026-03-24T00:00:00Z" },
      });
    });

    it("throws on missing fields", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.latest({
          device_ident: "sensor_01",
          start: "2026-03-23T00:00:00.000Z",
          end: "2026-03-24T00:00:00.000Z",
        }),
      ).rejects.toThrow("non-empty array");
    });

    it("throws on missing device_ident", async () => {
      const { ctx } = makeCtx();
      const tm = new TelemetryManager(ctx);

      await expect(
        tm.latest({
          fields: ["temperature"],
          start: "2026-03-23T00:00:00.000Z",
          end: "2026-03-24T00:00:00.000Z",
        }),
      ).rejects.toThrow("device_ident is required");
    });
  });
});
