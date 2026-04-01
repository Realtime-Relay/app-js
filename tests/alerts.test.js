import { describe, it, expect, vi } from "vitest";
import { encode as msgpackEncode } from "@msgpack/msgpack";
import { createMockContext, createMockConsumer } from "./setup.js";
import { DeviceManager } from "../src/device.js";
import { AlertManager } from "../src/alerts.js";
import { JSONCodec } from "nats.ws";

const jc = JSONCodec();

function makeCtx(consumer, responseOverrides = {}) {
  const ctx = createMockContext({
    consumer,
    responses: {
      "api.iot.alerts.test_org_123.create": {
        status: "ALERT_CREATE_SUCCESS",
        data: {
          id: "rule_1",
          name: "high_temp",
          type: "THRESHOLD",
          metric: "temperature",
          config: {
            scope: { type: "DEVICE", value: "dev_1" },
            duration: 60,
            recovery_duration: 30,
            cooldown: 120,
          },
        },
      },
      "api.iot.alerts.test_org_123.update": {
        status: "ALERT_UPDATE_SUCCESS",
        data: { id: "rule_1", name: "high_temp" },
      },
      "api.iot.alerts.test_org_123.delete": {
        status: "ALERT_DELETE_SUCCESS",
        data: { id: "rule_1" },
      },
      "api.iot.alerts.test_org_123.list": {
        status: "ALERT_LIST_SUCCESS",
        data: [{ id: "rule_1" }],
      },
      "api.iot.alerts.test_org_123.get": {
        status: "ALERT_GET_SUCCESS",
        data: {
          id: "rule_1",
          name: "high_temp",
          type: "THRESHOLD",
          metric: "temperature",
          config: { scope: { type: "DEVICE", value: "dev_1" } },
        },
      },
      "api.iot.alerts.test_org_123.ack": { status: "ALERT_ACK_SUCCESS" },
      "api.iot.alerts.test_org_123.ack_all": { status: "ALERT_ACK_SUCCESS" },
      "api.iot.alerts.test_org_123.mute": {
        status: "ALERT_RULE_MUTE_SUCCESS",
        data: {},
      },
      "api.iot.db.test_org_123.alerts.history": {
        status: "ALERT_FETCH_SUCCESS",
        data: {
          fire: [{ timestamp: "2026-03-25T00:00:00.000Z", rule_id: "rule_1" }],
          resolved: [],
        },
      },
      ...responseOverrides,
    },
  });
  const device = new DeviceManager(ctx);
  ctx.device = device;
  device.cache.set("sensor_01", { id: "dev_1", ident: "sensor_01" });
  return ctx;
}

describe("AlertManager", () => {
  describe("create", () => {
    it("creates THRESHOLD alert and returns wrapped object", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const alert = await am.create({
        name: "high_temp",
        type: "THRESHOLD",
        metric: "temperature",
        config: {
          scope: { type: "DEVICE", value: "dev_1" },
          operator: ">",
          value: 85,
          duration: 60,
          recovery_duration: 30,
        },
      });

      expect(alert.name).toBe("high_temp");
      expect(typeof alert.listen).toBe("function");
      expect(typeof alert.setEvaluator).toBe("function");
    });

    it("rejects invalid type", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(
        am.create({ name: "x", type: "INVALID", metric: "temp", config: {} }),
      ).rejects.toThrow("THRESHOLD or RATE_CHANGE");
    });

    it("rejects EPHEMERAL type (must use createEphemeral)", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(
        am.create({
          name: "x",
          type: "EPHEMERAL",
          metric: "temp",
          config: {
            scope: { type: "DEVICE", value: "dev_1" },
            duration: 10,
            recovery_duration: 5,
          },
        }),
      ).rejects.toThrow("createEphemeral");
    });
  });

  describe("createEphemeral", () => {
    it("creates ephemeral alert with valid recovery_eval_type", async () => {
      const ctx = makeCtx(undefined, {
        "api.iot.alerts.test_org_123.create_ephemeral": {
          status: "ALERT_CREATE_SUCCESS",
          data: { id: "eph_1", name: "evt_alert", type: "EPHEMERAL" },
        },
      });
      const am = new AlertManager(ctx);

      const alert = await am.createEphemeral({
        name: "evt_alert",
        config: {
          topic: {
            source: "EVENT",
            device_ident: "s-1",
            last_token: "door_opened",
          },
          duration: 1,
          recovery_duration: 5,
          recovery_eval_type: "TIMER",
        },
      });

      expect(alert.id).toBe("eph_1");
      expect(typeof alert.setEvaluator).toBe("function");
    });

    it("rejects invalid recovery_eval_type", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.createEphemeral({
          name: "x",
          config: {
            topic: {
              source: "TELEMETRY",
              device_ident: "s-1",
              last_token: "temp",
            },
            duration: 1,
            recovery_duration: 1,
            recovery_eval_type: "BOGUS",
          },
        }),
      ).rejects.toThrow("recovery_eval_type");
    });

    it("rejects invalid source", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.createEphemeral({
          name: "x",
          config: {
            topic: {
              source: "INVALID",
              device_ident: "s-1",
              last_token: "temp",
            },
            duration: 1,
            recovery_duration: 1,
          },
        }),
      ).rejects.toThrow("source must be");
    });
  });

  describe("updateEphemeral", () => {
    it("rejects invalid recovery_eval_type", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.updateEphemeral({
          id: "eph_1",
          config: { recovery_eval_type: "INVALID" },
        }),
      ).rejects.toThrow("recovery_eval_type");
    });

    it("accepts valid recovery_eval_type", async () => {
      const ctx = makeCtx(undefined, {
        "api.iot.alerts.test_org_123.update_ephemeral": {
          status: "ALERT_UPDATE_SUCCESS",
          data: { id: "eph_1", type: "EPHEMERAL" },
        },
      });
      const am = new AlertManager(ctx);

      const result = await am.updateEphemeral({
        id: "eph_1",
        config: { recovery_eval_type: "TIMER" },
      });

      expect(result.id).toBe("eph_1");
    });
  });

  describe("update", () => {
    it("injects env into payload", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await am.update({ id: "rule_1", name: "updated" });

      const [, rawData] = ctx.natsClient.request.mock.calls[0];
      const payload = JSON.parse(new TextDecoder().decode(rawData));
      expect(payload.env).toBe("production");
    });

    it("throws on missing id", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(am.update({ name: "x" })).rejects.toThrow("id is required");
    });
  });

  describe("delete", () => {
    it("returns true on success", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      expect(await am.delete("rule_1")).toBe(true);
    });

    it("throws on empty id", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(am.delete("")).rejects.toThrow("id is required");
    });
  });

  describe("list", () => {
    it("returns array", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const alerts = await am.list();
      expect(alerts).toHaveLength(1);
    });
  });

  describe("get", () => {
    it("returns alert with listen and setEvaluator", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const alert = await am.get("high_temp");
      expect(alert.id).toBe("rule_1");
      expect(typeof alert.listen).toBe("function");
      expect(typeof alert.setEvaluator).toBe("function");
    });
  });

  describe("ack", () => {
    it("sends ack request", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const result = await am.ack({
        device_id: "dev_1",
        alert_id: "rule_1",
        acked_by: "operator_jane",
      });
      expect(result).toBe(true);
      const [subject] = ctx.natsClient.request.mock.calls[0];
      expect(subject).toBe("api.iot.alerts.test_org_123.ack");
    });

    it("throws on missing acked_by", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(
        am.ack({ device_id: "dev_1", alert_id: "x" }),
      ).rejects.toThrow("acked_by");
    });

    it("throws on missing device_id", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(am.ack({ alert_id: "x", acked_by: "op" })).rejects.toThrow(
        "device_id",
      );
    });

    it("throws on missing alert_id", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(
        am.ack({ device_id: "dev_1", acked_by: "op" }),
      ).rejects.toThrow("alert_id");
    });
  });

  describe("ackAll", () => {
    it("sends ack_all request", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const result = await am.ackAll({ alert_id: "rule_1", acked_by: "op" });
      expect(result).toBe(true);
    });
  });

  describe("mute", () => {
    it("sends mute request with FOREVER", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const res = await am.mute({
        id: "rule_1",
        mute_config: { type: "FOREVER" },
      });
      expect(res.status).toBe("ALERT_RULE_MUTE_SUCCESS");
    });

    it("requires mute_till for TIME_BASED", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(
        am.mute({ id: "rule_1", mute_config: { type: "TIME_BASED" } }),
      ).rejects.toThrow("mute_till");
    });

    it("rejects invalid mute type", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      await expect(
        am.mute({ id: "rule_1", mute_config: { type: "INVALID" } }),
      ).rejects.toThrow("FOREVER or TIME_BASED");
    });
  });

  describe("unmute", () => {
    it("sends CLEAR mute request", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const res = await am.unmute("rule_1");
      expect(res.status).toBe("ALERT_RULE_MUTE_SUCCESS");
    });
  });

  describe("history", () => {
    const validStart = "2026-03-01T00:00:00.000Z";
    const validEnd = "2026-03-25T00:00:00.000Z";

    const alertHistoryResponse = {
      status: "ALERT_FETCH_SUCCESS",
      data: {
        has_more: false,
        cursor: null,
        data: {
          fire: [{ timestamp: "2026-03-25T00:00:00.000Z", rule_id: "rule_1" }],
          resolved: [],
        },
      },
    };

    function withMsgpackHistory(ctx) {
      const originalRequest = ctx.natsClient.request;
      ctx.natsClient.request = vi.fn(async (subject, data, opts) => {
        if (subject === "api.iot.db.test_org_123.alerts.history") {
          return { data: msgpackEncode(alertHistoryResponse) };
        }
        return originalRequest(subject, data, opts);
      });
    }

    it("fetches history by DEVICE", async () => {
      const ctx = makeCtx();
      withMsgpackHistory(ctx);
      const am = new AlertManager(ctx);

      const result = await am.history({
        rule_type: "DEVICE",
        device_ident: "sensor_01",
        rule_states: ["fire", "resolved"],
        start: validStart,
        end: validEnd,
      });

      expect(result.has_more).toBe(false);
      expect(result.data.fire).toHaveLength(1);
      expect(result.data.resolved).toHaveLength(0);

      const [subject] = ctx.natsClient.request.mock.calls[0];
      expect(subject).toBe("api.iot.db.test_org_123.alerts.history");
    });

    it("fetches history by RULE", async () => {
      const ctx = makeCtx();
      withMsgpackHistory(ctx);
      const am = new AlertManager(ctx);

      const result = await am.history({
        rule_type: "RULE",
        rule_id: "rule_1",
        rule_states: ["fire"],
        start: validStart,
        end: validEnd,
      });

      expect(result.data.fire).toHaveLength(1);
    });

    it("fetches history by RULE with optional device_ident", async () => {
      const ctx = makeCtx();
      withMsgpackHistory(ctx);
      const am = new AlertManager(ctx);

      const result = await am.history({
        rule_type: "RULE",
        rule_id: "rule_1",
        device_ident: "sensor_01",
        rule_states: ["fire"],
        start: validStart,
        end: validEnd,
      });

      expect(result.data.fire).toHaveLength(1);
    });

    it("defaults rule_states to fire and resolved", async () => {
      const ctx = makeCtx();
      withMsgpackHistory(ctx);
      const am = new AlertManager(ctx);

      await am.history({
        rule_type: "DEVICE",
        device_ident: "sensor_01",
        start: validStart,
        end: validEnd,
      });

      const [, rawData] = ctx.natsClient.request.mock.calls[0];
      const payload = JSON.parse(new TextDecoder().decode(rawData));
      expect(payload.rule_states).toEqual(["fire", "resolved"]);
    });

    it("resolves device_ident to device_id in payload", async () => {
      const ctx = makeCtx();
      withMsgpackHistory(ctx);
      const am = new AlertManager(ctx);

      await am.history({
        rule_type: "DEVICE",
        device_ident: "sensor_01",
        rule_states: ["fire"],
        start: validStart,
        end: validEnd,
      });

      const [, rawData] = ctx.natsClient.request.mock.calls[0];
      const payload = JSON.parse(new TextDecoder().decode(rawData));
      expect(payload.device_id).toBe("dev_1");
      expect(payload.rule_type).toBe("DEVICE");
    });

    it("throws if rule_type is missing", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_states: ["fire"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("rule_type is required");
    });

    it("throws if rule_type is invalid", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "INVALID",
          rule_states: ["fire"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("DEVICE or RULE");
    });

    it("throws if DEVICE but no device_ident", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "DEVICE",
          rule_states: ["fire"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("device_ident is required");
    });

    it("throws if RULE but no rule_id", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "RULE",
          rule_states: ["fire"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("rule_id is required");
    });

    it("throws if rule_states is empty", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "DEVICE",
          device_ident: "sensor_01",
          rule_states: [],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("non-empty array");
    });

    it("throws if rule_states contains invalid values", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "DEVICE",
          device_ident: "sensor_01",
          rule_states: ["fire", "invalid_state"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("invalid values");
    });

    it("rejects ack in rule_states (use ackHistory instead)", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "DEVICE",
          device_ident: "sensor_01",
          rule_states: ["fire", "ack"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("invalid values");
    });

    it("throws if start >= end", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "DEVICE",
          device_ident: "sensor_01",
          rule_states: ["fire"],
          start: validEnd,
          end: validStart,
        }),
      ).rejects.toThrow("start must be before end");
    });

    it("throws if not connected", async () => {
      const ctx = makeCtx();
      ctx.connected = false;
      const am = new AlertManager(ctx);

      await expect(
        am.history({
          rule_type: "DEVICE",
          device_ident: "sensor_01",
          rule_states: ["fire"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("Not connected");
    });
  });

  describe("ackHistory", () => {
    const validStart = "2026-03-01T00:00:00.000Z";
    const validEnd = "2026-03-25T00:00:00.000Z";

    const ackHistoryResponse = {
      status: "ALERT_ACK_FETCH_SUCCESS",
      data: {
        has_more: false,
        cursor: null,
        data: {
          ack: [{ acked_by: "operator_jane", acked_at: 1711324800000 }],
          ack_all: [],
        },
      },
    };

    function withMsgpackAckHistory(ctx) {
      const originalRequest = ctx.natsClient.request;
      ctx.natsClient.request = vi.fn(async (subject, data, opts) => {
        if (subject === "api.iot.db.test_org_123.alerts.ack_history") {
          return { data: msgpackEncode(ackHistoryResponse) };
        }
        return originalRequest(subject, data, opts);
      });
    }

    it("fetches ack history by rule_id", async () => {
      const ctx = makeCtx();
      withMsgpackAckHistory(ctx);
      const am = new AlertManager(ctx);

      const result = await am.ackHistory({
        rule_id: "rule_1",
        ack_states: ["ack", "ack_all"],
        start: validStart,
        end: validEnd,
      });

      expect(result.has_more).toBe(false);
      expect(result.data.ack).toHaveLength(1);
      expect(result.data.ack_all).toHaveLength(0);

      const [subject] = ctx.natsClient.request.mock.calls[0];
      expect(subject).toBe("api.iot.db.test_org_123.alerts.ack_history");
    });

    it("defaults ack_states to ack and ack_all", async () => {
      const ctx = makeCtx();
      withMsgpackAckHistory(ctx);
      const am = new AlertManager(ctx);

      await am.ackHistory({
        rule_id: "rule_1",
        start: validStart,
        end: validEnd,
      });

      const [, rawData] = ctx.natsClient.request.mock.calls[0];
      const payload = JSON.parse(new TextDecoder().decode(rawData));
      expect(payload.ack_states).toEqual(["ack", "ack_all"]);
    });

    it("sends rule_id and env in payload", async () => {
      const ctx = makeCtx();
      withMsgpackAckHistory(ctx);
      const am = new AlertManager(ctx);

      await am.ackHistory({
        rule_id: "rule_1",
        start: validStart,
        end: validEnd,
      });

      const [, rawData] = ctx.natsClient.request.mock.calls[0];
      const payload = JSON.parse(new TextDecoder().decode(rawData));
      expect(payload.rule_id).toBe("rule_1");
      expect(payload.env).toBe("production");
    });

    it("throws if rule_id is missing", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.ackHistory({
          ack_states: ["ack"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("rule_id is required");
    });

    it("throws if ack_states contains invalid values", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.ackHistory({
          rule_id: "rule_1",
          ack_states: ["ack", "invalid"],
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("invalid values");
    });

    it("throws if start >= end", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);

      await expect(
        am.ackHistory({
          rule_id: "rule_1",
          start: validEnd,
          end: validStart,
        }),
      ).rejects.toThrow("start must be before end");
    });

    it("throws if not connected", async () => {
      const ctx = makeCtx();
      ctx.connected = false;
      const am = new AlertManager(ctx);

      await expect(
        am.ackHistory({
          rule_id: "rule_1",
          start: validStart,
          end: validEnd,
        }),
      ).rejects.toThrow("Not connected");
    });
  });

  describe("setEvaluator", () => {
    it("throws for non-EPHEMERAL alerts", async () => {
      const ctx = makeCtx();
      const am = new AlertManager(ctx);
      const alert = await am.get("high_temp"); // type = THRESHOLD
      expect(() => alert.setEvaluator(() => true)).toThrow("EPHEMERAL");
    });
  });

  describe("listen (non-ephemeral)", () => {
    it("creates 1 wildcard JetStream consumer for all alert events", async () => {
      const consumer = createMockConsumer();
      const ctx = makeCtx(consumer);
      const am = new AlertManager(ctx);
      const alert = await am.get("high_temp");

      await alert.listen({
        onFire: vi.fn(),
        onResolved: vi.fn(),
        onAck: vi.fn(),
        onAckAll: vi.fn(),
      });

      const alertCalls = ctx.jetstream.consumers.get.mock.calls.filter(
        ([, opts]) => opts?.filter_subjects?.includes("alerts.listen"),
      );
      expect(alertCalls).toHaveLength(1);
      expect(alertCalls[0][1].filter_subjects).toBe(
        "import.test_org_123.production.alerts.listen.rule_1.*",
      );
    });
  });
});
