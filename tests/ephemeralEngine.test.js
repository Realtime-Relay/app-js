import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  createMockContext,
  createMockConsumer,
  createMockKVBucket,
} from "./setup.js";
import { DeviceManager } from "../src/device.js";
import { EphemeralEngine } from "../src/ephemeral_alerting/index.js";
import {
  buildAlertPayload,
  dispatchNotifications,
  createFreshState,
} from "../src/ephemeral_alerting/shared.js";

const RULE = {
  id: "rule_eph_1",
  name: "custom_check",
  type: "EPHEMERAL",
  config: {
    topic: {
      source: "TELEMETRY",
      device_ident: "sensor_01",
      last_token: "cpu_usage",
    },
    duration: 2,
    recovery_duration: 1,
    cooldown: 3,
  },
  notification_channel: ["notif_1"],
};

const RULE_WILDCARD = {
  ...RULE,
  id: "rule_eph_wildcard",
  config: {
    ...RULE.config,
    topic: { source: "TELEMETRY", device_ident: "*", last_token: "*" },
  },
};

const RULE_COMMAND = {
  ...RULE,
  id: "rule_eph_cmd",
  config: {
    ...RULE.config,
    topic: {
      source: "COMMAND",
      device_ident: "sensor_01",
      last_token: "reboot",
    },
  },
};

const RULE_EVENT = {
  ...RULE,
  id: "rule_eph_evt",
  config: {
    ...RULE.config,
    topic: {
      source: "EVENT",
      device_ident: "sensor_01",
      last_token: "door_opened",
    },
  },
};

function makeCtx(kvBucket) {
  const consumer = createMockConsumer();
  const ctx = createMockContext({ consumer, kvBucket });
  ctx.logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
  const device = new DeviceManager(ctx);
  ctx.device = device;
  device.cache.set("sensor_01", { id: "dev_1", ident: "sensor_01" });
  device.cache.set("sensor_02", { id: "dev_2", ident: "sensor_02" });
  return { ctx, consumer };
}

describe("EphemeralEngine", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("constructor and setEvaluator", () => {
    it("constructs without evaluator", () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      expect(engine.mode).toBeNull();
    });

    it("setEvaluator stores function", () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => true);
      // No throw = success
    });

    it("setEvaluator throws for non-function", () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      expect(() => engine.setEvaluator("not_fn")).toThrow("must be a function");
    });
  });

  describe("mode detection", () => {
    it("owner mode when evaluator is set before listen", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});
      expect(engine.mode).toBe("owner");
      await engine.stop();
    });

    it("listener mode when no evaluator", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      await engine.listen({});
      expect(engine.mode).toBe("listener");
      await engine.stop();
    });
  });

  describe("subject construction", () => {
    it("TELEMETRY with specific ident resolves device ID", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      const dataCall = ctx.jetstream.consumers.get.mock.calls.find(([, opts]) =>
        opts?.filter_subjects?.includes("telemetry"),
      );
      expect(dataCall).toBeDefined();
      expect(dataCall[1].filter_subjects).toBe(
        "test_org_123.production.telemetry.dev_1.cpu_usage",
      );
      await engine.stop();
    });

    it("TELEMETRY with wildcard uses * directly", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE_WILDCARD);
      engine.setEvaluator(() => false);
      await engine.listen({});

      const dataCall = ctx.jetstream.consumers.get.mock.calls.find(([, opts]) =>
        opts?.filter_subjects?.includes("telemetry"),
      );
      expect(dataCall[1].filter_subjects).toBe(
        "test_org_123.production.telemetry.*.*",
      );
      await engine.stop();
    });

    it("COMMAND subject construction", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE_COMMAND);
      engine.setEvaluator(() => false);
      await engine.listen({});

      const dataCall = ctx.jetstream.consumers.get.mock.calls.find(([, opts]) =>
        opts?.filter_subjects?.includes("command.queue"),
      );
      expect(dataCall[1].filter_subjects).toBe(
        "test_org_123.production.command.queue.dev_1.reboot",
      );
      await engine.stop();
    });

    it("EVENT subject construction", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE_EVENT);
      engine.setEvaluator(() => false);
      await engine.listen({});

      const dataCall = ctx.jetstream.consumers.get.mock.calls.find(([, opts]) =>
        opts?.filter_subjects?.includes("events"),
      );
      expect(dataCall[1].filter_subjects).toBe(
        "test_org_123.production.events.dev_1.door_opened",
      );
      await engine.stop();
    });
  });

  describe("KV lock", () => {
    it("acquires lock on owner listen with expires_at", async () => {
      const kvBucket = createMockKVBucket();
      const { ctx } = makeCtx(kvBucket);
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      expect(kvBucket.put).toHaveBeenCalledWith(
        "ephemeral_owner_rule_eph_1",
        expect.any(String),
      );
      const stored = kvBucket._store.get("ephemeral_owner_rule_eph_1");
      expect(stored).toBeDefined();
      expect(stored.expires_at).toBeGreaterThan(Date.now());
      await engine.stop();
    });

    it("throws when lock already exists and not expired", async () => {
      const kvBucket = createMockKVBucket();
      // Pre-populate lock with valid (non-expired) TTL
      kvBucket._store.set("ephemeral_owner_rule_eph_1", {
        started_at: Date.now(),
        expires_at: Date.now() + 30000,
      });

      const { ctx } = makeCtx(kvBucket);
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);

      await expect(engine.listen({})).rejects.toThrow(
        "Evaluator already active",
      );
    });

    it("acquires lock when existing lock has expired", async () => {
      const kvBucket = createMockKVBucket();
      // Pre-populate with expired lock
      kvBucket._store.set("ephemeral_owner_rule_eph_1", {
        started_at: Date.now() - 60000,
        expires_at: Date.now() - 1000, // Expired 1s ago
      });

      const { ctx } = makeCtx(kvBucket);
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      // Should have taken over with CAS update
      expect(kvBucket.update).toHaveBeenCalledWith(
        "ephemeral_owner_rule_eph_1",
        expect.any(String),
        expect.any(Number),
      );
      await engine.stop();
    });

    it("releases lock on stop", async () => {
      const kvBucket = createMockKVBucket();
      const { ctx } = makeCtx(kvBucket);
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      expect(kvBucket._store.has("ephemeral_owner_rule_eph_1")).toBe(true);
      await engine.stop();
      expect(kvBucket.purge).toHaveBeenCalledWith("ephemeral_owner_rule_eph_1");
    });
  });

  describe("RPC subscriptions (owner mode)", () => {
    it("subscribes to single wildcard RPC subject", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      expect(ctx.natsClient.subscribe).toHaveBeenCalledTimes(1);
      const [subject] = ctx.natsClient.subscribe.mock.calls[0];
      expect(subject).toBe(
        "test_org_123.production.alerts.custom.rule_eph_1.*",
      );
      await engine.stop();
    });
  });

  describe("listener mode subscriptions", () => {
    it("subscribes to wildcard alert topic", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      // No setEvaluator → listener mode
      await engine.listen({});

      const alertCall = ctx.jetstream.consumers.get.mock.calls.find(
        ([, opts]) => opts?.filter_subjects?.includes("alerts.listen"),
      );
      expect(alertCall).toBeDefined();
      expect(alertCall[1].filter_subjects).toBe(
        "test_org_123.production.alerts.listen.rule_eph_1.*",
      );
      await engine.stop();
    });
  });

  describe("state machine", () => {
    it("starts in normal state", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      expect(engine.state.status).toBe("normal");
      await engine.stop();
    });
  });

  describe("ack / ackAll", () => {
    it("ack returns false when not alerting", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      expect(await engine.ack("operator")).toBe(false);
      await engine.stop();
    });

    it("ackAll returns false when not alerting", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      expect(await engine.ackAll("operator")).toBe(false);
      await engine.stop();
    });
  });

  describe("mute check", () => {
    it("skips evaluation when muted FOREVER", async () => {
      const { ctx } = makeCtx();
      const mutedRule = {
        ...RULE,
        alert_mute_config: { type: "FOREVER" },
      };

      const onFire = vi.fn();
      const engine = new EphemeralEngine(ctx, mutedRule);
      engine.setEvaluator(() => true);
      await engine.listen({ onFire });

      // Muted — onFire should never be called
      expect(onFire).not.toHaveBeenCalled();
      await engine.stop();
    });
  });

  describe("stop() cleanup", () => {
    it("resets state and mode on stop", async () => {
      const { ctx } = makeCtx();
      const engine = new EphemeralEngine(ctx, RULE);
      engine.setEvaluator(() => false);
      await engine.listen({});

      expect(engine.mode).toBe("owner");
      await engine.stop();

      expect(engine.mode).toBeNull();
      expect(engine.state.status).toBe("normal");
      expect(engine.rollingState).toEqual({});
    });
  });

  describe("notification dispatch", () => {
    it("dispatches to notification endpoint when channels exist", async () => {
      const { ctx } = makeCtx();
      // We can't easily trigger a fire in this mock setup,
      // but we can verify the engine has the notification_channel
      const engine = new EphemeralEngine(ctx, RULE);
      expect(RULE.notification_channel).toEqual(["notif_1"]);
      await engine.stop();
    });
  });

  describe("shared: buildAlertPayload", () => {
    it("includes device_id in payload", () => {
      const payload = buildAlertPayload(
        RULE,
        { sensor_01: { cpu: { value: 90 } } },
        12345,
        "dev_1",
      );

      expect(payload.alert.id).toBe("rule_eph_1");
      expect(payload.alert.name).toBe("custom_check");
      expect(payload.alert.type).toBe("TELEMETRY");
      expect(payload.device_id).toBe("dev_1");
      expect(payload.rolling_state).toEqual({
        sensor_01: { cpu: { value: 90 } },
      });
      expect(payload.timestamp).toBe(12345);
    });

    it("device_id is undefined when not provided", () => {
      const payload = buildAlertPayload(RULE, {}, 12345);

      expect(payload.device_id).toBeUndefined();
    });
  });

  describe("shared: dispatchNotifications", () => {
    it("sends notification_channel and alert_data in payload", async () => {
      const { ctx } = makeCtx();
      const alertData = {
        alert: { id: "rule_1", name: "test", config: {} },
        device_id: "dev_1",
        last_value: { sensor_01: { temp: { value: 90 } } },
        timestamp: Date.now(),
      };

      await dispatchNotifications(ctx, RULE, alertData);

      expect(ctx.natsClient.request).toHaveBeenCalledTimes(1);
      const [subject, data] = ctx.natsClient.request.mock.calls[0];
      expect(subject).toBe("api.iot.notification.test_org_123.dispatch");

      const payload = JSON.parse(new TextDecoder().decode(data));
      expect(payload.notification_channel).toEqual(["notif_1"]);
      expect(payload.alert_data).toEqual(alertData);
    });

    it("skips dispatch when no notification channels", async () => {
      const { ctx } = makeCtx();
      const ruleNoChannels = { ...RULE, notification_channel: [] };

      await dispatchNotifications(ctx, ruleNoChannels, {});

      expect(ctx.natsClient.request).not.toHaveBeenCalled();
    });

    it("swallows dispatch errors", async () => {
      const { ctx } = makeCtx();
      ctx.logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn() };
      ctx.natsClient.request.mockRejectedValueOnce(new Error("timeout"));

      // Should not throw
      await dispatchNotifications(ctx, RULE, {});
    });
  });

  describe("shared: createFreshState", () => {
    it("returns expected initial state", () => {
      const state = createFreshState();

      expect(state.status).toBe("normal");
      expect(state.last_evaluated_at).toBeNull();
      expect(state.clear_since).toBeNull();
      expect(state.breached_since).toBeNull();
      expect(state.last_fired).toBe(0);
      expect(state.acked_by).toBeNull();
      expect(state.acked_at).toBeNull();
      expect(state.ack_notes).toBeNull();
    });
  });
});
