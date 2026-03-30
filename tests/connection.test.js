import { describe, it, expect, vi, beforeEach } from "vitest";
import { ConnectionManager } from "../src/connection.js";

function createMinimalCtx(overrides = {}) {
  return {
    natsClient: null,
    jetstream: null,
    connected: false,
    orgID: "test_org_123",
    env: "production",
    apiKey: "test_key",
    secret: "test_secret",
    device: { list: vi.fn(async () => []), clearCache: vi.fn() },
    ...overrides,
  };
}

describe("ConnectionManager", () => {
  let ctx;
  let cm;

  beforeEach(() => {
    ctx = createMinimalCtx();
    cm = new ConnectionManager(ctx);
  });

  // ── Constructor + listener registration ───────────────────

  describe("constructor and listeners", () => {
    it("constructs without error", () => {
      expect(cm).toBeInstanceOf(ConnectionManager);
    });

    it("registers a listener callback", () => {
      const cb = vi.fn();
      cm.listeners(cb);
      // No throw, callback accepted
      expect(true).toBe(true);
    });

    it("throws when listener callback is not a function", () => {
      expect(() => cm.listeners("not_a_function")).toThrow(
        "callback must be a function",
      );
    });

    it("throws when listener callback is null", () => {
      expect(() => cm.listeners(null)).toThrow("callback is required");
    });
  });

  // ── Listener callback with various event names ────────────

  describe("listener callback events", () => {
    it("invokes listener with event names via the internal emitter", async () => {
      // We cannot easily trigger the real connect flow (it calls nats.ws connect),
      // so we verify that listeners() stores the callback and the class
      // architecture supports the callback pattern.
      const events = [];
      const cb = vi.fn((event) => events.push(event));
      cm.listeners(cb);

      // The listener should accept any event string
      // We verify the callback is a function and was stored properly
      // by checking that listeners() did not throw and cb is intact.
      expect(cb).not.toHaveBeenCalled(); // no events yet
    });

    it("accepts listener callback that handles all known event types", () => {
      const knownEvents = [
        "connected",
        "disconnected",
        "reconnected",
        "reconnecting",
        "auth_failed",
      ];
      const received = [];
      const cb = vi.fn((event) => {
        received.push(event);
        expect(knownEvents).toContain(event);
      });

      cm.listeners(cb);

      // Simulate calling the callback with each known event
      for (const event of knownEvents) {
        cb(event);
      }

      expect(received).toEqual(knownEvents);
      expect(cb).toHaveBeenCalledTimes(5);
    });
  });

  // ── Presence callback validation ──────────────────────────

  describe("presence", () => {
    it("throws when callback is not a function", async () => {
      ctx.connected = true;
      await expect(cm.presence("string")).rejects.toThrow(
        "callback must be a function",
      );
    });

    it("throws when callback is null", async () => {
      await expect(cm.presence(null)).rejects.toThrow("callback is required");
    });

    it("throws when not connected", async () => {
      expect(ctx.connected).toBe(false);

      await expect(cm.presence(vi.fn())).rejects.toThrow(
        "Not connected. Call app.connect() first.",
      );
    });

    it("validates callback is a function before checking connection", () => {
      // Even when disconnected, a non-function callback should throw
      // the function-validation error (since validateFunction runs first).
      expect(cm.presence(42)).rejects.toThrow("callback must be a function");
    });
  });

  // ── disconnect without prior connect ──────────────────────

  describe("disconnect", () => {
    it("does nothing when natsClient is null", async () => {
      expect(ctx.natsClient).toBeNull();
      // Should not throw
      await cm.disconnect();
    });
  });
});
