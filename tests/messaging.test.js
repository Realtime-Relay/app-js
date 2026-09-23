import { describe, it, expect, vi } from "vitest";
import {
  encode as msgpackEncode,
  decode as msgpackDecode,
} from "@msgpack/msgpack";
import {
  createMockContext,
  createMockConsumer,
  createMockMessage,
} from "./setup.js";
import { MessagingManager } from "../src/messaging.js";

describe("MessagingManager", () => {
  describe("send", () => {
    it("publishes msgpack payload to the topic subject", async () => {
      const ctx = createMockContext();
      const mm = new MessagingManager(ctx);

      const result = await mm.send({
        topic: "orders.created",
        data: { id: 42 },
      });

      expect(result).toEqual({ sent: true });
      const [subject, payload] = ctx.jetstream.publish.mock.calls[0];
      expect(subject).toBe("test_org_123.production.messages.orders.created");
      const decoded = msgpackDecode(payload);
      expect(decoded.data).toEqual({ id: 42 });
      expect(typeof decoded.timestamp).toBe("number");
    });

    it("buffers when offline", async () => {
      const ctx = createMockContext();
      ctx.connected = false;
      const mm = new MessagingManager(ctx);

      const result = await mm.send({ topic: "chat", data: "hi" });

      expect(result).toEqual({ sent: false, buffered: true });
      expect(ctx.offlineBuffer).toHaveLength(1);
      expect(ctx.offlineBuffer[0].subject).toBe(
        "test_org_123.production.messages.chat",
      );
    });

    it("throws on missing data", async () => {
      const mm = new MessagingManager(createMockContext());
      await expect(mm.send({ topic: "chat" })).rejects.toThrow(
        "data is required",
      );
    });

    it("rejects wildcards in topic", async () => {
      const mm = new MessagingManager(createMockContext());
      await expect(mm.send({ topic: "orders.*", data: 1 })).rejects.toThrow(
        "invalid characters",
      );
    });
  });

  describe("stream", () => {
    it("creates one consumer on the org subject", async () => {
      const ctx = createMockContext({ consumer: createMockConsumer() });
      const mm = new MessagingManager(ctx);

      const result = await mm.stream({ topic: "orders.>", callback: vi.fn() });

      expect(result).toBe(true);
      const calls = ctx.jetstream.consumers.get.mock.calls;
      expect(calls).toHaveLength(1);
      const [stream, opts] = calls[0];
      expect(stream).toBe("test_org_123_stream");
      expect(opts.filter_subjects).toBe(
        "test_org_123.production.messages.orders.>",
      );
    });

    it("delivers the concrete topic, data and timestamp", async () => {
      const consumer = createMockConsumer();
      const ctx = createMockContext({ consumer });
      const mm = new MessagingManager(ctx);
      const callback = vi.fn();

      await mm.stream({ topic: "orders.*", callback });

      const msg = createMockMessage(
        msgpackEncode({ data: { id: 7 }, timestamp: 1000 }),
        "test_org_123.production.messages.orders.shipped",
      );
      await consumer._pushMessage(msg);

      expect(msg.ack).toHaveBeenCalled();
      expect(callback).toHaveBeenCalledWith({
        topic: "orders.shipped",
        data: { id: 7 },
        timestamp: 1000,
      });
    });

    it("returns false on duplicate subscription", async () => {
      const ctx = createMockContext({ consumer: createMockConsumer() });
      const mm = new MessagingManager(ctx);

      await mm.stream({ topic: "chat", callback: vi.fn() });
      const result = await mm.stream({ topic: "chat", callback: vi.fn() });

      expect(result).toBe(false);
    });

    it("rejects '>' before the last token", async () => {
      const mm = new MessagingManager(createMockContext());
      await expect(
        mm.stream({ topic: "a.>.b", callback: vi.fn() }),
      ).rejects.toThrow('">" can only be at the end');
    });

    it("throws when not connected", async () => {
      const ctx = createMockContext();
      ctx.connected = false;
      const mm = new MessagingManager(ctx);
      await expect(
        mm.stream({ topic: "chat", callback: vi.fn() }),
      ).rejects.toThrow("Not connected");
    });
  });

  describe("off", () => {
    it("deletes the consumer and stops delivery", async () => {
      const consumer = createMockConsumer();
      const ctx = createMockContext({ consumer });
      const mm = new MessagingManager(ctx);
      const callback = vi.fn();

      await mm.stream({ topic: "chat", callback });
      await mm.off({ topic: "chat" });

      expect(consumer.delete).toHaveBeenCalledTimes(1);

      await consumer._pushMessage(
        createMockMessage(
          msgpackEncode({ data: "late", timestamp: 1 }),
          "test_org_123.production.messages.chat",
        ),
      );
      expect(callback).not.toHaveBeenCalled();
    });

    it("is a no-op for unsubscribed topics", async () => {
      const mm = new MessagingManager(createMockContext());
      await expect(mm.off({ topic: "nope" })).resolves.toBeUndefined();
    });
  });

  describe("streamImport / offImport", () => {
    it("creates one consumer on the import subject", async () => {
      const ctx = createMockContext({ consumer: createMockConsumer() });
      const mm = new MessagingManager(ctx);

      const result = await mm.streamImport({
        topic: "orders.>",
        callback: vi.fn(),
      });

      expect(result).toBe(true);
      const calls = ctx.jetstream.consumers.get.mock.calls;
      expect(calls).toHaveLength(1);
      const [stream, opts] = calls[0];
      expect(stream).toBe("test_org_123_stream");
      expect(opts.filter_subjects).toBe(
        "import.test_org_123.production.messages.orders.>",
      );
    });

    it("strips the import prefix from the delivered topic", async () => {
      const consumer = createMockConsumer();
      const ctx = createMockContext({ consumer });
      const mm = new MessagingManager(ctx);
      const callback = vi.fn();

      await mm.streamImport({ topic: "orders.*", callback });
      await consumer._pushMessage(
        createMockMessage(
          msgpackEncode({ data: "x", timestamp: 5 }),
          "import.test_org_123.production.messages.orders.shipped",
        ),
      );

      expect(callback).toHaveBeenCalledWith({
        topic: "orders.shipped",
        data: "x",
        timestamp: 5,
      });
    });

    it("is independent of stream() for the same topic", async () => {
      const orgConsumer = createMockConsumer();
      const importConsumer = createMockConsumer();
      const ctx = createMockContext();
      ctx.jetstream.consumers.get
        .mockResolvedValueOnce(orgConsumer)
        .mockResolvedValueOnce(importConsumer);
      const mm = new MessagingManager(ctx);

      expect(await mm.stream({ topic: "chat", callback: vi.fn() })).toBe(true);
      expect(await mm.streamImport({ topic: "chat", callback: vi.fn() })).toBe(
        true,
      );

      await mm.offImport({ topic: "chat" });

      expect(importConsumer.delete).toHaveBeenCalledTimes(1);
      expect(orgConsumer.delete).not.toHaveBeenCalled();
    });

    it("returns false on duplicate subscription", async () => {
      const ctx = createMockContext({ consumer: createMockConsumer() });
      const mm = new MessagingManager(ctx);

      await mm.streamImport({ topic: "chat", callback: vi.fn() });
      const result = await mm.streamImport({ topic: "chat", callback: vi.fn() });

      expect(result).toBe(false);
    });
  });

  describe("deleteAllConsumers", () => {
    it("deletes both org and import consumers", async () => {
      const orgConsumer = createMockConsumer();
      const importConsumer = createMockConsumer();
      const ctx = createMockContext();
      ctx.jetstream.consumers.get
        .mockResolvedValueOnce(orgConsumer)
        .mockResolvedValueOnce(importConsumer);
      const mm = new MessagingManager(ctx);

      await mm.stream({ topic: "a", callback: vi.fn() });
      await mm.streamImport({ topic: "b", callback: vi.fn() });
      await mm.deleteAllConsumers();

      expect(orgConsumer.delete).toHaveBeenCalledTimes(1);
      expect(importConsumer.delete).toHaveBeenCalledTimes(1);
    });
  });
});
