import { vi } from "vitest";
import { JSONCodec } from "nats.ws";
import {
  encode as msgpackEncode,
  decode as msgpackDecode,
} from "@msgpack/msgpack";

const jc = JSONCodec();

export function createMockConsumer(messages = []) {
  let consumeCallback = null;
  let msgIndex = 0;

  const consumer = {
    consume: vi.fn(async (opts) => {
      consumeCallback = opts?.callback;
      // Deliver queued messages
      for (const msg of messages) {
        if (consumeCallback) {
          await consumeCallback(msg);
        }
      }
    }),
    next: vi.fn(async () => {
      if (msgIndex < messages.length) {
        return messages[msgIndex++];
      }
      return null;
    }),
    delete: vi.fn(async () => true),
    // Utility to push a new message after consumer is created
    _pushMessage: async (msg) => {
      if (consumeCallback) {
        await consumeCallback(msg);
      }
    },
    _getCallback: () => consumeCallback,
  };

  return consumer;
}

export function createMockMessage(data, subject = "test.subject") {
  return {
    data: data,
    subject: subject,
    timestamp: new Date(),
    working: vi.fn(),
    ack: vi.fn(),
    nak: vi.fn(),
  };
}

export function createMockJetStream(consumer) {
  return {
    publish: vi.fn(async (subject, data) => ({
      stream: "test_stream",
      seq: 1,
      duplicate: false,
    })),
    consumers: {
      get: vi.fn(async (streamName, opts) => consumer || createMockConsumer()),
    },
  };
}

export function createMockNatsClient(responses = {}) {
  const statusIterator = {
    [Symbol.asyncIterator]() {
      return {
        next: () => new Promise(() => {}), // Never resolves by default
      };
    },
  };

  return {
    request: vi.fn(async (subject, data, opts) => {
      const responder = responses[subject];
      if (responder) {
        const payload =
          typeof responder === "function" ? responder(data) : responder;
        return {
          json: () => payload,
          data: jc.encode(payload),
        };
      }
      return {
        json: () => ({ status: "UNKNOWN", data: {} }),
        data: jc.encode({ status: "UNKNOWN", data: {} }),
      };
    }),
    publish: vi.fn(),
    subscribe: vi.fn((subject) => {
      const sub = createMockSubscription();
      sub._subject = subject;
      return sub;
    }),
    status: vi.fn(() => statusIterator),
    closed: vi.fn(() => new Promise(() => {})), // Never resolves
    close: vi.fn(async () => {}),
    info: { client_id: "test_client_123" },
    _statusIterator: statusIterator,
  };
}

// ─── KV Mock ────────────────────────────────────────────────

export function createMockKVBucket() {
  const _store = new Map();

  return {
    _store,
    get: vi.fn(async (key) => {
      if (!_store.has(key)) {
        const err = new Error(`key not found: ${key}`);
        err.code = "KEY_NOT_FOUND";
        throw err;
      }
      const value = _store.get(key);
      const raw = typeof value === "string" ? value : JSON.stringify(value);
      return {
        value: jc.encode(value),
        json: () => value,
        string: () => raw,
        revision: 1,
      };
    }),
    put: vi.fn(async (key, value) => {
      const decoded = typeof value === "string" ? JSON.parse(value) : value;
      _store.set(key, decoded);
      return 1; // revision
    }),
    update: vi.fn(async (key, value, revision) => {
      const decoded = typeof value === "string" ? JSON.parse(value) : value;
      _store.set(key, decoded);
      return 2; // new revision
    }),
    create: vi.fn(async (key, value) => {
      if (_store.has(key)) {
        const err = new Error(`key already exists: ${key}`);
        err.code = "KEY_EXISTS";
        throw err;
      }
      const decoded = typeof value === "string" ? JSON.parse(value) : value;
      _store.set(key, decoded);
      return 1;
    }),
    delete: vi.fn(async (key) => {
      _store.delete(key);
    }),
    purge: vi.fn(async (key) => {
      _store.delete(key);
    }),
  };
}

export function createMockKV(bucket) {
  const kvBucket = bucket || createMockKVBucket();
  return {
    status: vi.fn(async () => ({ bucket: "test_bucket" })),
    _bucket: kvBucket,
    // The Kvm.get() returns the KV store
  };
}

// ─── Subscribe Mock ─────────────────────────────────────────

export function createMockSubscription() {
  let callback = null;
  const messages = [];
  let draining = false;

  const sub = {
    [Symbol.asyncIterator]() {
      let idx = 0;
      return {
        async next() {
          if (draining || idx >= messages.length) {
            return { done: true, value: undefined };
          }
          return { done: false, value: messages[idx++] };
        },
      };
    },
    callback: vi.fn((cb) => {
      callback = cb;
    }),
    drain: vi.fn(async () => {
      draining = true;
    }),
    unsubscribe: vi.fn(() => {
      draining = true;
    }),
    // Test utility: push an RPC message with respond capability
    _pushMessage: async (data, replySubject = null) => {
      const msg = {
        data: jc.encode(data),
        subject: sub._subject || "test.rpc",
        reply: replySubject,
        json: () => data,
        respond: vi.fn((responseData) => {}),
      };
      messages.push(msg);
      if (callback) {
        await callback(msg);
      }
      return msg;
    },
    _subject: "test.rpc",
  };

  return sub;
}

export function createMockCodec() {
  return {
    encode: vi.fn((data) => JSON.stringify(data)),
    decode: vi.fn((data) => {
      if (typeof data === "string") return JSON.parse(data);
      return data;
    }),
  };
}

/**
 * Wires the natsClient mock to act as a streaming-history server for a
 * single endpoint. Wraps `request` to reply with STREAM_STARTED (or
 * NO_STREAM if frames is empty), and wraps `subscribe` to yield the
 * provided frames as msgpack-encoded messages on the export subject.
 *
 *   ctx           — ctx returned by createMockContext
 *   requestSubject — e.g. "api.iot.db.test_org_123.telemetry.history"
 *   status         — STREAM_STARTED status string for this endpoint
 *                    (e.g. "TELEMETRY_FETCH_STREAM_STARTED")
 *   frames         — array of frame objects { last, data, ... }
 *                    Empty → server responds with *_NO_STREAM
 *   options.noStreamStatus — status to use when frames is empty
 *                    (default: replaces "_STREAM_STARTED" with "_SUCCESS_NO_STREAM")
 *   options.failureStatus  — if set, request returns *_FAILURE instead.
 */
export function mockStreamHistory(ctx, requestSubject, status, frames, options = {}) {
  const origRequest = ctx.natsClient.request;
  const origSubscribe = ctx.natsClient.subscribe;

  ctx.natsClient.request = vi.fn(async (subject, data, opts) => {
    if (subject !== requestSubject) {
      return origRequest(subject, data, opts);
    }

    let payload = {};
    try {
      payload =
        typeof data === "string"
          ? JSON.parse(data)
          : JSON.parse(new TextDecoder().decode(data));
    } catch {}

    if (options.failureStatus) {
      const reply = msgpackEncode({
        status: options.failureStatus,
        data: options.failureData ?? [],
      });
      return { json: () => msgpackDecode(reply), data: reply };
    }

    if (frames.length === 0) {
      const noStreamStatus =
        options.noStreamStatus ?? status.replace("_STREAM_STARTED", "_SUCCESS_NO_STREAM");
      const reply = msgpackEncode({
        status: noStreamStatus,
        data: { frames: 0 },
      });
      return { json: () => msgpackDecode(reply), data: reply };
    }

    // Match the App-SDK side's subject convention (import.* — broker maps from
    // server-side export.*).
    const exportSubject = `import.${ctx.orgID}.${ctx.env}.history.${payload.stream_token}`;
    const readySubject = `${exportSubject}.ready`;
    const reply = msgpackEncode({
      status,
      data: { subject: exportSubject, ready_subject: readySubject },
    });
    return { json: () => msgpackDecode(reply), data: reply };
  });

  ctx.natsClient.subscribe = vi.fn((subject) => {
    if (!subject.startsWith(`import.${ctx.orgID}.${ctx.env}.history.`)) {
      return origSubscribe(subject);
    }
    const messages = frames.map((f) => ({
      data: msgpackEncode(f),
      subject,
    }));
    return createMockSubscriptionFromMessages(messages, subject);
  });
}

function createMockSubscriptionFromMessages(messages, subject) {
  let unsubscribed = false;
  return {
    [Symbol.asyncIterator]() {
      let idx = 0;
      return {
        async next() {
          if (unsubscribed || idx >= messages.length) {
            return { done: true, value: undefined };
          }
          return { done: false, value: messages[idx++] };
        },
      };
    },
    unsubscribe: vi.fn(() => {
      unsubscribed = true;
    }),
    drain: vi.fn(() => {
      unsubscribed = true;
    }),
    _subject: subject,
  };
}

/**
 * Creates a shared context object that simulates the app's internal state.
 * Used by manager constructors.
 */
export function createMockContext(overrides = {}) {
  const natsClient = createMockNatsClient(overrides.responses || {});
  const jetstream = createMockJetStream(overrides.consumer);
  const kvBucket = overrides.kvBucket || createMockKVBucket();

  const ctx = {
    natsClient,
    jetstream,
    kvBucket,
    codec: createMockCodec(),
    orgID: overrides.orgID || "test_org_123",
    env: overrides.env || "production",
    connected: overrides.connected !== undefined ? overrides.connected : true,
    deviceCache: overrides.deviceCache || new Map(),
    consumerMap: {},
    offlineBuffer: [],
    ...overrides,
  };

  ctx.publishOrBuffer = vi.fn(async (subject, payload) => {
    if (ctx.connected && ctx.jetstream) {
      try {
        return await ctx.jetstream.publish(subject, payload);
      } catch {
        ctx.offlineBuffer.push({ subject, payload });
        return null;
      }
    }
    ctx.offlineBuffer.push({ subject, payload });
    return null;
  });

  return ctx;
}
