import {
  encode as msgpackEncode,
  decode as msgpackDecode,
} from "@msgpack/msgpack";
import {
  validateHierarchyName,
  validateHierarchyWildcard,
  validateFunction,
  validateConnected,
} from "./validation.js";

export class MessagingManager {
  #ctx;
  #streams = new Map(); // topic -> { consumer, callback }
  #importStreams = new Map(); // topic -> { consumer, callback }

  constructor(ctx) {
    this.#ctx = ctx;
  }

  #subject(topic) {
    return `${this.#ctx.orgID}.${this.#ctx.env}.messages.${topic}`;
  }

  #importSubject(topic) {
    return `import.${this.#subject(topic)}`;
  }

  /**
   * Send a message to every app streaming the topic. Goes through JetStream,
   * so it is buffered while offline and flushed on reconnect.
   *
   * params:
   *   topic  string  required — dot-separated, e.g. "orders.created"
   *   data   any     required — anything msgpack can encode
   *
   * Returns { sent: true } or { sent: false, buffered: true }.
   */
  async send(params) {
    validateHierarchyName(params.topic, "topic");

    if (params.data == null) {
      throw new Error("data is required");
    }

    const payload = msgpackEncode({
      data: params.data,
      timestamp: Date.now(),
    });

    const ack = await this.#ctx.publishOrBuffer(
      this.#subject(params.topic),
      payload,
    );

    return ack != null ? { sent: true } : { sent: false, buffered: true };
  }

  /**
   * Stream messages on {orgID}.{env}.messages.{topic} as they arrive. Only
   * messages sent after the call are delivered.
   *
   * params:
   *   topic     string  required — may use NATS wildcards: "orders.*",
   *                     "orders.>" (">" only as the last token)
   *   callback  function ({ topic, data, timestamp }) — `topic` is the
   *                     concrete topic, so wildcard streams can tell them apart
   *
   * Returns true, or false if the topic is already being streamed.
   */
  async stream(params) {
    return this.#listen(this.#streams, this.#subject.bind(this), params);
  }

  async off(params) {
    await this.#unlisten(this.#streams, params);
  }

  /**
   * Same as stream(), but on import.{orgID}.{env}.messages.{topic}.
   * Independent of stream(): the same topic can be streamed on both, and
   * each is stopped with its own off.
   */
  async streamImport(params) {
    return this.#listen(
      this.#importStreams,
      this.#importSubject.bind(this),
      params,
    );
  }

  async offImport(params) {
    await this.#unlisten(this.#importStreams, params);
  }

  async deleteAllConsumers() {
    for (const streams of [this.#streams, this.#importStreams]) {
      for (const [, { consumer }] of streams) {
        await consumer.delete();
      }
      streams.clear();
    }
  }

  // ─── Internals ───────────────────────────────────────────

  async #listen(streams, toSubject, params) {
    validateConnected(this.#ctx.connected);
    validateHierarchyWildcard(params.topic, "topic");
    validateFunction(params.callback, "callback");

    if (streams.has(params.topic)) {
      return false;
    }

    const consumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `appjs_messages_${crypto.randomUUID()}`,
        filter_subjects: toSubject(params.topic),
        replay_policy: "instant",
        opt_start_time: new Date(),
        ack_policy: "explicit",
        delivery_policy: "new",
      },
    );

    const entry = { consumer, callback: params.callback };
    streams.set(params.topic, entry);

    // subject = <prefix><topic...>
    const prefixLength = toSubject("").length;

    await consumer.consume({
      callback: async (msg) => {
        msg.working();
        const payload = msgpackDecode(msg.data);
        msg.ack();

        // Stopped, or replaced by a later stream() on the same topic.
        if (streams.get(params.topic) !== entry) return;

        entry.callback({
          topic: msg.subject.slice(prefixLength),
          data: payload.data,
          timestamp: payload.timestamp,
        });
      },
    });

    return true;
  }

  async #unlisten(streams, params) {
    validateHierarchyWildcard(params.topic, "topic");

    const entry = streams.get(params.topic);

    if (entry) {
      streams.delete(params.topic);
      await entry.consumer.delete();
    }
  }
}
