import { decode as msgpackDecode } from "@msgpack/msgpack";
import { createFreshState } from "./shared.js";

export class EphemeralListener {
  #ctx;
  #rule;
  #callbacks;
  #alertConsumer = null;
  #state;
  #running = true;

  constructor(ctx, rule, callbacks) {
    this.#ctx = ctx;
    this.#rule = rule;
    this.#callbacks = callbacks;
    this.#state = createFreshState();
  }

  async start() {
    const ruleId = this.#rule.id;
    const subject = `${this.#ctx.orgID}.${this.#ctx.env}.alerts.listen.${ruleId}.*`;

    const callbackMap = {
      fire: "onFire",
      resolved: "onResolved",
      ack: "onAck",
      ack_all: "onAckAll",
    };

    this.#alertConsumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `appjs_ephemeral_listen_${ruleId}_${crypto.randomUUID()}`,
        filter_subjects: subject,
        replay_policy: "instant",
        opt_start_time: new Date(),
        ack_policy: "explicit",
        delivery_policy: "new",
      },
    );

    await this.#alertConsumer.consume({
      callback: async (msg) => {
        msg.working();
        const data = msgpackDecode(msg.data);
        msg.ack();

        const tokens = msg.subject.split(".");
        const lastToken = tokens[tokens.length - 1];
        const cbName = callbackMap[lastToken];

        if (cbName && this.#callbacks[cbName]) {
          if (
            (lastToken === "fire" || lastToken === "resolved") &&
            typeof data.timestamp === "number"
          ) {
            data.timestamp = new Date(data.timestamp).toISOString();
          }

          this.#callbacks[cbName](data);
        }
      },
    });
  }

  async stop() {
    this.#running = false;

    if (this.#alertConsumer) {
      await this.#alertConsumer.delete();
      this.#alertConsumer = null;
    }

    this.#state = createFreshState();
  }

  // ─── Getters ─────────────────────────────────────────────

  get state() {
    return { ...this.#state };
  }

  get rollingState() {
    return {};
  }
}
