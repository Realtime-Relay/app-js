import { EphemeralOwner } from "./owner.js";
import { EphemeralListener } from "./listener.js";
import { createFreshState } from "./shared.js";

/**
 * Client-side ephemeral alert engine with dual-mode operation.
 *
 * Owner mode (evaluator set): subscribes to data, evaluates, fires/resolves, handles RPCs
 * Listener mode (no evaluator): subscribes to alert events, delegates ack to owner via RPC
 */
export class EphemeralEngine {
  #ctx;
  #rule;
  #evaluator = null;
  #delegate = null; // EphemeralOwner | EphemeralListener
  #running = false;
  #mode = null;

  constructor(ctx, rule) {
    this.#ctx = ctx;
    this.#rule = rule;
  }

  setEvaluator(fn) {
    if (typeof fn !== "function")
      throw new Error("evaluator must be a function");
    this.#evaluator = fn;
  }

  async listen(callbacks) {
    if (this.#running) return;

    this.#running = true;
    const cbs = callbacks || {};

    if (this.#evaluator) {
      this.#mode = "owner";
      this.#delegate = new EphemeralOwner(
        this.#ctx,
        this.#rule,
        this.#evaluator,
        cbs,
      );
    } else {
      this.#mode = "listener";
      this.#delegate = new EphemeralListener(this.#ctx, this.#rule, cbs);
    }

    await this.#delegate.start();
  }

  async stop() {
    this.#running = false;

    if (this.#delegate) {
      await this.#delegate.stop();
      this.#delegate = null;
    }

    this.#mode = null;
  }

  async ack(ackedBy, ackNotes = null) {
    if (!this.#delegate?.ack) return false;
    return this.#delegate.ack(ackedBy, ackNotes);
  }

  async ackAll(ackedBy, ackNotes = null) {
    if (!this.#delegate?.ackAll) return false;
    return this.#delegate.ackAll(ackedBy, ackNotes);
  }

  // ─── Getters ─────────────────────────────────────────────

  get state() {
    return this.#delegate?.state ?? createFreshState();
  }

  get rollingState() {
    return this.#delegate?.rollingState ?? {};
  }

  get mode() {
    return this.#mode;
  }
}
