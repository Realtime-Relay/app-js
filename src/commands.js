import { JSONCodec } from "nats.ws";
import {
  encode as msgpackEncode,
  decode as msgpackDecode,
} from "@msgpack/msgpack";
import {
  validateIdent,
  validateCommandName,
  validateNonEmptyArray,
  validateConnected,
  validateISO8601,
  validateObject,
} from "./validation.js";
import { httpHistory } from "./utils.js";

export class CommandManager {
  #ctx;
  #codec = JSONCodec();

  constructor(ctx) {
    this.#ctx = ctx;
  }

  async send(params) {
    validateCommandName(params.name);
    validateNonEmptyArray(params.device_ident, "device_ident");

    if (params.data == null) {
      throw new Error("data is required");
    }

    // Validate each ident
    for (const ident of params.device_ident) {
      validateIdent(ident, "device_ident[]");
    }

    // Resolve each ident individually to track found/unfound
    const result = {};

    for (const ident of params.device_ident) {
      let deviceId;

      try {
        deviceId = await this.#ctx.device.resolveDeviceId(ident);
      } catch {
        result[ident] = { sent: false, error: "Device not found" };
        continue;
      }

      const subject = `${this.#ctx.orgID}.${this.#ctx.env}.command.queue.${deviceId}.${params.name}`;

      const payload = msgpackEncode({
        value: params.data,
        timestamp: Date.now(),
      });

      const ack = await this.#ctx.publishOrBuffer(subject, payload);

      result[ident] =
        ack != null ? { sent: true } : { sent: false, buffered: true };
    }

    return result;
  }

  /**
   * Fetch historical command invocations from the influx-db-service.
   *
   * params:
   *   name           string   required, the command name
   *   device_idents  string[] required
   *   start, end     ISO8601 (end optional, defaults to now)
   *   interval, aggregate_fn  optional bucketing
   *
   * Returns: { <device_ident>: [{value, timestamp}, ...] }
   * Devices that couldn't be resolved appear as { error: "Device not found" }.
   */
  async history(params) {
    validateConnected(this.#ctx.connected);
    validateCommandName(params.name);
    validateNonEmptyArray(params.device_idents, "device_idents");
    validateISO8601(params.start, "start");

    for (const ident of params.device_idents) {
      validateIdent(ident, "device_idents[]");
    }

    const end = params.end || new Date().toISOString();
    if (params.end) validateISO8601(params.end, "end");

    // Resolve each ident → id, tracking unfound for the result.
    const idToIdent = {};
    const deviceIds = [];
    const commandHistory = {};

    for (const ident of params.device_idents) {
      commandHistory[ident] = [];
      try {
        const id = await this.#ctx.device.resolveDeviceId(ident);
        deviceIds.push(id);
        idToIdent[id] = ident;
      } catch {
        commandHistory[ident] = { error: "Device not found" };
      }
    }

    if (deviceIds.length === 0) {
      return commandHistory;
    }

    const payload = {
      device_ids: deviceIds,
      env: this.#ctx.env,
      command_name: params.name,
      start: params.start,
      end,
    };
    if (params.interval) payload.interval = params.interval;
    if (params.aggregate_fn) payload.aggregate_fn = params.aggregate_fn;

    const result = await httpHistory(
      this.#ctx,
      "/iot/db/command/history",
      payload,
    );

    if (result.error) {
      throw new Error(
        `Command history failed: ${result.errorMessage ?? result.status}`,
      );
    }

    // REST frames are the raw row: { <device_id>: { value, timestamp } }.
    for (const frame of result.frames) {
      for (const [deviceId, point] of Object.entries(frame)) {
        const ident = idToIdent[deviceId] || deviceId;
        if (!Array.isArray(commandHistory[ident])) {
          // Was marked unfound — but the device id resolved on the server.
          commandHistory[ident] = [];
        }
        commandHistory[ident].push({
          value: point.value,
          timestamp: point.timestamp,
        });
      }
    }

    return commandHistory;
  }
}
