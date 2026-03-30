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

  async history(params) {
    validateConnected(this.#ctx.connected);
    validateCommandName(params.name);
    validateNonEmptyArray(params.device_idents, "device_idents");
    validateISO8601(params.start, "start");

    for (const ident of params.device_idents) {
      validateIdent(ident, "device_idents[]");
    }

    const end = params.end || new Date().toISOString();

    if (params.end) {
      validateISO8601(params.end, "end");
    }

    // Resolve each ident individually to track found/unfound and build id→ident map
    const idToIdent = {};
    const deviceIds = [];
    const unfound = [];

    for (const ident of params.device_idents) {
      try {
        const id = await this.#ctx.device.resolveDeviceId(ident);
        deviceIds.push(id);
        idToIdent[id] = ident;
      } catch {
        unfound.push(ident);
      }
    }

    if (deviceIds.length === 0) {
      const result = {};
      for (const ident of unfound) {
        result[ident] = { error: "Device not found" };
      }
      return result;
    }

    var res = null;
    var commandHistory = {};
    var startCursor = params.start;

    for (let ident of params.device_idents) {
      commandHistory[ident] = [];
    }

    while (true) {
      try {
        res = await this.#ctx.natsClient.request(
          `api.iot.db.${this.#ctx.orgID}.command.history`,
          this.#codec.encode({
            device_ids: deviceIds,
            env: this.#ctx.env,
            command_name: params.name,
            start: startCursor,
            end,
          }),
          { timeout: 20000 },
        );

        const decoded = msgpackDecode(res.data);

        if (decoded.status == "COMMAND_FETCH_SUCCESS") {
          var data = decoded.data;

          var hasMore = data.has_more;

          var commandPage = data.data;

          for (const [deviceId, records] of Object.entries(commandPage)) {
            const ident = idToIdent[deviceId] || deviceId;

            commandHistory[ident] = commandHistory[ident].concat(records);
          }

          if (hasMore) {
            startCursor = data.cursor;

            continue;
          } else {
            // We got all the data, we're done

            break;
          }
        } else {
          // We weren't able to fetch command history

          break;
        }
      } catch (err) {
        console.error("Command history request failed", err);

        throw new Error("Command history request timed-out");
      }
    }

    return commandHistory;
  }
}
