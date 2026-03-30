import { JSONCodec } from "nats.ws";
import { decode as msgpackDecode } from "@msgpack/msgpack";
import {
  validateIdent,
  validateHierarchyName,
  validateHierarchyWildcard,
  validateArray,
  validateFunction,
  validateConnected,
} from "./validation.js";

export class HeirarchyGroupManager {
  #ctx;
  #codec = JSONCodec();
  #streamConsumers = new Map();

  constructor(ctx) {
    this.#ctx = ctx;
  }

  #subject(op) {
    return `api.iot.cohort.${this.#ctx.orgID}.heirarchy.${op}`;
  }

  async #request(op, payload) {
    const res = await this.#ctx.natsClient.request(
      this.#subject(op),
      this.#codec.encode(payload),
      { timeout: 20000 },
    );

    return res.json();
  }

  #wrapGroup(data) {
    if (!data) return data;

    const group = { ...data };
    group.stream = (params) =>
      this.#streamGroup(data.id, data.heirarchy, params);
    group.off = () => this.#offGroup(data.id);

    return group;
  }

  async #offGroup(groupId) {
    const consumer = this.#streamConsumers.get(groupId);

    if (consumer) {
      await consumer.delete();
      this.#streamConsumers.delete(groupId);
    }
  }

  // ─── CRUD ────────────────────────────────────────────────

  async create(params) {
    validateConnected(this.#ctx.connected);
    validateHierarchyName(params.name, "name");
    validateHierarchyName(params.heirarchy, "heirarchy");
    validateArray(params.device_idents, "device_idents");

    const deviceIds = await this.#ctx.device.resolveDeviceIds(
      params.device_idents,
    );

    const res = await this.#request("create", {
      device_ids: deviceIds,
      name: params.name,
      heirarchy: params.heirarchy,
    });

    if (res.data) {
      return this.#wrapGroup(res.data);
    }

    return res.data;
  }

  async update(params) {
    validateConnected(this.#ctx.connected);

    if (!params.id) throw new Error("id is required");

    const payload = { id: params.id };

    if (params.devices) {
      payload.devices = {
        add: params.devices.add
          ? await this.#ctx.device.resolveDeviceIds(params.devices.add)
          : [],
        remove: params.devices.remove
          ? await this.#ctx.device.resolveDeviceIds(params.devices.remove)
          : [],
      };
    }

    if (params.heirarchy) {
      validateHierarchyName(params.heirarchy, "heirarchy");
      payload.heirarchy = params.heirarchy;
    }

    const res = await this.#request("update", payload);

    if (res.data) {
      return this.#wrapGroup(res.data);
    }

    return res.data;
  }

  async delete(groupId) {
    validateConnected(this.#ctx.connected);

    if (!groupId) throw new Error("group_id is required");

    const res = await this.#request("delete", { id: groupId });

    return res.status === "HEIRARCHY_GROUP_DELETE_SUCCESS";
  }

  async list() {
    validateConnected(this.#ctx.connected);

    const res = await this.#request("list", {});

    return res.data || [];
  }

  async get(groupId) {
    validateConnected(this.#ctx.connected);

    if (!groupId) throw new Error("group_id is required");

    const res = await this.#request("get", { id: groupId });

    if (res.status === "HEIRARCHY_GROUP_GET_SUCCESS") {
      res.data.id = groupId;
      return this.#wrapGroup(res.data);
    }

    return res;
  }

  async listDevices(groupId) {
    validateConnected(this.#ctx.connected);

    if (!groupId) throw new Error("group_id is required");

    const res = await this.#ctx.natsClient.request(
      `api.iot.cohort.${this.#ctx.orgID}.heirarchy.device.list`,
      this.#codec.encode({ id: groupId }),
      { timeout: 20000 },
    );

    const data = res.json();

    return data.data || [];
  }

  // ─── Streaming ───────────────────────────────────────────

  async #streamGroup(groupId, groupHeirarchy, params) {
    validateConnected(this.#ctx.connected);
    validateFunction(params.callback, "callback");

    // metric vs metrics — mutually exclusive
    if (params.metric && params.metrics) {
      throw new Error("metric and metrics are mutually exclusive");
    }

    if (params.heirarchy) {
      validateHierarchyWildcard(params.heirarchy, "heirarchy");
    }

    if (params.metrics) {
      validateArray(params.metrics, "metrics");
      for (const m of params.metrics) {
        validateIdent(m, "metrics[]");
      }
    }

    // Determine metric token for NATS subject
    let metricToken = "*"; // default: all metrics
    let clientMetricFilter = null;

    if (params.metric === "*") {
      metricToken = "*";
    } else if (params.metrics && params.metrics.length === 1) {
      metricToken = params.metrics[0];
    } else if (params.metrics && params.metrics.length > 1) {
      metricToken = "*";
      clientMetricFilter = params.metrics;
    }

    // Hierarchy token: param override or group's stored hierarchy
    const heirarchyToken = params.heirarchy || groupHeirarchy;

    const subject = `import.${this.#ctx.orgID}.${this.#ctx.env}.heirarchy.listen.${metricToken}.${heirarchyToken}`;

    const consumer = await this.#ctx.jetstream.consumers.get(
      `${this.#ctx.orgID}_stream`,
      {
        name: `appjs_heirarchy_group_${groupId}_${crypto.randomUUID()}`,
        filter_subjects: subject,
        replay_policy: "instant",
        opt_start_time: new Date(),
        ack_policy: "explicit",
        delivery_policy: "new",
      },
    );

    this.#streamConsumers.set(groupId, consumer);

    const filterIdents = params.device_idents || null;

    await consumer.consume({
      callback: async (msg) => {
        msg.working();
        const data = msgpackDecode(msg.data);
        msg.ack();

        // Client-side device ident filter
        if (filterIdents && !filterIdents.includes(data.ident)) {
          return;
        }

        // Client-side metrics filter (only when multiple metrics specified)
        if (clientMetricFilter && !clientMetricFilter.includes(data.metric)) {
          return;
        }

        params.callback(data);
      },
    });
  }

  // ─── Cleanup ─────────────────────────────────────────────

  async deleteAllConsumers() {
    for (const [, consumer] of this.#streamConsumers) {
      await consumer.delete();
    }

    this.#streamConsumers.clear();
  }
}
