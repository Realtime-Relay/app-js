import { JSONCodec } from 'nats.ws';
import { encode as msgpackEncode } from '@msgpack/msgpack';

export const SUBJECT_MAP = {
    TELEMETRY: (orgID, env, deviceId, lastToken) => `${orgID}.${env}.telemetry.${deviceId}.${lastToken}`,
    COMMAND: (orgID, env, deviceId, lastToken) => `${orgID}.${env}.command.queue.${deviceId}.${lastToken}`,
    EVENT: (orgID, env, deviceId, lastToken) => `${orgID}.${env}.events.${deviceId}.${lastToken}`,
};

// Positions of device_id and last_token in each subject type
export const SUBJECT_DEVICE_INDEX = { TELEMETRY: 3, COMMAND: 4, EVENT: 3 };
export const SUBJECT_LAST_TOKEN_INDEX = { TELEMETRY: 4, COMMAND: 5, EVENT: 4 };

export const codec = JSONCodec();

export async function buildDataSubject(ctx, rule) {
    const topic = rule.config.topic;
    const builder = SUBJECT_MAP[topic.source];
    
    if (!builder){
        throw new Error(`Unknown topic source: ${topic.source}`);
    }

    let deviceId = topic.device_ident;

    if (deviceId !== '*') {
        deviceId = await ctx.device.resolveDeviceId(topic.device_ident);
    }

    return builder(ctx.orgID, ctx.env, deviceId, topic.last_token);
}

export function resolveIdentFromId(ctx, deviceId) {
    if (!ctx.device?.cache){
        return null;
    }

    for (const [ident, device] of ctx.device.cache) {
        if (device.id === deviceId) return ident;
    }

    return null;
}

export function buildAlertPayload(rule, rollingState, timestamp, deviceId) {
    return {
        alert: {
            id: rule.id,
            name: rule.name,
            type: rule.config.topic.source,
            config: rule.config,
        },
        device_id: deviceId,
        rolling_state: { ...rollingState },
        timestamp,
    };
}

export function isMuted(rule) {
    const muteConfig = rule.alert_mute_config;
    
    if (!muteConfig) return false;
    
    if (muteConfig.type === 'FOREVER') return true;
    
    if (muteConfig.type === 'TIME_BASED') {
        if (muteConfig.mute_till && Date.now() < muteConfig.mute_till) return true;
    }
    return false;
}

export function createFreshState() {
    return {
        status: 'normal',
        last_evaluated_at: null,
        clear_since: null,
        breached_since: null,
        last_fired: 0,
        acked_by: null,
        acked_at: null,
        ack_notes: null,
    };
}

export async function dispatchNotifications(ctx, rule, data) {
    const channels = rule.notification_channel;
    if (!channels || channels.length === 0) return;

    try {
        await ctx.natsClient.request(
            `api.iot.notification.${ctx.orgID}.dispatch`,
            codec.encode({
                notification_channel: channels,
                alert_data: data
            }),
            { timeout: 10000 }
        );
    } catch (err) {
        // Notification dispatch failure should not block alerting
        ctx.logger.error('Failed to dispatch notification', err);
    }
}

export async function publishEvent(ctx, rule, eventType, payload) {
    const subject = `${ctx.orgID}.${ctx.env}.alerts.listen.${rule.id}.${eventType}`;
    console.log(subject)
    await ctx.publishOrBuffer(subject, msgpackEncode(payload));
}
