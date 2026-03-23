import { describe, it, expect, vi } from 'vitest';
import { createMockContext, createMockConsumer } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { AlertManager } from '../src/alerts.js';

function makeCtx(consumer, responseOverrides = {}) {
    const ctx = createMockContext({
        consumer,
        responses: {
            'api.iot.alerts.test_org_123.create': {
                status: 'ALERT_CREATE_SUCCESS',
                data: { id: 'rule_1', name: 'high_temp', type: 'THRESHOLD', metric: 'temperature', config: { scope: { type: 'DEVICE', value: 'dev_1' }, duration: 60, recovery_duration: 30, cooldown: 120 } },
            },
            'api.iot.alerts.test_org_123.update': { status: 'ALERT_UPDATE_SUCCESS', data: { id: 'rule_1', name: 'high_temp' } },
            'api.iot.alerts.test_org_123.delete': { status: 'ALERT_DELETE_SUCCESS', data: { id: 'rule_1' } },
            'api.iot.alerts.test_org_123.list': { status: 'ALERT_LIST_SUCCESS', data: [{ id: 'rule_1' }] },
            'api.iot.alerts.test_org_123.get': {
                status: 'ALERT_GET_SUCCESS',
                data: { id: 'rule_1', name: 'high_temp', type: 'THRESHOLD', metric: 'temperature', config: { scope: { type: 'DEVICE', value: 'dev_1' } } },
            },
            'api.iot.alerts.test_org_123.ack': { status: 'ALERT_ACK_SUCCESS' },
            'api.iot.alerts.test_org_123.ack_all': { status: 'ALERT_ACK_SUCCESS' },
            'api.iot.alerts.test_org_123.mute': { status: 'ALERT_RULE_MUTE_SUCCESS', data: {} },
            ...responseOverrides,
        },
    });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });
    return ctx;
}

describe('AlertManager', () => {
    describe('create', () => {
        it('creates THRESHOLD alert and returns wrapped object', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const alert = await am.create({
                name: 'high_temp',
                type: 'THRESHOLD',
                metric: 'temperature',
                config: { scope: { type: 'DEVICE', value: 'dev_1' }, operator: '>', value: 85, duration: 60, recovery_duration: 30 },
            });

            expect(alert.name).toBe('high_temp');
            expect(typeof alert.listen).toBe('function');
            expect(typeof alert.setEvaluator).toBe('function');
        });

        it('rejects invalid type', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            await expect(am.create({ name: 'x', type: 'INVALID', metric: 'temp', config: {} })).rejects.toThrow('THRESHOLD, RATE_CHANGE, or EPHEMERAL');
        });

        it('rejects EPHEMERAL with non-DEVICE scope', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            await expect(am.create({
                name: 'x', type: 'EPHEMERAL', metric: 'temp',
                config: { scope: { type: 'LOGICAL_GROUP', value: 'g1' }, duration: 10, recovery_duration: 5 },
            })).rejects.toThrow('DEVICE');
        });
    });

    describe('delete', () => {
        it('returns true on success', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            expect(await am.delete('rule_1')).toBe(true);
        });
    });

    describe('list', () => {
        it('returns array', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const alerts = await am.list();
            expect(alerts).toHaveLength(1);
        });
    });

    describe('get', () => {
        it('returns alert with listen and setEvaluator', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const alert = await am.get('high_temp');
            expect(alert.id).toBe('rule_1');
            expect(typeof alert.listen).toBe('function');
            expect(typeof alert.setEvaluator).toBe('function');
        });
    });

    describe('ack', () => {
        it('sends ack request', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const alert = { id: 'rule_1', config: { scope: { value: 'dev_1' } } };
            const result = await am.ack({ alert, acked_by: 'operator_jane' });
            expect(result).toBe(true);
            const [subject] = ctx.natsClient.request.mock.calls[0];
            expect(subject).toBe('api.iot.alerts.test_org_123.ack');
        });

        it('throws on missing acked_by', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            await expect(am.ack({ alert: { id: 'x' } })).rejects.toThrow('acked_by');
        });
    });

    describe('ackAll', () => {
        it('sends ack_all request', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const result = await am.ackAll({ alert: { id: 'rule_1' }, acked_by: 'op' });
            expect(result).toBe(true);
        });
    });

    describe('mute', () => {
        it('sends mute request with FOREVER', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const res = await am.mute({ id: 'rule_1', mute_config: { type: 'FOREVER' } });
            expect(res.status).toBe('ALERT_RULE_MUTE_SUCCESS');
        });

        it('requires mute_till for TIME_BASED', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            await expect(am.mute({ id: 'rule_1', mute_config: { type: 'TIME_BASED' } })).rejects.toThrow('mute_till');
        });

        it('rejects invalid mute type', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            await expect(am.mute({ id: 'rule_1', mute_config: { type: 'INVALID' } })).rejects.toThrow('FOREVER or TIME_BASED');
        });
    });

    describe('unmute', () => {
        it('sends CLEAR mute request', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const res = await am.unmute('rule_1');
            expect(res.status).toBe('ALERT_RULE_MUTE_SUCCESS');
        });
    });

    describe('setEvaluator', () => {
        it('throws for non-EPHEMERAL alerts', async () => {
            const ctx = makeCtx();
            const am = new AlertManager(ctx);
            const alert = await am.get('high_temp'); // type = THRESHOLD
            expect(() => alert.setEvaluator(() => true)).toThrow('EPHEMERAL');
        });
    });

    describe('listen (non-ephemeral)', () => {
        it('creates 4 JetStream consumers for alert events', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const am = new AlertManager(ctx);
            const alert = await am.get('high_temp');

            await alert.listen({
                onFire: vi.fn(),
                onResolved: vi.fn(),
                onAck: vi.fn(),
                onAckAll: vi.fn(),
            });

            // consumers.get called for: get request (mock) + 4 alert event consumers
            const alertCalls = ctx.jetstream.consumers.get.mock.calls.filter(
                ([, opts]) => opts?.filter_subjects?.includes('alerts.listen')
            );
            expect(alertCalls).toHaveLength(4);

            const subjects = alertCalls.map(([, opts]) => opts.filter_subjects);
            expect(subjects).toContain('import.test_org_123.production.alerts.listen.rule_1.fire');
            expect(subjects).toContain('import.test_org_123.production.alerts.listen.rule_1.resolved');
            expect(subjects).toContain('import.test_org_123.production.alerts.listen.rule_1.ack');
            expect(subjects).toContain('import.test_org_123.production.alerts.listen.rule_1.ack_all');
        });
    });
});
