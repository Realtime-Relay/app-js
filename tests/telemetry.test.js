import { describe, it, expect, vi } from 'vitest';
import { createMockContext, createMockConsumer } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { TelemetryManager } from '../src/telemetry.js';

function makeCtx(overrides = {}) {
    const consumer = createMockConsumer();
    const ctx = createMockContext({
        consumer,
        responses: {
            'api.iot.devices.test_org_123.get': {
                status: 'DEVICE_GET_SUCCESS',
                data: { id: 'dev_1', ident: 'sensor_01', schema: { temperature: 'float', humidity: 'float' } },
            },
        },
        ...overrides,
    });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01', schema: { temperature: 'float', humidity: 'float' } });
    device.cache.set('sensor_02', { id: 'dev_2', ident: 'sensor_02', schema: { pressure: 'float' } });
    return { ctx, consumer };
}

describe('TelemetryManager', () => {
    describe('stream', () => {
        it('creates JetStream consumer with correct subject', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await tm.stream({
                device_ident: 'sensor_01',
                metric: 'temperature',
                callback: () => {},
            });

            expect(ctx.jetstream.consumers.get).toHaveBeenCalledWith(
                'test_org_123_stream',
                expect.objectContaining({
                    filter_subjects: 'test_org_123.production.telemetry.dev_1.temperature',
                })
            );
        });

        it('returns true on new subscription', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            const result = await tm.stream({
                device_ident: 'sensor_01',
                metric: 'temperature',
                callback: () => {},
            });

            expect(result).toBe(true);
        });

        it('returns false on duplicate subscription', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            const params = {
                device_ident: 'sensor_01',
                metric: 'temperature',
                callback: () => {},
            };

            await tm.stream(params);
            const result = await tm.stream(params);

            expect(result).toBe(false);
        });
    });

    describe('off', () => {
        it('without metric kills all consumers for device', async () => {
            const consumer1 = createMockConsumer();
            const consumer2 = createMockConsumer();
            let callCount = 0;
            const ctx = createMockContext({
                responses: {
                    'api.iot.devices.test_org_123.get': {
                        status: 'DEVICE_GET_SUCCESS',
                        data: { id: 'dev_1', ident: 'sensor_01', schema: { temperature: 'float', humidity: 'float' } },
                    },
                },
            });
            // Return different consumers on successive calls
            const consumers = [consumer1, consumer2];
            ctx.jetstream.consumers.get = vi.fn(async () => consumers[callCount++] || createMockConsumer());
            const device = new DeviceManager(ctx);
            ctx.device = device;
            device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01', schema: { temperature: 'float', humidity: 'float' } });

            const tm = new TelemetryManager(ctx);

            await tm.stream({ device_ident: 'sensor_01', metric: 'temperature', callback: () => {} });
            await tm.stream({ device_ident: 'sensor_01', metric: 'humidity', callback: () => {} });

            await tm.off({ device_ident: 'sensor_01' });

            expect(consumer1.delete).toHaveBeenCalled();
            expect(consumer2.delete).toHaveBeenCalled();
        });

        it('with metric array kills only specific consumers', async () => {
            const consumer1 = createMockConsumer();
            const consumer2 = createMockConsumer();
            let callCount = 0;
            const ctx = createMockContext({
                responses: {
                    'api.iot.devices.test_org_123.get': {
                        status: 'DEVICE_GET_SUCCESS',
                        data: { id: 'dev_1', ident: 'sensor_01', schema: { temperature: 'float', humidity: 'float' } },
                    },
                },
            });
            ctx.jetstream.consumers.get = vi.fn(async () => [consumer1, consumer2][callCount++]);
            const device = new DeviceManager(ctx);
            ctx.device = device;
            device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01', schema: { temperature: 'float', humidity: 'float' } });

            const tm = new TelemetryManager(ctx);

            await tm.stream({ device_ident: 'sensor_01', metric: 'temperature', callback: () => {} });
            await tm.stream({ device_ident: 'sensor_01', metric: 'humidity', callback: () => {} });

            await tm.off({ device_ident: 'sensor_01', metric: ['temperature'] });

            expect(consumer1.delete).toHaveBeenCalled();
            expect(consumer2.delete).not.toHaveBeenCalled();
        });
    });

    describe('history', () => {
        it('sends correct request with resolved device_id', async () => {
            const { ctx } = makeCtx({
                responses: {
                    'api.iot.devices.test_org_123.get': {
                        status: 'DEVICE_GET_SUCCESS',
                        data: { id: 'dev_1', ident: 'sensor_01' },
                    },
                    'api.iot.db.test_org_123.telemetry.history': {
                        status: 'SUCCESS',
                        data: [],
                    },
                },
            });
            const tm = new TelemetryManager(ctx);

            await tm.history({
                device_ident: 'sensor_01',
                fields: ['temperature'],
                start: '2025-01-01T00:00:00.000Z',
                end: '2025-01-02T00:00:00.000Z',
            });

            expect(ctx.natsClient.request).toHaveBeenCalledWith(
                'api.iot.db.test_org_123.telemetry.history',
                expect.anything(),
                { timeout: 20000 }
            );
        });

        it('throws when fields contain invalid schema keys', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.history({
                    device_ident: 'sensor_01',
                    fields: ['temperature', 'nonexistent'],
                    start: '2025-01-01T00:00:00.000Z',
                    end: '2025-01-02T00:00:00.000Z',
                })
            ).rejects.toThrow('fields contain invalid metrics: nonexistent');
        });

        it('validates start < end', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.history({
                    device_ident: 'sensor_01',
                    fields: ['temperature'],
                    start: '2025-01-02T00:00:00.000Z',
                    end: '2025-01-01T00:00:00.000Z',
                })
            ).rejects.toThrow('start must be before end');
        });
    });

    describe('validation', () => {
        it('throws on invalid ident', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.stream({ device_ident: 'bad ident!', metric: 'temp', callback: () => {} })
            ).rejects.toThrow('invalid characters');
        });

        it('throws on missing callback', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.stream({ device_ident: 'sensor_01', metric: 'temp' })
            ).rejects.toThrow('callback is required');
        });

        it('throws on missing metric', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.stream({ device_ident: 'sensor_01', callback: () => {} })
            ).rejects.toThrow('metric is required');
        });

        it('throws when not connected', async () => {
            const { ctx } = makeCtx({ connected: false });
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.stream({ device_ident: 'sensor_01', metric: 'temp', callback: () => {} })
            ).rejects.toThrow('Not connected');
        });
    });

    describe('metric validation', () => {
        it('allows * as metric', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            const result = await tm.stream({
                device_ident: 'sensor_01',
                metric: '*',
                callback: () => {},
            });
            expect(result).toBe(true);
        });

        it('allows metric that exists in device.schema', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            const result = await tm.stream({
                device_ident: 'sensor_01',
                metric: 'temperature',
                callback: () => {},
            });
            expect(result).toBe(true);
        });

        it('throws when metric is not a key in device.schema', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.stream({ device_ident: 'sensor_01', metric: 'nonexistent', callback: () => {} })
            ).rejects.toThrow('metric "nonexistent" is not a valid key in device schema');
        });

        it('includes valid keys in error message', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.stream({ device_ident: 'sensor_01', metric: 'pressure', callback: () => {} })
            ).rejects.toThrow('temperature, humidity');
        });
    });

    describe('latest', () => {
        it('sends history request with last 24h time range', async () => {
            const { ctx } = makeCtx({
                responses: {
                    'api.iot.devices.test_org_123.get': {
                        status: 'DEVICE_GET_SUCCESS',
                        data: { id: 'dev_1', ident: 'sensor_01', schema: { temperature: 'float' } },
                    },
                    'api.iot.db.test_org_123.telemetry.history': {
                        status: 'SUCCESS',
                        data: [{ value: 23.5, time: '2026-03-24T00:00:00Z' }],
                    },
                },
            });
            const tm = new TelemetryManager(ctx);

            const result = await tm.latest({
                device_ident: 'sensor_01',
                fields: ['temperature'],
            });

            expect(ctx.natsClient.request).toHaveBeenCalledWith(
                'api.iot.db.test_org_123.telemetry.history',
                expect.anything(),
                { timeout: 20000 }
            );
            expect(result.status).toBe('SUCCESS');
        });

        it('throws on missing fields', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.latest({ device_ident: 'sensor_01' })
            ).rejects.toThrow('non-empty array');
        });

        it('throws on missing device_ident', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.latest({ fields: ['temperature'] })
            ).rejects.toThrow('device_ident is required');
        });
    });
});
