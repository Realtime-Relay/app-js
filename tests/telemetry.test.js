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
                data: { id: 'dev_1', ident: 'sensor_01' },
            },
        },
        ...overrides,
    });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });
    device.cache.set('sensor_02', { id: 'dev_2', ident: 'sensor_02' });
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
                    filter_subjects: 'test_org_123.production.telemetry.sensor_01.temperature',
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
                        data: { id: 'dev_1', ident: 'sensor_01' },
                    },
                },
            });
            // Return different consumers on successive calls
            const consumers = [consumer1, consumer2];
            ctx.jetstream.consumers.get = vi.fn(async () => consumers[callCount++] || createMockConsumer());
            const device = new DeviceManager(ctx);
            ctx.device = device;
            device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });

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
            const ctx = createMockContext();
            ctx.jetstream.consumers.get = vi.fn(async () => [consumer1, consumer2][callCount++]);
            const device = new DeviceManager(ctx);
            ctx.device = device;
            device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });

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
                start: '2025-01-01T00:00:00Z',
                end: '2025-01-02T00:00:00Z',
            });

            expect(ctx.natsClient.request).toHaveBeenCalledWith(
                'api.iot.db.test_org_123.telemetry.history',
                expect.anything(),
                { timeout: 5000 }
            );
        });

        it('validates start < end', async () => {
            const { ctx } = makeCtx();
            const tm = new TelemetryManager(ctx);

            await expect(
                tm.history({
                    device_ident: 'sensor_01',
                    fields: ['temperature'],
                    start: '2025-01-02T00:00:00Z',
                    end: '2025-01-01T00:00:00Z',
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
});
