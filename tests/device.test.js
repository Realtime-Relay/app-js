import { describe, it, expect, vi, beforeEach } from 'vitest';
import { DeviceManager } from '../src/device.js';
import { createMockContext } from './setup.js';

const DEVICE_1 = {
    id: 'dev_1',
    ident: 'sensor_01',
    env: 'production',
    schema: { type: 'temperature' },
    config: { interval: 30 },
};

const DEVICE_2 = {
    id: 'dev_2',
    ident: 'sensor_02',
    env: 'production',
    schema: { type: 'humidity' },
    config: { interval: 60 },
};

function buildResponses(extra = {}) {
    return {
        'api.iot.devices.test_org_123.create': {
            status: 'DEVICE_CREATE_SUCCESS',
            data: DEVICE_1,
        },
        'api.iot.devices.test_org_123.update': {
            status: 'DEVICE_UPDATE_SUCCESS',
            data: { device: { ...DEVICE_1, config: { interval: 10 } } },
        },
        'api.iot.devices.test_org_123.delete': {
            status: 'DEVICE_DELETE_SUCCESS',
            data: {},
        },
        'api.iot.devices.test_org_123.list': {
            status: 'DEVICE_FETCH_SUCCESS',
            data: { devices: [DEVICE_1, DEVICE_2] },
        },
        'api.iot.devices.test_org_123.get': {
            status: 'DEVICE_GET_SUCCESS',
            data: DEVICE_1,
        },
        ...extra,
    };
}

describe('DeviceManager', () => {
    let ctx;
    let dm;

    beforeEach(() => {
        ctx = createMockContext({ responses: buildResponses() });
        dm = new DeviceManager(ctx);
    });

    // ── create ────────────────────────────────────────────────

    describe('create', () => {
        it('sends correct NATS subject and payload on success', async () => {
            const res = await dm.create({
                ident: 'sensor_01',
                schema: { type: 'temperature' },
                config: { interval: 30 },
            });

            expect(ctx.natsClient.request).toHaveBeenCalledOnce();
            const [subject, payload] = ctx.natsClient.request.mock.calls[0];
            expect(subject).toBe('api.iot.devices.test_org_123.create');

            // Payload is a Uint8Array from JSONCodec.encode()
            expect(payload).toBeDefined();
            expect(res.status).toBe('DEVICE_CREATE_SUCCESS');
            expect(res.data).toEqual(DEVICE_1);
        });

        it('adds device to cache on success', async () => {
            await dm.create({
                ident: 'sensor_01',
                schema: { type: 'temperature' },
                config: { interval: 30 },
            });

            expect(dm.cache.has('sensor_01')).toBe(true);
            expect(dm.cache.get('sensor_01')).toEqual(DEVICE_1);
        });

        it('returns failure response and does not cache on failure', async () => {
            const failCtx = createMockContext({
                responses: {
                    'api.iot.devices.test_org_123.create': {
                        status: 'DEVICE_CREATE_FAILED',
                        data: { error: 'duplicate ident' },
                    },
                },
            });
            const failDm = new DeviceManager(failCtx);

            const res = await failDm.create({
                ident: 'sensor_01',
                schema: { type: 'temperature' },
                config: { interval: 30 },
            });

            expect(res.status).toBe('DEVICE_CREATE_FAILED');
            expect(failDm.cache.has('sensor_01')).toBe(false);
        });
    });

    // ── update ────────────────────────────────────────────────

    describe('update', () => {
        it('resolves ident to id and sends update request', async () => {
            // Seed cache so resolveDeviceId finds it
            await dm.list();

            const res = await dm.update({
                ident: 'sensor_01',
                config: { interval: 10 },
            });

            expect(res.status).toBe('DEVICE_UPDATE_SUCCESS');

            // Verify the update call used the resolved device id
            const updateCall = ctx.natsClient.request.mock.calls.find(
                ([subj]) => subj === 'api.iot.devices.test_org_123.update'
            );
            expect(updateCall).toBeDefined();
        });

        it('updates cache with returned device data', async () => {
            await dm.list();
            await dm.update({ ident: 'sensor_01', config: { interval: 10 } });

            const cached = dm.cache.get('sensor_01');
            expect(cached.config).toEqual({ interval: 10 });
        });
    });

    // ── delete ────────────────────────────────────────────────

    describe('delete', () => {
        it('sends delete request and returns true on success', async () => {
            // Seed cache
            await dm.list();
            expect(dm.cache.has('sensor_01')).toBe(true);

            const result = await dm.delete('sensor_01');

            expect(result).toBe(true);
            const deleteCall = ctx.natsClient.request.mock.calls.find(
                ([subj]) => subj === 'api.iot.devices.test_org_123.delete'
            );
            expect(deleteCall).toBeDefined();
        });

        it('removes device from cache on success', async () => {
            await dm.list();
            await dm.delete('sensor_01');
            expect(dm.cache.has('sensor_01')).toBe(false);
        });
    });

    // ── list ──────────────────────────────────────────────────

    describe('list', () => {
        it('fetches all devices and returns array', async () => {
            const devices = await dm.list();

            expect(devices).toHaveLength(2);
            expect(devices[0].ident).toBe('sensor_01');
            expect(devices[1].ident).toBe('sensor_02');
        });

        it('populates cache with all devices', async () => {
            await dm.list();

            expect(dm.cache.size).toBe(2);
            expect(dm.cache.get('sensor_01')).toEqual(DEVICE_1);
            expect(dm.cache.get('sensor_02')).toEqual(DEVICE_2);
        });
    });

    // ── get ───────────────────────────────────────────────────

    describe('get', () => {
        it('returns cached device on cache hit', async () => {
            // Populate cache via list
            await dm.list();
            ctx.natsClient.request.mockClear();

            const device = await dm.get({ ident: 'sensor_01' });

            expect(device).toEqual(DEVICE_1);
            // Should not have made a new NATS request for get
            const getCalls = ctx.natsClient.request.mock.calls.filter(
                ([subj]) => subj === 'api.iot.devices.test_org_123.get'
            );
            expect(getCalls).toHaveLength(0);
        });

        it('makes NATS request on cache miss', async () => {
            const device = await dm.get({ ident: 'sensor_01' });

            expect(device).toEqual(DEVICE_1);
            const getCalls = ctx.natsClient.request.mock.calls.filter(
                ([subj]) => subj === 'api.iot.devices.test_org_123.get'
            );
            expect(getCalls).toHaveLength(1);
        });
    });

    // ── resolveDeviceId ───────────────────────────────────────

    describe('resolveDeviceId', () => {
        it('resolves from cache when available', async () => {
            await dm.list();
            ctx.natsClient.request.mockClear();

            const id = await dm.resolveDeviceId('sensor_01');
            expect(id).toBe('dev_1');

            // No additional NATS request should have been made
            expect(ctx.natsClient.request).not.toHaveBeenCalled();
        });

        it('falls back to get() on cache miss', async () => {
            const id = await dm.resolveDeviceId('sensor_01');
            expect(id).toBe('dev_1');

            const getCalls = ctx.natsClient.request.mock.calls.filter(
                ([subj]) => subj === 'api.iot.devices.test_org_123.get'
            );
            expect(getCalls).toHaveLength(1);
        });

        it('throws when device is not found', async () => {
            const emptyCtx = createMockContext({
                responses: {
                    'api.iot.devices.test_org_123.get': {
                        status: 'DEVICE_NOT_FOUND',
                        data: {},
                    },
                },
            });
            const emptyDm = new DeviceManager(emptyCtx);

            await expect(emptyDm.resolveDeviceId('nonexistent'))
                .rejects.toThrow('Device not found: nonexistent');
        });
    });

    // ── validation ────────────────────────────────────────────

    describe('validation', () => {
        it('throws on null ident', async () => {
            await expect(
                dm.create({ ident: null, schema: {}, config: {} })
            ).rejects.toThrow('ident is required');
        });

        it('throws on invalid characters in ident', async () => {
            await expect(
                dm.create({ ident: 'bad ident!', schema: {}, config: {} })
            ).rejects.toThrow('ident contains invalid characters');
        });

        it('throws when not connected', async () => {
            const disconnectedCtx = createMockContext({
                connected: false,
                responses: buildResponses(),
            });
            const disconnectedDm = new DeviceManager(disconnectedCtx);

            await expect(
                disconnectedDm.create({ ident: 'sensor_01', schema: {}, config: {} })
            ).rejects.toThrow('Not connected');
        });

        it('throws when ident is not a string', async () => {
            await expect(
                dm.create({ ident: 123, schema: {}, config: {} })
            ).rejects.toThrow('ident must be a string');
        });

        it('throws when schema is not an object', async () => {
            await expect(
                dm.create({ ident: 'valid_ident', schema: 'not_obj', config: {} })
            ).rejects.toThrow('schema must be an object');
        });
    });

    // ── Cache TTL ─────────────────────────────────────────────

    describe('Cache TTL', () => {
        it('invalidates cache after 2+ hours', async () => {
            await dm.list();
            expect(dm.cache.size).toBe(2);

            // Advance time past the 2-hour TTL
            const TWO_HOURS_MS = 2 * 60 * 60 * 1000;
            vi.useFakeTimers();
            vi.setSystemTime(Date.now() + TWO_HOURS_MS + 1);

            // get() should make a NATS request because cache is stale
            ctx.natsClient.request.mockClear();
            await dm.get({ ident: 'sensor_01' });

            const getCalls = ctx.natsClient.request.mock.calls.filter(
                ([subj]) => subj === 'api.iot.devices.test_org_123.get'
            );
            expect(getCalls).toHaveLength(1);

            vi.useRealTimers();
        });

        it('uses cache within 2-hour window', async () => {
            vi.useFakeTimers();
            vi.setSystemTime(Date.now());

            await dm.list();
            ctx.natsClient.request.mockClear();

            // Advance just under 2 hours
            const UNDER_TWO_HOURS = 2 * 60 * 60 * 1000 - 1000;
            vi.setSystemTime(Date.now() + UNDER_TWO_HOURS);

            await dm.get({ ident: 'sensor_01' });

            // Should have served from cache, no get request
            const getCalls = ctx.natsClient.request.mock.calls.filter(
                ([subj]) => subj === 'api.iot.devices.test_org_123.get'
            );
            expect(getCalls).toHaveLength(0);

            vi.useRealTimers();
        });
    });
});
