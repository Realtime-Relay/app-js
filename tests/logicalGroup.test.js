import { describe, it, expect, vi } from 'vitest';
import { createMockContext, createMockConsumer } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { LogicalGroupManager } from '../src/logicalGroup.js';

function makeCtx(consumer, responseOverrides = {}) {
    const ctx = createMockContext({
        consumer,
        responses: {
            'api.iot.cohort.test_org_123.logical.create': { status: 'LOGICAL_GROUP_CREATE_SUCCESS', data: { id: 'g_new', name: 'floor_1' } },
            'api.iot.cohort.test_org_123.logical.update': { status: 'LOGICAL_GROUP_UPDATE_SUCCESS', data: { id: 'g1', name: 'floor_1_updated' } },
            'api.iot.cohort.test_org_123.logical.delete': { status: 'LOGICAL_GROUP_DELETE_SUCCESS' },
            'api.iot.cohort.test_org_123.logical.list': { status: 'LOGICAL_GROUP_LIST_SUCCESS', data: [{ id: 'g1', name: 'floor_1' }] },
            'api.iot.cohort.test_org_123.logical.get': { status: 'LOGICAL_GROUP_GET_SUCCESS', data: { id: 'g1', name: 'floor_1' } },
            'api.iot.cohort.test_org_123.logical.device.list': { status: 'LOGICAL_DEVICE_LIST_SUCCESS', data: [{ id: 'dev_1' }] },
            ...responseOverrides,
        },
    });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });
    device.cache.set('sensor_02', { id: 'dev_2', ident: 'sensor_02' });
    return ctx;
}

describe('LogicalGroupManager', () => {
    describe('create', () => {
        it('resolves idents and sends correct subject', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            const group = await lgm.create({ name: 'floor_1', tags: ['temp'], device_idents: ['sensor_01'] });
            expect(group.id).toBe('g_new');
            expect(typeof group.stream).toBe('function');
            const [subject] = ctx.natsClient.request.mock.calls[0];
            expect(subject).toBe('api.iot.cohort.test_org_123.logical.create');
        });

        it('throws on invalid name', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            await expect(lgm.create({ name: 'a.b', tags: [], device_idents: [] })).rejects.toThrow('invalid characters');
        });
    });

    describe('update', () => {
        it('sends update request', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            const group = await lgm.update({
                id: 'g1',
                tags: { add: ['humidity'], remove: [] },
                devices: { add: ['sensor_02'], remove: [] },
            });
            expect(group.id).toBe('g1');
            expect(typeof group.stream).toBe('function');
        });
    });

    describe('delete', () => {
        it('returns true on success', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            expect(await lgm.delete('g1')).toBe(true);
        });
    });

    describe('list', () => {
        it('returns groups array', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            const groups = await lgm.list();
            expect(groups).toHaveLength(1);
            expect(groups[0].name).toBe('floor_1');
        });
    });

    describe('get', () => {
        it('returns group with stream and off methods', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            const group = await lgm.get('g1');
            expect(group.name).toBe('floor_1');
            expect(typeof group.stream).toBe('function');
            expect(typeof group.off).toBe('function');
        });
    });

    describe('listDevices', () => {
        it('returns devices array', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            const devices = await lgm.listDevices('g1');
            expect(devices).toHaveLength(1);
        });
    });

    describe('stream', () => {
        it('creates consumer with correct subject', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const lgm = new LogicalGroupManager(ctx);
            const group = await lgm.get('g1');

            await group.stream({ callback: vi.fn() });

            // consumers.get called twice: once for get request mock, once for stream
            const streamCall = ctx.jetstream.consumers.get.mock.calls.find(
                ([, opts]) => opts?.filter_subjects?.includes('group.listen')
            );
            expect(streamCall).toBeDefined();
            expect(streamCall[1].filter_subjects).toBe('import.test_org_123.production.group.listen.g1');
        });
    });

    describe('off', () => {
        it('deletes the consumer for the group', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const lgm = new LogicalGroupManager(ctx);
            const group = await lgm.get('g1');

            await group.stream({ callback: vi.fn() });
            await group.off();

            expect(consumer.delete).toHaveBeenCalled();
        });

        it('no-op if no active stream', async () => {
            const ctx = makeCtx();
            const lgm = new LogicalGroupManager(ctx);
            const group = await lgm.get('g1');

            // Should not throw
            await group.off();
        });
    });
});
