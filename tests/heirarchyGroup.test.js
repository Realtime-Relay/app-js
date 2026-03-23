import { describe, it, expect, vi } from 'vitest';
import { createMockContext, createMockConsumer } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { HeirarchyGroupManager } from '../src/heirarchyGroup.js';

function makeCtx(consumer) {
    const ctx = createMockContext({
        consumer,
        responses: {
            'api.iot.cohort.test_org_123.heirarchy.create': { status: 'HEIRARCHY_GROUP_CREATE_SUCCESS' },
            'api.iot.cohort.test_org_123.heirarchy.update': { status: 'HIERARCHY_GROUP_UPDATE_SUCCESS' },
            'api.iot.cohort.test_org_123.heirarchy.delete': { status: 'HEIRARCHY_GROUP_DELETE_SUCCESS' },
            'api.iot.cohort.test_org_123.heirarchy.list': { status: 'HEIRARCHY_GROUP_LIST_SUCCESS', data: [{ id: 'hg1', name: 'building_a' }] },
            'api.iot.cohort.test_org_123.heirarchy.get': { status: 'HEIRARCHY_GROUP_GET_SUCCESS', data: { id: 'hg1', name: 'building_a' } },
            'api.iot.cohort.test_org_123.heirarchy.device.list': { status: 'HEIRARCHY_DEVICE_LIST_SUCCESS', data: [{ id: 'dev_1' }] },
        },
    });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });
    return ctx;
}

describe('HeirarchyGroupManager', () => {
    describe('create', () => {
        it('sends correct subject with hierarchy name', async () => {
            const ctx = makeCtx();
            const hgm = new HeirarchyGroupManager(ctx);
            const res = await hgm.create({ name: 'building_a', heirarchy: 'campus.building_a', device_idents: ['sensor_01'] });
            expect(res.status).toBe('HEIRARCHY_GROUP_CREATE_SUCCESS');
            const [subject] = ctx.natsClient.request.mock.calls[0];
            expect(subject).toBe('api.iot.cohort.test_org_123.heirarchy.create');
        });

        it('validates hierarchy name allows dots', async () => {
            const ctx = makeCtx();
            const hgm = new HeirarchyGroupManager(ctx);
            // Dots allowed in hierarchy names
            await expect(hgm.create({ name: 'a.b', heirarchy: 'x.y', device_idents: [] })).resolves.toBeDefined();
        });

        it('rejects wildcards in hierarchy name on create', async () => {
            const ctx = makeCtx();
            const hgm = new HeirarchyGroupManager(ctx);
            await expect(hgm.create({ name: 'a', heirarchy: 'x.*', device_idents: [] })).rejects.toThrow('invalid characters');
        });
    });

    describe('delete', () => {
        it('returns true on success', async () => {
            const ctx = makeCtx();
            const hgm = new HeirarchyGroupManager(ctx);
            expect(await hgm.delete('hg1')).toBe(true);
        });
    });

    describe('list', () => {
        it('returns groups array', async () => {
            const ctx = makeCtx();
            const hgm = new HeirarchyGroupManager(ctx);
            const groups = await hgm.list();
            expect(groups).toHaveLength(1);
        });
    });

    describe('get', () => {
        it('returns group with stream method', async () => {
            const ctx = makeCtx();
            const hgm = new HeirarchyGroupManager(ctx);
            const group = await hgm.get('hg1');
            expect(group.name).toBe('building_a');
            expect(typeof group.stream).toBe('function');
        });
    });

    describe('listDevices', () => {
        it('returns devices array', async () => {
            const ctx = makeCtx();
            const hgm = new HeirarchyGroupManager(ctx);
            const devices = await hgm.listDevices('hg1');
            expect(devices).toHaveLength(1);
        });
    });

    describe('stream', () => {
        it('creates consumer with correct subject', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const hgm = new HeirarchyGroupManager(ctx);
            const group = await hgm.get('hg1');

            await group.stream({ callback: vi.fn() });

            const streamCall = ctx.jetstream.consumers.get.mock.calls.find(
                ([, opts]) => opts?.filter_subjects?.includes('heirarchy.listen')
            );
            expect(streamCall).toBeDefined();
            expect(streamCall[1].filter_subjects).toBe('import.test_org_123.production.heirarchy.listen.hg1');
        });

        it('accepts wildcard hierarchy filter', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const hgm = new HeirarchyGroupManager(ctx);
            const group = await hgm.get('hg1');

            // Should not throw — wildcards allowed in stream filter
            await expect(group.stream({ heirarchy: 'building_a.*', callback: vi.fn() })).resolves.toBeUndefined();
        });
    });
});
