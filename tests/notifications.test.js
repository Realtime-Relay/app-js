import { describe, it, expect, vi } from 'vitest';
import { createMockContext } from './setup.js';
import { NotificationManager } from '../src/notifications.js';

function makeCtx(responseOverrides = {}) {
    return createMockContext({
        responses: {
            'api.iot.notification.test_org_123.create': { status: 'NOTIFICATION_CREATE_SUCCESS', data: { name: 'ops', type: 'WEBHOOK' } },
            'api.iot.notification.test_org_123.update': { status: 'NOTIFICATION_UPDATE_SUCCESS', data: { name: 'ops' } },
            'api.iot.notification.test_org_123.delete': { status: 'NOTIFICATION_DELETE_SUCCESS' },
            'api.iot.notification.test_org_123.list': { status: 'NOTIFICATION_LIST_SUCCESS', data: [{ name: 'ops' }] },
            'api.iot.notification.test_org_123.get': { status: 'NOTIFICATION_FETCH_SUCCESS', data: { name: 'ops', type: 'WEBHOOK' } },
            ...responseOverrides,
        },
    });
}

describe('NotificationManager', () => {
    describe('create', () => {
        it('creates WEBHOOK notification', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            const res = await nm.create({
                name: 'ops',
                type: 'WEBHOOK',
                config: { endpoint: 'https://hook.example.com', headers: {} },
            });
            expect(res.status).toBe('NOTIFICATION_CREATE_SUCCESS');
            const [subject] = ctx.natsClient.request.mock.calls[0];
            expect(subject).toBe('api.iot.notification.test_org_123.create');
        });

        it('creates EMAIL notification', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            const res = await nm.create({
                name: 'alerts_email',
                type: 'EMAIL',
                config: { recipients: ['a@b.com'], subject: 'Alert', template: '{{rule.name}}' },
            });
            expect(res.status).toBe('NOTIFICATION_CREATE_SUCCESS');
        });

        it('throws on invalid type', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            await expect(nm.create({ name: 'x', type: 'SMS', config: {} })).rejects.toThrow('WEBHOOK');
        });

        it('throws on missing WEBHOOK endpoint', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            await expect(nm.create({ name: 'x', type: 'WEBHOOK', config: {} })).rejects.toThrow('endpoint');
        });

        it('throws on missing EMAIL recipients', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            await expect(nm.create({ name: 'x', type: 'EMAIL', config: { subject: 'x', template: 'x' } })).rejects.toThrow('recipients');
        });
    });

    describe('delete', () => {
        it('returns true on success', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            expect(await nm.delete('notif_1')).toBe(true);
        });

        it('throws on empty id', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            await expect(nm.delete('')).rejects.toThrow('required');
        });
    });

    describe('list', () => {
        it('returns array', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            const list = await nm.list();
            expect(list).toHaveLength(1);
        });
    });

    describe('get', () => {
        it('returns notification', async () => {
            const ctx = makeCtx();
            const nm = new NotificationManager(ctx);
            const res = await nm.get('notif_1');
            expect(res.status).toBe('NOTIFICATION_FETCH_SUCCESS');
        });
    });
});
