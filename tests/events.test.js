import { describe, it, expect, vi } from 'vitest';
import { createMockContext, createMockConsumer } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { EventManager } from '../src/events.js';

function makeCtx(consumer) {
    const ctx = createMockContext({ consumer });
    ctx.device = new DeviceManager(ctx);
    return ctx;
}

describe('EventManager', () => {
    describe('stream', () => {
        it('creates consumer with wildcard subject', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const em = new EventManager(ctx);

            const result = await em.stream({ name: 'door_opened', callback: vi.fn() });

            expect(result).toBe(true);
            expect(ctx.jetstream.consumers.get).toHaveBeenCalledTimes(1);
            const [, opts] = ctx.jetstream.consumers.get.mock.calls[0];
            expect(opts.filter_subjects).toBe('test_org_123.production.events.*.door_opened');
        });

        it('returns false on duplicate subscription', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const em = new EventManager(ctx);

            await em.stream({ name: 'door_opened', callback: vi.fn() });
            const result = await em.stream({ name: 'door_opened', callback: vi.fn() });

            expect(result).toBe(false);
        });

        it('throws on invalid name', async () => {
            const ctx = makeCtx();
            const em = new EventManager(ctx);
            await expect(em.stream({ name: 'a.b', callback: vi.fn() })).rejects.toThrow('invalid characters');
        });

        it('throws on non-function callback', async () => {
            const ctx = makeCtx();
            const em = new EventManager(ctx);
            await expect(em.stream({ name: 'test', callback: 'not_fn' })).rejects.toThrow('must be a function');
        });

        it('throws when not connected', async () => {
            const ctx = makeCtx();
            ctx.connected = false;
            const em = new EventManager(ctx);
            await expect(em.stream({ name: 'test', callback: vi.fn() })).rejects.toThrow('Not connected');
        });
    });

    describe('off', () => {
        it('deletes consumer for event name', async () => {
            const consumer = createMockConsumer();
            const ctx = makeCtx(consumer);
            const em = new EventManager(ctx);

            await em.stream({ name: 'door_opened', callback: vi.fn() });
            await em.off({ name: 'door_opened' });

            expect(consumer.delete).toHaveBeenCalled();
        });

        it('is a no-op for unsubscribed events', async () => {
            const ctx = makeCtx();
            const em = new EventManager(ctx);
            await expect(em.off({ name: 'nonexistent' })).resolves.toBeUndefined();
        });
    });
});
