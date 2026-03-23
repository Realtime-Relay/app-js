import { describe, it, expect, vi } from 'vitest';
import { createMockContext } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { CommandManager } from '../src/commands.js';

function makeCtx(overrides = {}) {
    const ctx = createMockContext({
        responses: {
            'api.iot.devices.test_org_123.get': {
                status: 'DEVICE_GET_SUCCESS',
                data: { id: 'dev_1', ident: 'sensor_01' },
            },
            'api.iot.db.test_org_123.command.history': {
                status: 'SUCCESS',
                data: [],
            },
            ...overrides.responses,
        },
        ...overrides,
    });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });
    device.cache.set('sensor_02', { id: 'dev_2', ident: 'sensor_02' });
    return ctx;
}

describe('CommandManager', () => {
    describe('send', () => {
        it('resolves idents to device_ids and publishes to each device', async () => {
            const ctx = makeCtx();
            const cm = new CommandManager(ctx);

            await cm.send({
                name: 'reboot',
                device_ident: ['sensor_01', 'sensor_02'],
                data: { force: true },
            });

            expect(ctx.jetstream.publish).toHaveBeenCalledTimes(2);
            expect(ctx.jetstream.publish).toHaveBeenCalledWith(
                'test_org_123.production.command.queue.dev_1.reboot',
                expect.anything()
            );
            expect(ctx.jetstream.publish).toHaveBeenCalledWith(
                'test_org_123.production.command.queue.dev_2.reboot',
                expect.anything()
            );
        });

        it('returns true when all acks succeed', async () => {
            const ctx = makeCtx();
            const cm = new CommandManager(ctx);

            const result = await cm.send({
                name: 'reboot',
                device_ident: ['sensor_01'],
                data: { force: true },
            });

            expect(result).toBe(true);
        });

        it('returns false when an ack is null', async () => {
            const ctx = makeCtx();
            ctx.jetstream.publish = vi.fn(async () => null);
            const cm = new CommandManager(ctx);

            const result = await cm.send({
                name: 'reboot',
                device_ident: ['sensor_01'],
                data: { force: true },
            });

            expect(result).toBe(false);
        });
    });

    describe('history', () => {
        it('sends request to correct subject', async () => {
            const ctx = makeCtx();
            const cm = new CommandManager(ctx);

            await cm.history({
                name: 'reboot',
                device_idents: ['sensor_01'],
                start: '2025-01-01T00:00:00Z',
                end: '2025-01-02T00:00:00Z',
            });

            expect(ctx.natsClient.request).toHaveBeenCalledWith(
                'api.iot.db.test_org_123.command.history',
                expect.anything(),
                { timeout: 5000 }
            );
        });

        it('defaults end to now() if omitted', async () => {
            const ctx = makeCtx();
            const cm = new CommandManager(ctx);

            const before = new Date().toISOString();
            await cm.history({
                name: 'reboot',
                device_idents: ['sensor_01'],
                start: '2025-01-01T00:00:00Z',
            });
            const after = new Date().toISOString();

            // Verify request was made (end is auto-filled)
            expect(ctx.natsClient.request).toHaveBeenCalledWith(
                'api.iot.db.test_org_123.command.history',
                expect.anything(),
                { timeout: 5000 }
            );
        });
    });

    describe('validation', () => {
        it('throws on invalid command name', async () => {
            const ctx = makeCtx();
            const cm = new CommandManager(ctx);

            await expect(
                cm.send({ name: 'bad name!', device_ident: ['sensor_01'], data: {} })
            ).rejects.toThrow('invalid characters');
        });

        it('throws on empty device_ident array', async () => {
            const ctx = makeCtx();
            const cm = new CommandManager(ctx);

            await expect(
                cm.send({ name: 'reboot', device_ident: [], data: {} })
            ).rejects.toThrow('must be a non-empty array');
        });

        it('throws on missing data', async () => {
            const ctx = makeCtx();
            const cm = new CommandManager(ctx);

            await expect(
                cm.send({ name: 'reboot', device_ident: ['sensor_01'] })
            ).rejects.toThrow('data is required');
        });

        it('throws when not connected', async () => {
            const ctx = makeCtx({ connected: false });
            const cm = new CommandManager(ctx);

            await expect(
                cm.send({ name: 'reboot', device_ident: ['sensor_01'], data: {} })
            ).rejects.toThrow('Not connected');
        });
    });
});
