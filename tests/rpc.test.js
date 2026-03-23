import { describe, it, expect, vi } from 'vitest';
import { createMockContext } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { RPCManager } from '../src/rpc.js';

function makeCtx() {
    const ctx = createMockContext({
        responses: {
            'test_org_123.production.command.rpc.dev_1.get_diagnostics': {
                cpu: 45, memory: 72,
            },
        },
    });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('gateway_01', { id: 'dev_1', ident: 'gateway_01' });
    return ctx;
}

describe('RPCManager', () => {
    describe('call', () => {
        it('resolves ident and sends request to correct subject', async () => {
            const ctx = makeCtx();
            const rpc = new RPCManager(ctx);

            const res = await rpc.call({
                device_ident: 'gateway_01',
                name: 'get_diagnostics',
                timeout: 20,
                data: { include_logs: true },
            });

            expect(ctx.natsClient.request).toHaveBeenCalledTimes(1);
            const [subject, , opts] = ctx.natsClient.request.mock.calls[0];
            expect(subject).toBe('test_org_123.production.command.rpc.dev_1.get_diagnostics');
            expect(opts.timeout).toBe(20000);
        });

        it('throws on missing device_ident', async () => {
            const ctx = makeCtx();
            const rpc = new RPCManager(ctx);
            await expect(rpc.call({ name: 'x', timeout: 5, data: {} })).rejects.toThrow('device_ident');
        });

        it('throws on missing name', async () => {
            const ctx = makeCtx();
            const rpc = new RPCManager(ctx);
            await expect(rpc.call({ device_ident: 'gateway_01', timeout: 5, data: {} })).rejects.toThrow('name');
        });

        it('throws on non-positive timeout', async () => {
            const ctx = makeCtx();
            const rpc = new RPCManager(ctx);
            await expect(rpc.call({ device_ident: 'gateway_01', name: 'x', timeout: 0, data: {} })).rejects.toThrow('positive number');
            await expect(rpc.call({ device_ident: 'gateway_01', name: 'x', timeout: -1, data: {} })).rejects.toThrow('positive number');
        });

        it('throws on missing data', async () => {
            const ctx = makeCtx();
            const rpc = new RPCManager(ctx);
            await expect(rpc.call({ device_ident: 'gateway_01', name: 'x', timeout: 5 })).rejects.toThrow('data is required');
        });

        it('throws when not connected', async () => {
            const ctx = makeCtx();
            ctx.connected = false;
            const rpc = new RPCManager(ctx);
            await expect(rpc.call({ device_ident: 'gateway_01', name: 'x', timeout: 5, data: {} })).rejects.toThrow('Not connected');
        });
    });
});
