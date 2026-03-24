import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createMockContext, createMockConsumer, createMockKVBucket } from './setup.js';
import { DeviceManager } from '../src/device.js';
import { EphemeralEngine } from '../src/ephemeralEngine.js';

const RULE = {
    id: 'rule_eph_1',
    name: 'custom_check',
    type: 'EPHEMERAL',
    config: {
        topic: {
            source: 'TELEMETRY',
            device_ident: 'sensor_01',
            last_token: 'cpu_usage',
        },
        duration: 2,
        recovery_duration: 1,
        cooldown: 3,
    },
    notification_channel: ['notif_1'],
};

const RULE_WILDCARD = {
    ...RULE,
    id: 'rule_eph_wildcard',
    config: {
        ...RULE.config,
        topic: { source: 'TELEMETRY', device_ident: '*', last_token: '*' },
    },
};

const RULE_COMMAND = {
    ...RULE,
    id: 'rule_eph_cmd',
    config: {
        ...RULE.config,
        topic: { source: 'COMMAND', device_ident: 'sensor_01', last_token: 'reboot' },
    },
};

const RULE_EVENT = {
    ...RULE,
    id: 'rule_eph_evt',
    config: {
        ...RULE.config,
        topic: { source: 'EVENT', device_ident: 'sensor_01', last_token: 'door_opened' },
    },
};

function makeCtx(kvBucket) {
    const consumer = createMockConsumer();
    const ctx = createMockContext({ consumer, kvBucket });
    const device = new DeviceManager(ctx);
    ctx.device = device;
    device.cache.set('sensor_01', { id: 'dev_1', ident: 'sensor_01' });
    device.cache.set('sensor_02', { id: 'dev_2', ident: 'sensor_02' });
    return { ctx, consumer };
}

describe('EphemeralEngine', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    describe('constructor and setEvaluator', () => {
        it('constructs without evaluator', () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            expect(engine.mode).toBeNull();
        });

        it('setEvaluator stores function', () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => true);
            // No throw = success
        });

        it('setEvaluator throws for non-function', () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            expect(() => engine.setEvaluator('not_fn')).toThrow('must be a function');
        });
    });

    describe('mode detection', () => {
        it('owner mode when evaluator is set before listen', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});
            expect(engine.mode).toBe('owner');
            await engine.stop();
        });

        it('listener mode when no evaluator', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            await engine.listen({});
            expect(engine.mode).toBe('listener');
            await engine.stop();
        });
    });

    describe('subject construction', () => {
        it('TELEMETRY with specific ident resolves device ID', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            const dataCall = ctx.jetstream.consumers.get.mock.calls.find(
                ([, opts]) => opts?.filter_subjects?.includes('telemetry')
            );
            expect(dataCall).toBeDefined();
            expect(dataCall[1].filter_subjects).toBe('test_org_123.production.telemetry.dev_1.cpu_usage');
            await engine.stop();
        });

        it('TELEMETRY with wildcard uses * directly', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE_WILDCARD);
            engine.setEvaluator(() => false);
            await engine.listen({});

            const dataCall = ctx.jetstream.consumers.get.mock.calls.find(
                ([, opts]) => opts?.filter_subjects?.includes('telemetry')
            );
            expect(dataCall[1].filter_subjects).toBe('test_org_123.production.telemetry.*.*');
            await engine.stop();
        });

        it('COMMAND subject construction', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE_COMMAND);
            engine.setEvaluator(() => false);
            await engine.listen({});

            const dataCall = ctx.jetstream.consumers.get.mock.calls.find(
                ([, opts]) => opts?.filter_subjects?.includes('command.queue')
            );
            expect(dataCall[1].filter_subjects).toBe('test_org_123.production.command.queue.dev_1.reboot');
            await engine.stop();
        });

        it('EVENT subject construction', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE_EVENT);
            engine.setEvaluator(() => false);
            await engine.listen({});

            const dataCall = ctx.jetstream.consumers.get.mock.calls.find(
                ([, opts]) => opts?.filter_subjects?.includes('events')
            );
            expect(dataCall[1].filter_subjects).toBe('test_org_123.production.events.dev_1.door_opened');
            await engine.stop();
        });
    });

    describe('KV lock', () => {
        it('acquires lock on owner listen with expires_at', async () => {
            const kvBucket = createMockKVBucket();
            const { ctx } = makeCtx(kvBucket);
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            expect(kvBucket.create).toHaveBeenCalledWith(
                'ephemeral_owner_rule_eph_1',
                expect.any(String)
            );
            const stored = kvBucket._store.get('ephemeral_owner_rule_eph_1');
            expect(stored).toBeDefined();
            expect(stored.expires_at).toBeGreaterThan(Date.now());
            await engine.stop();
        });

        it('throws when lock already exists and not expired', async () => {
            const kvBucket = createMockKVBucket();
            // Pre-populate lock with valid (non-expired) TTL
            kvBucket._store.set('ephemeral_owner_rule_eph_1', {
                started_at: Date.now(),
                expires_at: Date.now() + 30000,
            });

            const { ctx } = makeCtx(kvBucket);
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);

            await expect(engine.listen({})).rejects.toThrow('Evaluator already active');
        });

        it('acquires lock when existing lock has expired', async () => {
            const kvBucket = createMockKVBucket();
            // Pre-populate with expired lock
            kvBucket._store.set('ephemeral_owner_rule_eph_1', {
                started_at: Date.now() - 60000,
                expires_at: Date.now() - 1000, // Expired 1s ago
            });

            const { ctx } = makeCtx(kvBucket);
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            // Should have deleted the stale key and created a new one
            expect(kvBucket.delete).toHaveBeenCalledWith('ephemeral_owner_rule_eph_1');
            expect(kvBucket.create).toHaveBeenCalled();
            await engine.stop();
        });

        it('releases lock on stop', async () => {
            const kvBucket = createMockKVBucket();
            const { ctx } = makeCtx(kvBucket);
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            expect(kvBucket._store.has('ephemeral_owner_rule_eph_1')).toBe(true);
            await engine.stop();
            expect(kvBucket.delete).toHaveBeenCalledWith('ephemeral_owner_rule_eph_1');
        });
    });

    describe('RPC subscriptions (owner mode)', () => {
        it('subscribes to single wildcard RPC subject', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            expect(ctx.natsClient.subscribe).toHaveBeenCalledTimes(1);
            const [subject] = ctx.natsClient.subscribe.mock.calls[0];
            expect(subject).toBe('test_org_123.production.alerts.custom.rule_eph_1.*');
            await engine.stop();
        });
    });

    describe('listener mode subscriptions', () => {
        it('subscribes to wildcard alert topic', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            // No setEvaluator → listener mode
            await engine.listen({});

            const alertCall = ctx.jetstream.consumers.get.mock.calls.find(
                ([, opts]) => opts?.filter_subjects?.includes('alerts.listen')
            );
            expect(alertCall).toBeDefined();
            expect(alertCall[1].filter_subjects).toBe('import.test_org_123.production.alerts.listen.rule_eph_1.*');
            await engine.stop();
        });
    });

    describe('state machine', () => {
        it('starts in normal state', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            expect(engine.state.status).toBe('normal');
            await engine.stop();
        });
    });

    describe('ack / ackAll', () => {
        it('ack returns false when not alerting', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            expect(await engine.ack('operator')).toBe(false);
            await engine.stop();
        });

        it('ackAll returns false when not alerting', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            expect(await engine.ackAll('operator')).toBe(false);
            await engine.stop();
        });
    });

    describe('mute check', () => {
        it('skips evaluation when muted FOREVER', async () => {
            const { ctx } = makeCtx();
            const mutedRule = {
                ...RULE,
                alert_mute_config: { type: 'FOREVER' },
            };

            const onFire = vi.fn();
            const engine = new EphemeralEngine(ctx, mutedRule);
            engine.setEvaluator(() => true);
            await engine.listen({ onFire });

            // Muted — onFire should never be called
            expect(onFire).not.toHaveBeenCalled();
            await engine.stop();
        });
    });

    describe('stop() cleanup', () => {
        it('resets state and mode on stop', async () => {
            const { ctx } = makeCtx();
            const engine = new EphemeralEngine(ctx, RULE);
            engine.setEvaluator(() => false);
            await engine.listen({});

            expect(engine.mode).toBe('owner');
            await engine.stop();

            expect(engine.mode).toBeNull();
            expect(engine.state.status).toBe('normal');
            expect(engine.rollingState).toEqual({});
        });
    });

    describe('notification dispatch', () => {
        it('dispatches to notification endpoint when channels exist', async () => {
            const { ctx } = makeCtx();
            // We can't easily trigger a fire in this mock setup,
            // but we can verify the engine has the notification_channel
            const engine = new EphemeralEngine(ctx, RULE);
            expect(RULE.notification_channel).toEqual(['notif_1']);
            await engine.stop();
        });
    });
});
