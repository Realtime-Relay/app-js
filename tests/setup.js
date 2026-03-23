import { vi } from 'vitest';

export function createMockConsumer(messages = []) {
    let consumeCallback = null;
    let msgIndex = 0;

    const consumer = {
        consume: vi.fn(async (opts) => {
            consumeCallback = opts?.callback;
            // Deliver queued messages
            for (const msg of messages) {
                if (consumeCallback) {
                    await consumeCallback(msg);
                }
            }
        }),
        next: vi.fn(async () => {
            if (msgIndex < messages.length) {
                return messages[msgIndex++];
            }
            return null;
        }),
        delete: vi.fn(async () => true),
        // Utility to push a new message after consumer is created
        _pushMessage: async (msg) => {
            if (consumeCallback) {
                await consumeCallback(msg);
            }
        },
        _getCallback: () => consumeCallback,
    };

    return consumer;
}

export function createMockMessage(data, subject = 'test.subject') {
    return {
        data: data,
        subject: subject,
        timestamp: new Date(),
        working: vi.fn(),
        ack: vi.fn(),
        nak: vi.fn(),
    };
}

export function createMockJetStream(consumer) {
    return {
        publish: vi.fn(async (subject, data) => ({
            stream: 'test_stream',
            seq: 1,
            duplicate: false,
        })),
        consumers: {
            get: vi.fn(async (streamName, opts) => consumer || createMockConsumer()),
        },
    };
}

export function createMockNatsClient(responses = {}) {
    const statusIterator = {
        [Symbol.asyncIterator]() {
            return {
                next: () => new Promise(() => {}), // Never resolves by default
            };
        },
    };

    return {
        request: vi.fn(async (subject, data, opts) => {
            const responder = responses[subject];
            if (responder) {
                const payload = typeof responder === 'function' ? responder(data) : responder;
                return {
                    json: () => payload,
                    data: JSON.stringify(payload),
                };
            }
            return {
                json: () => ({ status: 'UNKNOWN', data: {} }),
                data: '{}',
            };
        }),
        publish: vi.fn(),
        status: vi.fn(() => statusIterator),
        closed: vi.fn(() => new Promise(() => {})), // Never resolves
        close: vi.fn(async () => {}),
        info: { client_id: 'test_client_123' },
        _statusIterator: statusIterator,
    };
}

export function createMockCodec() {
    return {
        encode: vi.fn((data) => JSON.stringify(data)),
        decode: vi.fn((data) => {
            if (typeof data === 'string') return JSON.parse(data);
            return data;
        }),
    };
}

/**
 * Creates a shared context object that simulates the app's internal state.
 * Used by manager constructors.
 */
export function createMockContext(overrides = {}) {
    const natsClient = createMockNatsClient(overrides.responses || {});
    const jetstream = createMockJetStream(overrides.consumer);

    return {
        natsClient,
        jetstream,
        codec: createMockCodec(),
        orgID: overrides.orgID || 'test_org_123',
        env: overrides.env || 'production',
        connected: overrides.connected !== undefined ? overrides.connected : true,
        deviceCache: overrides.deviceCache || new Map(),
        consumerMap: {},
        ...overrides,
    };
}
