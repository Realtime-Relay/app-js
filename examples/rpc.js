import { RelayApp } from '../src/index.js';

const app = new RelayApp({
    api_key: process.env.RELAY_API_KEY,
    secret: process.env.RELAY_SECRET,
    mode: 'test',
});

app.connection.listeners((event) => {
    console.log(`[connection] ${event}`);
});

await app.connect();
console.log('Connected to RelayX');

// ── RPC call to a device ─────────────────────────────────────

try {
    const result = await app.rpc.call({
        device_ident: 's-3',
        name: 'reboot_device',
        timeout: 10,
        data: { graceful: true },
    });

    console.log('RPC response:', result);
} catch (err) {
    console.error('RPC failed:', err.message);
}

// ── Clean up ─────────────────────────────────────────────────

await app.disconnect();
console.log('Disconnected');
