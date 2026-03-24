import { RelayApp } from '../src/index.js';

const app = new RelayApp({
    api_key: process.env.RELAY_API_KEY,
    secret: process.env.RELAY_SECRET,
    mode: 'test',
});

// ── Connection lifecycle ─────────────────────────────────────

app.connection.listeners((event) => {
    console.log(`[connection] ${event}`);
});

await app.connect();
console.log('Connected to RelayX');

// ── 10s delay to test offline behavior (disconnect network during this window) ──
console.log('Waiting 20s — disconnect network to test offline buffering...');
await new Promise((r) => setTimeout(r, 20000));

// ── Send a command to a single device ────────────────────────

const rebootResult = await app.command.send({
    name: 'setConfig',
    device_ident: ['s-3'],
    data: { force: false },
});

console.log('setConfig sent:', rebootResult); // true if JetStream ack received

// ── Send a command to multiple devices at once ───────────────

const configResult = await app.command.send({
    name: 'update_config',
    device_ident: ['sensor_01', 'sensor_02', 'sensor_03'],
    data: {
        sampling_interval: 15,
        reporting_mode: 'batch',
    },
});

console.log('Config push sent to 3 devices:', configResult);

// ── Query command history ────────────────────────────────────

const history = await app.command.history({
    name: 'setConfig',
    device_idents: ['s-3'],
    start: '2026-03-01T00:00:00.000Z',
    end: '2026-03-24T23:59:59.000Z',
});

console.log(history);

// // ── Clean up ─────────────────────────────────────────────────

await app.disconnect();
console.log('Disconnected');
