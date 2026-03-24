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

// ── Listen for device presence events ────────────────────────

await app.connection.presence((data) => {
    console.log(`[presence] ${data.device_ident} → ${data.event}`, data.data ?? '');
});

console.log('Listening for presence events... (Ctrl+C to stop)');
