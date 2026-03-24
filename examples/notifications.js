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
console.log('Connected to RelayX\n');

// ── Create a WEBHOOK notification ────────────────────────────

const webhook = await app.notification.create({
    name: 'ops_webhook',
    type: 'WEBHOOK',
    config: {
        endpoint: 'https://hooks.example.com/relay-alerts',
        headers: { 'X-Api-Key': 'secret_123' },
    },
});
console.log('Created WEBHOOK:', webhook.status);

// ── Create an EMAIL notification ─────────────────────────────

const email = await app.notification.create({
    name: 'ops_email',
    type: 'EMAIL',
    config: {
        recipients: ['ops@example.com', 'alerts@example.com'],
        subject: 'RelayX Alert: {{rule.name}}',
        template: 'Alert {{rule.name}} fired on device {{device_ident}} at {{timestamp}}',
    },
});
console.log('Created EMAIL:', email.status);

// ── List all notifications ───────────────────────────────────

const all = await app.notification.list();
console.log(`\nNotifications (${all.length}):`);
all.forEach((n) => console.log(`  - ${n.name} (${n.type})`));

// ── Get a specific notification ──────────────────────────────

if (all.length > 0) {
    const fetched = await app.notification.get(all[0].id);
    console.log(`\nGet: ${fetched.data?.name}, type: ${fetched.data?.type}`);
}

// ── Update a notification ────────────────────────────────────

const updated = await app.notification.update({
    name: 'ops_webhook',
    type: 'WEBHOOK',
    config: {
        endpoint: 'https://hooks.example.com/relay-alerts-v2',
        headers: { 'X-Api-Key': 'new_secret_456' },
    },
});
console.log('\nUpdated:', updated.status);

// ── Delete a notification ────────────────────────────────────

if (all.length > 0) {
    const deleted = await app.notification.delete(all[0].id);
    console.log('Deleted:', deleted);
}

await app.disconnect();
console.log('\nDisconnected');
