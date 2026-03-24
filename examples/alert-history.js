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

// ── Query alert history by DEVICE ────────────────────────────

const now = new Date();
const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

console.log('─── History by DEVICE ───────────────────────');
console.log(`Device: s-3`);
console.log(`Range: ${oneWeekAgo.toISOString()} → ${now.toISOString()}\n`);

try {
    const deviceHistory = await app.alert.history({
        rule_type: 'DEVICE',
        device_ident: 's-3',
        rule_states: ['fire', 'resolved'],
        start: oneWeekAgo.toISOString(),
        end: now.toISOString(),
    });

    console.log(JSON.stringify(deviceHistory, null, 2))
} catch (err) {
    console.error('Error fetching device history:', err.message);
}

// ── Query alert history by RULE ──────────────────────────────

// First, get a rule ID from an existing alert
const alerts = await app.alert.list();
const firstAlert = alerts[0];

if (firstAlert) {
    console.log(`\n─── History by RULE ─────────────────────────`);
    console.log(`Rule: ${firstAlert.name} (${firstAlert.id})`);
    console.log(`Range: ${oneWeekAgo.toISOString()} → ${now.toISOString()}\n`);

    try {
        const ruleHistory = await app.alert.history({
            rule_type: 'RULE',
            rule_id: firstAlert.id,
            rule_states: ['fire', 'resolved', 'ack', 'ack_all'],
            start: oneWeekAgo.toISOString(),
            end: now.toISOString(),
        });

        console.log(JSON.stringify(ruleHistory, null, 2))
    } catch (err) {
        console.error('Error fetching rule history:', err.message);
    }

    // ── Query by RULE scoped to a specific device ────────────

    console.log(`\n─── History by RULE + DEVICE ────────────────`);
    console.log(`Rule: ${firstAlert.name}, Device: s-3\n`);

    try {
        const scopedHistory = await app.alert.history({
            rule_type: 'RULE',
            rule_id: firstAlert.id,
            device_ident: 's-3',
            rule_states: ['fire', 'resolved', 'ack', 'ack_all'],
            start: oneWeekAgo.toISOString(),
            end: now.toISOString(),
        });

        console.log(JSON.stringify(scopedHistory, null, 2))
    } catch (err) {
        console.error('Error fetching scoped history:', err.message);
    }
} else {
    console.log('\nNo alerts found — skipping RULE history queries.');
}

// ── Cleanup ──────────────────────────────────────────────────

await app.disconnect();
console.log('\nDisconnected.');
