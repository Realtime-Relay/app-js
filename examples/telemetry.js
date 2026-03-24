import { RelayApp } from '../src/index.js';
import { createInterface } from 'readline';

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

// ── Interactive CLI ──────────────────────────────────────────

const rl = createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise((res) => rl.question(q, res));

async function run() {
    while (true) {
        const op = (await ask('\nOperation (on/off/latest/quit): ')).trim().toLowerCase();

        if (op === 'quit' || op === 'q') break;

        if (!['on', 'off', 'latest'].includes(op)) {
            console.log('Invalid operation. Use "on", "off", "latest", or "quit".');
            continue;
        }

        const ident = (await ask('Device ident: ')).trim();
        if (!ident) { console.log('Ident cannot be empty.'); continue; }

        if (op === 'latest') {
            const fieldsRaw = (await ask('Fields (comma-separated): ')).trim();
            const fields = fieldsRaw.split(',').map((f) => f.trim()).filter(Boolean);
            if (!fields.length) { console.log('At least one field required.'); continue; }

            const data = await app.telemetry.latest({ device_ident: ident, fields });
            console.log('Latest (24h):', JSON.stringify(data, null, 2));
            continue;
        }

        const metric = (await ask('Metric (* for all): ')).trim() || '*';

        if (op === 'on') {
            const result = await app.telemetry.stream({
                device_ident: ident,
                metric,
                callback: (data) => {
                    console.log(`[telemetry] ${ident}/${metric}:`, data);
                },
            });
            console.log(result ? `Streaming ${ident}/${metric}` : `Already streaming ${ident}/${metric}`);

        } else {
            if (metric === '*') {
                await app.telemetry.off({ device_ident: ident });
                console.log(`Stopped all telemetry for ${ident}`);
            } else {
                await app.telemetry.off({ device_ident: ident, metric: [metric] });
                console.log(`Stopped telemetry for ${ident}/${metric}`);
            }
        }
    }

    rl.close();
    await app.disconnect();
    console.log('Disconnected');
}

run().catch(console.error);
