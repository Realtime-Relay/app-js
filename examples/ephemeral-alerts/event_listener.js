import { RelayApp } from '../../src/index.js';
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

// ── Get the ephemeral event alert (created by event_owner.js) ──

const ALERT_NAME = 'ephemeral_event_door';

const ephAlert = await app.alert.get(ALERT_NAME);

if (!ephAlert) {
    console.error(`Alert "${ALERT_NAME}" not found. Run event_owner.js first.`);
    await app.disconnect();
    process.exit(1);
}

console.log(`Found alert: ${ephAlert.name} (id: ${ephAlert.id})\n`);

// ── Listen in LISTENER mode (no setEvaluator) ────────────────

var deviceID = null;

await ephAlert.listen({
    onFire: (data) => {
        console.log('\n[FIRE]', JSON.stringify(data, null, 2));
        deviceID = data.device_id;
        printMenu();
    },

    onResolved: (data) => {
        console.log('\n[RESOLVED]', JSON.stringify(data, null, 2));
        deviceID = data.device_id;
        printMenu();
    },

    onAck: (data) => {
        console.log('\n[ACK]', JSON.stringify(data, null, 2));
        printMenu();
    },

    onAckAll: (data) => {
        console.log('\n[ACK_ALL]', JSON.stringify(data, null, 2));
        printMenu();
    },
});

console.log('Listening for event alert events (listener mode).\n');

// ── Interactive terminal input ───────────────────────────────

const rl = createInterface({
    input: process.stdin,
    output: process.stdout,
});

function printMenu() {
    console.log('\n─── Commands ───────────────────');
    console.log('  1  → ack');
    console.log('  2  → ack_all');
    console.log('  3  → mute (FOREVER)');
    console.log('  4  → mute (TIME_BASED, 60s)');
    console.log('  5  → unmute');
    console.log('  q  → quit');
    console.log('────────────────────────────────');
}

printMenu();
rl.prompt();

rl.on('line', async (input) => {
    const cmd = input.trim();

    try {
        switch (cmd) {
            case '1': {
                console.log('Sending ack...');
                const result = await app.alert.ack({
                    device_id: deviceID,
                    alert_id: ephAlert.id,
                    acked_by: 'listener_operator',
                    ack_notes: 'Acked from event listener terminal',
                });
                console.log('Ack result:', result);
                break;
            }

            case '2': {
                console.log('Sending ack_all...');
                const result = await app.alert.ackAll({
                    alert_id: ephAlert.id,
                    acked_by: 'listener_operator',
                    ack_notes: 'AckAll from event listener terminal',
                });
                console.log('AckAll result:', result);
                break;
            }

            case '3': {
                console.log('Muting FOREVER...');
                const result = await app.alert.mute({
                    id: ephAlert.id,
                    mute_config: { type: 'FOREVER' },
                });
                console.log('Mute result:', result);
                break;
            }

            case '4': {
                const muteTill = new Date(Date.now() + 60000).toISOString();
                console.log(`Muting TIME_BASED until ${muteTill}...`);
                const result = await app.alert.mute({
                    id: ephAlert.id,
                    mute_config: { type: 'TIME_BASED', mute_till: muteTill },
                });
                console.log('Mute result:', result);
                break;
            }

            case '5': {
                console.log('Unmuting...');
                const result = await app.alert.unmute(ephAlert.id);
                console.log('Unmute result:', result);
                break;
            }

            case 'q': {
                console.log('Stopping...');
                await ephAlert.stop();
                await app.disconnect();
                console.log('Disconnected.');
                rl.close();
                process.exit(0);
            }

            default:
                console.log(`Unknown command: "${cmd}"`);
                break;
        }
    } catch (err) {
        console.error('Error:', err.message);
    }

    rl.prompt();
});

rl.on('close', async () => {
    await ephAlert.stop();
    await app.disconnect();
    process.exit(0);
});
