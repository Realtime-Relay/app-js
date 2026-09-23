/**
 * app.messaging.streamImport() — listen on import.<org>.<env>.messages.<topic>.
 *
 * Something else has to publish onto the import subject; this example only
 * listens. app.messaging.send() publishes to the non-import subject, so its
 * messages arrive on stream(), not here.
 *
 * Run:
 *   RELAY_API_KEY=... RELAY_SECRET=... node examples/messaging-import.js [topic]
 *
 * topic defaults to ">" (everything). Wildcards: "*" matches one token,
 * ">" matches the rest and must be last — e.g. "orders.*", "orders.>".
 */

import { RelayApp } from "../src/index.js";

const TOPIC = process.argv[2] ?? ">";

const app = new RelayApp({
  api_key: process.env.API_KEY,
  secret: process.env.API_SECRET,
  mode: "production",
});

app.connection.listeners((event) => {
  console.log(`[connection] ${event}`);
});

await app.connect();
console.log("Connected to RelayX\n");

// `topic` is the concrete topic with the import.<org>.<env>.messages. prefix
// removed, so a wildcard stream can tell messages apart.
await app.messaging.streamImport({
  topic: TOPIC,
  callback: ({ topic, data, timestamp }) => {
    const ts = new Date(timestamp).toISOString();
    console.log(`${ts} [${topic}]`, JSON.stringify(data));
  },
});

console.log(`Listening on import messages "${TOPIC}". Ctrl+C to stop.\n`);

async function shutdown() {
  console.log("\nStopping stream…");
  await app.messaging.offImport({ topic: TOPIC });
  await app.disconnect();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
