import { RelayApp } from "../src/index.js";
import { createInterface } from "readline";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

app.connection.listeners((event) => {
  console.log(`[connection] ${event}`);
});

await app.connect();
console.log("Connected to RelayX\n");

// ── Interactive CLI ──────────────────────────────────────────

const rl = createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise((res) => rl.question(q, res));

async function run() {
  while (true) {
    const op = (await ask("\nOperation (on/off/history/quit): "))
      .trim()
      .toLowerCase();

    if (op === "quit" || op === "q") break;

    if (!["on", "off", "history"].includes(op)) {
      console.log('Invalid operation. Use "on", "off", "history", or "quit".');
      continue;
    }

    if (op === "history") {
      const ident = (await ask("Device ident: ")).trim();
      if (!ident) {
        console.log("Ident cannot be empty.");
        continue;
      }

      const namesRaw = (await ask("Event names (comma-separated): ")).trim();
      const event_names = namesRaw
        .split(",")
        .map((n) => n.trim())
        .filter(Boolean);

      if (!event_names.length) {
        console.log("At least one event name required.");
        continue;
      }

      const start = "2025-01-01T00:00:00.000Z"
      
      const end = new Date().toISOString()

      const aggMode = (
        await ask("Aggregate? (count <interval> | <enter> to skip): ")
      ).trim();

      const params = {
        device_ident: ident,
        event_names,
        start,
        end,
      };

      if (aggMode) {
        const [fn, interval] = aggMode.split(/\s+/);
        if (!fn || !interval) {
          console.log('Format: "count 5m" or similar.');
          continue;
        }
        params.aggregate_fn = fn;
        params.interval = interval;
      }

      try {
        const data = await app.events.history(params);
        const counts = Object.fromEntries(
          Object.entries(data).map(([k, v]) => [k, v.length]),
        );
        console.log("Counts:", counts);
        console.log(JSON.stringify(data, null, 2));
      } catch (err) {
        console.error("Failed:", err.message);
      }
      continue;
    }

    // on / off
    const name = (await ask("Event name: ")).trim();
    if (!name) {
      console.log("Event name cannot be empty.");
      continue;
    }

    if (op === "on") {
      const identsRaw = (
        await ask('Device idents ("*" or comma-separated): ')
      ).trim();
      if (!identsRaw) {
        console.log("Device idents cannot be empty.");
        continue;
      }

      const device_ident =
        identsRaw === "*"
          ? "*"
          : identsRaw
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean);

      if (Array.isArray(device_ident) && device_ident.length === 0) {
        console.log("At least one device ident required.");
        continue;
      }

      try {
        const ok = await app.events.stream({
          name,
          device_ident,
          callback: (payload) => {
            // payload shape: { <device_ident>: <event_data> }
            for (const [ident, data] of Object.entries(payload)) {
              console.log(`[event:${name}] ${ident}`, data);
            }
          },
        });
        if (ok) {
          const label = Array.isArray(device_ident)
            ? device_ident.join(",")
            : device_ident;
          console.log(`Streaming "${name}" for [${label}]`);
        } else {
          console.log(`Already streaming "${name}"`);
        }
      } catch (err) {
        console.error("Failed:", err.message);
      }
    } else {
      await app.events.off({ name });
      console.log(`Stopped streaming "${name}"`);
    }
  }

  rl.close();
  await app.disconnect();
  console.log("Disconnected");
}

run().catch(console.error);
