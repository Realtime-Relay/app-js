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

const VALID_LEVELS = ["info", "warn", "error"];

function parseLevels(raw) {
  if (!raw || raw === "*") return undefined; // default = all
  return raw
    .split(",")
    .map((l) => l.trim())
    .filter(Boolean);
}

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

    const ident = (await ask("Device ident: ")).trim();
    if (!ident) {
      console.log("Ident cannot be empty.");
      continue;
    }

    if (op === "history") {
      const levelsRaw = (
        await ask("Levels (* / info,warn,error): ")
      ).trim() || "*";
      const levels = parseLevels(levelsRaw);

      const start = "2025-01-01T00:00:00.000Z"
      const end = new Date().toISOString()

      const aggMode = (
        await ask("Aggregate? (count <interval> | <enter> to skip): ")
      ).trim();

      const params = { device_ident: ident, start, end };
      if (levels) params.levels = levels;
      if (aggMode) {
        const [fn, interval] = aggMode.split(/\s+/);
        if (!fn || !interval) {
          console.log('Format: "count 1h" or similar.');
          continue;
        }
        params.aggregate_fn = fn;
        params.interval = interval;
      }

      try {
        const data = await app.log.history(params);
        const counts = Object.fromEntries(
          Object.entries(data).map(([k, v]) => [k, v.length]),
        );
        console.log("Counts by level:", counts);
        for (const [level, entries] of Object.entries(data)) {
          if (!entries.length) continue;
          console.log(`\n--- ${level.toUpperCase()} (${entries.length}) ---`);
          for (const e of entries.slice(0, 20)) {
            const ts = new Date(e.timestamp).toISOString();
            console.log(`  ${ts}  ${e.value}`);
          }
          if (entries.length > 20) {
            console.log(`  … ${entries.length - 20} more`);
          }
        }
      } catch (err) {
        console.error("Failed:", err.message);
      }
      continue;
    }

    // on / off
    if (op === "on") {
      const levelsRaw =
        (await ask("Levels (* / info,warn,error): ")).trim() || "*";
      const levels =
        levelsRaw === "*"
          ? "*"
          : levelsRaw
              .split(",")
              .map((l) => l.trim())
              .filter(Boolean);

      try {
        await app.log.stream({
          device_ident: ident,
          levels,
          callback: (entry) => {
            const ts = new Date(entry.timestamp).toISOString();
            const tag = `[${entry.level.toUpperCase()}]`.padEnd(8);
            console.log(`${ts} ${tag} ${ident}: ${entry.data}`);
          },
        });
        const label =
          Array.isArray(levels) ? levels.join(",") : levels === "*" ? "all" : levels;
        console.log(`Streaming ${ident} logs (${label})`);
      } catch (err) {
        console.error("Failed:", err.message);
      }
    } else {
      await app.log.off({ device_ident: ident });
      console.log(`Stopped log stream for ${ident}`);
    }
  }

  rl.close();
  await app.disconnect();
  console.log("Disconnected");
}

run().catch(console.error);
