import { RelayApp } from "../src/index.js";
import { createInterface } from "readline";
import { writeFileSync } from "fs";

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
    const op = (await ask("\nOperation (on/off/latest/quit): "))
      .trim()
      .toLowerCase();

    if (op === "quit" || op === "q") break;

    if (!["on", "off", "latest"].includes(op)) {
      console.log('Invalid operation. Use "on", "off", "latest", or "quit".');
      continue;
    }

    const ident = (await ask("Device ident: ")).trim();
    if (!ident) {
      console.log("Ident cannot be empty.");
      continue;
    }

    if (op === "latest") {
      const fieldsRaw = (await ask("Fields (comma-separated): ")).trim();
      const fields = fieldsRaw
        .split(",")
        .map((f) => f.trim())
        .filter(Boolean);

      if (!fields.length) {
        console.log("At least one field required.");
        continue;
      }

      const data = await app.telemetry.history({
        device_ident: ident,
        fields,
        start: "2025-01-01T00:00:00.000Z",
        end: "2026-12-31T23:59:59.000Z",
      });
      console.log("Latest (24h):", JSON.stringify(data, null, 2));

      writeFileSync("data.json", JSON.stringify(data, null, 2), "utf-8");

      console.log(data.temperature.length);
      console.log(data.humidity.length);

      const serialized = JSON.stringify(data);
      const sizeInBytes = new TextEncoder().encode(serialized).length;
      const sizeInKB = sizeInBytes / 1024;
      const sizeInMB = sizeInKB / 1024;

      console.log("Size (MB)", sizeInMB);

      continue;
    }

    const metricRaw =
      (await ask("Metrics (* for all, or comma-separated): ")).trim() || "*";
    const metric =
      metricRaw === "*"
        ? "*"
        : metricRaw
            .split(",")
            .map((m) => m.trim())
            .filter(Boolean);

    if (op === "on") {
      const label = Array.isArray(metric) ? metric.join(",") : "*";
      await app.telemetry.stream({
        device_ident: ident,
        metric,
        callback: (data) => {
          console.log(`[telemetry] ${ident}/${data.metric}:`, data.data);
        },
      });
      console.log(`Streaming ${ident}/${label}`);
    } else {
      if (metric === "*") {
        await app.telemetry.off({ device_ident: ident });
        console.log(`Stopped all telemetry for ${ident}`);
      } else {
        await app.telemetry.off({ device_ident: ident, metric });
        console.log(`Stopped telemetry for ${ident}/${metric.join(",")}`);
      }
    }
  }

  rl.close();
  await app.disconnect();
  console.log("Disconnected");
}

run().catch(console.error);
