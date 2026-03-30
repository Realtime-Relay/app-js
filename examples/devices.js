import { RelayApp } from "../src/index.js";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "test",
});

app.connection.listeners((event) => {
  console.log(`[connection] ${event}`);
});

await app.connect();
console.log("Connected to RelayX\n");

// ── List all devices ─────────────────────────────────────────

const devices = await app.device.list();
console.log(`Devices (${devices.length}):`);
devices.forEach((d) => console.log(`  - ${d.ident} (${d.id})`));

// ── Create a device ──────────────────────────────────────────

const created = await app.device.create({
  ident: "test_sensor_01",
  schema: { temperature: "float", humidity: "float" },
  config: { interval: 30 },
});
console.log("\nCreated:", created?.ident, created?.id);

// ── Get a device ─────────────────────────────────────────────

const device = await app.device.get({ ident: "test_sensor_01" });
console.log("\nGet:", device.ident, device.id);

// ── Update a device ──────────────────────────────────────────

const updated = await app.device.update({
  id: created.id,
  ident: "test_sensor_02",
  schema: { temperature: "float", humidity: "float", pressure: "float" },
  config: { interval: 15 },
});
console.log("\nUpdated:", updated);

// ── Delete a device ──────────────────────────────────────────

const deleted = await app.device.delete("test_sensor_02");
console.log("\nDeleted:", deleted);

// ── Verify deletion ──────────────────────────────────────────

const after = await app.device.list();
console.log(`\nDevices after delete (${after.length}):`);
after.forEach((d) => console.log(`  - ${d.ident} (${d.id})`));

await app.disconnect();
console.log("\nDisconnected");
