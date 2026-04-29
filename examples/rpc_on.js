import { RelayApp } from "../src/index.js";

const app = new RelayApp({
  api_key: process.env.RELAY_API_KEY,
  secret: process.env.RELAY_SECRET,
  mode: "production",
});

app.connection.listeners((event) => {
  console.log(`[connection] ${event}`);
});

await app.connect();
console.log("Connected to RelayX");

// ── RPC call to a device ─────────────────────────────────────

try {
  const result = await app.rpc.call({
    device_ident: "current-sensor",
    name: "state",
    timeout: 10,
    data: { on: false },
  });

  console.log("RPC response:", result);
} catch (err) {
  console.error("RPC failed:", err.message);
}

// ── Clean up ─────────────────────────────────────────────────

await app.disconnect();
console.log("Disconnected");
