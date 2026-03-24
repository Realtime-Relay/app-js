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

// ══════════════════════════════════════════════════════════════
// LOGICAL GROUPS — CRUD + List + Device List
// ══════════════════════════════════════════════════════════════

console.log('── Logical Groups ──');

// ── Create ───────────────────────────────────────────────────

const lgCreated = await app.logicalGroup.create({
    name: 'floor_1_sensors',
    tags: ['temperature', 'indoor'],
    device_idents: ['s-3'],
});
console.log('Created:', lgCreated);

// ── List ─────────────────────────────────────────────────────

const lgList = await app.logicalGroup.list();
console.log(`\nLogical groups (${lgList.length}):`);
lgList.forEach((g) => console.log(`  - ${g.name} (${g.id})`));

// ── Get ──────────────────────────────────────────────────────

if (lgList.length > 0) {
    const lg = await app.logicalGroup.get(lgList[0].id);
    console.log(`\nGet: ${lg.name}`);

    // ── Device List ──────────────────────────────────────────

    const lgDevices = await app.logicalGroup.listDevices(lgList[0].id);
    console.log(`Devices in group: ${lgDevices.length}`);

    // ── Update ───────────────────────────────────────────────

    const lgUpdated = await app.logicalGroup.update({
        id: lgList[0].id,
        tags: { add: ['humidity'], remove: [] },
    });
    console.log('Updated:', lgUpdated.status);

    // ── Delete ───────────────────────────────────────────────

    const lgDeleted = await app.logicalGroup.delete(lgList[0].id);
    console.log('Deleted:', lgDeleted);
}

// ══════════════════════════════════════════════════════════════
// HIERARCHY GROUPS — CRUD + List + Device List
// ══════════════════════════════════════════════════════════════

console.log('\n── Hierarchy Groups ──');

// ── Create ───────────────────────────────────────────────────

const hgCreated = await app.heirarchyGroup.create({
    name: 'building_a',
    heirarchy: 'campus.building_a',
    device_idents: ['s-3'],
});
console.log('Created:', hgCreated);

// ── List ─────────────────────────────────────────────────────

const hgList = await app.heirarchyGroup.list();
console.log(`\nHierarchy groups (${hgList.length}):`);
hgList.forEach((g) => console.log(`  - ${g.name} (${g.id})`));

// ── Get ──────────────────────────────────────────────────────

if (hgList.length > 0) {
    const hg = await app.heirarchyGroup.get(hgList[0].id);
    console.log(`\nGet: ${hg.name}`);

    // ── Device List ──────────────────────────────────────────

    const hgDevices = await app.heirarchyGroup.listDevices(hgList[0].id);
    console.log(`Devices in group: ${hgDevices.length}`);

    // ── Update ───────────────────────────────────────────────

    const hgUpdated = await app.heirarchyGroup.update({
        id: hgList[0].id,
        heirarchy: 'campus.building_a.floor_1',
    });
    console.log('Updated:', hgUpdated.status);

    // ── Delete ───────────────────────────────────────────────

    const hgDeleted = await app.heirarchyGroup.delete(hgList[0].id);
    console.log('Deleted:', hgDeleted);
}

await app.disconnect();
console.log('\nDisconnected');
