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
// LOGICAL GROUP — Create + Stream
// ══════════════════════════════════════════════════════════════

console.log('── Logical Group Stream ──');

var lgList = await app.logicalGroup.list();

for(let lg of lgList){
    var deleted = await app.logicalGroup.delete(lg.id)
    console.log(`Old group delete => ${deleted}`)
}

const lgCreated = await app.logicalGroup.create({
    name: 'floor_1_stream',
    tags: ['temperature'],
    device_idents: ['s-3'],
});
console.log('Created:', lgCreated);

lgList = await app.logicalGroup.list();
const lgGroup = lgList.find((g) => g.name === 'floor_1_stream');

if (lgGroup) {
    const lg = await app.logicalGroup.get(lgGroup.id);

    // ── Stream all events ────────────────────────────────────

    // await lg.stream({
    //     callback: (data) => {
    //         console.log('[logical-group]', data);
    //     },
    // });
    console.log('Streaming logical group events...');

    // ── Stream with device filter ────────────────────────────

    await lg.stream({
        device_idents: ['s-3'],
        callback: (data) => {
            console.log('[logical-group filtered]', data);
        },
    });
}

// ══════════════════════════════════════════════════════════════
// HIERARCHY GROUP — Create + Stream
// ══════════════════════════════════════════════════════════════

console.log('\n── Hierarchy Group Stream ──');

var hgList = await app.heirarchyGroup.list();

for(let hg of hgList){
    var deleted = await app.heirarchyGroup.delete(hg.id)
    console.log(`Old group delete => ${deleted}`)
}

const hgCreated = await app.heirarchyGroup.create({
    name: 'building_a_stream',
    heirarchy: 'campus.building_a.floor_1',
    device_idents: ['s-3'],
});
console.log('Created:', hgCreated);

hgList = await app.heirarchyGroup.list();
const hgGroup = hgList.find((g) => g.name === 'building_a_stream');

if (hgGroup) {
    const hg = await app.heirarchyGroup.get(hgGroup.id);

    // ── Stream all events ────────────────────────────────────

    // await hg.stream({
    //     callback: (data) => {
    //         console.log('[hierarchy-group]', data);
    //     },
    // });
    console.log('Streaming hierarchy group events...');

    // ── Stream with hierarchy wildcard filter ────────────────

    await hg.stream({
        heirarchy: 'campus.*.*',
        callback: (data) => {
            console.log('[hierarchy-group filtered]', data);
        },
    });
}

console.log('\nListening for group events... (Ctrl+C to stop)');
