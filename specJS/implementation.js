app = new RelayApp({
    api_key: "",
    secret: "",
    mode: "production" | "test"
})

app.connection.listeners((event) => {
    /**
     * connected
     * disconnected
     * reconnecting
     * reconnected
     * auth_failed
     * reconnect_failed
     */
})

app.connect()
app.disconnect()

// ------------- TELEMETRY ---------------

app.telemetry.stream({
    device_ident: "<ident>",
    metric: "<metric or *>", // specific metric or all metrics
    calback: (data) => {
        // Process stream
        // { metric: "<metric name>", value: "<value>", timestamp: <unix timestamp> }
    }
})

app.telemetry.off({
    device_ident: "<ident>"
})

var telemetryHistory = await app.telemetry.history({
    device_ident: "<ident>",
    fields: ["temperature", ...],
    start: "ISO8601 datetime",
    end: "ISO8601 datetime"
})

// ------------- COMMANDS ----------------

await app.command.send({
    name: "<command_name>",
    device_ident: ["<ident_1>", ...],
    data: {}
})

var commandHistory = await app.command.history({
    name: "<command_name>",
    device_idents: ["<ident_1>", "<ident_2>", ...],
    start: "ISO8601 datetime",
    end: "ISO8601 datetime" // Optional, if not specified, send now()
})

var response = await app.rpc.call({
    device_ident: "<>",
    name: "<rpc_name>",
    timeout: 20, // seconds
    data: {

    }
})

// ------------- DEVICES ----------------

// Get / Set Config
app.device.setConfig("<device_ident>", {/** DEVICE CONFIG */})

var config = await app.device.getConfig("<device_ident>")

// Device CRUD & List
var device = await app.device.create({ // Limit check
    ident: "<unique ident>",
    schema: {},
    config: {}
})

var device = await app.device.update({
    ident: "<unique ident>",
    schema: {},
    config: {}
})

var deleted = await app.device.delete("<unqiue ident>")

var devices = await app.device.list()

var device = await app.device.get({
    ident: "<unqiue ident>"
})

// Presence
app.connection.presence((data) => {
    // Process presence data
})

// ------------- EVENTS ----------------

app.events.stream({
    name: "<event_name>",
    calback: (data) => {
        // Process stream
    }
})

app.events.off({
    name: "<event_name>"
})

// ------------- ALERTS ----------------

var alert = await app.alert.create({ // Limit check
    // This is the alert config
    name: "<unique_name>",
    decription: "<>", // optional
    type: "<>", // THRESHOLD, RATE_CHANGE or EPHEMERAL
    metric: "<>", // Metric of device
    config: { 
        scope: {
            type: "<>", // DEVICE, LOGICAL_GROUP, HEIRARCHY
            value: "" // Device ID for DEVICE and group_id for LOGICAL_GROUP or HEIRARCHY
        },
        operator: "", // >, >=, ==, <=, < | required only for THRESHOLD, RATE_CHANGE
        value: value, // number | required only for THRESHOLD, RATE_CHANGE
        duration: duration, // duration in seconds where the metric value is in breach of the operator for the alert to fire. Always required
        recovery_duration: value, // duration in seconds where the value is not in breach of the operator for the alert to recover, Always required
        cooldown: cooldown // cooldown period in seconds before another alert is fired (if in breach of operator). Optional for all, 0 if not provided
    },
    notification_channel: ["notification_id", ...], // Optional, if not provided, default to [],
    evalulator: (data) => { // Required only for EPHEMERAL, this is not sent to backend
        // Return true to fire alert
        // Return false to fire recovery flow
    }
})

var alert = await app.alert.update({
    // Alert config.
})

var deleted = await app.alert.delete(alertName)

var alerts = await app.alert.list()

var alert = await app.alert.get("<alert-name>")

var ack = await app.alert.ack({
    alert: alert,
    acked_by: "<string>"
})

var ackAll = await app.alert.ackAll({
    alert: alert,
    acked_by: "<string>"
})

await app.alert.mute({
    id: "<alert id>",
    mute_config: {
        type: "<>", // FORVER, TIME_BASED
        mute_till: "ISO8601 timestamp utc" // Required only for type = TIME_BASED
    }
})

await app.alert.unmute("<alert id>")

await alert.listen({ // alert from get, create or update
    onFire: (data) => {
        // Process alert
    },
    onResolved: (data) => {
        // Process alert resolved
    },
    onAck: (data) => {
        // Process ack data
    },
    onAckAll: (data) => {
        // Process ack all data
    }
})

// Init Ephermal Alerts
var alertEphemeral = await app.alert.get("<alert-name>")

alertEphemeral.setEvaulator((data) => {
    // Return true to fire alert
    // Return false to fire recovery flow
})

// ------------- LOGICAL GROUPS ----------------

var logicalGroup = await app.logicalGroup.create({
    name: "<>",
    tags: ["<tag_1>", ...],
    device_idents: ["<ident_1>", ...]
})

var logicalGroup = await app.logicalGroup.update({
    id: "<group_id>",
    name: "<>",
    tags: {
        add: ["<tag_1>", ...],
        remove: ["<tag_1>", ...]
    },
    devices: {
        add: ["<ident_1>", ...],
        remove: ["<ident_1>", ...]
    }
})

var deleted = await app.logicalGroup.delete("<group_id>")

var logicalGroups = await app.logicalGroup.list()

var devices = await app.logicalGroup.listDevices("<group_id>")

var logicalGroup = await app.logicalGroup.get("<group_id>")

logicalGroup.stream({
    device_idents: ["<ident_1>", ..], //optional for filtering
    callback: (data) => {

    }
})

// ------------- HEIRARCHY GROUPS ----------------

var heirarchyGroup = await app.heirarchyGroup.create({
    name: "<>",
    heirarchy: "token.token",
    device_idents: ["<ident_1>", ...]
})

var heirarchyGroup = await app.heirarchyGroup.update({
    id: "<group_id>",
    name: "<>",
    heirarchy: "token.token",
    devices: {
        add: ["<ident_1>", ...],
        remove: ["<ident_1>", ...]
    }
})

var deleted = await app.heirarchyGroup.delete("<group_id>")

var heirarchyGroups = await app.heirarchyGroup.list()

var devices = await app.heirarchyGroup.listDevices("<group_id>")

var heirarchyGroup = await app.heirarchyGroup.get("<group_id>")

heirarchyGroup.stream({
    device_idents: ["<ident_1>", ..], //optional for filtering
    heirarchy: "<wildcard_topic>", // optional for filtering
    metrics: ["<metric_1>", ...], // metrics can't exist with metric
    metric: "*", // metric can't exist with metrics
    callback: (data) => {
        
    }
})

// ------------- NOTIFICATIONS ----------------
// can create for type = EMAIL OR WEBHOOK only!

// Create webhook notification
var notification = await app.notification.create({
    name: "<>",
    type: "WEBHOOK", 
    config: {
        endpoint: "<URL>",
        headers: {
            "<header_name>": "<Header_value>",
            ...
        }
    }
})

// Create email notification
var notification = await app.notification.create({
    name: "<>",
    type: "EMAIL", 
    config: {
        recipients: ["<email_id_1>", ...],
        subject: "<>",
        template: "<>"
    }
})

// Updates accordingly. errors will be thrown from the backend if type and config for type has a mismatch
// Make sure to send the entire config
var notification = await app.notification.update({
    name: "<>",
    type: "<>", 
    config: {
        // According to type
    }
})

var deleted = await app.notification.delete("<notif_id>")

var notifs = await app.notification.list()

var notification = await app.notification.get("<notif_id>")

