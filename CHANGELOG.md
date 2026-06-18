# Changelog

All notable changes to `@relay-x/app-sdk` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-18

### Added
- **OTA module** (`app.ota`) for over-the-air firmware management:
  - Firmware: `firmwareUpload()`, `firmwareList()`, `firmwareDelete()`.
  - Rollouts: `createRollout()`, `updateRollout()`, `deleteRollout()`,
    `toggleRollout()`, `retryRollout()`, `installRollout()`, `rolloutList()`.
  - Jobs: `jobsList()`, `jobHistory()`.
  - Live device job-phase updates: `onJobPhaseUpdate(callback)` /
    `offJobPhaseUpdate()`.
- Install-later flow: `installRollout()` triggers the install phase for rollouts
  staged as download-only (INSTALL_ONLY).
- New examples: `telemetry-history.js`, `events-history.js`, `logs-history.js`,
  `ota.js`, `ota-deploy.js`, `ota-rollout.js`, `ota-install-later.js`,
  `ota-clear.js`, and the `ota-tests/` suite.

### Changed
- **History reads now go over HTTP** (the influx-db-service) instead of NATS
  streaming. Affects `telemetry.history()`, `telemetry.latest()`,
  `events.history()`, `commands.history()`, `logs.history()`, and
  `alerts.history()`. Results are paginated and fetched transparently; the auth
  token is fetched once and refreshed on a 401/403. Method inputs and return
  shapes are otherwise unchanged.

### Removed
- **BREAKING:** The `onFrame` live callback has been removed from every
  `history()` method. HTTP pagination cannot deliver per-frame live updates. Use
  the returned data directly, or subscribe to live data via the relevant
  manager's streaming method (e.g. `telemetry.stream()`).

[0.2.0]: https://github.com/relay-x/app-sdk/releases/tag/v0.2.0
