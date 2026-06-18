# OTA test scripts

App-SDK harness that exercises the 69-test Miro plan ("OTA Pipeline — Test Plan").
Each test in the Miro board (T01..T69) has a matching named export here, grouped
by area into `area-<letter>-<name>.js` files. A single runner dispatches them.

## Layout

| File | Area | Tests | Needs device? |
|------|------|-------|---------------|
| `area-a-upload.js`    | A · Upload    | T01–T08 | no |
| `area-b-lifecycle.js` | B · Lifecycle | T09–T19, **T73** | T14/T15 yes (offline-device); T73 BLOCKED (multi-device) |
| `area-c-discovery.js` | C · Discovery | T20–T26 | yes |
| `area-d-download.js`  | D · Download  | T27–T34 | yes |
| `area-e-install.js`   | E · Install   | T35–T42, **T70–T72** | T70 yes (full INSTALL_ONLY flow); T71/T72 BLOCKED (need direct push) |
| `area-f-status.js`    | F · Status    | T43–T47 | yes (T43/T46/T47 install a real job; T45 BLOCKED) |
| `area-g-fifo.js`      | G · FIFO      | T48–T53 | yes |
| `area-h-retry.js`     | H · Retry     | T54–T58 | T54/T55/T58 yes; T56/T57 no |
| `area-i-scale.js`     | I · Scale     | T59–T66 | mixed (T59/T60 app-side; T61–T66 mostly infra/device) |
| `area-j-wiring.js`    | J · Wiring    | T67–T69 | T67 yes; T68/T69 infra |

App-only tests (T01–T19, T56, T57, T59, T60) assert hard and PASS/FAIL on their own.
Device-required tests either orchestrate the setup + poll `jobsList` for the expected
end state (auto-PASS on success), or print step-by-step manual repro and return BLOCKED.

## Run

```bash
cd ~/Code/Relay/AppSDK/app-js

# minimum: app + secret
export RELAY_API_KEY=...
export RELAY_SECRET=...

# device-side tests need at least this:
export OTA_TEST_DEVICE_ID=<device id>               # The device's id. In this setup the
                                                     # same value lives in 3 places: the JWT
                                                     # nats.org_data.api_key_id, Devices._id,
                                                     # and the <device> token in NATS subjects.
                                                     # Used for both targeting ({type:"devices"})
                                                     # and matching jobs in jobsList.

# optional:
export OTA_TEST_BIN=/path/to/test.bin               # default = device-cpp example build
export OTA_TEST_DEVICE_VERSION=1.0.2                # T64 (dedup) — version device runs now
export OTA_TEST_GROUP_ID=<group_id>                 # T18 (group target shapes)
export OTA_TEST_GROUP_TYPE=logical_group            # or hierarchy_group
export OTA_TEST_ORG2_API_KEY=...                    # T19, T60 (cross-org isolation)
export OTA_TEST_ORG2_SECRET=...
export OTA_TEST_WAIT_DOWNLOAD_MS=90000              # device download timeout
export OTA_TEST_WAIT_INSTALL_MS=180000              # device install + reboot + commit

# pick what to run:
node examples/ota-tests/run.js T01                  # one test
node examples/ota-tests/run.js T01 T02 T05          # several
node examples/ota-tests/run.js A                    # whole area
node examples/ota-tests/run.js T20-T26              # range
node examples/ota-tests/run.js app-only             # skip device-required
node examples/ota-tests/run.js all                  # everything

# step mode — pauses after each test so you can inspect logs / update Miro
# before continuing. Enter = next, s = skip, q = quit.
node examples/ota-tests/run.js A --step
node examples/ota-tests/run.js all --step
```

## Output

```
T01 A  Upload valid bin ............. PASS  812ms — fw=66b… size=1112432
T02 A  Duplicate (name,version) ..... PASS  531ms
T03 A  Empty file rejected .......... PASS  102ms
...
summary  17 pass  0 fail  52 blocked  of 69
```

`BLOCKED` means the test ran the orchestration but needs a manual step to confirm
the assertion — read the reason for what to do on the device side or in the NATS
account config. After you confirm, mark the row PASS in the Miro table directly.

## What each test does (one-line index)

| T## | What it checks |
|-----|----------------|
| T01 | firmwareUpload — id/sha256/size returned |
| T02 | dup (name,version) → `VERSION_EXISTS` |
| T03 | empty body rejected |
| T04 | > 64MB rejected (`FILE_TOO_LARGE`) |
| T05 | firmwareList pagination + size-0 excluded |
| T06 | firmwareDelete — gone afterward |
| T07 | delete blocked while ACTIVE, allowed after STOPPED |
| T08 | HTTP token exchange (app side) |
| T09 | createRollout DRAFT — zero jobs |
| T10 | updateRollout DRAFT only |
| T11 | deleteRollout DRAFT only |
| T12 | activate snapshots 1 job/device PENDING |
| T13 | empty target → `NO_TARGET_DEVICES` |
| T14 | pause |
| T15 | resume keeps same snapshot |
| T16 | STOPPED is terminal |
| T17 | invalid transitions |
| T18 | target shapes (all/devices/group/exclude) |
| T19 | cross-org `NOT_FOUND` |
| T20 | nudge → immediate action |
| T21 | poll on connect (BLOCKED — manual) |
| T22 | re-poll interval (BLOCKED — long wait) |
| T23 | manual check() (BLOCKED — device call) |
| T24 | FIFO-gated nudge |
| T25 | lost-nudge backstop (BLOCKED — account-config) |
| T26 | duplicate nudge (BLOCKED — manual replay) |
| T27 | happy download |
| T28 | sha mismatch (BLOCKED — tamper artifact) |
| T29 | no OTA partition (BLOCKED — flash setup) |
| T30 | no HTTP token (BLOCKED — perms) |
| T31 | URL fetch fails (BLOCKED — break bearer) |
| T32 | WiFi drop mid-download (BLOCKED — manual) |
| T33 | on_download veto + force_download override |
| T34 | DOWNLOAD_ONLY stages and parks |
| T35 | install happy path |
| T36 | pre_install veto + force_install |
| T37 | post_install reject (BLOCKED — hook setup) |
| T38 | crash-loop image (BLOCKED — bad build) |
| T39 | downgrade reports INSTALLED (BLOCKED — labels) |
| T40 | A/B partition flip (BLOCKED — device inspection) |
| T41 | power-cut in PENDING_VERIFY (BLOCKED — manual) |
| T42 | custom on_install (BLOCKED — hook) |
| T43 | phase ledger end-to-end |
| T44 | failure phases reflected (BLOCKED — triggered with T28/T30/T33) |
| T45 | engine drops device-set PENDING (BLOCKED — custom fw) |
| T46 | status matched by api_key_id |
| T47 | jobHistory pagination |
| T48 | sequential rollouts |
| T49 | stepped upgrade chain |
| T50 | PAUSED head blocks queue |
| T51 | re-poll before install |
| T52 | retry-older-mid-newer (BLOCKED — orchestration) |
| T53 | busy-device discovery (completion re-poll) |
| T54 | retry re-arms (BLOCKED — needs a failed job) |
| T55 | retry filters (BLOCKED — needs mixed phases) |
| T56 | retry guards (DRAFT / STOPPED) |
| T57 | zero re-arm → zero nudges |
| T58 | device re-attempt on retry (BLOCKED) |
| T59 | fan-out to N devices |
| T60 | multi-org isolation |
| T61 | engine horizontal scale (BLOCKED — infra) |
| T62 | concurrent status atomicity (BLOCKED — load gen) |
| T63 | reconnect mid-flow (BLOCKED — NATS drop) |
| T64 | dedup on already-installed version |
| T65 | no busy-loop on persistent fail (BLOCKED — pair with T29) |
| T66 | token expiry / refresh (BLOCKED — long wait) |
| T67 | device identity = api_key_id |
| T68 | account subject mappings (BLOCKED — account config) |
| T69 | partition table + flash size (BLOCKED — `idf.py partition-table`) |
| T70 | INSTALL_ONLY happy path — DOWNLOAD_ONLY stage + PAUSE/RESUME → device installs without re-downloading |
| T71 | INSTALL_ONLY dedup — receives push for already-running version → INSTALLED instantly (BLOCKED — needs rollout.install or direct nats publish) |
| T72 | INSTALL_ONLY negatives — no staged image / version mismatch → FAILED (BLOCKED — same as T71) |
| T73 | Resume routes by phase — PENDING/DOWNLOADING get firmware_update; DOWNLOADED get INSTALL_ONLY (BLOCKED — multi-device) |

## Cleanup

Each test that creates a rollout stops it before returning. Firmwares are left
in place (they're cheap and have unique `(name, version)` per run thanks to a
process-tagged version scheme). To wipe everything created during these tests:

```bash
node examples/ota-clear.js
```

## Notes

* `device_id` everywhere is the **api_key_id** from the device's JWT (the
  `nats.org_data.api_key_id` claim), not Mongo `Devices._id`. See T67.
* Versions are namespaced under `9.<test#>.<patch>` per run so reruns don't
  fail with `VERSION_EXISTS`. Combined with the new
  `@@unique([org_id, name, version])`, the test name also varies per run.
* The runner shares one `RelayApp` across tests. If you need test isolation,
  run one at a time.
