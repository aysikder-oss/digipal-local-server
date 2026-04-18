# local-server tests

Integration coverage for the local hub. Currently focused on the sync
resilience hardening from Task #352.

## Running

The hub's native dependency `better-sqlite3` is compiled for Electron's Node
ABI (NODE_MODULE_VERSION 125). To exercise the SQL-bound tests, run them
through Electron's bundled Node:

```bash
ELECTRON_RUN_AS_NODE=1 ./node_modules/.bin/electron --test tests/
```

Plain `node --test tests/` will still run the non-sqlite tests (e.g. the
backoff math) and skip the rest with a clear message — useful in CI
environments without the Electron binary.

## What's covered

`sync-resilience.test.js` exercises the four scenarios from the Task #352
plan:

1. **Network drop / reconnect** — mirrors the live `scheduleReconnect`
   formula and asserts exponential growth followed by a jittered plateau
   at the cap (no permanent stop).
2. **Missing-ack accumulation** — `pruneAckedChanges` removes acked rows
   while preserving unpushed ones; `getOldestUnpushedAgeMs` reports the
   age the heartbeat health check uses for alerting.
3. **Multi-NIC selection** — opens a real WS server on loopback and
   asserts `cachedSocketLocalIp` is populated from the active socket's
   `localAddress` rather than the first NIC.
4. **Rapid insert/update/delete** — `hasPendingInsert` blocks UPDATE/DELETE
   while a pending INSERT exists, then unblocks once the INSERT acks.

Plus an extra check that `pruneOrphanIdMappings` strips both vanished-row
mappings and any mappings for tables outside the synced allowlist.
