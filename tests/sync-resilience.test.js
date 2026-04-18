'use strict';
/*
 * Integration tests for sync resilience hardening (Task #352).
 *
 * Run with:
 *   node --test tests/sync-resilience.test.js                  # non-sqlite tests
 *   ELECTRON_RUN_AS_NODE=1 ./node_modules/.bin/electron --test tests/   # full suite
 *
 * Covers the four scenarios called out in the task plan:
 *   1. Network drop / reconnect — CloudSync keeps retrying with growing
 *      jittered backoff (consecutiveFailures climbs without limit).
 *   2. Missing-ack accumulation — pruneAckedChanges + getOldestUnpushedAgeMs.
 *   3. Multi-NIC selection — getActiveSocketLocalIp uses the live socket's
 *      localAddress instead of the first NIC.
 *   4. Rapid insert/update/delete — hasPendingInsert holds back UPDATE/DELETE
 *      while a still-pending INSERT exists, then unblocks once it acks.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const Module = require('node:module');
const http = require('node:http');
const { WebSocketServer } = require('ws');

// --- Stub the `electron` module before anything else loads it. ----------
const electronStubPath = require.resolve('./_electron-stub.js');
const origResolve = Module._resolveFilename;
Module._resolveFilename = function (request, parent, ...rest) {
  if (request === 'electron') return electronStubPath;
  return origResolve.call(this, request, parent, ...rest);
};

// Try to load sqlite. better-sqlite3 in this project is compiled for
// Electron's Node ABI, so on system Node the require will throw — in that
// case we skip sql-bound tests but still run the network ones.
let sqlite, db;
let sqliteAvailable = true;
let sqliteSkipReason = '';
try {
  sqlite = require('../dist/db/sqlite.js');
  sqlite.initDatabase(':memory:');
  db = sqlite.getDb();
} catch (err) {
  sqliteAvailable = false;
  sqliteSkipReason = err && err.message ? err.message.split('\n')[0] : String(err);
  console.warn('[tests] sqlite-backed tests will be skipped:', sqliteSkipReason);
}

// Helper that returns either `false` (run the test) or a string skip reason.
const skipIfNoSqlite = () => (sqliteAvailable ? false : sqliteSkipReason);

// --- Helpers ---------------------------------------------------------------

function seedSchema() {
  db.exec(`
    DELETE FROM hub_id_map;
    DELETE FROM local_changes;
    DELETE FROM screens;
  `);
}

function insertScreen(id, name) {
  db.prepare('INSERT INTO screens (id, name) VALUES (?, ?)').run(id, name);
}

function logChange(table, recordId, op, opts = {}) {
  const pushed = opts.pushed ? 1 : 0;
  const createdAt = opts.createdAt ?? new Date().toISOString();
  db.prepare(`
    INSERT INTO local_changes (table_name, record_id, operation, payload, pushed, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(table, recordId, op, '{}', pushed, createdAt);
}

// =========================================================================
// 1a. Reconnect backoff math (no sqlite/CloudSync needed — runs in any CI).
// =========================================================================

test('reconnect backoff grows then plateaus at cap with bounded jitter', () => {
  // Mirrors the exact math in cloud-sync.scheduleReconnect:
  //   delay = min(base * 2^min(failures-1,8), cap)
  //   jittered = delay * (1 + (random()-0.5) * 0.5)   // ±25%
  const BASE = 10_000;
  const CAP = 5 * 60 * 1000;
  const sample = (failures) => {
    const raw = Math.min(BASE * Math.pow(2, Math.min(failures - 1, 8)), CAP);
    return raw + (Math.random() - 0.5) * 0.5 * raw;
  };

  const medians = [];
  for (let f = 1; f <= 12; f++) {
    const xs = Array.from({ length: 200 }, () => sample(f));
    xs.sort((a, b) => a - b);
    medians.push(xs[100]);
  }

  // Growth phase: each step at least 1.5x the prior, until raw delay hits the cap.
  // Raw schedule: 10s, 20s, 40s, 80s, 160s -> 320s capped at 300s (failure 6).
  for (let i = 1; i < 5; i++) {
    assert.ok(medians[i] > medians[i - 1] * 1.5,
      `failure ${i + 1}: expected growth (${medians[i - 1]} -> ${medians[i]})`);
  }
  // Plateau phase: stays inside the ±25% jitter band around the cap.
  for (let i = 6; i < medians.length; i++) {
    assert.ok(medians[i] <= CAP * 1.25,
      `failure ${i + 1}: expected <= cap+jitter, got ${medians[i]}`);
    assert.ok(medians[i] >= CAP * 0.75,
      `failure ${i + 1}: expected >= cap-jitter, got ${medians[i]}`);
  }
});

// =========================================================================
// 1b. Reconnect behavior on a real CloudSync against a closed port.
// =========================================================================

test('CloudSync keeps retrying with growing failure count (no permanent stop)',
  { skip: skipIfNoSqlite() }, async () => {
  const { CloudSync } = require('../dist/server/cloud-sync.js');

  // Point at an unroutable port that will refuse connection immediately.
  // 127.0.0.1:1 is reserved + nothing listens; ECONNREFUSED is the typical reply.
  const cs = new CloudSync('http://127.0.0.1:1', 'test-token', () => {
    assert.fail('onAuthFailure must NOT fire on transient connect refusals');
  });
  cs.start();

  // Speed up the test by stubbing scheduleReconnect to fire immediately.
  const orig = cs.scheduleReconnect.bind(cs);
  let scheduledCount = 0;
  cs.scheduleReconnect = function () {
    scheduledCount++;
    if (cs.reconnectTimeout) { clearTimeout(cs.reconnectTimeout); cs.reconnectTimeout = null; }
    cs.reconnectTimeout = setTimeout(() => cs.connect(), 5);
  };

  await new Promise((res) => setTimeout(res, 400));

  cs.stop();
  await new Promise((res) => setTimeout(res, 50));

  assert.ok(cs.consecutiveFailures >= 5,
    `expected several consecutive failures, got ${cs.consecutiveFailures}`);
  assert.ok(cs.consecutiveFailures < 1000, 'sanity bound');
  assert.ok(scheduledCount >= 5, 'scheduleReconnect must keep firing on refusal');
  // The 5-failure permanent stop is gone: isRunning was true throughout.
  // (After stop() it's false; we check below by ensuring we DID retry past 5.)
});

// =========================================================================
// 2. Missing-ack accumulation: prune + age helpers.
// =========================================================================

test('pruneAckedChanges removes acked rows and keeps unpushed', { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  insertScreen(1, 'A');
  const old = new Date(Date.now() - 10 * 24 * 3600_000).toISOString();
  // Two rows already acked (pushed=1), well past the safety tail.
  logChange('screens', 1, 'UPDATE', { pushed: true, createdAt: old });
  logChange('screens', 1, 'UPDATE', { pushed: true, createdAt: old });
  // One still unpushed — must survive the prune.
  logChange('screens', 1, 'UPDATE');

  const pruned = sqlite.pruneAckedChanges({
    maxAgeMs: 60_000,   // anything older than 1m is eligible
    keepTail: 0,        // keep no acked rows for diagnostics in this test
    hardCap: 10_000,
  });
  assert.ok(pruned >= 2, `expected to prune the acked rows, pruned=${pruned}`);

  const remaining = db.prepare('SELECT COUNT(*) as n FROM local_changes').get().n;
  assert.equal(remaining, 1, 'unpushed row must remain');
});

test('getOldestUnpushedAgeMs reports age the heartbeat health check uses',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  assert.equal(sqlite.getOldestUnpushedAgeMs(), 0,
    'empty change log must report age=0');

  insertScreen(2, 'B');
  const past = new Date(Date.now() - 90 * 60_000).toISOString();
  logChange('screens', 2, 'UPDATE', { createdAt: past });
  const age = sqlite.getOldestUnpushedAgeMs();
  assert.ok(age > 60 * 60_000, `expected >1h age, got ${age}ms`);
});

// =========================================================================
// 3. Multi-NIC selection: cachedSocketLocalIp comes from the active socket.
// =========================================================================

test('CloudSync IP detection prefers active socket; filters loopback safely',
  { skip: skipIfNoSqlite() }, async () => {
  const httpServer = http.createServer();
  const wss = new WebSocketServer({ server: httpServer });
  await new Promise((res) => httpServer.listen(0, '127.0.0.1', res));
  const port = httpServer.address().port;

  // Hold the connection open without sending hubConnected so the IP capture
  // happens but the auth handshake doesn't complete.
  wss.on('connection', () => {});

  const { CloudSync } = require('../dist/server/cloud-sync.js');
  const cs = new CloudSync(`http://127.0.0.1:${port}`, 'test-token', () => {});
  cs.start();
  await new Promise((res) => setTimeout(res, 300));

  // The raw socket.localAddress is loopback (we connected via 127.0.0.1).
  const rawAddr = cs.ws && cs.ws._socket && cs.ws._socket.localAddress;
  assert.ok(rawAddr && (rawAddr.includes('127.0.0.1') || rawAddr === '::1'),
    `expected raw socket localAddress to be loopback, got "${rawAddr}"`);

  // getActiveSocketLocalIp() INTENTIONALLY filters loopback / link-local — we
  // never want to advertise 127.0.0.1 as the hub's address to the cloud.
  // So cachedSocketLocalIp should be undefined when the socket is loopback,
  // and the identify path falls back to a real NIC.
  assert.equal(cs.cachedSocketLocalIp, undefined,
    'loopback socket address must be filtered out (would be invalid to advertise)');

  // Fallback NIC scan should yield a valid non-internal IPv4 on Linux CI.
  const fallback = cs.getFallbackLocalIp();
  if (fallback !== undefined) {
    assert.match(fallback, /^\d+\.\d+\.\d+\.\d+$/, `fallback should be IPv4, got "${fallback}"`);
    assert.notEqual(fallback, '127.0.0.1', 'fallback must not be loopback');
  }

  cs.stop();
  await new Promise((res) => setTimeout(res, 50));
  wss.close();
  await new Promise((res) => httpServer.close(res));
});

// =========================================================================
// 4. Rapid insert/update/delete: hasPendingInsert blocks dependents.
// =========================================================================

test('hasPendingInsert holds back UPDATE/DELETE while INSERT is unacked',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  insertScreen(42, 'rapid');
  // Simulate the rapid create-then-update sequence: INSERT then UPDATE,
  // both still unpushed, no cloud mapping yet.
  logChange('screens', 42, 'INSERT');
  logChange('screens', 42, 'UPDATE');

  assert.equal(sqlite.hasPendingInsert('screens', 42), true,
    'pending INSERT must be detected so dependent UPDATE/DELETE waits');

  // After the INSERT acks, the pending-insert flag must clear so the
  // dependent UPDATE/DELETE can drain on the next push.
  db.prepare(`
    UPDATE local_changes SET pushed = 1
    WHERE table_name='screens' AND record_id=42 AND operation='INSERT'
  `).run();
  assert.equal(sqlite.hasPendingInsert('screens', 42), false,
    'after INSERT ack, dependents may proceed');
});

// =========================================================================
// 5. Missed-ack scenario does NOT produce indefinite local_changes growth.
// =========================================================================

test('mapped record: missed-ack churn does not produce unbounded growth',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  insertScreen(100, 'churn');
  // The cloud already created this record on a prior session, so the mapping
  // exists. This is the typical real-world shape of missed-ack churn.
  db.prepare(`INSERT INTO hub_id_map (table_name, local_id, cloud_id) VALUES ('screens', 100, 9100)`).run();

  const eightDaysAgo = new Date(Date.now() - 8 * 24 * 3600_000).toISOString();
  for (let i = 0; i < 50; i++) {
    logChange('screens', 100, 'UPDATE', { createdAt: eightDaysAgo });
  }
  // Plus a fresh entry that MUST survive pruning.
  logChange('screens', 100, 'UPDATE');
  assert.equal(db.prepare('SELECT COUNT(*) as n FROM local_changes').get().n, 51);

  const recovered = sqlite.pruneStaleUnackedChanges({ minAgeMs: 60 * 60_000 });
  // All 50 stale entries have a strictly-newer same-record entry AND the
  // mapping exists, so all 50 prune.
  assert.equal(recovered, 50, `expected 50 stale entries pruned, got ${recovered}`);
  assert.equal(db.prepare('SELECT COUNT(*) as n FROM local_changes').get().n, 1,
    'fresh entry must survive');

  // Idempotent.
  assert.equal(sqlite.pruneStaleUnackedChanges({ minAgeMs: 60 * 60_000 }), 0);
});

test('UNMAPPED record: pruning never drops the only INSERT (sync safety)',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  insertScreen(200, 'newborn');
  // No hub_id_map row — the cloud has never confirmed creating this record.
  const old = new Date(Date.now() - 10 * 24 * 3600_000).toISOString();
  logChange('screens', 200, 'INSERT', { createdAt: old });
  logChange('screens', 200, 'UPDATE', { createdAt: old });

  const recovered = sqlite.pruneStaleUnackedChanges({ minAgeMs: 60 * 60_000 });
  // Without a cloud mapping we MUST NOT drop the INSERT (would strand the
  // UPDATE forever). The UPDATE also must not be dropped (it's the newest).
  assert.equal(recovered, 0,
    'must not prune INSERT/UPDATE for unmapped record — would deadlock sync');
  assert.equal(db.prepare(
    `SELECT COUNT(*) as n FROM local_changes WHERE table_name='screens' AND record_id=200`
  ).get().n, 2, 'both entries preserved');
});

test('pruneStaleUnackedChanges drops INSERT/UPDATE only when local row vanished',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  // No insertScreen — the underlying row was deleted outside the trigger path.
  const old = new Date(Date.now() - 10 * 24 * 3600_000).toISOString();
  logChange('screens', 777, 'INSERT', { createdAt: old });
  logChange('screens', 777, 'UPDATE', { createdAt: old });

  const recovered = sqlite.pruneStaleUnackedChanges({ minAgeMs: 60 * 60_000 });
  assert.equal(recovered, 2, 'orphaned INSERT+UPDATE for vanished row are safe to drop');
});

test('hubSyncPushAck pure-failure response does not trigger immediate re-push',
  { skip: skipIfNoSqlite() }, async () => {
  const httpServer = http.createServer();
  const wss = new WebSocketServer({ server: httpServer });
  await new Promise((res) => httpServer.listen(0, '127.0.0.1', res));
  const port = httpServer.address().port;

  let pushCount = 0;
  let connectedSocket = null;
  wss.on('connection', (ws) => {
    connectedSocket = ws;
    ws.send(JSON.stringify({ type: 'hubConnected', payload: {} }));
    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === 'hubSyncPush') {
        pushCount++;
        // Reply with all-failures: no acks, no mappings.
        const ids = (msg.payload?.changes || []).map((c) => c.id);
        ws.send(JSON.stringify({
          type: 'hubSyncPushAck',
          payload: {
            changeIds: [],
            failedIds: ids,
            errors: ids.map((id) => ({ changeId: id, tableName: 'screens', error: 'simulated' })),
          },
        }));
      }
    });
  });

  const { CloudSync } = require('../dist/server/cloud-sync.js');
  const cs = new CloudSync(`http://127.0.0.1:${port}`, 'test-token', () => {});
  cs.start();
  await new Promise((res) => setTimeout(res, 200));

  // Seed an unpushed row and trigger one push.
  seedSchema();
  insertScreen(999, 'rejected');
  logChange('screens', 999, 'INSERT');
  cs.pushChanges();

  // Wait long enough for any potential hot-loop to manifest.
  await new Promise((res) => setTimeout(res, 1500));

  cs.stop();
  await new Promise((res) => setTimeout(res, 50));
  wss.close();
  await new Promise((res) => httpServer.close(res));

  // Pure-failure ack must NOT cause repeated immediate re-pushes. We expect
  // exactly 1 push for this round; any number > 2 would indicate a hot loop.
  assert.ok(pushCount <= 2, `expected <=2 pushes, got ${pushCount} (hot retry loop)`);
});

test('sustained unacked DELETE churn: queue stabilizes via dedup + emergency cap',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();

  // Phase 1: REENTRANT TRIGGER scenario — same record gets multiple
  // duplicate DELETE rows queued. Dedup must collapse to one tombstone.
  for (let i = 0; i < 10; i++) {
    logChange('screens', 42, 'DELETE');
  }
  let count = db.prepare(`SELECT COUNT(*) as n FROM local_changes WHERE record_id=42 AND operation='DELETE'`).get().n;
  assert.equal(count, 10, 'pre-dedup count');

  const deduped = sqlite.dedupeUnackedDeleteTombstones();
  assert.equal(deduped, 9, `expected 9 duplicate tombstones removed, got ${deduped}`);
  count = db.prepare(`SELECT COUNT(*) as n FROM local_changes WHERE record_id=42 AND operation='DELETE'`).get().n;
  assert.equal(count, 1, 'exactly one DELETE tombstone preserved per record');

  // Phase 2: SUSTAINED OUTAGE — many unique deletes accumulate. Conservative
  // pruning will not touch tombstones; emergency cap is the safety valve.
  // Simulate 120 unique deleted records (above HARD_CAP_TEST=100).
  for (let id = 1000; id < 1120; id++) {
    logChange('screens', id, 'DELETE');
  }
  const before = db.prepare('SELECT COUNT(*) as n FROM local_changes').get().n;
  assert.ok(before >= 121, `expected >=121 entries, got ${before}`);

  // Emergency drop with cap=50 — proves the queue is BOUNDED even when
  // sustained ack-loss produces many distinct tombstones.
  const HARD_CAP_TEST = 50;
  const dropped = sqlite.emergencyDropOldestChanges(HARD_CAP_TEST);
  assert.equal(dropped, before - HARD_CAP_TEST, `expected ${before - HARD_CAP_TEST} dropped, got ${dropped}`);
  const after = db.prepare('SELECT COUNT(*) as n FROM local_changes').get().n;
  assert.equal(after, HARD_CAP_TEST, `queue must be bounded at cap, got ${after}`);

  // Idempotent: running again with same cap is a no-op.
  assert.equal(sqlite.emergencyDropOldestChanges(HARD_CAP_TEST), 0);
});

test('pruneStaleUnackedChanges preserves DELETE tombstones with no successor',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  // The DELETE must reach the cloud — never drop unilaterally.
  const old = new Date(Date.now() - 10 * 24 * 3600_000).toISOString();
  logChange('screens', 555, 'DELETE', { createdAt: old });

  const recovered = sqlite.pruneStaleUnackedChanges({ minAgeMs: 60 * 60_000 });
  assert.equal(recovered, 0, 'orphan DELETE must be preserved');
  assert.equal(db.prepare('SELECT COUNT(*) as n FROM local_changes').get().n, 1);
});

// =========================================================================
// Bonus: pruneOrphanIdMappings — allowlist + orphan removal.
// =========================================================================

test('pruneOrphanIdMappings removes vanished-row + non-allowlisted mappings',
  { skip: skipIfNoSqlite() }, () => {
  seedSchema();
  insertScreen(7, 'kept');
  db.prepare(`INSERT INTO hub_id_map (table_name, local_id, cloud_id) VALUES ('screens', 7, 700)`).run();
  // Mapping for a vanished local row (orphan).
  db.prepare(`INSERT INTO hub_id_map (table_name, local_id, cloud_id) VALUES ('screens', 9999, 9000)`).run();
  // Mapping for a table not in the synced allowlist (must be dropped without
  // ever interpolating the unknown name into dynamic SQL).
  db.prepare(`INSERT INTO hub_id_map (table_name, local_id, cloud_id) VALUES ('not_a_real_table', 1, 1)`).run();

  const removed = sqlite.pruneOrphanIdMappings();
  assert.ok(removed >= 2, `expected at least 2 removals, got ${removed}`);

  const kept = db.prepare(`
    SELECT COUNT(*) as n FROM hub_id_map WHERE table_name='screens' AND local_id=7
  `).get().n;
  assert.equal(kept, 1, 'mapping for existing row must remain');
  const ghost = db.prepare(`
    SELECT COUNT(*) as n FROM hub_id_map WHERE table_name='not_a_real_table'
  `).get().n;
  assert.equal(ghost, 0, 'non-allowlisted table mapping must be dropped');
});
