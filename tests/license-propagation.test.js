'use strict';
/*
 * Integration tests for subscription/license propagation latency (Task #353).
 *
 * Run with:
 *   ELECTRON_RUN_AS_NODE=1 ./node_modules/.bin/electron --test tests/license-propagation.test.js
 *
 * Covers:
 *   1. Pairing-grace shields a freshly paired screen from the "1 free screen"
 *      enforcement, even when a higher-id older free screen exists.
 *   2. clearExpiredPairingGrace sweeps stale grace markers so they cannot
 *      shield screens forever.
 *   3. enforceLocalFreeScreenLimit always keeps grace-protected screens and
 *      expires the unprotected excess instead.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');

const electronStubPath = require.resolve('./_electron-stub.js');
const origResolve = Module._resolveFilename;
Module._resolveFilename = function (request, parent, ...rest) {
  if (request === 'electron') return electronStubPath;
  return origResolve.call(this, request, parent, ...rest);
};

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
const skipIfNoSqlite = () => (sqliteAvailable ? false : sqliteSkipReason);

function resetScreens() {
  db.exec('DELETE FROM screens;');
}

function insertScreen({ id, name, pairingCode, licenseStatus = 'active', plan = 'free', createdAt }) {
  db.prepare(`
    INSERT INTO screens (id, name, pairing_code, license_status, plan, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(id, name, pairingCode, licenseStatus, plan, createdAt || new Date().toISOString());
}

function getStatus(id) {
  return db.prepare('SELECT license_status, pairing_grace_until FROM screens WHERE id = ?').get(id);
}

// =========================================================================
// 1. Pairing-grace shields a freshly paired screen from free-cap enforcement.
// =========================================================================

test('pairing-grace shields a freshly paired screen from free-screen enforcement', { skip: skipIfNoSqlite() }, () => {
  resetScreens();
  // Older free screen — would normally win the "keep oldest" tiebreaker.
  insertScreen({
    id: 1, name: 'old-free', pairingCode: 'OLD-001',
    createdAt: new Date(Date.now() - 60_000).toISOString(),
  });
  // Newly-paired screen on a paid plan that hasn't synced yet (still
  // looks "free" locally because licenses haven't been pulled).
  insertScreen({
    id: 2, name: 'new-paired', pairingCode: 'NEW-002',
    createdAt: new Date().toISOString(),
  });

  // Cloud told us NEW-002 just paired; mark a 10-min grace.
  const updated = sqlite.setScreenPairingGrace('NEW-002', 10 * 60 * 1000);
  assert.equal(updated, 1, 'grace marker should land on exactly one screen');

  sqlite.enforceLocalFreeScreenLimit();

  const a = getStatus(1);
  const b = getStatus(2);
  // Grace-protected screen (id 2) survives; the unprotected older one is expired.
  assert.equal(b.license_status, 'active', 'grace-protected screen stays active');
  assert.equal(a.license_status, 'expired', 'unprotected excess screen is expired');
});

// =========================================================================
// 2. clearExpiredPairingGrace sweeps stale markers.
// =========================================================================

test('clearExpiredPairingGrace removes markers whose window has passed', { skip: skipIfNoSqlite() }, () => {
  resetScreens();
  insertScreen({ id: 10, name: 's10', pairingCode: 'P-010' });
  insertScreen({ id: 11, name: 's11', pairingCode: 'P-011' });

  // One in the past, one in the future.
  const past = new Date(Date.now() - 60_000).toISOString();
  const future = new Date(Date.now() + 60_000).toISOString();
  db.prepare('UPDATE screens SET pairing_grace_until = ? WHERE id = ?').run(past, 10);
  db.prepare('UPDATE screens SET pairing_grace_until = ? WHERE id = ?').run(future, 11);

  sqlite.clearExpiredPairingGrace();

  assert.equal(getStatus(10).pairing_grace_until, null, 'expired marker is cleared');
  assert.ok(getStatus(11).pairing_grace_until, 'fresh marker is preserved');
});

// =========================================================================
// 3. enforceLocalFreeScreenLimit calls clearExpiredPairingGrace internally,
//    so an expired grace cannot shield a screen forever.
// =========================================================================

test('enforce sweeps expired grace markers before evaluating the cap', { skip: skipIfNoSqlite() }, () => {
  resetScreens();
  insertScreen({
    id: 20, name: 'older', pairingCode: 'OLD-020',
    createdAt: new Date(Date.now() - 120_000).toISOString(),
  });
  insertScreen({
    id: 21, name: 'newer-stale-grace', pairingCode: 'STALE-021',
    createdAt: new Date().toISOString(),
  });
  // Stale grace window — already expired.
  db.prepare('UPDATE screens SET pairing_grace_until = ? WHERE id = ?')
    .run(new Date(Date.now() - 1000).toISOString(), 21);

  sqlite.enforceLocalFreeScreenLimit();

  // With grace expired and swept, normal "keep oldest free screen" wins.
  assert.equal(getStatus(20).license_status, 'active', 'older free screen kept');
  assert.equal(getStatus(21).license_status, 'expired', 'stale-grace screen is no longer protected');
  assert.equal(getStatus(21).pairing_grace_until, null, 'stale grace marker is gone');
});

// =========================================================================
// 4. setScreenPairingGrace gracefully no-ops on unknown pairing codes.
// =========================================================================

test('setScreenPairingGrace returns 0 for unknown pairing codes', { skip: skipIfNoSqlite() }, () => {
  resetScreens();
  const updated = sqlite.setScreenPairingGrace('DOES-NOT-EXIST', 60_000);
  assert.equal(updated, 0);
});

// =========================================================================
// 5. Regression: pairing_grace_until is HUB-LOCAL — it must never enter
//    the local_changes log (would push to cloud) and must be stripped from
//    getFullRow output (defense-in-depth).
// =========================================================================

test('pairing_grace_until writes do not enqueue local_changes for sync', { skip: skipIfNoSqlite() }, () => {
  resetScreens();
  insertScreen({ id: 30, name: 's30', pairingCode: 'PG-030' });
  // Drain any rows produced by the seed insert above so we measure ONLY the
  // grace write below.
  db.exec('DELETE FROM local_changes;');

  sqlite.setScreenPairingGrace('PG-030', 60_000);
  sqlite.clearExpiredPairingGrace();

  const count = db.prepare('SELECT COUNT(*) AS c FROM local_changes').get().c;
  assert.equal(count, 0, 'pairing-grace writes must not be tracked for cloud sync');
});

test('getFullRow strips pairing_grace_until before sync push', { skip: skipIfNoSqlite() }, () => {
  resetScreens();
  insertScreen({ id: 31, name: 's31', pairingCode: 'PG-031' });
  sqlite.setScreenPairingGrace('PG-031', 60_000);

  const row = sqlite.getFullRow('screens', 31);
  assert.ok(row, 'row should exist');
  assert.equal(row.pairing_grace_until, undefined, 'hub-local column must be stripped from sync payload');
  assert.equal(row.pairing_code, 'PG-031', 'other columns survive');
});
