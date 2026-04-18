'use strict';
/*
 * Tests for media-downloader (Task #354):
 *   - Schema-aware JSON walker collects /objects/ refs from arbitrary tree
 *     shapes, including new fields the codebase doesn't statically know about.
 *   - Walker rewrite swaps refs anywhere in the tree (top-level scalar,
 *     deeply nested, inside arrays) and matches both /objects/... refs and
 *     absolute URLs that embed the same path.
 *   - normalizeToObjectPath handles the common cases.
 *   - Configurable concurrency reads/writes via getConfig with sane bounds.
 *   - Backfill re-queues completed rows whose local file is missing.
 *
 * Run:
 *   ELECTRON_RUN_AS_NODE=1 ./node_modules/.bin/electron --test tests/media-downloader.test.js
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const Module = require('node:module');

const electronStubPath = require.resolve('./_electron-stub.js');
const origResolve = Module._resolveFilename;
Module._resolveFilename = function (request, parent, ...rest) {
  if (request === 'electron') return electronStubPath;
  return origResolve.call(this, request, parent, ...rest);
};

let mediaDl;
let sqlite;
let sqliteAvailable = true;
let sqliteSkipReason = '';
try {
  mediaDl = require('../dist/server/media-downloader.js');
  sqlite = require('../dist/db/sqlite.js');
  sqlite.initDatabase(':memory:');
} catch (err) {
  sqliteAvailable = false;
  sqliteSkipReason = err && err.message ? err.message.split('\n')[0] : String(err);
  console.warn('[tests] media-downloader sqlite-backed tests will be skipped:', sqliteSkipReason);
}
const skipIfNoSqlite = () => (sqliteAvailable ? false : sqliteSkipReason);

// =========================================================================
// Pure helpers — no DB needed; runs even when better-sqlite3 ABI is mismatched.
// =========================================================================

// Re-load the helpers via the __testing__ export when available; otherwise
// require the source-built file in a degraded mode.
let helpers = mediaDl?.__testing__;
if (!helpers) {
  // Last resort: load from the built JS even if sqlite init failed earlier.
  try {
    const mod = require('../dist/server/media-downloader.js');
    helpers = mod.__testing__;
  } catch {}
}

const helpersAvail = helpers ? false : 'media-downloader helpers unavailable';

test('normalizeToObjectPath: bare path passes through', { skip: helpersAvail }, () => {
  assert.equal(helpers.normalizeToObjectPath('/objects/foo/bar.png'), '/objects/foo/bar.png');
});

test('normalizeToObjectPath: absolute URL with /objects/ in path is normalized', { skip: helpersAvail }, () => {
  assert.equal(
    helpers.normalizeToObjectPath('https://cloud.example/api/whatever/objects/abc/def.jpg?x=1'),
    '/objects/abc/def.jpg',
  );
});

test('normalizeToObjectPath: non-object URL returns null', { skip: helpersAvail }, () => {
  assert.equal(helpers.normalizeToObjectPath('https://example.com/foo.png'), null);
  assert.equal(helpers.normalizeToObjectPath('not a url'), null);
  assert.equal(helpers.normalizeToObjectPath(''), null);
});

test('walkCollectObjectPaths: extracts refs from deeply nested + arbitrary fields', { skip: helpersAvail }, () => {
  const tree = {
    pages: [
      {
        backgroundImage: '/objects/bg.jpg',
        elements: [
          { properties: { imageUrl: '/objects/a.png' } },
          { properties: { videoUrl: 'https://cloud.example/api/objects/b.mp4' } },
          // Field name the agent has NEVER seen — schema evolved.
          { props: { brandLogo: { src: '/objects/logo.svg' } } },
          { meta: { gallery: ['/objects/g1.jpg', '/objects/g2.jpg', 'irrelevant'] } },
        ],
      },
    ],
    // A future top-level field that holds media.
    headerImage: '/objects/header.png',
  };
  const out = new Set();
  helpers.walkCollectObjectPaths(tree, out);
  const sorted = [...out].sort();
  assert.deepEqual(sorted, [
    '/objects/a.png',
    '/objects/b.mp4',
    '/objects/bg.jpg',
    '/objects/g1.jpg',
    '/objects/g2.jpg',
    '/objects/header.png',
    '/objects/logo.svg',
  ]);
});

test('walkRewrite: rewrites a deep ref + leaves unrelated values intact', { skip: helpersAvail }, () => {
  const tree = {
    pages: [
      {
        elements: [
          { properties: { imageUrl: '/objects/a.png', videoUrl: '/objects/b.mp4' } },
          { weirdNewField: { nested: ['noop', '/objects/a.png'] } },
        ],
      },
    ],
  };
  const r = helpers.walkRewrite(tree, '/objects/a.png', '/media/local-a.png');
  assert.equal(r.changed, true);
  assert.equal(tree.pages[0].elements[0].properties.imageUrl, '/media/local-a.png');
  assert.equal(tree.pages[0].elements[0].properties.videoUrl, '/objects/b.mp4');
  assert.equal(tree.pages[0].elements[1].weirdNewField.nested[1], '/media/local-a.png');
});

test('walkRewrite: matches absolute URLs that embed the object path', { skip: helpersAvail }, () => {
  const tree = { src: 'https://cloud.example/api/x/objects/foo.png?cb=1' };
  const r = helpers.walkRewrite(tree, '/objects/foo.png', '/media/foo.png');
  assert.equal(r.changed, true);
  assert.equal(tree.src, '/media/foo.png');
});

test('walkRewrite: returns unchanged=false when ref absent', { skip: helpersAvail }, () => {
  const tree = { src: '/objects/other.png' };
  const r = helpers.walkRewrite(tree, '/objects/missing.png', '/media/x.png');
  assert.equal(r.changed, false);
  assert.equal(tree.src, '/objects/other.png');
});

// =========================================================================
// Configurable concurrency
// =========================================================================

test('media downloader concurrency: defaults to a value > 3 and is configurable', { skip: skipIfNoSqlite() }, () => {
  // Default is the new safer cap (raised from the old hardcoded 3).
  const def = mediaDl.getMediaDownloaderConcurrency();
  assert.ok(def > 3, `default concurrency should be > old hardcoded 3, got ${def}`);

  const applied = mediaDl.setMediaDownloaderConcurrency(12);
  assert.equal(applied, 12);
  assert.equal(mediaDl.getMediaDownloaderConcurrency(), 12);

  // Hard cap is enforced.
  const capped = mediaDl.setMediaDownloaderConcurrency(9999);
  assert.ok(capped <= 32, `value should be clamped to hard cap, got ${capped}`);

  // Invalid values are rejected.
  assert.throws(() => mediaDl.setMediaDownloaderConcurrency(0));
  assert.throws(() => mediaDl.setMediaDownloaderConcurrency(-5));

  // Reset so this doesn't poison other tests.
  mediaDl.setMediaDownloaderConcurrency(6);
});

// =========================================================================
// Backfill: completed rows pointing to vanished local files get re-queued.
// =========================================================================

test('backfillMissingLocalFiles re-queues completed rows whose file is gone', { skip: skipIfNoSqlite() }, () => {
  const db = sqlite.getDb();
  db.exec('DELETE FROM media_downloads;');

  // Row A: local file exists -> should remain completed.
  // Row B: local file is missing -> should be reset to pending.
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mediadl-test-'));
  // backfillMissingLocalFiles uses getMediaDir()/uploads. file-storage.ts
  // builds it under app.getPath('home')/DigipalMedia/uploads — so we use
  // the same path the stub returns.
  const stub = require('./_electron-stub.js');
  const uploadsDir = path.join(stub.app.getPath('home'), 'DigipalMedia', 'uploads');
  fs.mkdirSync(uploadsDir, { recursive: true });

  const presentName = 'present.png';
  fs.writeFileSync(path.join(uploadsDir, presentName), 'data');
  const missingName = 'gone-' + Math.random().toString(36).slice(2) + '.png';

  db.prepare(`INSERT INTO media_downloads (object_path, local_filename, status, content_id, field, file_size, bytes_downloaded)
    VALUES (?, ?, 'completed', 1, 'data', 4, 4)`).run('/objects/present.png', presentName);
  db.prepare(`INSERT INTO media_downloads (object_path, local_filename, status, content_id, field, file_size, bytes_downloaded)
    VALUES (?, ?, 'completed', 2, 'data', 4, 4)`).run('/objects/gone.png', missingName);

  const reQueued = mediaDl.backfillMissingLocalFiles();
  assert.ok(reQueued >= 1, 'should re-queue the missing-file row');

  const rowA = db.prepare("SELECT status FROM media_downloads WHERE object_path = '/objects/present.png'").get();
  const rowB = db.prepare("SELECT status, local_filename, bytes_downloaded FROM media_downloads WHERE object_path = '/objects/gone.png'").get();
  assert.equal(rowA.status, 'completed', 'present file should stay completed');
  assert.equal(rowB.status, 'pending', 'missing file should be re-queued');
  // CRITICAL: local_filename must be PRESERVED so the re-downloaded bytes
  // land back at the same /media/<file> path that JSON content rows
  // already reference. Clearing it would orphan every existing reference.
  assert.equal(rowB.local_filename, missingName, 'local_filename must be preserved across backfill so existing /media/ refs heal');
  assert.equal(rowB.bytes_downloaded, 0, 'byte counter should be reset');
});

// =========================================================================
// Regression: backfill must NOT orphan existing /media/<file> refs in JSON.
// =========================================================================

test('regression: backfill keeps content JSON refs valid after redownload', { skip: skipIfNoSqlite() }, () => {
  const db = sqlite.getDb();
  db.exec('DELETE FROM media_downloads;');
  db.exec('DELETE FROM contents;');

  const stub = require('./_electron-stub.js');
  const uploadsDir = path.join(stub.app.getPath('home'), 'DigipalMedia', 'uploads');
  fs.mkdirSync(uploadsDir, { recursive: true });

  // Simulate: a previous successful download. Content JSON has been
  // rewritten from /objects/foo.png -> /media/cloud-abcdef.png; the
  // local file has since been deleted by the user.
  const localName = 'cloud-fixedname.png';
  const objectPath = '/objects/foo-evolved.png';
  fs.writeFileSync(path.join(uploadsDir, localName), 'old');

  db.prepare(`INSERT INTO media_downloads (object_path, local_filename, status, content_id, field, file_size, bytes_downloaded)
    VALUES (?, ?, 'completed', 100, 'canvas_data', 3, 3)`).run(objectPath, localName);

  const canvas = JSON.stringify({
    pages: [{ elements: [{ properties: { imageUrl: `/media/${localName}` } }] }],
  });
  db.prepare(`INSERT INTO contents (id, name, type, data, canvas_data) VALUES (?, ?, ?, ?, ?)`)
    .run(100, 'evolved', 'canvas', '', canvas);

  // User deletes the file.
  fs.unlinkSync(path.join(uploadsDir, localName));

  // Backfill discovers it and re-queues.
  const reQueued = mediaDl.backfillMissingLocalFiles();
  assert.ok(reQueued >= 1);

  const row = db.prepare(`SELECT local_filename, status FROM media_downloads WHERE object_path = ?`).get(objectPath);
  assert.equal(row.status, 'pending');
  // Filename preserved -> when re-download finishes and writes to
  // /media/<localName>, the JSON ref already there is automatically valid.
  assert.equal(row.local_filename, localName);

  // Verify the content row's reference is still pointing at the SAME local
  // path (not at the cloud /objects/ path) — once the file is back on
  // disk, every player request for /media/<localName> succeeds with no
  // additional rewrite needed.
  const contentRow = db.prepare('SELECT canvas_data FROM contents WHERE id = 100').get();
  const parsed = JSON.parse(contentRow.canvas_data);
  assert.equal(parsed.pages[0].elements[0].properties.imageUrl, `/media/${localName}`,
    'JSON ref must remain pointing at the same /media/<file> across backfill');
});

// =========================================================================
// scanAndQueueAllCloudContent: schema-aware, finds refs in unfamiliar JSON fields.
// =========================================================================

test('scanAndQueueAllCloudContent picks up media refs in arbitrary JSON shapes', { skip: skipIfNoSqlite() }, () => {
  const db = sqlite.getDb();
  db.exec('DELETE FROM media_downloads;');
  db.exec('DELETE FROM contents;');

  // Use a schema we KNOW the static walker doesn't hard-code: a top-level
  // "headerImage" inside canvas_data, and a nested array of media in a new
  // "gallery" field. Both should still be discovered.
  const canvas = JSON.stringify({
    headerImage: '/objects/new-header.png',
    pages: [
      { elements: [{ widgetThatDoesntExistYet: { src: '/objects/widget-img.jpg' } }] },
    ],
    gallery: ['/objects/g-a.png', { url: '/objects/g-b.png' }],
  });
  db.prepare(`INSERT INTO contents (id, name, type, data, canvas_data) VALUES (?, ?, ?, ?, ?)`)
    .run(900, 'evolved', 'image', '', canvas);

  const queued = mediaDl.scanAndQueueAllCloudContent();
  assert.ok(queued >= 4, `expected at least 4 queued from evolved schema, got ${queued}`);

  const objectPaths = db.prepare("SELECT object_path FROM media_downloads ORDER BY object_path").all().map(r => r.object_path);
  assert.ok(objectPaths.includes('/objects/new-header.png'));
  assert.ok(objectPaths.includes('/objects/widget-img.jpg'));
  assert.ok(objectPaths.includes('/objects/g-a.png'));
  assert.ok(objectPaths.includes('/objects/g-b.png'));
});
