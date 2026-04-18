import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import { getDb, getSyncState, insertErrorLog, withoutTriggers, getConfig, setConfig } from '../db/sqlite';
import { getMediaDir } from './file-storage';

const DEFAULT_MAX_CONCURRENT = 6;
const MAX_CONCURRENT_HARD_CAP = 32;
const MAX_RETRIES = 3;
const PROCESS_INTERVAL_MS = 5_000;
const PARTIAL_EXT = '.partial';

let processing = false;
let processTimer: NodeJS.Timeout | null = null;
let isRunning = false;

/**
 * Tables and columns that may contain media references (either as plain
 * `/objects/<path>` strings or embedded inside JSON blobs).
 *
 * We avoid hard-coding individual JSON field names (backgroundImage, imageUrl,
 * videoUrl, etc.) — instead the rewrite/scan walkers crawl the parsed JSON
 * tree and match any string that resolves to an `/objects/` path. This way a
 * new field added on the cloud side keeps working without a hub rebuild.
 *
 * `kind: 'scalar'` means the column holds a single URL string.
 * `kind: 'json'` means the column holds a JSON document we should walk.
 * `kind: 'auto'` means the column may be either — we try JSON first, fall
 * back to scalar matching.
 */
type ColumnKind = 'scalar' | 'json' | 'auto';
interface MediaColumn { table: string; column: string; kind: ColumnKind }

const MEDIA_COLUMNS: MediaColumn[] = [
  { table: 'contents',         column: 'data',          kind: 'auto'   },
  { table: 'contents',         column: 'url',           kind: 'scalar' },
  { table: 'contents',         column: 'thumbnail_url', kind: 'scalar' },
  { table: 'contents',         column: 'canvas_data',   kind: 'json'   },
  { table: 'contents',         column: 'settings',      kind: 'json'   },
  { table: 'design_templates', column: 'thumbnail_url', kind: 'scalar' },
  { table: 'design_templates', column: 'canvas_data',   kind: 'json'   },
  { table: 'kiosks',           column: 'data',          kind: 'json'   },
  { table: 'kiosks',           column: 'settings',      kind: 'json'   },
  { table: 'playlists',        column: 'data',          kind: 'json'   },
];

export function startMediaDownloader() {
  if (isRunning) return;
  isRunning = true;
  console.log('[media-dl] Media downloader started');

  const db = getDb();

  // Re-arm any rows left in 'downloading' on a prior crash. We do NOT reset
  // bytes_downloaded — the partial file on disk will let us resume via
  // HTTP Range on the next attempt.
  const staleCount = db.prepare("UPDATE media_downloads SET status = 'pending' WHERE status = 'downloading'").run().changes;
  if (staleCount > 0) {
    console.log(`[media-dl] Reset ${staleCount} stale downloading entries to pending (will resume from byte offset)`);
  }

  // Backfill: completed rows whose local file has gone missing (manual
  // delete, disk corruption, profile move) get re-queued so the next
  // process tick re-fetches them.
  const reQueued = backfillMissingLocalFiles();
  if (reQueued > 0) {
    console.log(`[media-dl] Backfill: re-queued ${reQueued} downloads whose local files were missing`);
  }

  processTimer = setInterval(() => processQueue(), PROCESS_INTERVAL_MS);
  processQueue();
}

export function stopMediaDownloader() {
  isRunning = false;
  if (processTimer) {
    clearInterval(processTimer);
    processTimer = null;
  }
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

const CONFIG_KEY_CONCURRENCY = 'media_downloader_concurrency';

export function getMediaDownloaderConcurrency(): number {
  const raw = getConfig(CONFIG_KEY_CONCURRENCY);
  if (!raw) return DEFAULT_MAX_CONCURRENT;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 1) return DEFAULT_MAX_CONCURRENT;
  return Math.min(Math.floor(n), MAX_CONCURRENT_HARD_CAP);
}

export function setMediaDownloaderConcurrency(value: number): number {
  if (!Number.isFinite(value) || value < 1) {
    throw new Error('Concurrency must be a positive integer');
  }
  const clamped = Math.min(Math.floor(value), MAX_CONCURRENT_HARD_CAP);
  setConfig(CONFIG_KEY_CONCURRENCY, String(clamped));
  return clamped;
}

// ---------------------------------------------------------------------------
// URL normalization + JSON walker
// ---------------------------------------------------------------------------

function normalizeToObjectPath(value: string): string | null {
  if (typeof value !== 'string' || value.length === 0) return null;
  if (value.startsWith('/objects/')) return value;
  try {
    const url = new URL(value);
    const objectsIdx = url.pathname.indexOf('/objects/');
    if (objectsIdx !== -1) return url.pathname.slice(objectsIdx);
  } catch { /* not a URL */ }
  return null;
}

/**
 * Walk an arbitrary JSON-like value and collect every string that resolves
 * to an `/objects/...` reference.
 */
function walkCollectObjectPaths(node: any, out: Set<string>): void {
  if (node == null) return;
  if (typeof node === 'string') {
    const norm = normalizeToObjectPath(node);
    if (norm) out.add(norm);
    return;
  }
  if (Array.isArray(node)) {
    for (const item of node) walkCollectObjectPaths(item, out);
    return;
  }
  if (typeof node === 'object') {
    for (const key of Object.keys(node)) walkCollectObjectPaths(node[key], out);
  }
}

/**
 * Walk and rewrite any string in the tree matching `oldRef` to `newRef`.
 * - When `oldRef` is a `/objects/...` path, we match by normalized object path
 *   so absolute https://... URLs that embed the same path are also rewritten.
 * - Otherwise we match by exact string equality (used for /media/<file>
 *   re-rewrite during backfill).
 *
 * Mutates the tree in place. Returns whether anything changed.
 */
function walkRewriteString(s: string, oldRef: string, newRef: string): string | null {
  if (s === oldRef) return newRef;
  if (oldRef.startsWith('/objects/')) {
    const norm = normalizeToObjectPath(s);
    if (norm === oldRef) return newRef;
  }
  return null;
}

function walkRewrite(node: any, oldRef: string, newRef: string): { value: any; changed: boolean } {
  if (typeof node === 'string') {
    const r = walkRewriteString(node, oldRef, newRef);
    if (r !== null) return { value: r, changed: true };
    return { value: node, changed: false };
  }
  if (Array.isArray(node)) {
    let changed = false;
    for (let i = 0; i < node.length; i++) {
      const r = walkRewrite(node[i], oldRef, newRef);
      if (r.changed) { node[i] = r.value; changed = true; }
    }
    return { value: node, changed };
  }
  if (node && typeof node === 'object') {
    let changed = false;
    for (const k of Object.keys(node)) {
      const r = walkRewrite(node[k], oldRef, newRef);
      if (r.changed) { node[k] = r.value; changed = true; }
    }
    return { value: node, changed };
  }
  return { value: node, changed: false };
}

/** Exposed for unit testing the schema-aware traversal. */
export const __testing__ = { normalizeToObjectPath, walkCollectObjectPaths, walkRewrite };

// ---------------------------------------------------------------------------
// Queueing
// ---------------------------------------------------------------------------

export function queueMediaDownload(objectPath: string, contentId: number, field: string = 'data'): boolean {
  if (!objectPath || !objectPath.startsWith('/objects/')) return false;

  const db = getDb();
  const existing = db.prepare('SELECT id, status, local_filename FROM media_downloads WHERE object_path = ?').get(objectPath) as any;
  if (existing) {
    if (existing.status === 'completed' && existing.local_filename) {
      rewriteAllReferences(objectPath, `/media/${existing.local_filename}`);
      return false;
    }
    if (existing.status === 'pending' || existing.status === 'downloading') return false;
    if (existing.status === 'failed') {
      db.prepare('UPDATE media_downloads SET status = ?, retry_count = 0, error = NULL, content_id = ?, field = ? WHERE id = ?')
        .run('pending', contentId, field, existing.id);
      return true;
    }
    return false;
  }

  db.prepare('INSERT INTO media_downloads (object_path, content_id, field, status, bytes_downloaded) VALUES (?, ?, ?, ?, 0)')
    .run(objectPath, contentId, field, 'pending');
  return true;
}

function tryQueueField(value: string | undefined | null, contentId: number, field: string): boolean {
  if (!value || typeof value !== 'string') return false;
  const objectPath = normalizeToObjectPath(value);
  if (!objectPath) return false;
  return queueMediaDownload(objectPath, contentId, field);
}

function queueAllPathsFromJson(jsonText: string | null | undefined, recordId: number, field: string): number {
  if (!jsonText || typeof jsonText !== 'string') return 0;
  let parsed: any;
  try { parsed = JSON.parse(jsonText); } catch { return 0; }
  const paths = new Set<string>();
  walkCollectObjectPaths(parsed, paths);
  let queued = 0;
  for (const p of paths) {
    if (queueMediaDownload(p, recordId, field)) queued++;
  }
  return queued;
}

export function queueContentMediaDownloads(contentId: number): number {
  const db = getDb();
  const content = db.prepare('SELECT * FROM contents WHERE id = ?').get(contentId) as any;
  if (!content) return 0;

  let queued = 0;
  if (tryQueueField(content.data, contentId, 'data')) queued++;
  if (tryQueueField(content.url, contentId, 'url')) queued++;
  if (tryQueueField(content.thumbnail_url, contentId, 'thumbnail_url')) queued++;
  queued += queueAllPathsFromJson(content.canvas_data, contentId, 'canvas_data');
  queued += queueAllPathsFromJson(content.settings, contentId, 'settings');
  // `data` may itself be JSON (rich content). Try as JSON too — duplicates are ignored.
  queued += queueAllPathsFromJson(content.data, contentId, 'data');
  return queued;
}

export function queueDesignTemplateMediaDownloads(templateId: number): number {
  const db = getDb();
  const template = db.prepare('SELECT * FROM design_templates WHERE id = ?').get(templateId) as any;
  if (!template) return 0;

  let queued = 0;
  if (tryQueueField(template.thumbnail_url, templateId, 'dt_thumbnail_url')) queued++;
  queued += queueAllPathsFromJson(template.canvas_data, templateId, 'dt_canvas_data');
  return queued;
}

export function queueKioskMediaDownloads(kioskId: number): number {
  const db = getDb();
  const kiosk = db.prepare('SELECT * FROM kiosks WHERE id = ?').get(kioskId) as any;
  if (!kiosk) return 0;

  let queued = 0;
  queued += queueAllPathsFromJson(kiosk.data, kioskId, 'kiosk_data');
  queued += queueAllPathsFromJson(kiosk.settings, kioskId, 'kiosk_settings');
  return queued;
}

export function scanAndQueueAllCloudContent(): number {
  const db = getDb();
  let totalQueued = 0;

  for (const { table, column, kind } of MEDIA_COLUMNS) {
    let rows: any[] = [];
    try {
      rows = db.prepare(`SELECT id, ${column} AS v FROM ${table} WHERE ${column} LIKE '%/objects/%'`).all() as any[];
    } catch {
      continue; // table or column may not exist on older DBs
    }
    for (const row of rows) {
      if (!row.v) continue;
      if (kind === 'scalar') {
        if (tryQueueField(row.v, row.id, `${table}.${column}`)) totalQueued++;
      } else if (kind === 'json') {
        totalQueued += queueAllPathsFromJson(row.v, row.id, `${table}.${column}`);
      } else {
        // auto: try JSON, then fall back to scalar
        const before = totalQueued;
        totalQueued += queueAllPathsFromJson(row.v, row.id, `${table}.${column}`);
        if (totalQueued === before) {
          if (tryQueueField(row.v, row.id, `${table}.${column}`)) totalQueued++;
        }
      }
    }
  }

  if (totalQueued > 0) {
    console.log(`[media-dl] Scanned ${MEDIA_COLUMNS.length} media columns — queued ${totalQueued} downloads`);
  }
  return totalQueued;
}

// ---------------------------------------------------------------------------
// Backfill
// ---------------------------------------------------------------------------

/**
 * Re-queues any download whose `local_filename` no longer exists on disk.
 * Called on startup so a manually-deleted media file or a wiped uploads
 * directory doesn't permanently break references on screens.
 *
 * IMPORTANT: we deliberately keep `local_filename` so the redownload writes
 * to the SAME `/media/<filename>` the JSON content rows already reference.
 * If we cleared the filename and let downloadMediaFile pick a new one, the
 * subsequent rewriteAllReferences(object_path → /media/<newFile>) would
 * find nothing to rewrite — JSON columns no longer contain the original
 * `object_path`, they contain `/media/<oldFile>` — and player references
 * would stay broken forever.
 */
export function backfillMissingLocalFiles(): number {
  const db = getDb();
  const completed = db.prepare(
    "SELECT id, object_path, local_filename, content_id, field FROM media_downloads WHERE status = 'completed' AND local_filename IS NOT NULL"
  ).all() as any[];

  const uploadsDir = path.join(getMediaDir(), 'uploads');
  let reQueued = 0;
  for (const row of completed) {
    const filePath = path.join(uploadsDir, row.local_filename);
    if (fs.existsSync(filePath)) continue;
    // Preserve local_filename — the re-downloaded bytes will land at the
    // same /media/<filename> so existing JSON refs heal automatically.
    // Also clean up any stale partial so we restart from byte 0.
    const partialPath = filePath + PARTIAL_EXT;
    try { if (fs.existsSync(partialPath)) fs.unlinkSync(partialPath); } catch {}
    db.prepare(
      "UPDATE media_downloads SET status = 'pending', retry_count = 0, error = 'local file missing', bytes_downloaded = 0 WHERE id = ?"
    ).run(row.id);
    reQueued++;
    insertErrorLog({
      level: 'warn',
      source: 'media-downloader',
      message: `Local media file missing — re-queueing download: ${row.object_path}`,
      context: { localFilename: row.local_filename, contentId: row.content_id, field: row.field },
    });
  }
  return reQueued;
}

// ---------------------------------------------------------------------------
// Download loop
// ---------------------------------------------------------------------------

async function processQueue() {
  if (processing || !isRunning) return;
  processing = true;

  try {
    const db = getDb();
    const maxConcurrent = getMediaDownloaderConcurrency();
    const activeCount = (db.prepare("SELECT COUNT(*) as c FROM media_downloads WHERE status = 'downloading'").get() as any)?.c || 0;
    const slotsAvailable = maxConcurrent - activeCount;
    if (slotsAvailable <= 0) return;

    const pending = db.prepare(
      "SELECT * FROM media_downloads WHERE status = 'pending' AND retry_count < ? ORDER BY created_at ASC LIMIT ?"
    ).all(MAX_RETRIES, slotsAvailable) as any[];

    if (pending.length === 0) return;

    const downloads = pending.map(item => downloadMediaFile(item));
    await Promise.allSettled(downloads);
  } catch (err: any) {
    console.error('[media-dl] Queue processing error:', err.message);
  } finally {
    processing = false;
  }
}

async function downloadMediaFile(item: any) {
  const db = getDb();
  db.prepare("UPDATE media_downloads SET status = 'downloading' WHERE id = ?").run(item.id);

  const syncState = getSyncState();
  if (!syncState?.cloud_url || !syncState?.hub_token) {
    db.prepare("UPDATE media_downloads SET status = 'failed', error = 'No cloud connection configured' WHERE id = ?").run(item.id);
    return;
  }

  const cloudUrl = syncState.cloud_url.replace(/^ws/, 'http');
  const objectPathSuffix = item.object_path.replace(/^\/objects\//, '');
  const downloadUrl = `${cloudUrl}/api/hub/media/${objectPathSuffix}`;

  const uploadsDir = path.join(getMediaDir(), 'uploads');
  if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });

  // Resume support: keep previously-chosen filename + partial file across
  // restarts. If we don't have one yet, defer the choice until we know the
  // content-type from the response headers (the file extension matters for
  // local mime detection on the player side).
  let localFilename: string | null = item.local_filename || null;
  let bytesDownloaded: number = Number(item.bytes_downloaded) || 0;

  // If a partial file is missing despite a recorded byte offset, we restart
  // from zero — the bytes are gone.
  if (localFilename) {
    const partialPath = path.join(uploadsDir, localFilename + PARTIAL_EXT);
    if (!fs.existsSync(partialPath)) {
      bytesDownloaded = 0;
      db.prepare('UPDATE media_downloads SET bytes_downloaded = 0 WHERE id = ?').run(item.id);
    } else {
      // Trust on-disk size over the recorded offset (truncate/expand as needed).
      const onDisk = fs.statSync(partialPath).size;
      if (onDisk !== bytesDownloaded) {
        bytesDownloaded = onDisk;
        db.prepare('UPDATE media_downloads SET bytes_downloaded = ? WHERE id = ?').run(bytesDownloaded, item.id);
      }
    }
  }

  try {
    const headers: Record<string, string> = { 'x-hub-token': syncState.hub_token };
    if (bytesDownloaded > 0) {
      headers['Range'] = `bytes=${bytesDownloaded}-`;
      console.log(`[media-dl] Resuming: ${item.object_path} from byte ${bytesDownloaded}`);
    } else {
      console.log(`[media-dl] Downloading: ${item.object_path}`);
    }

    const res = await fetch(downloadUrl, {
      headers,
      signal: AbortSignal.timeout(30 * 60 * 1000),
    });

    // 200 = full body returned (server ignored / didn't honor Range).
    // 206 = partial content (resume succeeded).
    // 416 = range not satisfiable (the file is shorter than our offset
    //   — the upstream object was replaced; restart from zero).
    if (res.status === 416) {
      // Reset and re-attempt next tick.
      bytesDownloaded = 0;
      db.prepare("UPDATE media_downloads SET status = 'pending', bytes_downloaded = 0, error = 'range not satisfiable, restarting' WHERE id = ?").run(item.id);
      if (localFilename) {
        const partialPath = path.join(uploadsDir, localFilename + PARTIAL_EXT);
        try { if (fs.existsSync(partialPath)) fs.unlinkSync(partialPath); } catch {}
      }
      return;
    }
    if (!res.ok || !res.body) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }

    // Did the server honor our Range request?
    const isPartial = res.status === 206;
    if (bytesDownloaded > 0 && !isPartial) {
      // Server gave us the full body even though we asked for a range —
      // can't safely append. Truncate the partial and start over.
      bytesDownloaded = 0;
      if (localFilename) {
        const partialPath = path.join(uploadsDir, localFilename + PARTIAL_EXT);
        try { if (fs.existsSync(partialPath)) fs.unlinkSync(partialPath); } catch {}
      }
    }

    if (!localFilename) {
      const contentType = res.headers.get('content-type') || 'application/octet-stream';
      const ext = getExtensionFromMimeOrPath(contentType, item.object_path);
      localFilename = `cloud-${crypto.randomBytes(8).toString('hex')}${ext}`;
      db.prepare('UPDATE media_downloads SET local_filename = ? WHERE id = ?').run(localFilename, item.id);
    }
    const partialPath = path.join(uploadsDir, localFilename + PARTIAL_EXT);
    const finalPath = path.join(uploadsDir, localFilename);

    // Stream the response to the partial file.
    const writeStream = fs.createWriteStream(partialPath, { flags: bytesDownloaded > 0 ? 'a' : 'w' });
    let writtenSinceCheckpoint = 0;
    const CHECKPOINT_BYTES = 256 * 1024; // persist offset every 256KB

    try {
      // Node's WHATWG stream → async iterator
      // @ts-ignore — Node's fetch returns a web ReadableStream
      for await (const chunk of res.body as any) {
        const buf: Buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        await new Promise<void>((resolve, reject) => {
          writeStream.write(buf, (err) => err ? reject(err) : resolve());
        });
        bytesDownloaded += buf.length;
        writtenSinceCheckpoint += buf.length;
        if (writtenSinceCheckpoint >= CHECKPOINT_BYTES) {
          db.prepare('UPDATE media_downloads SET bytes_downloaded = ? WHERE id = ?').run(bytesDownloaded, item.id);
          writtenSinceCheckpoint = 0;
        }
      }
    } finally {
      await new Promise<void>((resolve) => writeStream.end(() => resolve()));
    }

    fs.renameSync(partialPath, finalPath);

    const fileSize = bytesDownloaded;
    console.log(`[media-dl] Downloaded: ${item.object_path} -> ${localFilename} (${(fileSize / 1024).toFixed(1)}KB)`);

    db.prepare(
      "UPDATE media_downloads SET status = 'completed', local_filename = ?, file_size = ?, bytes_downloaded = ?, completed_at = datetime('now') WHERE id = ?"
    ).run(localFilename, fileSize, fileSize, item.id);

    rewriteAllReferences(item.object_path, `/media/${localFilename}`);

  } catch (err: any) {
    const retryCount = (item.retry_count || 0) + 1;
    const status = retryCount >= MAX_RETRIES ? 'failed' : 'pending';
    db.prepare(
      "UPDATE media_downloads SET status = ?, retry_count = ?, error = ? WHERE id = ?"
    ).run(status, retryCount, err.message, item.id);

    if (status === 'failed') {
      console.error(`[media-dl] Failed permanently: ${item.object_path} — ${err.message}`);
      insertErrorLog({
        level: 'warn',
        source: 'media-downloader',
        message: `Media download failed after ${MAX_RETRIES} retries: ${item.object_path}`,
        context: { error: err.message, contentId: item.content_id },
      });
    } else {
      console.log(`[media-dl] Retry ${retryCount}/${MAX_RETRIES} for: ${item.object_path}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Reference rewriting
// ---------------------------------------------------------------------------

/**
 * Rewrite every occurrence of `oldRef` to `newRef` across the configured
 * media-bearing columns. Logs loudly via the error reporter when we find a
 * row that LIKE-matches the old reference but the schema-aware walker can't
 * actually rewrite it — that's the case the task description calls out
 * ("if a reference can't be rewritten, log it loudly rather than silently
 * leaving a broken cloud URL").
 */
function rewriteAllReferences(oldRef: string, newRef: string) {
  const db = getDb();
  const unresolved: Array<{ table: string; column: string; rowId: number }> = [];

  withoutTriggers(() => {
    for (const { table, column, kind } of MEDIA_COLUMNS) {
      let rows: any[] = [];
      try {
        rows = db.prepare(`SELECT id, ${column} AS v FROM ${table} WHERE ${column} LIKE ?`)
          .all(`%${oldRef}%`) as any[];
      } catch {
        continue;
      }
      for (const row of rows) {
        if (!row.v) continue;
        const updated = rewriteOneFieldValue(row.v, kind, oldRef, newRef);
        if (updated !== null) {
          db.prepare(`UPDATE ${table} SET ${column} = ? WHERE id = ?`).run(updated, row.id);
        } else {
          // The string is in the column (LIKE matched) but our walker
          // couldn't replace it — schema may have evolved in a way we
          // don't recognize, or the value is malformed JSON.
          unresolved.push({ table, column, rowId: row.id });
        }
      }
    }
  });

  if (unresolved.length > 0) {
    const sample = unresolved.slice(0, 5);
    console.error(`[media-dl] ${unresolved.length} unresolved reference(s) for ${oldRef} — broken URL may ship to players`);
    insertErrorLog({
      level: 'error',
      source: 'media-downloader',
      message: `Could not rewrite media reference ${oldRef} -> ${newRef} in ${unresolved.length} row(s); schema may have evolved`,
      context: { oldRef, newRef, sample },
    });
  }
}

function rewriteOneFieldValue(value: string, kind: ColumnKind, oldRef: string, newRef: string): string | null {
  // For scalar columns, do exact / normalized comparison.
  if (kind === 'scalar') {
    const r = walkRewriteString(value, oldRef, newRef);
    return r;
  }

  // JSON / auto: try parsing first.
  let parsed: any;
  let parsedOk = false;
  try {
    parsed = JSON.parse(value);
    parsedOk = parsed !== null && typeof parsed === 'object';
  } catch {
    parsedOk = false;
  }

  if (parsedOk) {
    const r = walkRewrite(parsed, oldRef, newRef);
    return r.changed ? JSON.stringify(parsed) : null;
  }

  // auto fallback: treat as scalar if JSON parse failed
  if (kind === 'auto') {
    return walkRewriteString(value, oldRef, newRef);
  }
  return null;
}

// ---------------------------------------------------------------------------
// Misc helpers + read-side accessors
// ---------------------------------------------------------------------------

function getExtensionFromMimeOrPath(mimeType: string, objectPath: string): string {
  const pathExt = path.extname(objectPath).toLowerCase();
  if (pathExt && pathExt.length > 1 && pathExt.length < 6) return pathExt;

  const mimeMap: Record<string, string> = {
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/svg+xml': '.svg',
    'video/mp4': '.mp4',
    'video/webm': '.webm',
    'video/quicktime': '.mov',
    'application/pdf': '.pdf',
    'audio/mpeg': '.mp3',
    'audio/wav': '.wav',
  };
  return mimeMap[mimeType] || '';
}

export function getMediaDownloadStats(): { pending: number; downloading: number; completed: number; failed: number } {
  const db = getDb();
  const stats = db.prepare(`
    SELECT status, COUNT(*) as count FROM media_downloads GROUP BY status
  `).all() as any[];
  const result = { pending: 0, downloading: 0, completed: 0, failed: 0 };
  for (const row of stats) {
    if (row.status in result) {
      (result as any)[row.status] = row.count;
    }
  }
  return result;
}

export function getLocalFilenameForObjectPath(objectPath: string): string | null {
  const db = getDb();
  const row = db.prepare("SELECT local_filename FROM media_downloads WHERE object_path = ? AND status = 'completed'").get(objectPath) as any;
  return row?.local_filename || null;
}

export function getFailedDownloadsByContent(): Array<{ contentId: number; objectPath: string; error: string | null }> {
  const db = getDb();
  return db.prepare(
    "SELECT content_id as contentId, object_path as objectPath, error FROM media_downloads WHERE status = 'failed' ORDER BY created_at DESC"
  ).all() as any[];
}

export function getContentDownloadStatus(contentId: number): { total: number; completed: number; failed: number; pending: number } {
  const db = getDb();
  const rows = db.prepare(
    "SELECT status, COUNT(*) as count FROM media_downloads WHERE content_id = ? GROUP BY status"
  ).all(contentId) as any[];
  const result = { total: 0, completed: 0, failed: 0, pending: 0 };
  for (const row of rows) {
    result.total += row.count;
    if (row.status === 'completed') result.completed = row.count;
    else if (row.status === 'failed') result.failed = row.count;
    else result.pending += row.count;
  }
  return result;
}
