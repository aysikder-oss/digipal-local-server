import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import { getDb, getSyncState, insertErrorLog, withoutTriggers } from '../db/sqlite';
import { getMediaDir } from './file-storage';

const MAX_CONCURRENT = 3;
const MAX_RETRIES = 3;
const PROCESS_INTERVAL_MS = 5_000;

let processing = false;
let processTimer: NodeJS.Timeout | null = null;
let isRunning = false;

export function startMediaDownloader() {
  if (isRunning) return;
  isRunning = true;
  console.log('[media-dl] Media downloader started');

  const db = getDb();
  const staleCount = db.prepare("UPDATE media_downloads SET status = 'pending' WHERE status = 'downloading'").run().changes;
  if (staleCount > 0) {
    console.log(`[media-dl] Reset ${staleCount} stale downloading entries to pending`);
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

  db.prepare('INSERT INTO media_downloads (object_path, content_id, field, status) VALUES (?, ?, ?, ?)')
    .run(objectPath, contentId, field, 'pending');
  return true;
}

function normalizeToObjectPath(value: string): string | null {
  if (value.startsWith('/objects/')) return value;
  try {
    const url = new URL(value);
    const objectsIdx = url.pathname.indexOf('/objects/');
    if (objectsIdx !== -1) return url.pathname.slice(objectsIdx);
  } catch {}
  return null;
}

function tryQueueField(value: string | undefined | null, contentId: number, field: string): boolean {
  if (!value || typeof value !== 'string') return false;
  const objectPath = normalizeToObjectPath(value);
  if (!objectPath) return false;
  return queueMediaDownload(objectPath, contentId, field);
}

export function queueContentMediaDownloads(contentId: number): number {
  const db = getDb();
  const content = db.prepare('SELECT * FROM contents WHERE id = ?').get(contentId) as any;
  if (!content) return 0;

  let queued = 0;

  if (tryQueueField(content.data, contentId, 'data')) queued++;
  if (tryQueueField(content.url, contentId, 'url')) queued++;
  if (tryQueueField(content.thumbnail_url, contentId, 'thumbnail_url')) queued++;

  if (content.canvas_data && typeof content.canvas_data === 'string') {
    try {
      const canvasData = JSON.parse(content.canvas_data);
      const mediaPaths = extractCanvasMediaPaths(canvasData);
      for (const mediaPath of mediaPaths) {
        if (queueMediaDownload(mediaPath, contentId, 'canvas_data')) queued++;
      }
    } catch {}
  }

  return queued;
}

export function queueDesignTemplateMediaDownloads(templateId: number): number {
  const db = getDb();
  const template = db.prepare('SELECT * FROM design_templates WHERE id = ?').get(templateId) as any;
  if (!template) return 0;

  let queued = 0;

  if (tryQueueField(template.thumbnail_url, templateId, 'dt_thumbnail_url')) queued++;

  if (template.canvas_data && typeof template.canvas_data === 'string') {
    try {
      const canvasData = JSON.parse(template.canvas_data);
      const imagePaths = extractCanvasMediaPaths(canvasData);
      for (const imgPath of imagePaths) {
        if (queueMediaDownload(imgPath, templateId, 'dt_canvas_data')) queued++;
      }
    } catch {}
  }

  return queued;
}

function extractCanvasMediaPaths(canvasData: any): string[] {
  const paths: string[] = [];
  if (!canvasData || !canvasData.pages) return paths;

  function tryNormalize(value: any): void {
    if (typeof value !== 'string') return;
    const normalized = normalizeToObjectPath(value);
    if (normalized) paths.push(normalized);
  }

  for (const page of canvasData.pages) {
    tryNormalize(page.backgroundImage);
    if (!page.elements) continue;
    for (const element of page.elements) {
      tryNormalize(element.properties?.imageUrl);
      tryNormalize(element.properties?.videoUrl);
      tryNormalize(element.properties?.src);
    }
  }
  return paths;
}

export function scanAndQueueAllCloudContent(): number {
  const db = getDb();
  let totalQueued = 0;

  const contents = db.prepare(`SELECT id FROM contents WHERE data LIKE '/objects/%' OR data LIKE '%/objects/%' OR url LIKE '/objects/%' OR url LIKE '%/objects/%' OR thumbnail_url LIKE '/objects/%' OR thumbnail_url LIKE '%/objects/%' OR canvas_data LIKE '%/objects/%'`).all() as any[];
  for (const content of contents) {
    totalQueued += queueContentMediaDownloads(content.id);
  }

  try {
    const templates = db.prepare(`SELECT id FROM design_templates WHERE thumbnail_url LIKE '/objects/%' OR thumbnail_url LIKE '%/objects/%' OR canvas_data LIKE '%/objects/%'`).all() as any[];
    for (const template of templates) {
      totalQueued += queueDesignTemplateMediaDownloads(template.id);
    }
  } catch {}

  if (totalQueued > 0) {
    console.log(`[media-dl] Scanned all content + templates — queued ${totalQueued} media downloads`);
  }
  return totalQueued;
}

async function processQueue() {
  if (processing || !isRunning) return;
  processing = true;

  try {
    const db = getDb();
    const activeCount = (db.prepare("SELECT COUNT(*) as c FROM media_downloads WHERE status = 'downloading'").get() as any)?.c || 0;
    const slotsAvailable = MAX_CONCURRENT - activeCount;
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

  try {
    console.log(`[media-dl] Downloading: ${item.object_path}`);

    const res = await fetch(downloadUrl, {
      headers: {
        'x-hub-token': syncState.hub_token,
      },
      signal: AbortSignal.timeout(5 * 60 * 1000),
    });

    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }

    const contentType = res.headers.get('content-type') || 'application/octet-stream';
    const ext = getExtensionFromMimeOrPath(contentType, item.object_path);
    const uniqueName = `cloud-${crypto.randomBytes(8).toString('hex')}${ext}`;
    const uploadsDir = path.join(getMediaDir(), 'uploads');
    const filePath = path.join(uploadsDir, uniqueName);

    const arrayBuffer = await res.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    fs.writeFileSync(filePath, buffer);

    const fileSize = buffer.length;
    console.log(`[media-dl] Downloaded: ${item.object_path} -> ${uniqueName} (${(fileSize / 1024).toFixed(1)}KB)`);

    db.prepare(
      "UPDATE media_downloads SET status = 'completed', local_filename = ?, file_size = ?, completed_at = datetime('now') WHERE id = ?"
    ).run(uniqueName, fileSize, item.id);

    rewriteAllReferences(item.object_path, `/media/${uniqueName}`);

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

function rewriteScalarField(value: string | null | undefined, objectPath: string, localUrl: string): string | null {
  if (!value || typeof value !== 'string') return null;
  if (value === objectPath) return localUrl;
  const normalized = normalizeToObjectPath(value);
  if (normalized === objectPath) return localUrl;
  return null;
}

function rewriteCanvasDataStructurally(canvasJson: string, objectPath: string, localUrl: string): string | null {
  try {
    const data = JSON.parse(canvasJson);
    if (!data?.pages) return null;
    let changed = false;

    for (const page of data.pages) {
      if (page.backgroundImage) {
        const norm = normalizeToObjectPath(page.backgroundImage);
        if (norm === objectPath) { page.backgroundImage = localUrl; changed = true; }
      }
      if (!page.elements) continue;
      for (const el of page.elements) {
        if (!el.properties) continue;
        for (const key of ['imageUrl', 'videoUrl', 'src']) {
          if (el.properties[key]) {
            const norm = normalizeToObjectPath(el.properties[key]);
            if (norm === objectPath) { el.properties[key] = localUrl; changed = true; }
          }
        }
      }
    }
    return changed ? JSON.stringify(data) : null;
  } catch {
    return null;
  }
}

function rewriteAllReferences(objectPath: string, localUrl: string) {
  const db = getDb();

  withoutTriggers(() => {
    db.prepare('UPDATE contents SET data = ? WHERE data = ?').run(localUrl, objectPath);
    db.prepare('UPDATE contents SET url = ? WHERE url = ?').run(localUrl, objectPath);
    db.prepare('UPDATE contents SET thumbnail_url = ? WHERE thumbnail_url = ?').run(localUrl, objectPath);

    const scalarContents = db.prepare("SELECT id, data, url, thumbnail_url FROM contents WHERE data LIKE ? OR url LIKE ? OR thumbnail_url LIKE ?")
      .all(`%${objectPath}%`, `%${objectPath}%`, `%${objectPath}%`) as any[];
    for (const row of scalarContents) {
      const newData = rewriteScalarField(row.data, objectPath, localUrl);
      if (newData) db.prepare('UPDATE contents SET data = ? WHERE id = ?').run(newData, row.id);
      const newUrl = rewriteScalarField(row.url, objectPath, localUrl);
      if (newUrl) db.prepare('UPDATE contents SET url = ? WHERE id = ?').run(newUrl, row.id);
      const newThumb = rewriteScalarField(row.thumbnail_url, objectPath, localUrl);
      if (newThumb) db.prepare('UPDATE contents SET thumbnail_url = ? WHERE id = ?').run(newThumb, row.id);
    }

    const contentRows = db.prepare("SELECT id, canvas_data FROM contents WHERE canvas_data LIKE ?").all(`%${objectPath}%`) as any[];
    for (const row of contentRows) {
      const updated = rewriteCanvasDataStructurally(row.canvas_data, objectPath, localUrl);
      if (updated) db.prepare('UPDATE contents SET canvas_data = ? WHERE id = ?').run(updated, row.id);
    }

    db.prepare('UPDATE design_templates SET thumbnail_url = ? WHERE thumbnail_url = ?').run(localUrl, objectPath);
    const dtScalar = db.prepare("SELECT id, thumbnail_url FROM design_templates WHERE thumbnail_url LIKE ?")
      .all(`%${objectPath}%`) as any[];
    for (const row of dtScalar) {
      const newThumb = rewriteScalarField(row.thumbnail_url, objectPath, localUrl);
      if (newThumb) db.prepare('UPDATE design_templates SET thumbnail_url = ? WHERE id = ?').run(newThumb, row.id);
    }

    const dtRows = db.prepare("SELECT id, canvas_data FROM design_templates WHERE canvas_data LIKE ?").all(`%${objectPath}%`) as any[];
    for (const row of dtRows) {
      const updated = rewriteCanvasDataStructurally(row.canvas_data, objectPath, localUrl);
      if (updated) db.prepare('UPDATE design_templates SET canvas_data = ? WHERE id = ?').run(updated, row.id);
    }
  });
}

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
