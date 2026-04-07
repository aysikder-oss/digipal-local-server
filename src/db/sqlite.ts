import Database from 'better-sqlite3';
import path from 'path';
import { app } from 'electron';

let db: Database.Database;

export function getDb(): Database.Database {
  return db;
}

export function initDatabase() {
  const dbPath = path.join(app.getPath('userData'), 'digipal-local.db');
  db = new Database(dbPath);

  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS screens (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pairing_code TEXT UNIQUE NOT NULL,
      name TEXT,
      platform TEXT,
      model TEXT,
      os_version TEXT,
      app_version TEXT,
      resolution TEXT,
      is_online INTEGER DEFAULT 0,
      last_seen_at TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS playlists (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      subscriber_id INTEGER,
      data TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS contents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      subscriber_id INTEGER,
      name TEXT NOT NULL,
      type TEXT NOT NULL,
      url TEXT,
      local_path TEXT,
      thumbnail_url TEXT,
      file_size INTEGER DEFAULT 0,
      duration INTEGER,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS schedules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      subscriber_id INTEGER,
      screen_id INTEGER,
      playlist_id INTEGER,
      start_time INTEGER,
      end_time INTEGER,
      days_of_week TEXT,
      enabled INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS sync_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_sync_at TEXT,
      hub_token TEXT,
      cloud_url TEXT,
      subscriber_id INTEGER
    );

    INSERT OR IGNORE INTO sync_state (id) VALUES (1);

    CREATE TABLE IF NOT EXISTS local_changes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      table_name TEXT NOT NULL,
      record_id INTEGER NOT NULL,
      operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
      payload TEXT,
      pushed INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_local_changes_unpushed ON local_changes(pushed) WHERE pushed = 0;
  `);

  setupChangeTriggers(db);

  return db;
}

function setupChangeTriggers(database: Database.Database) {
  const tables = ['screens', 'playlists', 'contents', 'schedules'];
  for (const table of tables) {
    database.exec(`
      CREATE TRIGGER IF NOT EXISTS trg_${table}_insert AFTER INSERT ON ${table}
      BEGIN
        INSERT INTO local_changes (table_name, record_id, operation, payload)
        VALUES ('${table}', NEW.id, 'INSERT', json_object('id', NEW.id));
      END;

      CREATE TRIGGER IF NOT EXISTS trg_${table}_update AFTER UPDATE ON ${table}
      BEGIN
        INSERT INTO local_changes (table_name, record_id, operation, payload)
        VALUES ('${table}', NEW.id, 'UPDATE', json_object('id', NEW.id));
      END;

      CREATE TRIGGER IF NOT EXISTS trg_${table}_delete AFTER DELETE ON ${table}
      BEGIN
        INSERT INTO local_changes (table_name, record_id, operation, payload)
        VALUES ('${table}', OLD.id, 'DELETE', json_object('id', OLD.id));
      END;
    `);
  }
}

export function getConfig(key: string): string | undefined {
  const row = db.prepare('SELECT value FROM config WHERE key = ?').get(key) as { value: string } | undefined;
  return row?.value;
}

export function setConfig(key: string, value: string): void {
  db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run(key, value);
}

export function getSyncState() {
  return db.prepare('SELECT * FROM sync_state WHERE id = 1').get() as any;
}

export function updateSyncState(updates: Record<string, any>) {
  const keys = Object.keys(updates);
  const sets = keys.map(k => `${k} = ?`).join(', ');
  db.prepare(`UPDATE sync_state SET ${sets} WHERE id = 1`).run(...keys.map(k => updates[k]));
}

export function getUnpushedChanges(): any[] {
  return db.prepare('SELECT * FROM local_changes WHERE pushed = 0 ORDER BY id ASC LIMIT 100').all();
}

export function markChangesPushed(ids: number[]): void {
  if (ids.length === 0) return;
  const placeholders = ids.map(() => '?').join(',');
  db.prepare(`UPDATE local_changes SET pushed = 1 WHERE id IN (${placeholders})`).run(...ids);
}

export function getFullRow(tableName: string, recordId: number): any {
  const allowed = ['screens', 'playlists', 'contents', 'schedules'];
  if (!allowed.includes(tableName)) return null;
  return db.prepare(`SELECT * FROM ${tableName} WHERE id = ?`).get(recordId);
}

export function upsertRow(tableName: string, data: Record<string, any>): void {
  const allowed = ['screens', 'playlists', 'contents', 'schedules'];
  if (!allowed.includes(tableName)) return;
  const keys = Object.keys(data);
  const cols = keys.join(', ');
  const placeholders = keys.map(() => '?').join(', ');
  const updates = keys.filter(k => k !== 'id').map(k => `${k} = excluded.${k}`).join(', ');
  db.prepare(`INSERT INTO ${tableName} (${cols}) VALUES (${placeholders}) ON CONFLICT(id) DO UPDATE SET ${updates}`)
    .run(...keys.map(k => data[k]));
}

export function deleteRow(tableName: string, recordId: number): void {
  const allowed = ['screens', 'playlists', 'contents', 'schedules'];
  if (!allowed.includes(tableName)) return;
  db.prepare(`DELETE FROM ${tableName} WHERE id = ?`).run(recordId);
}

export function withoutTriggers(fn: () => void): void {
  const tables = ['screens', 'playlists', 'contents', 'schedules'];
  for (const table of tables) {
    db.exec(`DROP TRIGGER IF EXISTS trg_${table}_insert`);
    db.exec(`DROP TRIGGER IF EXISTS trg_${table}_update`);
    db.exec(`DROP TRIGGER IF EXISTS trg_${table}_delete`);
  }
  try {
    fn();
  } finally {
    setupChangeTriggers(db);
  }
}
