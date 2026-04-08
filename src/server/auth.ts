import crypto from 'crypto';
import { getDb, getSyncState } from '../db/sqlite';

export interface Subscriber {
  id: number;
  name: string;
  email: string;
  company: string | null;
  status: string;
  plan: string;
  accountRole: string;
  avatarUrl?: string | null;
}

export interface AuthResult {
  success: boolean;
  subscriber?: Subscriber;
  error?: string;
}

interface SessionRow {
  id: string;
  subscriber_id: number;
  expires_at: number;
  created_at: string;
}

interface SubscriberRow {
  id: number;
  name: string;
  email: string;
  password_hash: string;
  company: string | null;
  status: string;
  plan: string;
  account_role: string;
  avatar_url: string | null;
}

const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;

function hashPassword(password: string, salt?: string): { hash: string; salt: string } {
  const s = salt || crypto.randomBytes(16).toString('hex');
  const hash = crypto.pbkdf2Sync(password, s, 10000, 64, 'sha512').toString('hex');
  return { hash, salt: s };
}

function verifyPassword(password: string, storedHash: string): boolean {
  const [salt, hash] = storedHash.split(':');
  if (!salt || !hash) return false;
  const { hash: computed } = hashPassword(password, salt);
  return computed === hash;
}

function formatHash(salt: string, hash: string): string {
  return `${salt}:${hash}`;
}

export function initSessionTable(): void {
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      expires_at INTEGER NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_sessions_subscriber ON sessions(subscriber_id);
    CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
  `);
}

export function createSession(subscriberId: number): string {
  const db = getDb();
  const sessionId = crypto.randomBytes(32).toString('hex');
  const expiresAt = Date.now() + SESSION_TTL_MS;
  db.prepare('INSERT INTO sessions (id, subscriber_id, expires_at) VALUES (?, ?, ?)').run(sessionId, subscriberId, expiresAt);
  return sessionId;
}

export function getSession(sessionId: string): { subscriberId: number } | null {
  const db = getDb();
  const row = db.prepare('SELECT * FROM sessions WHERE id = ?').get(sessionId) as SessionRow | undefined;
  if (!row) return null;
  if (row.expires_at < Date.now()) {
    db.prepare('DELETE FROM sessions WHERE id = ?').run(sessionId);
    return null;
  }
  return { subscriberId: row.subscriber_id };
}

export function deleteSession(sessionId: string): void {
  const db = getDb();
  db.prepare('DELETE FROM sessions WHERE id = ?').run(sessionId);
}

export function cleanExpiredSessions(): void {
  const db = getDb();
  db.prepare('DELETE FROM sessions WHERE expires_at < ?').run(Date.now());
}

const DEFAULT_CLOUD_URL = process.env.CLOUD_URL || 'https://app.digipal.io';

interface CloudTeamMembership {
  team?: { id: number; name: string; description?: string; ownerId?: number };
  role?: { id: number; name: string; permissions?: unknown } | null;
  membership?: { id: number; teamId: number; subscriberId: number; roleId?: number | null };
}

function hydrateTeamMemberships(subscriberId: number, cloudUrl: string): void {
  (async () => {
    try {
      const db = getDb();
      const sessions = db.prepare('SELECT id FROM sessions WHERE subscriber_id = ? ORDER BY expires_at DESC LIMIT 1').get(subscriberId) as { id: string } | undefined;
      if (!sessions) return;

      const res = await fetch(`${cloudUrl}/api/customer/teams`, {
        headers: { 'Cookie': `connect.sid=${sessions.id}`, 'Authorization': `Bearer ${sessions.id}` },
      });
      if (!res.ok) return;

      const memberships = await res.json() as CloudTeamMembership[];
      if (!Array.isArray(memberships)) return;

      for (const m of memberships) {
        if (!m.team?.id) continue;
        db.prepare(`
          INSERT INTO teams (id, name, description, owner_id, created_at)
          VALUES (?, ?, ?, ?, datetime('now'))
          ON CONFLICT(id) DO UPDATE SET name = excluded.name, description = excluded.description, owner_id = excluded.owner_id
        `).run(m.team.id, m.team.name || '', m.team.description || '', m.team.ownerId || subscriberId);

        if (m.role?.id) {
          const permJson = m.role.permissions ? (typeof m.role.permissions === 'string' ? m.role.permissions : JSON.stringify(m.role.permissions)) : '[]';
          db.prepare(`
            INSERT INTO team_roles (id, team_id, name, permissions, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET name = excluded.name, permissions = excluded.permissions
          `).run(m.role.id, m.team.id, m.role.name || '', permJson);
        }

        const roleId = m.role?.id || m.membership?.roleId || null;
        db.prepare(`
          INSERT INTO team_members (team_id, subscriber_id, role_id, joined_at)
          VALUES (?, ?, ?, datetime('now'))
          ON CONFLICT(team_id, subscriber_id) DO UPDATE SET role_id = excluded.role_id
        `).run(m.team.id, subscriberId, roleId);
      }
    } catch (e) {
      console.log('[auth] Failed to hydrate team memberships:', (e as Error).message);
    }
  })();
}

export async function authenticateUser(email: string, password: string): Promise<AuthResult> {
  const db = getDb();
  const syncState = getSyncState();
  const cloudUrl = syncState?.cloud_url || DEFAULT_CLOUD_URL;

  const localSub = db.prepare('SELECT * FROM subscribers WHERE email = ?').get(email) as SubscriberRow | undefined;

  if (cloudUrl) {
    try {
      const res = await fetch(`${cloudUrl}/api/customer/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });

      if (res.ok) {
        const data = await res.json() as Record<string, unknown>;
        const raw = (data.subscriber || data.user || data) as Record<string, unknown>;
        if (raw && typeof raw.id === 'number' && !raw.requires2fa) {
          const { hash, salt } = hashPassword(password);
          const storedHash = formatHash(salt, hash);

          const name = String(raw.name || '');
          const company = raw.company ? String(raw.company) : null;
          const status = String(raw.status || 'active');
          const plan = String(raw.plan || 'free');
          const accountRole = String(raw.accountRole || raw.account_role || 'viewer');

          db.prepare(`
            INSERT INTO subscribers (id, name, email, password_hash, company, status, plan, account_role, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
              name = excluded.name,
              email = excluded.email,
              password_hash = excluded.password_hash,
              company = excluded.company,
              status = excluded.status,
              plan = excluded.plan,
              account_role = excluded.account_role
          `).run(raw.id, name, email, storedHash, company, status, plan, accountRole);

          hydrateTeamMemberships(raw.id as number, cloudUrl);

          const subscriber: Subscriber = {
            id: raw.id, name, email, company, status, plan, accountRole,
          };
          return { success: true, subscriber };
        }
      } else {
        const errData = await res.json().catch(() => ({ message: '' })) as { message?: string };
        if (res.status === 401 || res.status === 403) {
          return { success: false, error: errData.message || 'Invalid credentials' };
        }
      }
    } catch (e) {
      console.log('[auth] Cloud unavailable, falling back to local auth');
    }
  }

  if (localSub && localSub.password_hash) {
    if (verifyPassword(password, localSub.password_hash)) {
      return {
        success: true,
        subscriber: {
          id: localSub.id,
          name: localSub.name,
          email: localSub.email,
          company: localSub.company,
          status: localSub.status,
          plan: localSub.plan,
          accountRole: localSub.account_role,
        },
      };
    }
    return { success: false, error: 'Invalid credentials' };
  }

  return { success: false, error: 'Invalid credentials' };
}

export function getSessionSubscriber(sessionSubscriberId: number): Subscriber | null {
  const db = getDb();
  const sub = db.prepare('SELECT * FROM subscribers WHERE id = ?').get(sessionSubscriberId) as SubscriberRow | undefined;
  if (!sub) return null;
  return {
    id: sub.id,
    name: sub.name,
    email: sub.email,
    company: sub.company,
    status: sub.status,
    plan: sub.plan,
    accountRole: sub.account_role,
    avatarUrl: sub.avatar_url,
  };
}
