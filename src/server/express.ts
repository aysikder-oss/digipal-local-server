import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import http from 'http';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import { WebSocketServer, WebSocket } from 'ws';
import os from 'os';
import { getDb, getSyncState, updateSyncState, isHubRevoked, isScreenAllowedToPlay, getUnpushedChangeCount, getUnpushedChanges, getFullRow, markChangesPushed, upsertRow, withoutTriggers, SYNCED_TABLES, insertErrorLog, getRecentErrorLogs, getErrorLogCount, pruneOldErrorLogs } from '../db/sqlite';
import { startMdns, stopMdns, scanForExistingHubs, getMdnsStatus } from './mdns';
import { CloudSync } from './cloud-sync';
import { getConnectedPlayers, registerPlayer, unregisterPlayer, broadcastToPlayers } from './player-bus';
import { SqliteStorage, rowsToCamel, rowToCamel } from '../db/sqlite-storage';
import { authenticateUser, getSessionSubscriber, initSessionTable, createSession, getSession, deleteSession, cleanExpiredSessions } from './auth';
import { saveUploadedFile, getMediaDir, getMediaDiskUsage, deleteLocalFile } from './file-storage';
import { generateQrSvg, generateQrDataUrl, generateQrBuffer } from './qr-generator';

let server: http.Server | null = null;
let wss: WebSocketServer | null = null;
let cloudSync: CloudSync | null = null;
let hubBlocked = false;
let discoveredHubs: Array<{ name: string; host: string; port: number }> = [];
let boundPort: number = parseInt(String(process.env.LOCAL_PORT || 8787));
let sessionCleanupInterval: NodeJS.Timeout | null = null;
const storage = new SqliteStorage();

let initialSyncStatus: { inProgress: boolean; step: string; error: string | null; completedAt: string | null } = {
  inProgress: false, step: '', error: null, completedAt: null,
};

let hubRegistrationInProgress = false;

async function getCloudSession(cloudUrl: string, email: string, password: string): Promise<string | null> {
  try {
    console.log(`[initial-sync] Authenticating with cloud at ${cloudUrl}/api/customer/login`);
    const res = await fetch(`${cloudUrl}/api/customer/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });

    console.log(`[initial-sync] Cloud login status: ${res.status}`);
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      console.log(`[initial-sync] Cloud login failed: ${body.substring(0, 200)}`);
      return null;
    }

    const cookies: string[] = [];

    try {
      const getter = (res.headers as any).getSetCookie;
      if (typeof getter === 'function') {
        const setCookies = getter.call(res.headers) as string[];
        console.log(`[initial-sync] getSetCookie returned ${setCookies.length} cookies`);
        for (const c of setCookies) {
          const nameVal = c.split(';')[0];
          if (nameVal) cookies.push(nameVal);
        }
      }
    } catch (e: any) {
      console.log(`[initial-sync] getSetCookie failed: ${e.message}`);
    }

    if (cookies.length === 0) {
      const raw = res.headers.get('set-cookie');
      console.log(`[initial-sync] Fallback set-cookie header: ${raw ? raw.substring(0, 100) + '...' : 'null'}`);
      if (raw) {
        for (const part of raw.split(/,(?=\s*\w+=)/)) {
          const nameVal = part.trim().split(';')[0];
          if (nameVal) cookies.push(nameVal);
        }
      }
    }

    console.log(`[initial-sync] Extracted ${cookies.length} cookie(s): ${cookies.map(c => c.split('=')[0]).join(', ')}`);
    return cookies.length > 0 ? cookies.join('; ') : null;
  } catch (e: any) {
    console.log('[initial-sync] Failed to get cloud session:', e.message);
    return null;
  }
}

async function verifyHubToken(cloudUrl: string, hubToken: string): Promise<'valid' | 'invalid' | 'unreachable'> {
  try {
    const res = await fetch(`${cloudUrl}/api/hub/verify`, {
      headers: { 'x-hub-token': hubToken },
    });
    if (res.ok) return 'valid';
    if (res.status === 401 || res.status === 404) return 'invalid';
    return 'unreachable';
  } catch {
    return 'unreachable';
  }
}

async function registerHubWithCloud(cloudUrl: string, email: string, password: string, subscriberId: number): Promise<{ hubToken: string; hubId: number } | null> {
  if (hubRegistrationInProgress) {
    console.log('[initial-sync] Hub registration already in progress — skipping');
    return null;
  }

  if (isHubRevoked()) {
    console.log('[initial-sync] Hub was revoked — skipping registration');
    return null;
  }

  const syncState = getSyncState();
  if (syncState?.hub_token) {
    if (syncState.subscriber_id === subscriberId && syncState.cloud_url === cloudUrl) {
      const verifyResult = await verifyHubToken(cloudUrl, syncState.hub_token);
      if (verifyResult === 'valid') {
        console.log('[initial-sync] Hub token verified against cloud — reusing existing registration');
        return { hubToken: syncState.hub_token, hubId: 0 };
      }
      if (verifyResult === 'unreachable') {
        console.log('[initial-sync] Cloud unreachable during token verify — keeping existing token');
        return { hubToken: syncState.hub_token, hubId: 0 };
      }
      console.log('[initial-sync] Hub token is stale (cloud returned 401) — clearing and re-registering');
      if (cloudSync) {
        cloudSync.stop();
        cloudSync = null;
      }
    } else {
      console.log('[initial-sync] Different account detected — clearing old hub token and re-registering');
    }
    updateSyncState({ hub_token: null, hub_name: null, subscriber_id: null, cloud_url: null, hub_revoked: 0 });
    if (cloudSync) {
      cloudSync.stop();
      cloudSync = null;
    }
  }

  const hubName = `${os.hostname()} Local Server`;
  console.log(`[initial-sync] Registering hub "${hubName}" with cloud at ${cloudUrl}/api/hub/register`);

  hubRegistrationInProgress = true;
  try {
    const res = await fetch(`${cloudUrl}/api/hub/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password, name: hubName }),
    });

    if (!res.ok) {
      const body = await res.text().catch(() => '');
      console.error(`[initial-sync] Hub registration failed: HTTP ${res.status} — ${body.substring(0, 200)}`);
      return null;
    }

    const result = await res.json();
    if (!result.hubToken || typeof result.hubToken !== 'string') {
      console.error('[initial-sync] Hub registration returned invalid response — missing hubToken');
      return null;
    }

    console.log(`[initial-sync] Hub registered successfully — hubId: ${result.hubId}, token: hub_***${result.hubToken.slice(-6)}`);

    updateSyncState({
      hub_token: result.hubToken,
      cloud_url: cloudUrl,
      subscriber_id: subscriberId,
      hub_name: hubName,
      hub_revoked: 0,
    });

    return { hubToken: result.hubToken, hubId: result.hubId };
  } catch (e: any) {
    console.error('[initial-sync] Hub registration error:', e.message);
    return null;
  } finally {
    hubRegistrationInProgress = false;
  }
}

function startCloudSyncIfNeeded() {
  if (cloudSync) return;
  const syncState = getSyncState();
  if (syncState?.hub_token && syncState?.cloud_url && !isHubRevoked()) {
    console.log('[cloud-sync] Starting CloudSync WebSocket...');
    cloudSync = new CloudSync(syncState.cloud_url, syncState.hub_token, () => {
      console.log('[cloud-sync] Auth failure — hub token rejected by cloud, clearing for re-registration on next login');
      cloudSync = null;
      updateSyncState({ hub_token: null, hub_name: null });
      initialSyncStatus = { inProgress: false, step: '', error: 'Hub token rejected — please log out and log back in to re-register', completedAt: null };
    });
    cloudSync.start();
  }
}

async function autoSyncOnStartup() {
  const db = getDb();
  const syncState = getSyncState();

  if (!syncState) {
    console.log('[auto-sync] No sync state — skipping startup sync');
    return;
  }

  if (!syncState.hub_token) {
    console.log('[auto-sync] No hub token — user must log in to register with cloud');
    initialSyncStatus = { inProgress: false, step: '', error: null, completedAt: null };
    return;
  }

  if (isHubRevoked()) {
    console.log('[auto-sync] Hub is revoked — skipping startup sync');
    return;
  }

  if (syncState.sync_enabled === 0) {
    console.log('[auto-sync] Hub token found but sync disabled by user preference — skipping');
    return;
  }

  const verifyResult = await verifyHubToken(syncState.cloud_url, syncState.hub_token);
  if (verifyResult === 'invalid') {
    console.log('[auto-sync] Hub token is stale (cloud returned 401) — clearing for re-registration on next login');
    updateSyncState({ hub_token: null, hub_name: null });
    initialSyncStatus = { inProgress: false, step: '', error: 'Hub token expired — please log in to re-register', completedAt: null };
    return;
  }
  if (verifyResult === 'unreachable') {
    console.log('[auto-sync] Cloud unreachable during token verify — proceeding with existing token');
  }

  startCloudSyncIfNeeded();
  console.log('[auto-sync] Hub token verified — CloudSync WebSocket started');

  const contentCount = (db.prepare('SELECT COUNT(*) as c FROM contents').get() as any)?.c || 0;
  const screenCount = (db.prepare('SELECT COUNT(*) as c FROM screens').get() as any)?.c || 0;
  const licenseCount = (db.prepare('SELECT COUNT(*) as c FROM licenses').get() as any)?.c || 0;
  const playlistCount = (db.prepare('SELECT COUNT(*) as c FROM playlists').get() as any)?.c || 0;
  const needsFullPull = !syncState.last_sync_at || contentCount === 0 || screenCount === 0 || licenseCount === 0 || playlistCount === 0;
  if (needsFullPull) {
    console.log('[auto-sync] No local data or never synced — attempting full data pull...');
    initialSyncStatus = { inProgress: true, step: 'Pulling data from cloud...', error: null, completedAt: null };
    try {
      const totalSynced = await pullFullDataViaHubToken(syncState.cloud_url, syncState.hub_token);
      console.log(`[auto-sync] Full pull complete — ${totalSynced} rows synced`);
      initialSyncStatus = { inProgress: false, step: 'Complete', error: null, completedAt: new Date().toISOString() };
    } catch (e: any) {
      console.error('[auto-sync] Full pull failed:', e.message);
      initialSyncStatus = { inProgress: false, step: '', error: 'Failed to pull data from cloud', completedAt: null };
    }
  }
}

const CLOUD_SYNC_ENDPOINTS = [
  { path: '/api/customer/screens', table: 'screens' },
  { path: '/api/customer/contents', table: 'contents' },
  { path: '/api/customer/playlists', table: 'playlists' },
  { path: '/api/customer/folders', table: 'content_folders' },
  { path: '/api/customer/schedules', table: 'schedules' },
  { path: '/api/customer/screen-groups', table: 'screen_groups' },
  { path: '/api/customer/broadcasts', table: 'broadcasts' },
  { path: '/api/customer/qr-codes', table: 'smart_qr_codes' },
  { path: '/api/customer/video-walls', table: 'video_walls' },
  { path: '/api/customer/kiosks', table: 'kiosks' },
  { path: '/api/customer/smart-triggers', table: 'smart_triggers' },
  { path: '/api/customer/design-templates', table: 'design_templates' },
  { path: '/api/customer/licenses', table: 'licenses' },
  { path: '/api/customer/subscription-groups', table: 'subscription_groups' },
  { path: '/api/customer/teams', table: 'teams' },
  { path: '/api/customer/team-members', table: 'team_members' },
  { path: '/api/customer/team-roles', table: 'team_roles' },
  { path: '/api/customer/team-screens', table: 'team_screens' },
  { path: '/api/customer/team-categories', table: 'team_categories' },
  { path: '/api/customer/notifications', table: 'notifications' },
  { path: '/api/customer/approvals', table: 'approval_logs' },
  { path: '/api/customer/widgets', table: 'widget_definitions' },
  { path: '/api/customer/layout-templates', table: 'layout_templates' },
  { path: '/api/customer/template-categories', table: 'template_categories' },
  { path: '/api/customer/emergency-alerts/custom-feeds', table: 'custom_alert_feeds' },
];

async function pullAllDataFromCloud(cloudUrl: string, sessionCookie: string, onStep?: (step: string) => void): Promise<number> {
  const db = getDb();
  let totalSynced = 0;

  for (const ep of CLOUD_SYNC_ENDPOINTS) {
    try {
      onStep?.(`Syncing ${ep.table.replace(/_/g, ' ')}...`);
      const res = await fetch(`${cloudUrl}${ep.path}`, {
        headers: { 'Cookie': sessionCookie, 'Accept': 'application/json' },
      });
      if (!res.ok) {
        console.log(`[cloud-pull] ${ep.table}: HTTP ${res.status}`);
        continue;
      }
      const raw = await res.json();
      const data = Array.isArray(raw) ? raw : (raw && typeof raw === 'object' && Array.isArray(raw.data)) ? raw.data : null;
      if (!data || data.length === 0) {
        const responseType = raw === null ? 'null' : Array.isArray(raw) ? 'empty array' : typeof raw === 'object' ? `object with keys: ${Object.keys(raw).join(',')}` : typeof raw;
        console.log(`[cloud-pull] ${ep.table}: ${data ? '0 rows' : `non-array response (${responseType})`}`);
        continue;
      }

      withoutTriggers(() => {
        for (const row of data) {
          try {
            if (row && typeof row.id !== 'undefined') {
              upsertRow(ep.table, row);
            }
          } catch (e: any) {
            console.log(`[cloud-pull] Row error in ${ep.table}: ${e.message}`);
          }
        }
      });
      totalSynced += data.length;
      console.log(`[cloud-pull] Synced ${data.length} rows for ${ep.table}`);
    } catch (e: any) {
      console.log(`[cloud-pull] Skipped ${ep.table}: ${e.message}`);
    }
  }

  updateSyncState({ last_sync_at: new Date().toISOString() });
  return totalSynced;
}

const FULL_PULL_TABLE_MAP: Record<string, string> = {
  screens: 'screens',
  contents: 'contents',
  playlists: 'playlists',
  contentFolders: 'content_folders',
  schedules: 'schedules',
  playlistItems: 'playlist_items',
  videoWalls: 'video_walls',
  videoWallScreens: 'video_wall_screens',
  kiosks: 'kiosks',
  broadcasts: 'broadcasts',
  smartQrCodes: 'smart_qr_codes',
  smartTriggers: 'smart_triggers',
  screenGroups: 'screen_groups',
  screenGroupMembers: 'screen_group_members',
  designTemplates: 'design_templates',
  licenses: 'licenses',
  subscriptionGroups: 'subscription_groups',
  teams: 'teams',
  teamMembers: 'team_members',
  teamRoles: 'team_roles',
  teamScreens: 'team_screens',
  teamCategories: 'team_categories',
  layoutTemplates: 'layout_templates',
  templateCategories: 'template_categories',
  widgetCategories: 'widget_categories',
  widgetDefinitions: 'widget_definitions',
  emergencyAlertConfigs: 'emergency_alert_configs',
  customAlertFeeds: 'custom_alert_feeds',
  notifications: 'notifications',
  subscribers: 'subscribers',
  approvalLogs: 'approval_logs',
};

async function pullFullDataViaHubToken(cloudUrl: string, hubToken: string): Promise<number> {
  try {
    const res = await fetch(`${cloudUrl}/api/hub/sync/full-pull`, {
      headers: { 'x-hub-token': hubToken, 'Accept': 'application/json' },
    });
    if (!res.ok) {
      console.log(`[full-pull] Cloud returned ${res.status}`);
      return 0;
    }
    const data = await res.json() as Record<string, any[]>;
    const db = getDb();
    let totalSynced = 0;

    const PRIORITY_KEYS = ['licenses', 'subscriptionGroups'];
    const sortedEntries = Object.entries(data).sort(([a], [b]) => {
      const aPri = PRIORITY_KEYS.includes(a) ? 0 : 1;
      const bPri = PRIORITY_KEYS.includes(b) ? 0 : 1;
      return aPri - bPri;
    });

    withoutTriggers(() => {
      for (const [key, rows] of sortedEntries) {
        const tableName = FULL_PULL_TABLE_MAP[key] || key;
        if (!Array.isArray(rows)) continue;
        for (const row of rows) {
          try {
            if (row && typeof row.id !== 'undefined') {
              upsertRow(tableName, row);
              totalSynced++;
            }
          } catch (e: any) {
            console.log(`[full-pull] Row error in ${tableName}: ${e.message}`);
          }
        }
        if (rows.length > 0) {
          console.log(`[full-pull] Synced ${rows.length} rows for ${tableName}`);
        }
      }
    });

    updateSyncState({ last_sync_at: new Date().toISOString() });
    console.log(`[full-pull] Complete — ${totalSynced} total rows`);
    return totalSynced;
  } catch (e: any) {
    console.error('[full-pull] Failed:', e.message);
    return 0;
  }
}

async function runInitialCloudSync(subscriberId: number, cloudUrl: string, email: string, password: string, capturedCookie?: string) {
  initialSyncStatus = { inProgress: true, step: 'Registering with cloud...', error: null, completedAt: null };

  const hubResult = await registerHubWithCloud(cloudUrl, email, password, subscriberId);
  if (hubResult) {
    startCloudSyncIfNeeded();
  } else {
    console.log('[initial-sync] Hub registration failed — will proceed with data pull via REST if session available');
  }

  initialSyncStatus.step = 'Authenticating with cloud...';

  let sessionCookie = capturedCookie || null;
  if (!sessionCookie) {
    console.log('[initial-sync] No pre-captured cookie — calling getCloudSession');
    sessionCookie = await getCloudSession(cloudUrl, email, password);
  } else {
    console.log('[initial-sync] Using pre-captured cloud session cookie from login');
  }

  if (!sessionCookie) {
    console.log('[initial-sync] Could not get cloud session');
    if (hubResult) {
      initialSyncStatus.step = 'Pulling data from cloud...';
      try {
        const syncState = getSyncState();
        const totalSynced = await pullFullDataViaHubToken(syncState!.cloud_url, syncState!.hub_token);
        console.log(`[initial-sync] Full pull via hub token complete — ${totalSynced} rows`);
        initialSyncStatus = { inProgress: false, step: 'Complete', error: null, completedAt: new Date().toISOString() };
      } catch (e: any) {
        console.error('[initial-sync] Full pull via hub token failed:', e.message);
        initialSyncStatus = { inProgress: false, step: '', error: 'Failed to pull data', completedAt: null };
      }
    } else {
      initialSyncStatus = { inProgress: false, step: '', error: 'Could not authenticate with cloud for data sync', completedAt: null };
    }
    return;
  }

  updateSyncState({ cloud_session_cookie: sessionCookie });
  console.log('[initial-sync] Cloud session obtained and saved');

  initialSyncStatus.step = 'Pulling data from cloud...';

  const syncState = getSyncState();
  let totalSynced = 0;

  if (syncState?.hub_token) {
    console.log('[initial-sync] Pulling data via hub token (full pull)...');
    try {
      totalSynced = await pullFullDataViaHubToken(syncState.cloud_url, syncState.hub_token);
      console.log(`[initial-sync] Hub token pull complete — ${totalSynced} rows`);
    } catch (e: any) {
      console.error('[initial-sync] Hub token pull failed, falling back to REST:', e.message);
      totalSynced = 0;
    }
  }

  if (totalSynced === 0) {
    console.log('[initial-sync] Pulling data via REST endpoints...');
    totalSynced = await pullAllDataFromCloud(cloudUrl, sessionCookie, (step) => {
      initialSyncStatus.step = step;
    });
  }

  const finalSyncState = getSyncState();
  const hubRegistered = !!finalSyncState?.hub_token && !finalSyncState?.hub_revoked;

  if (hubRegistered) {
    initialSyncStatus = { inProgress: false, step: 'Complete', error: null, completedAt: new Date().toISOString() };
    console.log(`[initial-sync] Initial cloud data sync complete — ${totalSynced} total rows`);
  } else {
    initialSyncStatus = { inProgress: false, step: '', error: 'Hub registration failed — data was synced via REST but real-time sync is not active', completedAt: null };
    console.log(`[initial-sync] Data sync finished (${totalSynced} rows) but hub registration failed — no hub token`);
    insertErrorLog({
      level: 'warn',
      source: 'initial-sync',
      message: 'Initial sync completed but hub registration failed — no hub token obtained',
      context: { subscriberId, totalSynced },
    });
  }
}

const MAX_UPLOAD_SIZE = 500 * 1024 * 1024;

type PermissionKey =
  | 'content.view' | 'content.create' | 'content.edit' | 'content.delete' | 'content.approve'
  | 'screens.view' | 'screens.pair' | 'screens.edit' | 'screens.delete' | 'screens.control'
  | 'playlists.view' | 'playlists.create' | 'playlists.edit' | 'playlists.delete'
  | 'schedules.view' | 'schedules.create' | 'schedules.edit' | 'schedules.delete'
  | 'design.view' | 'design.create' | 'design.edit' | 'design.delete'
  | 'teams.view' | 'teams.manage'
  | 'billing.view' | 'billing.manage'
  | 'analytics.view';

const ALL_PERMISSIONS: PermissionKey[] = [
  'content.view', 'content.create', 'content.edit', 'content.delete', 'content.approve',
  'screens.view', 'screens.pair', 'screens.edit', 'screens.delete', 'screens.control',
  'playlists.view', 'playlists.create', 'playlists.edit', 'playlists.delete',
  'schedules.view', 'schedules.create', 'schedules.edit', 'schedules.delete',
  'design.view', 'design.create', 'design.edit', 'design.delete',
  'teams.view', 'teams.manage',
  'billing.view', 'billing.manage',
  'analytics.view',
];

interface DigipalSession {
  subscriberId: number;
  token: string;
  permissions: PermissionKey[];
  accountRole: string;
}

declare module 'express-serve-static-core' {
  interface Request {
    session: DigipalSession;
    resource?: OwnableRow;
    file?: { buffer: Buffer; originalname: string; mimetype: string; size: number };
  }
}

function resolvePermissions(subscriberId: number): PermissionKey[] {
  try {
    const db = getDb();
    const sub = db.prepare('SELECT account_role FROM subscribers WHERE id = ?').get(subscriberId) as { account_role: string } | undefined;
    if (!sub) return [];
    if (sub.account_role === 'owner' || !sub.account_role) return [...ALL_PERMISSIONS];

    const membership = db.prepare(`
      SELECT tm.role_id, tr.permissions FROM team_members tm
      LEFT JOIN team_roles tr ON tr.id = tm.role_id
      WHERE tm.subscriber_id = ?
    `).all(subscriberId) as Array<{ role_id: number | null; permissions: string | null }>;

    const permSet = new Set<PermissionKey>();
    permSet.add('content.view');
    permSet.add('screens.view');
    permSet.add('playlists.view');
    permSet.add('schedules.view');

    for (const m of membership) {
      if (m.permissions) {
        try {
          const perms = JSON.parse(m.permissions) as PermissionKey[];
          perms.forEach(p => permSet.add(p));
        } catch { /* ignore invalid JSON */ }
      }
    }

    return Array.from(permSet);
  } catch (e: any) {
    console.error('[resolvePermissions] Error:', e.message);
    return [];
  }
}

function sessionMiddleware(req: Request, _res: Response, next: NextFunction) {
  const cookies = (req as any).cookies as Record<string, string> | undefined;
  const token = req.headers.authorization?.replace('Bearer ', '') || cookies?.session;
  if (token) {
    const sess = getSession(token);
    if (sess) {
      const permissions = resolvePermissions(sess.subscriberId);
      const db = getDb();
      const sub = db.prepare('SELECT account_role FROM subscribers WHERE id = ?').get(sess.subscriberId) as { account_role: string } | undefined;
      req.session = {
        subscriberId: sess.subscriberId,
        token,
        permissions,
        accountRole: sub?.account_role || 'viewer',
      };
    }
  }
  next();
}

function requireAuth(req: Request, res: Response, next: NextFunction) {
  if (!req.session?.subscriberId) {
    return res.status(401).json({ message: 'Authentication required' });
  }
  next();
}

function requirePermission(...permissions: PermissionKey[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.session?.permissions) {
      return res.status(403).json({ message: 'No permissions resolved' });
    }
    const hasAll = permissions.every(p => req.session.permissions.includes(p));
    if (!hasAll) {
      return res.status(403).json({ message: "You don't have permission to perform this action" });
    }
    next();
  };
}

function validateTeamAccess(req: Request, res: Response, next: NextFunction) {
  try {
    const teamId = req.query.teamId ? Number(req.query.teamId) : (req.body?.teamId ? Number(req.body.teamId) : null);
    if (!teamId) return next();
    const db = getDb();
    const membership = db.prepare('SELECT id FROM team_members WHERE team_id = ? AND subscriber_id = ?').get(teamId, req.session.subscriberId) as { id: number } | undefined;
    const isOwner = db.prepare('SELECT id FROM teams WHERE id = ? AND owner_id = ?').get(teamId, req.session.subscriberId) as { id: number } | undefined;
    if (!membership && !isOwner) {
      return res.status(403).json({ message: 'You do not have access to this team' });
    }
    next();
  } catch (e: any) {
    console.error('[validateTeamAccess] Error:', e.message);
    next();
  }
}

function cookieParser(req: Request, _res: Response, next: NextFunction) {
  const cookieHeader = req.headers.cookie || '';
  const cookies: Record<string, string> = {};
  cookieHeader.split(';').forEach((c) => {
    const [key, ...rest] = c.trim().split('=');
    if (key) cookies[key] = rest.join('=');
  });
  (req as any).cookies = cookies;
  next();
}

interface ScreenRow {
  id: number;
  pairing_code: string;
  name: string;
  owner_id: number;
  [key: string]: unknown;
}

interface OwnableRow {
  owner_id?: number;
  ownerId?: number;
  subscriber_id?: number;
  subscriberId?: number;
  team_id?: number;
  teamId?: number;
  [key: string]: unknown;
}

function assertOwnership(row: OwnableRow | null | undefined, subscriberId: number): boolean {
  if (!row) return false;
  if (row.owner_id === subscriberId || row.ownerId === subscriberId) return true;
  if (row.subscriber_id === subscriberId || row.subscriberId === subscriberId) return true;
  const db = getDb();
  const teamIds = (db.prepare('SELECT team_id FROM team_members WHERE subscriber_id = ?').all(subscriberId) as Array<{ team_id: number }>).map(r => r.team_id);
  if (row.team_id && teamIds.includes(row.team_id)) return true;
  if (row.teamId && teamIds.includes(row.teamId)) return true;
  return false;
}

const ALLOWED_OWNERSHIP_TABLES = new Set([
  'screens', 'contents', 'content_folders', 'playlists',
  'schedules', 'video_walls', 'kiosks', 'smart_triggers', 'broadcasts',
  'screen_groups', 'teams', 'team_members', 'team_roles', 'team_categories',
  'dooh_campaigns', 'dooh_ad_requests',
  'layout_templates', 'smart_qr_codes', 'custom_alert_feeds',
  'directory_venues', 'notifications',
]);

const PARENT_OWNERSHIP_MAP: Record<string, { table: string; fk: string; parentTable: string }> = {
  playlist_items: { table: 'playlist_items', fk: 'playlist_id', parentTable: 'playlists' },
  team_screens: { table: 'team_screens', fk: 'team_id', parentTable: 'teams' },
  dooh_ad_slots: { table: 'dooh_ad_slots', fk: 'screen_id', parentTable: 'screens' },
  directory_floors: { table: 'directory_floors', fk: 'venue_id', parentTable: 'directory_venues' },
  directory_categories: { table: 'directory_categories', fk: 'venue_id', parentTable: 'directory_venues' },
  directory_stores: { table: 'directory_stores', fk: 'venue_id', parentTable: 'directory_venues' },
  directory_promotions: { table: 'directory_promotions', fk: 'venue_id', parentTable: 'directory_venues' },
};

function requireOwnership(table: string) {
  const parentDef = PARENT_OWNERSHIP_MAP[table];
  if (!parentDef && !ALLOWED_OWNERSHIP_TABLES.has(table)) {
    console.error(`[requireOwnership] Unknown table "${table}" — skipping ownership check`);
    return (_req: Request, _res: Response, next: NextFunction) => next();
  }
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      if (!req.session?.subscriberId) return res.status(401).json({ message: 'Authentication required' });
      const db = getDb();
      const row = db.prepare(`SELECT * FROM ${parentDef ? parentDef.table : table} WHERE id = ?`).get(Number(req.params.id)) as OwnableRow | undefined;
      if (!row) return res.status(404).json({ message: 'Not found' });
      if (parentDef) {
        const fkValue = row[parentDef.fk] as number | undefined;
        const parent = db.prepare(`SELECT * FROM ${parentDef.parentTable} WHERE id = ?`).get(fkValue) as OwnableRow | undefined;
        if (!parent || !assertOwnership(parent, req.session.subscriberId)) return res.status(403).json({ message: 'Access denied' });
      } else {
        if (!assertOwnership(row, req.session.subscriberId)) return res.status(403).json({ message: 'Access denied' });
      }
      req.resource = row;
      next();
    } catch (e: any) {
      console.error(`[requireOwnership] Error for ${table}:`, e.message);
      res.status(500).json({ message: e.message, source: 'requireOwnership', table });
    }
  };
}

function requireParentOwnership(parentTable: string, paramName: string) {
  if (!ALLOWED_OWNERSHIP_TABLES.has(parentTable)) {
    throw new Error(`requireParentOwnership: unknown table "${parentTable}"`);
  }
  return async (req: Request, res: Response, next: NextFunction) => {
    if (!req.session?.subscriberId) return res.status(401).json({ message: 'Authentication required' });
    const db = getDb();
    const parent = db.prepare(`SELECT * FROM ${parentTable} WHERE id = ?`).get(Number(req.params[paramName])) as OwnableRow | undefined;
    if (!parent) return res.status(404).json({ message: 'Parent resource not found' });
    if (!assertOwnership(parent, req.session.subscriberId)) return res.status(403).json({ message: 'Access denied' });
    next();
  };
}

function validateBodyOwnership(parentTable: string, bodyField: string) {
  if (!ALLOWED_OWNERSHIP_TABLES.has(parentTable) && !PARENT_OWNERSHIP_MAP[parentTable]) {
    throw new Error(`validateBodyOwnership: unknown table "${parentTable}"`);
  }
  return async (req: Request, res: Response, next: NextFunction) => {
    if (!req.session?.subscriberId) return res.status(401).json({ message: 'Authentication required' });
    const parentId = req.body?.[bodyField];
    if (!parentId) return next();
    const db = getDb();
    const parent = db.prepare(`SELECT * FROM ${parentTable} WHERE id = ?`).get(Number(parentId)) as OwnableRow | undefined;
    if (!parent) return res.status(404).json({ message: `Parent ${parentTable} not found` });
    if (!assertOwnership(parent, req.session.subscriberId)) return res.status(403).json({ message: 'Access denied' });
    next();
  };
}

export function setHubBlocked(blocked: boolean) { hubBlocked = blocked; }

export function setDiscoveredHubs(hubs: Array<{ name: string; host: string; port: number }>) {
  discoveredHubs = hubs;
}

function parseMultipart(fieldName: string) {
  return (req: Request, res: Response, next: NextFunction) => {
    const contentType = req.headers['content-type'] || '';
    if (!contentType.includes('multipart/form-data')) {
      return next();
    }

    const boundary = contentType.split('boundary=')[1];
    if (!boundary) return next();

    const fields: Record<string, string> = {};
    const rawChunks: Buffer[] = [];
    let totalSize = 0;
    let aborted = false;

    req.on('data', (chunk: Buffer) => {
      if (aborted) return;
      totalSize += chunk.length;
      if (totalSize > MAX_UPLOAD_SIZE) {
        aborted = true;
        req.unpipe();
        res.status(413).json({ message: 'File too large' });
        return;
      }
      rawChunks.push(chunk);
    });

    req.on('end', () => {
      if (aborted) return;
      try {
        const body = Buffer.concat(rawChunks);
        const boundaryBuf = Buffer.from(`--${boundary}`);

        const parts: Buffer[] = [];
        let start = 0;
        while (true) {
          const idx = body.indexOf(boundaryBuf, start);
          if (idx === -1) break;
          if (start > 0) {
            parts.push(body.subarray(start, idx));
          }
          start = idx + boundaryBuf.length;
          if (body[start] === 0x0d && body[start + 1] === 0x0a) start += 2;
        }

        for (const part of parts) {
          const headerEndIdx = part.indexOf('\r\n\r\n');
          if (headerEndIdx === -1) continue;

          const headerStr = part.subarray(0, headerEndIdx).toString('utf8');
          let partBody = part.subarray(headerEndIdx + 4);
          if (partBody.length >= 2 && partBody[partBody.length - 2] === 0x0d && partBody[partBody.length - 1] === 0x0a) {
            partBody = partBody.subarray(0, partBody.length - 2);
          }

          const filenameMatch = headerStr.match(/filename="([^"]+)"/);
          const nameMatch = headerStr.match(/name="([^"]+)"/);

          if (filenameMatch && nameMatch && nameMatch[1] === fieldName) {
            req.file = {
              buffer: Buffer.from(partBody),
              originalname: filenameMatch[1],
              mimetype: (headerStr.match(/Content-Type:\s*(.+)/i)?.[1]?.trim()) || 'application/octet-stream',
              size: partBody.length,
            };
          } else if (nameMatch) {
            fields[nameMatch[1]] = partBody.toString('utf8');
          }
        }

        req.body = { ...req.body, ...fields };
        next();
      } catch (err: any) {
        res.status(400).json({ message: 'Failed to parse upload' });
      }
    });

    req.on('error', (err: Error) => {
      if (!aborted) next(err);
    });
  };
}

export async function startServer(port: number): Promise<number> {
  process.on('uncaughtException', (err) => {
    console.error('[startup] Uncaught exception:', err.message);
    try {
      insertErrorLog({
        level: 'critical',
        source: 'process',
        message: `Uncaught exception: ${err.message}`,
        stack: err.stack,
        context: { phase: 'runtime' },
      });
    } catch {}
  });

  process.on('unhandledRejection', (reason: any) => {
    const msg = reason instanceof Error ? reason.message : String(reason);
    const stack = reason instanceof Error ? reason.stack : undefined;
    console.error('[startup] Unhandled rejection:', msg);
    try {
      insertErrorLog({
        level: 'error',
        source: 'process',
        message: `Unhandled rejection: ${msg}`,
        stack,
        context: { phase: 'runtime' },
      });
    } catch {}
  });

  try {
    initSessionTable();
  } catch (err: any) {
    insertErrorLog({
      level: 'critical',
      source: 'startup',
      message: `Session table init failed: ${err.message}`,
      stack: err.stack,
      context: { phase: 'startup' },
    });
    throw err;
  }

  cleanExpiredSessions();
  sessionCleanupInterval = setInterval(() => cleanExpiredSessions(), 60 * 60 * 1000);

  const app = express();

  app.use(cors({
    origin: (origin: string | undefined, callback: (err: Error | null, allow?: boolean) => void) => {
      if (!origin) return callback(null, true);
      try {
        const url = new URL(origin);
        const h = url.hostname;
        const isLocal = h === 'localhost' || h === '127.0.0.1' || h === '::1';
        const isPrivateIP = /^(10\.\d+\.\d+\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+\.\d+|192\.168\.\d+\.\d+)$/.test(h);
        if (isLocal || isPrivateIP) {
          return callback(null, true);
        }
      } catch (e) { /* invalid origin */ }
      callback(new Error('CORS not allowed'));
    },
    credentials: true,
  }));
  app.use(cookieParser);

  const distPublicPath = path.join(__dirname, '../../dist/public');
  const rendererSrc = path.join(__dirname, '../../src/renderer');
  const resPath = (process as NodeJS.Process & { resourcesPath?: string }).resourcesPath;
  const rendererRes = resPath ? path.join(resPath, 'renderer') : '';
  const frontendPath = fs.existsSync(path.join(distPublicPath, 'index.html'))
    ? distPublicPath
    : (rendererRes && fs.existsSync(path.join(rendererRes, 'index.html')))
      ? rendererRes
      : fs.existsSync(path.join(rendererSrc, 'index.html'))
        ? rendererSrc
        : distPublicPath;
  app.use(express.static(frontendPath));

  const mediaPath = (() => { try { return getMediaDir(); } catch { return null; } })();
  if (mediaPath) {
    app.use('/media', express.static(path.join(mediaPath, 'uploads')));
  }

  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ extended: true }));
  app.use(sessionMiddleware);

  app.use((req: Request, res: Response, next: NextFunction) => {
    if (req.path.startsWith('/api/')) {
      const start = Date.now();
      const origJson = res.json.bind(res);
      res.json = function(body: any) {
        const ms = Date.now() - start;
        if (res.statusCode >= 400) {
          const bodyStr = JSON.stringify(body)?.slice(0, 500);
          console.error(`[http] ${req.method} ${req.originalUrl} → ${res.statusCode} (${ms}ms) session=${!!req.session?.subscriberId} body=${bodyStr}`);
          if (res.statusCode >= 500) {
            insertErrorLog({
              level: 'error',
              source: 'api',
              route: req.originalUrl,
              method: req.method,
              statusCode: res.statusCode,
              message: body?.message || body?.error || `HTTP ${res.statusCode}`,
              stack: body?.stack || undefined,
              context: { responseTime: ms, subscriberId: req.session?.subscriberId },
            });
          }
        }
        return origJson(body);
      };
    }
    next();
  });

  app.get('/api/mdns-status', (_req: Request, res: Response) => {
    res.json(getMdnsStatus());
  });

  app.get('/api/diagnostics', (_req: Request, res: Response) => {
    try {
      const db = getDb();
      const tables = ['screens', 'contents', 'playlists', 'schedules', 'subscribers', 'content_folders', 'broadcasts', 'design_templates', 'kiosks', 'video_walls', 'error_logs'];
      const counts: Record<string, number> = {};
      for (const t of tables) {
        try { counts[t] = (db.prepare(`SELECT COUNT(*) as c FROM ${t}`).get() as any).c; } catch { counts[t] = -1; }
      }
      const sessions = db.prepare('SELECT id, subscriber_id, created_at FROM sessions').all();
      const syncState = getSyncState();
      const recentErrors = getRecentErrorLogs(10);
      const unpushedChanges = getUnpushedChangeCount();
      const version = require('../../package.json').version;
      res.json({
        ok: true,
        version,
        counts,
        sessions,
        syncState: {
          hub_token: syncState?.hub_token ? '***' : null,
          cloud_url: syncState?.cloud_url,
          last_sync_at: syncState?.last_sync_at,
          last_cloud_contact_at: syncState?.last_cloud_contact_at,
          hub_revoked: syncState?.hub_revoked,
          sync_enabled: syncState?.sync_enabled,
        },
        unpushedChanges,
        recentErrors,
        cloudSyncConnected: cloudSync?.isConnected() ?? false,
        hubBlocked,
        uptime: process.uptime(),
        memoryUsage: process.memoryUsage(),
      });
    } catch (e: any) { res.json({ ok: false, error: e.message, stack: e.stack }); }
  });

  app.get('/api/diagnostics/errors', (_req: Request, res: Response) => {
    try {
      const limit = Math.min(parseInt(_req.query.limit as string) || 50, 200);
      const offset = parseInt(_req.query.offset as string) || 0;
      const errors = getRecentErrorLogs(limit, offset);
      const total = getErrorLogCount();
      res.json({ errors, total, limit, offset });
    } catch (e: any) { res.json({ errors: [], total: 0, error: e.message }); }
  });

  app.post('/api/customer/login', async (req: Request, res: Response) => {
    if (hubBlocked) return res.status(503).json({ message: 'Hub is blocked — another Digipal hub was detected on this network. Only one hub is allowed per network.' });
    const { email, password, rememberMe } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email and password required' });
    const result = await authenticateUser(email, password);
    if (!result.success) return res.status(401).json({ message: result.error });

    const subscriber = result.subscriber!;
    const capturedCookie = result.cloudSessionCookie;
    const syncState = getSyncState();
    const cloudUrl = syncState?.cloud_url || 'https://digipalsignage.com';
    const isOwner = subscriber.accountRole === 'owner';
    const hubAlreadyRegistered = !!syncState?.hub_token && !syncState?.hub_revoked;

    if (!isOwner && !hubAlreadyRegistered) {
      return res.status(403).json({ message: 'This local server has not been set up yet. The account owner must log in first to register this hub.' });
    }

    const sessionId = createSession(subscriber.id, !!rememberMe);
    const cookieMaxAge = rememberMe ? 90 * 24 * 60 * 60 : 24 * 60 * 60;
    res.setHeader('Set-Cookie', `session=${sessionId}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${cookieMaxAge}`);

    if (isOwner && !hubAlreadyRegistered) {
      const syncPromise = runInitialCloudSync(subscriber.id as number, cloudUrl, email, password, capturedCookie);
      const timeoutPromise = new Promise<void>((resolve) => setTimeout(resolve, 15000));
      await Promise.race([syncPromise, timeoutPromise]).catch(e =>
        console.error('[initial-sync] Error during login-await:', e.message)
      );

      const updatedSyncState = getSyncState();
      const syncSucceeded = !!updatedSyncState?.hub_token && !updatedSyncState?.hub_revoked;
      res.json({
        ...subscriber,
        syncStatus: syncSucceeded ? 'complete' : 'pending',
        syncError: syncSucceeded ? null : (initialSyncStatus.error || 'Hub registration still in progress'),
      });
    } else {
      if (isOwner && capturedCookie) {
        updateSyncState({ cloud_session_cookie: capturedCookie });
      }
      if (isOwner && hubAlreadyRegistered) {
        const verifyResult = await verifyHubToken(cloudUrl, syncState!.hub_token);
        if (verifyResult === 'invalid') {
          console.log('[login] Stored hub token is stale — re-registering');
          updateSyncState({ hub_token: null, hub_name: null });
          if (cloudSync) { cloudSync.stop(); cloudSync = null; }
          const syncPromise = runInitialCloudSync(subscriber.id as number, cloudUrl, email, password, capturedCookie);
          const timeoutPromise = new Promise<void>((resolve) => setTimeout(resolve, 15000));
          await Promise.race([syncPromise, timeoutPromise]).catch(e =>
            console.error('[login] Re-registration error:', e.message)
          );
          const updatedSyncState = getSyncState();
          const syncSucceeded = !!updatedSyncState?.hub_token && !updatedSyncState?.hub_revoked;
          res.json({
            ...subscriber,
            syncStatus: syncSucceeded ? 'complete' : 'pending',
            syncError: syncSucceeded ? null : (initialSyncStatus.error || 'Hub re-registration in progress'),
          });
          return;
        }
        if (!cloudSync) {
          startCloudSyncIfNeeded();
        }
      }
      res.json({
        ...subscriber,
        syncStatus: hubAlreadyRegistered ? 'complete' : 'not_required',
        syncError: null,
      });
    }
  });

  app.get('/api/customer/sync-status', requireAuth, (_req: Request, res: Response) => {
    const currentSyncState = getSyncState();
    const hubRegistered = !!currentSyncState?.hub_token && !currentSyncState?.hub_revoked;
    res.json({
      ...initialSyncStatus,
      hubRegistered,
    });
  });

  app.post('/api/customer/force-sync', requireAuth, async (req: Request, res: Response) => {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email and password required for sync' });
    const syncState = getSyncState();
    const cloudUrl = syncState?.cloud_url || 'https://digipalsignage.com';

    res.json({ message: 'Sync started' });

    try {
      await runInitialCloudSync(req.session.subscriberId, cloudUrl, email, password);
      console.log('[force-sync] Sync completed successfully');
    } catch (e: any) {
      console.error('[force-sync] Error:', e.message);
    }
  });

  app.post('/api/customer/hub/sync', requireAuth, async (req: Request, res: Response) => {
    const sub = getSessionSubscriber(req.session.subscriberId);
    if (!sub || sub.accountRole !== 'owner') {
      return res.status(403).json({ message: 'Only the account owner can trigger hub sync' });
    }
    if (cloudSync?.isConnected()) {
      cloudSync.triggerForceSync();
      res.json({ sent: true });
    } else {
      res.json({ sent: false });
    }
  });

  app.post('/api/customer/hub/token/regenerate', requireAuth, async (req: Request, res: Response) => {
    const sub = getSessionSubscriber(req.session.subscriberId);
    if (!sub || sub.accountRole !== 'owner') {
      return res.status(403).json({ message: 'Only the account owner can regenerate the hub token' });
    }
    const syncState = getSyncState();
    if (!syncState?.cloud_session_cookie || !syncState?.cloud_url) {
      return res.status(400).json({ message: 'Not connected to cloud — please log out and log back in first' });
    }
    try {
      const cloudRes = await fetch(`${syncState.cloud_url}/api/customer/hub/token/regenerate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Cookie': syncState.cloud_session_cookie },
      });
      if (!cloudRes.ok) {
        const body = await cloudRes.text().catch(() => '');
        return res.status(cloudRes.status).json({ message: body || 'Cloud rejected token regeneration' });
      }
      const result = await cloudRes.json() as any;
      if (result.hubToken) {
        if (cloudSync) {
          cloudSync.stop();
          cloudSync = null;
        }
        updateSyncState({ hub_token: result.hubToken });
        startCloudSyncIfNeeded();
        console.log('[hub] Token regenerated via cloud — CloudSync restarted');
      }
      res.json({ message: 'Token regenerated successfully' });
    } catch (e: any) {
      console.error('[hub] Token regeneration error:', e.message);
      res.status(500).json({ message: 'Failed to regenerate token — cloud may be unreachable' });
    }
  });

  app.get('/api/instagram/accounts', requireAuth, (_req: Request, res: Response) => {
    res.json([]);
  });

  app.get('/api/customer/feature-toggles', requireAuth, (_req: Request, res: Response) => {
    res.json([]);
  });

  const LOCAL_PLAN_PRICES: Record<string, number> = {
    free: 0, starter: 12, pro: 25, custom_signage: 0,
    self_service: 45, professional_setup: 45, directory: 109,
    wayfinding_2d: 179, wayfinding_3d: 0, dooh_addon: 50,
  };
  const LOCAL_PLAN_FEATURES: Record<string, any> = {
    free: { playlists: true, schedules: false, videoWalls: false, designStudio: true, analytics: false, teamManagement: false, splitScreen: false, kioskMode: false, remoteControl: false, widgets: false, smartTriggers: false, aiAnalytics: false, kioskDesigner: false, offlinePlayback: true, remoteView: false, doohAds: false, directory: false, wayfinding: false, wayfinding3d: false, directoryIdleContent: false, broadcasts: false, emergencyAlerts: false, knowledgeBase: true, screenCast: false, smartQr: false },
    starter: { playlists: true, schedules: true, videoWalls: false, designStudio: true, analytics: true, teamManagement: false, splitScreen: true, kioskMode: true, remoteControl: true, widgets: true, smartTriggers: false, aiAnalytics: false, kioskDesigner: false, offlinePlayback: true, remoteView: false, doohAds: false, directory: false, wayfinding: false, wayfinding3d: false, directoryIdleContent: false, broadcasts: false, emergencyAlerts: false, knowledgeBase: true, screenCast: false, smartQr: false },
    pro: { playlists: true, schedules: true, videoWalls: false, designStudio: true, analytics: true, teamManagement: true, splitScreen: true, kioskMode: true, remoteControl: true, widgets: true, smartTriggers: false, aiAnalytics: false, kioskDesigner: false, offlinePlayback: true, remoteView: true, doohAds: false, directory: false, wayfinding: false, wayfinding3d: false, directoryIdleContent: false, broadcasts: true, emergencyAlerts: true, knowledgeBase: true, screenCast: true, smartQr: true },
    custom_signage: { playlists: true, schedules: true, videoWalls: true, designStudio: true, analytics: true, teamManagement: true, splitScreen: true, kioskMode: true, remoteControl: true, widgets: true, smartTriggers: true, aiAnalytics: true, kioskDesigner: false, offlinePlayback: true, remoteView: true, doohAds: false, directory: false, wayfinding: false, wayfinding3d: false, directoryIdleContent: false, broadcasts: true, emergencyAlerts: true, knowledgeBase: true, screenCast: true, smartQr: true },
    self_service: { playlists: true, schedules: true, videoWalls: false, designStudio: true, analytics: true, teamManagement: true, splitScreen: true, kioskMode: true, remoteControl: true, widgets: true, smartTriggers: true, aiAnalytics: false, kioskDesigner: true, offlinePlayback: true, remoteView: true, doohAds: false, directory: false, wayfinding: false, wayfinding3d: false, directoryIdleContent: false, broadcasts: true, emergencyAlerts: true, knowledgeBase: true, screenCast: true, smartQr: true },
    professional_setup: { playlists: true, schedules: true, videoWalls: false, designStudio: true, analytics: true, teamManagement: true, splitScreen: true, kioskMode: true, remoteControl: true, widgets: true, smartTriggers: true, aiAnalytics: true, kioskDesigner: true, offlinePlayback: true, remoteView: true, doohAds: false, directory: false, wayfinding: false, wayfinding3d: false, directoryIdleContent: false, broadcasts: true, emergencyAlerts: true, knowledgeBase: true, screenCast: true, smartQr: true },
    directory: { playlists: true, schedules: true, videoWalls: false, designStudio: true, analytics: false, teamManagement: false, splitScreen: false, kioskMode: true, remoteControl: true, widgets: false, smartTriggers: false, aiAnalytics: false, kioskDesigner: false, offlinePlayback: true, remoteView: false, doohAds: false, directory: true, wayfinding: false, wayfinding3d: false, directoryIdleContent: true, broadcasts: false, emergencyAlerts: false, knowledgeBase: true, screenCast: false, smartQr: false },
    wayfinding_2d: { playlists: true, schedules: true, videoWalls: false, designStudio: true, analytics: true, teamManagement: true, splitScreen: false, kioskMode: true, remoteControl: true, widgets: false, smartTriggers: false, aiAnalytics: false, kioskDesigner: false, offlinePlayback: true, remoteView: false, doohAds: false, directory: true, wayfinding: true, wayfinding3d: false, directoryIdleContent: true, broadcasts: false, emergencyAlerts: false, knowledgeBase: true, screenCast: false, smartQr: false },
    wayfinding_3d: { playlists: true, schedules: true, videoWalls: true, designStudio: true, analytics: true, teamManagement: true, splitScreen: true, kioskMode: true, remoteControl: true, widgets: true, smartTriggers: true, aiAnalytics: true, kioskDesigner: true, offlinePlayback: true, remoteView: true, doohAds: false, directory: true, wayfinding: true, wayfinding3d: true, directoryIdleContent: true, broadcasts: true, emergencyAlerts: true, knowledgeBase: true, screenCast: true, smartQr: true },
    dooh_addon: { playlists: false, schedules: false, videoWalls: false, designStudio: false, analytics: false, teamManagement: false, splitScreen: false, kioskMode: false, remoteControl: false, widgets: false, smartTriggers: false, aiAnalytics: false, kioskDesigner: false, offlinePlayback: false, remoteView: false, doohAds: true, directory: false, wayfinding: false, wayfinding3d: false, directoryIdleContent: false, broadcasts: false, emergencyAlerts: false, knowledgeBase: false, screenCast: false, smartQr: false },
  };
  const LOCAL_PLAN_CONFIGS = [
    { tier: 'free', name: 'Free', price: 0 },
    { tier: 'starter', name: 'Starter', price: 12 },
    { tier: 'pro', name: 'Pro', price: 25 },
    { tier: 'custom_signage', name: 'Custom', price: 0 },
    { tier: 'self_service', name: 'Self-Service', price: 45 },
    { tier: 'professional_setup', name: 'Professional Setup', price: 45 },
    { tier: 'directory', name: 'Directory', price: 109 },
    { tier: 'wayfinding_2d', name: '2D Wayfinding', price: 179 },
    { tier: 'wayfinding_3d', name: '3D & Custom', price: 0 },
    { tier: 'dooh_addon', name: 'DOOH Ad Management', price: 50 },
  ];
  const LICENSE_PLAN_RANK: Record<string, number> = {
    free: 0, starter: 1, pro: 2, custom_signage: 3,
    self_service: 4, professional_setup: 5, directory: 6,
    wayfinding_2d: 7, wayfinding_3d: 8, dooh_addon: 9,
  };

  app.get('/api/customer/subscription', requireAuth, async (req: Request, res: Response) => {
    try {
      const subscriberId = req.session.subscriberId;
      const sub = getSessionSubscriber(subscriberId);

      const PLAN_PRICES = LOCAL_PLAN_PRICES;
      const PLAN_FEATURES = LOCAL_PLAN_FEATURES;

      function isLicActive(lic: any): boolean {
        return lic && (lic.status === 'active' || lic.status === 'canceling' || lic.status === 'trial');
      }
      function getEffPlan(lic: any): string {
        if (!lic) return 'free';
        return isLicActive(lic) ? (lic.planTier || 'free') : 'free';
      }
      function getBestLic(lics: any[], screenId: number): any | undefined {
        const active = lics.filter((l: any) => l.screenId === screenId && isLicActive(l));
        if (active.length === 0) return undefined;
        if (active.length === 1) return active[0];
        return active.reduce((best: any, lic: any) => {
          const bestRank = LICENSE_PLAN_RANK[best.planTier] ?? (best.planTier?.startsWith('custom_') ? 10 : -1);
          const licRank = LICENSE_PLAN_RANK[lic.planTier] ?? (lic.planTier?.startsWith('custom_') ? 10 : -1);
          return licRank > bestRank ? lic : best;
        });
      }

      const subscriberLicenses = await storage.getLicensesBySubscriber(subscriberId);
      const customerScreens = await storage.getAllScreensByOwner(subscriberId);
      const customerPlaylists = await storage.getPlaylistsByOwner(subscriberId);
      const customerVideoWalls = await storage.getVideoWallsByOwner(subscriberId);

      const customPlanPrices: Record<string, number> = {};
      const customPlanNames: Record<string, string> = {};
      const customPlanFeatures: Record<string, any> = {};
      try {
        const db = getDb();
        const cpExists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='custom_plans'").get();
        if (cpExists) {
          const customPlans = db.prepare('SELECT * FROM custom_plans WHERE subscriber_id = ?').all(subscriberId);
          for (const cp of customPlans as any[]) {
            const tier = `custom_${cp.id}`;
            customPlanPrices[tier] = cp.price || 0;
            customPlanNames[tier] = cp.name || tier;
            if (cp.features) {
              try { customPlanFeatures[tier] = JSON.parse(cp.features); } catch { customPlanFeatures[tier] = PLAN_FEATURES.custom_signage; }
            } else {
              customPlanFeatures[tier] = PLAN_FEATURES.custom_signage;
            }
          }
        }
      } catch {}

      function resolvePrice(tier: string): number {
        if (PLAN_PRICES[tier] !== undefined) return PLAN_PRICES[tier];
        if (customPlanPrices[tier] !== undefined) return customPlanPrices[tier];
        return 0;
      }
      function resolveFeatures(tier: string): any {
        if (PLAN_FEATURES[tier]) return PLAN_FEATURES[tier];
        if (customPlanFeatures[tier]) return customPlanFeatures[tier];
        return PLAN_FEATURES.custom_signage;
      }

      const deviceBreakdown: Record<string, number> = { free: 0, starter: 0, pro: 0, custom_signage: 0, self_service: 0, professional_setup: 0, directory: 0, wayfinding_2d: 0, wayfinding_3d: 0, dooh_addon: 0, custom: 0 };
      const paidBreakdown: Record<string, number> = { free: 0, starter: 0, pro: 0, custom_signage: 0, self_service: 0, professional_setup: 0, directory: 0, wayfinding_2d: 0, wayfinding_3d: 0, dooh_addon: 0, custom: 0 };
      let totalMonthly = 0;

      for (const lic of subscriberLicenses) {
        if (isLicActive(lic)) {
          const tier = lic.planTier || 'free';
          const isAdminFree = lic.adminGranted && !lic.stripeSubscriptionId;
          const price = (lic.status === 'trial' || isAdminFree) ? 0 : resolvePrice(tier);
          const interval = lic.billingInterval || 'monthly';
          if (tier.startsWith('custom_')) {
            paidBreakdown.custom = (paidBreakdown.custom || 0) + 1;
            totalMonthly += interval === 'annual' ? Math.round(price * 10 / 12) : price;
          } else {
            paidBreakdown[tier] = (paidBreakdown[tier] || 0) + 1;
            totalMonthly += interval === 'annual' ? Math.round(price * 10 / 12) : price;
          }
        }
      }

      const screenList = (customerScreens || []).map((s: any) => {
        const screenLicense = getBestLic(subscriberLicenses, s.id);
        const effectivePlan = getEffPlan(screenLicense);
        const isCustom = effectivePlan.startsWith('custom_');
        const planTier = isCustom ? 'custom' : effectivePlan;

        deviceBreakdown[planTier] = (deviceBreakdown[planTier] || 0) + 1;

        return {
          id: s.id,
          name: s.name,
          plan: planTier,
          planName: isCustom ? (customPlanNames[effectivePlan] || 'Custom') : undefined,
          price: resolvePrice(effectivePlan),
          features: resolveFeatures(effectivePlan),
          isOnline: s.isOnline ?? false,
          licenseId: screenLicense?.id || null,
          licenseStatus: screenLicense?.status || null,
          billingInterval: screenLicense?.billingInterval || null,
          currentPeriodEnd: screenLicense?.currentPeriodEnd || null,
        };
      });

      res.json({
        model: 'license-based' as const,
        isLocalServer: true,
        subscriber: sub ? { name: sub.name || '', email: sub.email || '', company: sub.company || null } : undefined,
        totalScreens: (customerScreens || []).length,
        totalLicenses: subscriberLicenses.filter((l: any) => isLicActive(l)).length,
        availableLicenses: subscriberLicenses.filter((l: any) => isLicActive(l) && !l.screenId).length,
        deviceBreakdown,
        paidBreakdown,
        totalMonthly,
        usage: {
          screens: (customerScreens || []).length,
          playlists: (customerPlaylists || []).length,
          videoWalls: (customerVideoWalls || []).length,
        },
        screens: screenList,
        licenses: subscriberLicenses.map((l: any) => ({
          id: l.id,
          subscriberId: l.subscriberId || subscriberId,
          planTier: l.planTier || 'free',
          billingInterval: l.billingInterval || 'monthly',
          status: l.status || 'active',
          stripeSubscriptionId: l.stripeSubscriptionId || null,
          currentPeriodEnd: l.currentPeriodEnd || null,
          cancelAtPeriodEnd: l.cancelAtPeriodEnd ?? false,
          screenId: l.screenId ?? null,
          createdAt: l.createdAt || null,
          adminGranted: l.adminGranted ?? false,
        })),
        doohAddonActive: false,
        doohFreeAccess: false,
        doohAccessReason: 'local_server',
      });
    } catch (error: any) {
      console.error("Error fetching customer subscription:", error);
      res.status(500).json({ message: "Failed to fetch subscription" });
    }
  });

  app.get('/api/customer/hub', requireAuth, (_req: Request, res: Response) => {
    const syncState = getSyncState();
    if (!syncState?.hub_token) {
      res.json([]);
      return;
    }
    const lanAddresses: string[] = [];
    const interfaces = os.networkInterfaces();
    for (const name of Object.keys(interfaces)) {
      for (const iface of interfaces[name] || []) {
        if (iface.family === 'IPv4' && !iface.internal) {
          lanAddresses.push(iface.address);
        }
      }
    }
    res.json([{
      id: 0,
      subscriberId: syncState.subscriber_id || 0,
      name: syncState.hub_name || os.hostname(),
      hubToken: syncState.hub_token,
      isOnline: !!(cloudSync?.isConnected()),
      lastSeenAt: syncState.last_cloud_contact_at || null,
      lastSyncAt: syncState.last_sync_at || null,
      connectedScreenCount: 0,
      version: require('../../package.json').version,
      createdAt: null,
      serverUrl: lanAddresses.length > 0 ? `http://${lanAddresses[0]}:${boundPort}` : `http://localhost:${boundPort}`,
      lanAddresses,
      port: boundPort,
    }]);
  });

  app.get('/api/customer/login-activity', requireAuth, (_req: Request, res: Response) => {
    res.json([]);
  });

  app.get('/api/customer/active-sessions', requireAuth, (req: Request, res: Response) => {
    res.json([{
      id: req.session.token,
      current: true,
      createdAt: new Date().toISOString(),
      device: 'Local Server',
    }]);
  });

  app.get('/api/customer/trials/status', requireAuth, (_req: Request, res: Response) => {
    res.json({ active: false, trialEnd: null, plan: null });
  });

  app.get('/api/customer/custom-plans', requireAuth, async (req: Request, res: Response) => {
    try {
      const db = getDb();
      const tableExists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='custom_plans'").get();
      if (tableExists) {
        const plans = db.prepare('SELECT * FROM custom_plans WHERE subscriber_id = ?').all(req.session.subscriberId);
        res.json(plans.map((p: any) => ({
          id: p.id,
          name: p.name,
          price: p.price,
          minQuantity: p.min_quantity || null,
          maxDevices: p.max_devices || null,
          tier: `custom_${p.id}`,
        })));
      } else {
        res.json([]);
      }
    } catch {
      res.json([]);
    }
  });

  app.get('/api/plan-configurations', (_req: Request, res: Response) => {
    res.json({
      plans: LOCAL_PLAN_CONFIGS.map(c => ({
        id: null,
        tier: c.tier,
        name: c.name,
        price: c.price,
        features: LOCAL_PLAN_FEATURES[c.tier] || LOCAL_PLAN_FEATURES.free,
      })),
      hiddenFeatures: [],
    });
  });

  const widgetsHandler = (req: Request, res: Response) => {
    try {
      const db = getDb();
      const widgets = db.prepare('SELECT * FROM widget_definitions ORDER BY name').all();
      res.json(widgets);
    } catch { res.json([]); }
  };
  app.get('/api/widgets', requireAuth, widgetsHandler);
  app.get('/api/customer/widgets', requireAuth, widgetsHandler);

  app.get('/api/customer/cloud-sync/status', requireAuth, (_req: Request, res: Response) => {
    const syncState = getSyncState();
    const unpushedCount = getUnpushedChangeCount();
    const isConnected = !!(cloudSync?.isConnected() && !isHubRevoked());
    const isRegistered = !!syncState?.hub_token;
    const syncEnabled = syncState?.sync_enabled !== 0;

    let syncDetail = '';
    if (isHubRevoked()) {
      syncDetail = 'Hub revoked by admin';
    } else if (initialSyncStatus.inProgress) {
      syncDetail = initialSyncStatus.step || 'Syncing...';
    } else if (initialSyncStatus.error) {
      syncDetail = initialSyncStatus.error;
    } else if (!isRegistered) {
      syncDetail = 'Not registered — log in to connect';
    } else if (isConnected && syncEnabled) {
      syncDetail = 'Connected — syncing automatically';
    } else if (syncEnabled && !isConnected) {
      syncDetail = 'Reconnecting to cloud...';
    } else {
      syncDetail = 'Sync paused';
    }

    res.json({
      syncEnabled,
      isConnected,
      isRegistered,
      lastSyncAt: syncState?.last_sync_at || null,
      lastCloudContact: syncState?.last_cloud_contact_at || null,
      unpushedChanges: unpushedCount,
      hubName: syncState?.hub_name || null,
      hubRevoked: isHubRevoked(),
      initialSync: initialSyncStatus,
      syncDetail,
    });
  });

  app.post('/api/customer/cloud-sync/toggle', requireAuth, (req: Request, res: Response) => {
    const { enabled } = req.body;
    const shouldEnable = enabled !== false;

    updateSyncState({ sync_enabled: shouldEnable ? 1 : 0 });

    if (shouldEnable) {
      const syncState = getSyncState();
      if (syncState?.hub_token && syncState?.cloud_url && !isHubRevoked()) {
        if (!cloudSync) {
          startCloudSyncIfNeeded();
        } else {
          cloudSync.start();
        }
        console.log('[cloud-sync] Sync enabled — WebSocket started');
      }
    } else {
      if (cloudSync) {
        cloudSync.stop();
        cloudSync = null;
        console.log('[cloud-sync] Sync disabled — WebSocket stopped');
      }
    }

    res.json({ syncEnabled: shouldEnable, message: shouldEnable ? 'Cloud sync enabled' : 'Cloud sync disabled' });
  });

  app.post('/api/customer/cloud-sync/pull', requireAuth, async (req: Request, res: Response) => {
    const syncState = getSyncState();
    const cloudUrl = syncState?.cloud_url || 'https://digipalsignage.com';
    const hubToken = syncState?.hub_token;
    const fullSync = req.query.full === 'true' || req.body?.full === true;

    if (!hubToken) {
      return res.status(400).json({ message: 'No hub token — please log in to register this hub first.' });
    }

    try {
      const since = syncState?.last_sync_at || new Date(0).toISOString();
      const pullRes = await fetch(`${cloudUrl}/api/hub/sync/pull?since=${encodeURIComponent(since)}`, {
        headers: { 'x-hub-token': hubToken, 'Accept': 'application/json' },
      });

      if (!pullRes.ok) {
        const err = await pullRes.json().catch(() => ({ message: 'Unknown error' }));
        return res.status(pullRes.status).json({ message: err.message || 'Cloud pull failed' });
      }

      const { changes } = await pullRes.json() as { changes: Array<{ tableName: string; recordId: number; operation: string; payload?: string }> };

      const hasIncrementalChanges = Array.isArray(changes) && changes.length > 0;

      if (!hasIncrementalChanges || fullSync) {
        console.log(`[cloud-pull] ${fullSync ? 'Full sync requested' : 'No incremental changes'} — performing full data pull via hub token...`);
        const totalSynced = await pullFullDataViaHubToken(cloudUrl, hubToken);
        if (totalSynced > 0) {
          broadcastToPlayers({ type: 'contentUpdated', payload: {} });
        }
        return res.json({ message: `Full sync complete — ${totalSynced} items synced`, totalSynced, summary: {} });
      }

      const summary: Record<string, { added: number; updated: number; deleted: number }> = {};
      let totalSynced = 0;
      const db = getDb();

      const allowedTables = new Set(SYNCED_TABLES);
      withoutTriggers(() => {
        for (const change of changes) {
          if (!change.tableName || typeof change.recordId === 'undefined') continue;
          const table = change.tableName;
          if (!allowedTables.has(table)) {
            console.log(`[cloud-pull] Skipping disallowed table: ${table}`);
            continue;
          }
          if (!summary[table]) summary[table] = { added: 0, updated: 0, deleted: 0 };

          try {
            if (change.operation === 'DELETE') {
              db.prepare(`DELETE FROM ${table} WHERE id = ?`).run(change.recordId);
              summary[table].deleted++;
            } else {
              const data = change.payload ? JSON.parse(change.payload) : {};
              if (data.id === undefined) data.id = change.recordId;
              const existing = db.prepare(`SELECT id FROM ${table} WHERE id = ?`).get(change.recordId);
              upsertRow(table, data);
              if (existing) summary[table].updated++; else summary[table].added++;
            }
            totalSynced++;
          } catch (e: any) {
            console.log(`[cloud-pull] Skipped ${table}/${change.recordId}: ${e.message}`);
          }
        }
      });

      updateSyncState({ last_sync_at: new Date().toISOString() });
      if (totalSynced > 0) {
        broadcastToPlayers({ type: 'contentUpdated', payload: {} });
      }

      res.json({ message: `Pulled ${totalSynced} changes from cloud`, summary, totalSynced });
    } catch (e: any) {
      console.error('[cloud-pull] Error:', e.message);
      res.status(500).json({ message: `Pull failed: ${e.message}` });
    }
  });

  app.post('/api/customer/cloud-sync/push', requireAuth, async (_req: Request, res: Response) => {
    const unpushed = getUnpushedChanges();
    if (unpushed.length === 0) {
      return res.json({ message: 'No local changes to push', totalPushed: 0 });
    }

    const syncState = getSyncState();
    const cloudUrl = syncState?.cloud_url || 'https://digipalsignage.com';
    const hubToken = syncState?.hub_token;

    if (!hubToken) {
      return res.status(400).json({ message: 'No hub token — please log in to register this hub first.' });
    }

    const changes = unpushed.map((change: any) => {
      const row = change.operation !== 'DELETE'
        ? getFullRow(change.table_name, change.record_id)
        : null;
      return {
        id: change.id,
        tableName: change.table_name,
        recordId: change.record_id,
        operation: change.operation,
        data: row || JSON.parse(change.payload || '{}'),
        payload: change.payload || JSON.stringify(row || {}),
      };
    });

    try {
      const pushRes = await fetch(`${cloudUrl}/api/hub/sync/push`, {
        method: 'POST',
        headers: { 'x-hub-token': hubToken, 'Content-Type': 'application/json' },
        body: JSON.stringify({ changes }),
      });

      if (!pushRes.ok) {
        const err = await pushRes.json().catch(() => ({ message: 'Unknown error' }));
        return res.status(pushRes.status).json({ message: err.message || 'Cloud push failed' });
      }

      const result = await pushRes.json();
      const ids = unpushed.map((c: any) => c.id);
      markChangesPushed(ids);
      res.json({ message: `Pushed ${result.applied || changes.length} changes to cloud`, totalPushed: result.applied || changes.length });
    } catch (e: any) {
      console.error('[cloud-push] Error:', e.message);
      res.status(500).json({ message: `Push failed: ${e.message}` });
    }
  });

  app.post('/api/customer/logout', (req: Request, res: Response) => {
    const token = req.session?.token;
    if (token) deleteSession(token);
    res.setHeader('Set-Cookie', 'session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0');
    res.json({ ok: true });
  });

  app.post('/api/customer/logout-and-reset', (_req: Request, res: Response) => {
    try {
      console.log('[logout-reset] Clearing all sessions and sync state...');
      if (cloudSync) {
        cloudSync.stop();
        cloudSync = null;
      }
      updateSyncState({
        hub_token: null,
        hub_name: null,
        subscriber_id: null,
        cloud_url: null,
        cloud_session_cookie: null,
        hub_revoked: 0,
        last_sync_at: null,
        last_cloud_contact_at: null,
        sync_enabled: 1,
      });
      const db = getDb();
      db.prepare('DELETE FROM sessions').run();
      initialSyncStatus = { inProgress: false, step: '', error: '', completedAt: null };
      res.setHeader('Set-Cookie', 'session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0');
      console.log('[logout-reset] Reset complete — ready for fresh login');
      res.json({ ok: true });
    } catch (e: any) {
      console.error('[logout-reset] Error:', e.message);
      res.status(500).json({ message: e.message });
    }
  });

  app.get('/api/customer/me', requireAuth, async (req: Request, res: Response) => {
    const sub = getSessionSubscriber(req.session.subscriberId);
    if (!sub) return res.status(401).json({ message: 'Session expired' });
    const perms = resolvePermissions(sub.id);
    const role = (sub as any).account_role || (sub as any).accountRole || 'viewer';
    const now = new Date().toISOString();
    const currentSyncState = getSyncState();
    const hubRegistered = !!currentSyncState?.hub_token && !currentSyncState?.hub_revoked;

    res.json({
      ...sub,
      accountRole: role,
      consentTosAt: (sub as any).consentTosAt || now,
      consentPrivacyAt: (sub as any).consentPrivacyAt || now,
      hubRegistered,
      syncStatus: hubRegistered ? 'complete' : (initialSyncStatus.inProgress ? 'pending' : 'not_started'),
      workspace: {
        teamId: null,
        teamName: null,
        role: role,
        permissions: perms,
        isOwner: role === 'owner',
      },
    });
  });

  app.patch('/api/customer/me', requireAuth, async (req: Request, res: Response) => {
    try {
      const allowedFields = ['name', 'firstName', 'lastName', 'avatarUrl', 'phone', 'company', 'timezone', 'language', 'promotionalEmails'];
      const safeUpdates: Record<string, unknown> = {};
      for (const key of allowedFields) {
        if (key in req.body) safeUpdates[key] = req.body[key];
      }
      if (Object.keys(safeUpdates).length === 0) {
        return res.status(400).json({ message: 'No valid fields to update' });
      }
      const updated = await storage.updateSubscriber(req.session.subscriberId, safeUpdates);
      const { passwordHash, ...safe } = updated as any;
      res.json(safe);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/customer/accept-terms', requireAuth, async (req: Request, res: Response) => {
    const sub = getSessionSubscriber(req.session.subscriberId);
    if (!sub) return res.status(401).json({ message: 'Session expired' });
    const now = new Date().toISOString();
    const perms = resolvePermissions(sub.id);
    const role = (sub as any).account_role || (sub as any).accountRole || 'viewer';
    res.json({
      ...sub,
      accountRole: role,
      consentTosAt: now,
      consentPrivacyAt: now,
      workspace: {
        teamId: null,
        teamName: null,
        role: role,
        permissions: perms,
        isOwner: role === 'owner',
      },
    });
  });

  app.get('/api/customer/features', requireAuth, (_req: Request, res: Response) => {
    const cloudFeatures = cloudSync?.getCachedFeatures();
    if (cloudFeatures) {
      res.json(cloudFeatures);
      return;
    }
    const db = getDb();
    const licenses = db.prepare('SELECT plan_tier, status FROM licenses').all() as any[];
    const activeLicenses = licenses.filter(l => l.status === 'active' || l.status === 'canceling' || l.status === 'trial');
    if (activeLicenses.length > 0) {
      const features: Record<string, boolean> = {
        playlists: true, schedules: false, analytics: false, designStudio: true,
        smartTriggers: false, aiAnalytics: false, kioskDesigner: false, videoWalls: false,
        doohAds: false, teamManagement: false, broadcasts: false, knowledgeBase: false,
        directory: false, screenCast: false, smartQr: false, splitScreen: false,
        kioskMode: false, remoteControl: false, widgets: false, offlinePlayback: true,
        remoteView: false, wayfinding: false, wayfinding3d: false,
        directoryIdleContent: false, emergencyAlerts: false,
      };
      const TIER_FEATURES: Record<string, string[]> = {
        starter: ['schedules', 'analytics', 'splitScreen', 'kioskMode', 'remoteControl', 'widgets'],
        pro: ['schedules', 'analytics', 'splitScreen', 'kioskMode', 'remoteControl', 'widgets',
               'smartTriggers', 'kioskDesigner', 'videoWalls', 'teamManagement', 'broadcasts',
               'screenCast', 'smartQr', 'remoteView'],
        self_service: ['schedules', 'analytics', 'splitScreen', 'kioskMode', 'remoteControl', 'widgets',
                        'smartTriggers', 'kioskDesigner', 'videoWalls', 'teamManagement', 'broadcasts',
                        'screenCast', 'smartQr', 'remoteView'],
        directory: ['directory', 'directoryIdleContent'],
        wayfinding_2d: ['directory', 'directoryIdleContent', 'wayfinding'],
        wayfinding_3d: ['directory', 'directoryIdleContent', 'wayfinding', 'wayfinding3d'],
        dooh_addon: ['doohAds'],
      };
      for (const lic of activeLicenses) {
        const tier = String(lic.plan_tier || '');
        const tierFeats = TIER_FEATURES[tier];
        if (tierFeats) {
          for (const f of tierFeats) features[f] = true;
        }
        if (tier.startsWith('custom_') || tier === 'professional_setup') {
          Object.keys(features).forEach(k => { features[k] = true; });
          features.aiAnalytics = false;
          features.knowledgeBase = false;
        }
      }
      res.json(features);
      return;
    }
    res.json({
      playlists: true, schedules: false, analytics: false, designStudio: true,
      smartTriggers: false, aiAnalytics: false, kioskDesigner: false, videoWalls: false,
      doohAds: false, teamManagement: false, broadcasts: false, knowledgeBase: false,
      directory: false, screenCast: false, smartQr: false, splitScreen: false,
      kioskMode: false, remoteControl: false, widgets: false, offlinePlayback: true,
      remoteView: false, wayfinding: false, wayfinding3d: false,
      directoryIdleContent: false, emergencyAlerts: false,
    });
  });

  app.get('/api/customer/storage', requireAuth, async (_req: Request, res: Response) => {
    try {
      const { totalBytes } = getMediaDiskUsage();
      const usedMb = Math.round(totalBytes / 1024 / 1024);
      const totalMb = 10240;
      const percentUsed = totalMb > 0 ? Math.round((usedMb / totalMb) * 100) : 0;
      res.json({ usedBytes: totalBytes, totalBytes: totalMb * 1024 * 1024, usedMb, totalMb, percentUsed, licenseCount: 1 });
    } catch { res.json({ usedBytes: 0, totalBytes: 10240 * 1024 * 1024, usedMb: 0, totalMb: 10240, percentUsed: 0, licenseCount: 1 }); }
  });

  app.get('/api/customer/workspaces', requireAuth, async (req: Request, res: Response) => {
    const sub = getSessionSubscriber(req.session.subscriberId);
    const perms = resolvePermissions(req.session.subscriberId);
    const role = (sub as any)?.account_role || (sub as any)?.accountRole || 'viewer';
    res.json({
      workspaces: [{
        teamId: null,
        teamName: 'Personal',
        role: role,
        permissions: perms,
        isOwner: role === 'owner',
      }],
      teamCategories: [],
    });
  });

  app.get('/api/customer/permissions', requireAuth, (req: Request, res: Response) => {
    const perms = resolvePermissions(req.session.subscriberId);
    res.json({ permissions: perms });
  });

  app.post('/api/customer/change-password', requireAuth, async (req: Request, res: Response) => {
    try {
      const db = getDb();
      const { currentPassword, newPassword } = req.body;
      if (!newPassword) return res.status(400).json({ message: 'New password required' });
      const sub = getSessionSubscriber(req.session.subscriberId);
      if (!sub) return res.status(401).json({ message: 'Session expired' });
      if (currentPassword) {
        const check = await authenticateUser((sub as any).email, currentPassword);
        if (!check.success) return res.status(401).json({ message: 'Current password is incorrect' });
      }
      const salt = crypto.randomBytes(16).toString('hex');
      const hash = crypto.pbkdf2Sync(newPassword, salt, 10000, 64, 'sha512').toString('hex');
      const storedHash = `${salt}:${hash}`;
      db.prepare('UPDATE subscribers SET password_hash = ?, must_change_password = 0 WHERE id = ?').run(storedHash, req.session.subscriberId);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  const customerPathMap: Record<string, string> = {
    '/api/customer/screens': '/api/screens',
    '/api/customer/contents': '/api/contents',
    '/api/customer/folders': '/api/content-folders',
    '/api/customer/playlists': '/api/playlists',
    '/api/customer/schedules': '/api/schedules',
    '/api/customer/teams': '/api/teams',
    '/api/customer/notifications': '/api/notifications',
    '/api/customer/screen-groups': '/api/screen-groups',
    '/api/customer/kiosks': '/api/kiosks',
    '/api/customer/smart-triggers': '/api/smart-triggers',
    '/api/customer/broadcasts': '/api/broadcasts',
    '/api/customer/video-walls': '/api/video-walls',
    '/api/customer/approvals': '/api/approvals',
    '/api/customer/widgets': '/api/widgets',
    '/api/customer/qr-codes': '/api/qr-codes',
    '/api/customer/announcements': '/api/announcements',
    '/api/customer/analytics': '/api/analytics',
    '/api/customer/team-members': '/api/team-members',
    '/api/customer/team-roles': '/api/team-roles',
    '/api/customer/team-screens': '/api/team-screens',
    '/api/customer/team-categories': '/api/team-categories',
    '/api/customer/check-name': '/api/check-name',
    '/api/customer/licenses': '/api/licenses',
    '/api/customer/subscription-groups': '/api/subscription-groups',
    '/api/customer/onboarding': '/api/onboarding',
    '/api/customer/nav-order': '/api/nav-order',
    '/api/customer/layout-templates': '/api/layout-templates',
    '/api/customer/template-categories': '/api/template-categories',
    '/api/customer/widget-categories': '/api/widget-categories',
    '/api/customer/playlist-items': '/api/playlist-items',
    '/api/customer/emergency-alerts': '/api/emergency-alerts',
    '/api/customer/design-templates': '/api/design-templates',
    '/api/customer/video-wall-screens': '/api/video-wall-screens',
  };
  app.use((req: Request, _res: Response, next: NextFunction) => {
    for (const [prefix, target] of Object.entries(customerPathMap)) {
      if (req.path === prefix || req.path.startsWith(prefix + '/')) {
        const before = req.url;
        req.url = req.url.replace(prefix, target);
        console.log(`[path-rewrite] ${before} → ${req.url}`);
        break;
      }
    }
    next();
  });

  app.get('/api/qr-codes', requireAuth, async (req: Request, res: Response) => {
    try {
      const codes = await storage.getSmartQrCodes(req.session.subscriberId);
      res.json(codes);
    } catch (e: any) { res.json([]); }
  });

  app.get('/api/announcements', requireAuth, async (_req: Request, res: Response) => {
    res.json([]);
  });

  app.get('/api/analytics/overview', requireAuth, async (req: Request, res: Response) => {
    try {
      const db = getDb();
      const subscriberId = req.session.subscriberId;
      const totalScreens = (db.prepare('SELECT COUNT(*) as count FROM screens WHERE owner_id = ?').get(subscriberId) as any)?.count || 0;
      const onlineScreens = (db.prepare('SELECT COUNT(*) as count FROM screens WHERE owner_id = ? AND is_online = 1').get(subscriberId) as any)?.count || 0;
      const pairedScreens = (db.prepare('SELECT COUNT(*) as count FROM screens WHERE owner_id = ? AND is_paired = 1').get(subscriberId) as any)?.count || 0;

      const screens = rowsToCamel(db.prepare('SELECT * FROM screens WHERE owner_id = ? ORDER BY name').all(subscriberId) as any[]);

      const allEvents = db.prepare(`
        SELECT se.*, s.name as screen_name FROM screen_events se
        LEFT JOIN screens s ON se.screen_id = s.id
        WHERE s.owner_id = ?
        ORDER BY se.created_at DESC LIMIT 50
      `).all(subscriberId) as any[];
      const recentEvents = allEvents.map((e: any) => {
        const camel = rowToCamel(e);
        camel.screenName = e.screen_name;
        return camel;
      });

      let totalUptimeHours = 0;
      for (const s of screens) {
        const analytics = await storage.getScreenAnalytics(s.id);
        totalUptimeHours += analytics.uptimeHours || 0;
        (s as any).uptimeHours = analytics.uptimeHours || 0;
      }

      res.json({
        totalScreens,
        onlineScreens,
        pairedScreens,
        totalUptimeHours: Math.round(totalUptimeHours),
        screens,
        recentEvents,
      });
    } catch (e: any) {
      console.error('[analytics/overview] Error:', e.message);
      res.json({ totalScreens: 0, onlineScreens: 0, pairedScreens: 0, totalUptimeHours: 0, screens: [], recentEvents: [] });
    }
  });

  app.post('/api/uploads/request-url', requireAuth, (req: Request, res: Response) => {
    const { name } = req.body;
    const ext = path.extname(name || '.bin').toLowerCase();
    const allowedExts = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.mp4', '.webm', '.pdf'];
    if (!allowedExts.includes(ext)) {
      return res.status(400).json({ message: `File type ${ext} not allowed` });
    }
    const uniqueName = `${Date.now()}-${crypto.randomBytes(8).toString('hex')}${ext}`;
    const uploadURL = `/api/uploads/direct/${uniqueName}`;
    const objectPath = `/media/${uniqueName}`;
    res.json({ uploadURL, objectPath });
  });

  const ALLOWED_UPLOAD_EXTS = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.mp4', '.webm', '.pdf'];

  app.put('/api/uploads/direct/:fileName', requireAuth, (req: Request, res: Response) => {
    const MAX_UPLOAD_SIZE = 10 * 1024 * 1024;
    const targetName = path.basename(req.params.fileName).replace(/[^a-zA-Z0-9._-]/g, '');
    if (!targetName || targetName.startsWith('.')) {
      return res.status(400).json({ message: 'Invalid file name' });
    }
    const ext = path.extname(targetName).toLowerCase();
    if (!ALLOWED_UPLOAD_EXTS.includes(ext)) {
      return res.status(400).json({ message: `File type ${ext} not allowed` });
    }
    const chunks: Buffer[] = [];
    let totalSize = 0;
    req.on('data', (chunk: Buffer) => {
      totalSize += chunk.length;
      if (totalSize > MAX_UPLOAD_SIZE) {
        res.status(413).json({ message: 'File too large (max 10MB)' });
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (res.headersSent) return;
      try {
        const buffer = Buffer.concat(chunks);
        const dir = getMediaDir();
        const filePath = path.join(dir, 'uploads', targetName);
        fs.writeFileSync(filePath, buffer);
        res.json({ ok: true, fileName: targetName });
      } catch (e: any) {
        res.status(500).json({ message: e.message });
      }
    });
  });

  app.post('/api/uploads/confirm', requireAuth, (_req: Request, res: Response) => {
    res.json({ ok: true });
  });

  app.post('/api/auth/login', async (req: Request, res: Response) => {
    if (hubBlocked) return res.status(503).json({ message: 'Hub is blocked — another Digipal hub was detected on this network. Only one hub is allowed per network.' });
    const { email, password, rememberMe } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email and password required' });

    const result = await authenticateUser(email, password);
    if (!result.success) return res.status(401).json({ message: result.error });

    const subscriber = result.subscriber!;
    const capturedCookie = result.cloudSessionCookie;
    const syncState = getSyncState();
    const cloudUrl = syncState?.cloud_url || 'https://digipalsignage.com';
    const isOwner = subscriber.accountRole === 'owner';
    const hubAlreadyRegistered = !!syncState?.hub_token && !syncState?.hub_revoked;

    if (!isOwner && !hubAlreadyRegistered) {
      return res.status(403).json({ message: 'This local server has not been set up yet. The account owner must log in first to register this hub.' });
    }

    const sessionId = createSession(subscriber.id, !!rememberMe);
    const cookieMaxAge = rememberMe ? 90 * 24 * 60 * 60 : 24 * 60 * 60;
    res.setHeader('Set-Cookie', `session=${sessionId}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${cookieMaxAge}`);

    if (isOwner && !hubAlreadyRegistered) {
      const syncPromise = runInitialCloudSync(subscriber.id, cloudUrl, email, password, capturedCookie);
      const timeoutPromise = new Promise<void>((resolve) => setTimeout(resolve, 15000));
      await Promise.race([syncPromise, timeoutPromise]).catch(e =>
        console.error('[auth/login] Cloud sync error during login-await:', e.message)
      );

      const updatedSyncState = getSyncState();
      const syncSucceeded = !!updatedSyncState?.hub_token && !updatedSyncState?.hub_revoked;
      res.json({
        subscriber: { ...subscriber, syncStatus: syncSucceeded ? 'complete' : 'pending' },
        token: sessionId,
      });
    } else {
      if (isOwner && capturedCookie) {
        updateSyncState({ cloud_session_cookie: capturedCookie });
      }
      if (isOwner && hubAlreadyRegistered) {
        const verifyResult = await verifyHubToken(cloudUrl, syncState!.hub_token);
        if (verifyResult === 'invalid') {
          console.log('[auth/login] Stored hub token is stale — re-registering');
          updateSyncState({ hub_token: null, hub_name: null });
          if (cloudSync) { cloudSync.stop(); cloudSync = null; }
          const syncPromise = runInitialCloudSync(subscriber.id, cloudUrl, email, password, capturedCookie);
          const timeoutPromise = new Promise<void>((resolve) => setTimeout(resolve, 15000));
          await Promise.race([syncPromise, timeoutPromise]).catch(e =>
            console.error('[auth/login] Re-registration error:', e.message)
          );
          const updatedSyncState = getSyncState();
          const syncSucceeded = !!updatedSyncState?.hub_token && !updatedSyncState?.hub_revoked;
          res.json({
            subscriber: { ...subscriber, syncStatus: syncSucceeded ? 'complete' : 'pending' },
            token: sessionId,
          });
          return;
        }
        if (!cloudSync) {
          startCloudSyncIfNeeded();
        }
      }
      res.json({ subscriber: { ...subscriber, syncStatus: 'complete' }, token: sessionId });
    }
  });

  app.post('/api/auth/logout', (req: Request, res: Response) => {
    const token = req.session?.token;
    if (token) deleteSession(token);
    res.setHeader('Set-Cookie', 'session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0');
    res.json({ ok: true });
  });

  app.get('/api/auth/me', requireAuth, async (req: Request, res: Response) => {
    const sub = getSessionSubscriber(req.session.subscriberId);
    if (!sub) return res.status(401).json({ message: 'Session expired' });
    res.json({ subscriber: sub });
  });

  app.get('/api/status', (_req: Request, res: Response) => {
    const syncState = getSyncState();
    const hubRevoked = isHubRevoked();
    const unpushedCount = getUnpushedChangeCount();
    const cloudUrl = syncState?.cloud_url || process.env.CLOUD_URL || 'https://digipalsignage.com';

    const lanAddresses: string[] = [];
    const interfaces = os.networkInterfaces();
    for (const name of Object.keys(interfaces)) {
      for (const iface of interfaces[name] || []) {
        if (iface.family === 'IPv4' && !iface.internal) {
          lanAddresses.push(iface.address);
        }
      }
    }

    res.json({
      status: hubBlocked ? 'blocked' : hubRevoked ? 'revoked' : 'running',
      hubBlocked,
      hubRevoked,
      discoveredHubs: hubBlocked ? discoveredHubs : [],
      connectedPlayers: getConnectedPlayers().size,
      lastSync: syncState?.last_sync_at,
      lastCloudContact: syncState?.last_cloud_contact_at,
      unpushedChanges: unpushedCount,
      hubName: syncState?.hub_name,
      version: require('../../package.json').version,
      mode: 'local',
      isLocalServer: true,
      cloudUrl,
      lanAddresses,
      serverUrl: lanAddresses.length > 0 ? `http://${lanAddresses[0]}:${boundPort}` : `http://localhost:${boundPort}`,
      port: boundPort,
    });
  });

  app.get('/api/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok', timestamp: Date.now() });
  });

  app.post('/api/screens/tv/register', async (req: Request, res: Response) => {
    try {
      const db = getDb();
      const { existingCode } = req.body || {};
      if (existingCode) {
        const existing = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(existingCode) as any;
        if (existing) {
          return res.json(rowToCamel(existing));
        }
      }
      const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
      let code = '';
      for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
      while (db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(code)) {
        code = '';
        for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
      }
      db.prepare('INSERT INTO screens (name, pairing_code, is_paired, is_online) VALUES (?, ?, 0, 0)').run('New TV', code);
      const newScreen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(code) as any;
      res.json(rowToCamel(newScreen));
    } catch (err: any) {
      console.error('[tv-register] Error:', err.message);
      res.status(500).json({ message: err.message });
    }
  });

  app.get('/api/hub/setup-status', async (_req: Request, res: Response) => {
    const syncState = getSyncState();
    const isSetup = !!(syncState?.hub_token && syncState?.cloud_url);
    res.json({ isSetup, hubRevoked: isHubRevoked(), hubName: syncState?.hub_name });
  });

  app.post('/api/hub/scan', async (_req: Request, res: Response) => {
    const hubs = await scanForExistingHubs(5000);
    res.json({ hubs });
  });

  app.get('/api/screens', requireAuth, requirePermission('screens.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const screens = await storage.getScreensByOwner(req.session.subscriberId, undefined, teamId);
      res.json(screens);
    } catch (e: any) { console.error('[route] GET /api/screens error:', e); res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id', requireAuth, requirePermission('screens.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const screen = await storage.getScreen(Number(req.params.id));
      if (!screen) return res.status(404).json({ message: 'Screen not found' });
      res.json(screen);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screens', requireAuth, requirePermission('screens.pair'), async (req: Request, res: Response) => {
    try {
      const screen = await storage.createScreen({ ...req.body, ownerId: req.session.subscriberId });
      res.json(screen);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/screens/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const screen = await storage.updateScreen(Number(req.params.id), req.body);
      res.json(screen);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.put('/api/screens/bulk-layout', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const { screenIds, layout, customLayoutConfig, layoutZones } = req.body;
      if (!screenIds || !Array.isArray(screenIds) || !layout) {
        return res.status(400).json({ message: 'screenIds and layout are required' });
      }
      const results = [];
      for (const screenId of screenIds) {
        const existingScreen = await storage.getScreen(screenId);
        if (!existingScreen) continue;
        const existingZones = (existingScreen.layoutZones as Array<{ zoneId: string; contentId?: number; playlistId?: number }>) || [];
        const existingMap = new Map(existingZones.map((z: any) => [z.zoneId, z]));
        const newZoneList = (layoutZones || []) as Array<{ zoneId: string }>;
        const mergedZones = newZoneList.map((newZone) => {
          const existing = existingMap.get(newZone.zoneId);
          if (existing && (existing.contentId || existing.playlistId)) {
            return { zoneId: newZone.zoneId, contentId: existing.contentId, playlistId: existing.playlistId };
          }
          return { zoneId: newZone.zoneId };
        });
        const upd: Record<string, unknown> = { layout, layoutZones: mergedZones };
        if (customLayoutConfig !== undefined) upd.customLayoutConfig = customLayoutConfig;
        const updated = await storage.updateScreen(screenId, upd);
        results.push(updated);
      }
      res.json({ updated: results.length, screens: results });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.put('/api/screens/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const { ownerId, ...updates } = req.body;
      const screen = await storage.updateScreen(Number(req.params.id), updates);
      res.json(screen);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.put('/api/screens/:id/schedules-timezone', requireAuth, requirePermission('screens.edit'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const { timezone } = req.body;
      if (!timezone || typeof timezone !== 'string') {
        return res.status(400).json({ message: 'timezone is required' });
      }
      const schedules = await storage.getSchedules(Number(req.params.id));
      let updated = 0;
      for (const sched of schedules) {
        if ((sched.timezone || 'UTC') !== timezone) {
          await storage.updateSchedule(sched.id, { timezone });
          updated++;
        }
      }
      res.json({ updated });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/screens/:id', requireAuth, requirePermission('screens.delete'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      await storage.deleteScreen(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id/groups', requireAuth, requirePermission('screens.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const groups = await storage.getGroupsForScreen(Number(req.params.id));
      res.json(groups);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.put('/api/screens/:id/groups', requireAuth, requirePermission('screens.edit'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      await storage.setScreenGroups(Number(req.params.id), req.body.groupIds || []);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id/schedules', requireAuth, requirePermission('schedules.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const schedules = await storage.getSchedules(Number(req.params.id));
      res.json(schedules);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id/events', requireAuth, requirePermission('screens.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const events = await storage.getScreenEvents(Number(req.params.id), Number(req.query.limit) || 50);
      res.json(events);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id/commands', requireAuth, requirePermission('screens.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const commands = await storage.getScreenCommands(Number(req.params.id));
      res.json(commands);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screens/:id/commands', requireAuth, requirePermission('screens.control'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const command = await storage.createScreenCommand({ screenId: Number(req.params.id), ...req.body });
      const screen = await storage.getScreen(Number(req.params.id));
      if (screen?.pairingCode) {
        const players = getConnectedPlayers();
        const ws = players.get(screen.pairingCode);
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'command', payload: command }));
        }
      }
      res.json(command);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screens/bulk/command', requireAuth, requirePermission('screens.control'), async (req: Request, res: Response) => {
    try {
      const { screenIds, command, payload } = req.body;
      if (!Array.isArray(screenIds) || !command) return res.status(400).json({ message: 'screenIds and command required' });
      const results = [];
      for (const screenId of screenIds) {
        const screen = await storage.getScreen(screenId);
        if (!screen) continue;
        const cmd = await storage.createScreenCommand({ screenId, command, payload: payload || null });
        if (screen.pairingCode) {
          const players = getConnectedPlayers();
          const ws = players.get(screen.pairingCode);
          if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'command', payload: cmd }));
          }
        }
        results.push(cmd);
      }
      res.json({ sent: results.length, commands: results });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screens/:id/command', requireAuth, requirePermission('screens.control'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const { command, payload } = req.body;
      if (!command) return res.status(400).json({ message: 'Command required' });
      const cmd = await storage.createScreenCommand({ screenId: Number(req.params.id), command, payload: payload || null });
      const screen = await storage.getScreen(Number(req.params.id));
      if (screen?.pairingCode) {
        const players = getConnectedPlayers();
        const ws = players.get(screen.pairingCode);
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'command', payload: cmd }));
        }
      }
      res.json(cmd);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screens/:id/snapshot/request', requireAuth, requirePermission('screens.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const screen = await storage.getScreen(Number(req.params.id));
      if (!screen) return res.status(404).json({ message: 'Screen not found' });
      const cmd = await storage.createScreenCommand({ screenId: screen.id, command: 'screenshot', payload: null });
      if (screen.pairingCode) {
        const players = getConnectedPlayers();
        const ws = players.get(screen.pairingCode);
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'command', payload: cmd }));
        }
      }
      res.json({ requested: true, commandId: cmd.id });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id/snapshot', requireAuth, requirePermission('screens.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const snapshot = await storage.getLatestSnapshot(Number(req.params.id));
      res.json(snapshot || null);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id/analytics', requireAuth, requirePermission('analytics.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const analytics = await storage.getScreenAnalytics(Number(req.params.id));
      res.json(analytics);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screens/:id/triggers', requireAuth, requirePermission('screens.view'), requireOwnership('screens'), async (req: Request, res: Response) => {
    try {
      const triggers = await storage.getSmartTriggersByScreen(Number(req.params.id));
      res.json(triggers);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/contents', requireAuth, requirePermission('content.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const page = req.query.page ? Number(req.query.page) : undefined;
      const limit = req.query.limit ? Number(req.query.limit) : undefined;
      const contents = await storage.getContentsByOwner(req.session.subscriberId, page && limit ? { page, limit } : undefined, teamId);
      res.json(contents);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/contents/:id', requireAuth, requirePermission('content.view'), requireOwnership('contents'), async (req: Request, res: Response) => {
    try {
      const content = await storage.getContent(Number(req.params.id));
      if (!content) return res.status(404).json({ message: 'Content not found' });
      res.json(content);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/contents', requireAuth, requirePermission('content.create'), parseMultipart('file'), async (req: Request, res: Response) => {
    try {
      const file = req.file;
      let localPath = '';
      let url = '';
      if (file) {
        const saved = saveUploadedFile(file.buffer, file.originalname);
        localPath = saved.filePath;
        url = `/media/${saved.fileName}`;
      }
      const content = await storage.createContent({
        ...req.body,
        ownerId: req.session.subscriberId,
        localPath: localPath || req.body.localPath,
        url: url || req.body.url || req.body.data,
        fileSize: file?.size || req.body.fileSize,
        mimeType: file?.mimetype || req.body.mimeType,
      });
      res.json(content);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/contents/:id', requireAuth, requirePermission('content.edit'), requireOwnership('contents'), async (req: Request, res: Response) => {
    try {
      const content = await storage.updateContent(Number(req.params.id), req.body);
      res.json(content);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/contents/:id', requireAuth, requirePermission('content.delete'), requireOwnership('contents'), async (req: Request, res: Response) => {
    try {
      const content = await storage.getContent(Number(req.params.id));
      if (content?.localPath) deleteLocalFile(path.basename(content.localPath));
      await storage.deleteContent(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/contents/bulk/delete', requireAuth, requirePermission('content.delete'), async (req: Request, res: Response) => {
    try {
      const { ids } = req.body;
      if (!Array.isArray(ids)) return res.status(400).json({ message: 'ids array required' });
      let deleted = 0;
      for (const id of ids) {
        const content = await storage.getContent(Number(id));
        if (!content) continue;
        if (!assertOwnership(content as unknown as OwnableRow, req.session.subscriberId)) continue;
        if (content?.localPath) deleteLocalFile(path.basename(content.localPath));
        await storage.deleteContent(Number(id));
        deleted++;
      }
      res.json({ ok: true, deleted });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/contents/bulk/move', requireAuth, requirePermission('content.edit'), async (req: Request, res: Response) => {
    try {
      const { ids, folderId } = req.body;
      if (!Array.isArray(ids)) return res.status(400).json({ message: 'ids array required' });
      const db = getDb();
      let moved = 0;
      for (const id of ids) {
        const content = await storage.getContent(Number(id));
        if (!content) continue;
        if (!assertOwnership(content as unknown as OwnableRow, req.session.subscriberId)) continue;
        db.prepare('UPDATE contents SET folder_id = ? WHERE id = ?').run(folderId ?? null, Number(id));
        moved++;
      }
      res.json({ ok: true, moved });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/contents/:id/move', requireAuth, requirePermission('content.edit'), requireOwnership('contents'), async (req: Request, res: Response) => {
    try {
      const db = getDb();
      db.prepare('UPDATE contents SET folder_id = ? WHERE id = ?').run(req.body.folderId ?? null, Number(req.params.id));
      const content = await storage.getContent(Number(req.params.id));
      res.json(content);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/check-name', requireAuth, async (req: Request, res: Response) => {
    try {
      const { table, name, excludeId } = req.query;
      const db = getDb();
      const t = String(table || 'contents');
      const allowedTables = ['contents', 'playlists', 'screens', 'schedules', 'broadcasts', 'kiosks', 'video_walls', 'smart_triggers'];
      if (!allowedTables.includes(t)) return res.status(400).json({ message: 'Invalid table' });
      let query = `SELECT id FROM ${t} WHERE name = ? AND owner_id = ?`;
      const params: any[] = [String(name), req.session.subscriberId];
      if (excludeId) { query += ' AND id != ?'; params.push(Number(excludeId)); }
      const existing = db.prepare(query).get(...params);
      res.json({ exists: !!existing });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/content-folders', requireAuth, requirePermission('content.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const folders = await storage.getContentFoldersByOwner(req.session.subscriberId, teamId);
      res.json(folders);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/content-folders', requireAuth, requirePermission('content.create'), async (req: Request, res: Response) => {
    try {
      const folder = await storage.createContentFolder({ ...req.body, ownerId: req.session.subscriberId });
      res.json(folder);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/content-folders/:id', requireAuth, requirePermission('content.edit'), requireOwnership('content_folders'), async (req: Request, res: Response) => {
    try {
      const folder = await storage.updateContentFolder(Number(req.params.id), req.body);
      res.json(folder);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/content-folders/:id', requireAuth, requirePermission('content.delete'), requireOwnership('content_folders'), async (req: Request, res: Response) => {
    try {
      await storage.deleteContentFolder(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/upload', requireAuth, requirePermission('content.create'), parseMultipart('file'), async (req: Request, res: Response) => {
    try {
      const file = req.file;
      if (!file) return res.status(400).json({ message: 'No file uploaded' });
      const saved = saveUploadedFile(file.buffer, file.originalname);
      res.json({ url: `/media/${saved.fileName}`, localPath: saved.filePath, fileName: saved.fileName, size: file.size, mimeType: file.mimetype });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/storage/usage', requireAuth, requirePermission('content.view'), async (_req: Request, res: Response) => {
    try {
      const usage = getMediaDiskUsage();
      res.json(usage);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/playlists', requireAuth, requirePermission('playlists.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const playlists = await storage.getPlaylistsByOwner(req.session.subscriberId, teamId);
      res.json(playlists);
    } catch (e: any) { console.error('[route] GET /api/playlists error:', e); res.status(500).json({ message: e.message }); }
  });

  app.get('/api/playlists/:id', requireAuth, requirePermission('playlists.view'), requireOwnership('playlists'), async (req: Request, res: Response) => {
    try {
      const playlist = await storage.getPlaylistWithItems(Number(req.params.id));
      if (!playlist) return res.status(404).json({ message: 'Playlist not found' });
      res.json(playlist);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/playlists', requireAuth, requirePermission('playlists.create'), async (req: Request, res: Response) => {
    try {
      const playlist = await storage.createPlaylist({ ...req.body, ownerId: req.session.subscriberId });
      res.json(playlist);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/playlists/:id', requireAuth, requirePermission('playlists.edit'), requireOwnership('playlists'), async (req: Request, res: Response) => {
    try {
      const playlist = await storage.updatePlaylist(Number(req.params.id), req.body);
      res.json(playlist);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/playlists/:id', requireAuth, requirePermission('playlists.delete'), requireOwnership('playlists'), async (req: Request, res: Response) => {
    try {
      await storage.deletePlaylist(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/playlists/:id/items', requireAuth, requirePermission('playlists.view'), requireOwnership('playlists'), async (req: Request, res: Response) => {
    try {
      const items = await storage.getPlaylistItems(Number(req.params.id));
      res.json(items);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/playlists/:id/items', requireAuth, requirePermission('playlists.edit'), requireOwnership('playlists'), async (req: Request, res: Response) => {
    try {
      const item = await storage.addPlaylistItem({ playlistId: Number(req.params.id), ...req.body });
      res.json(item);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/playlist-items/:id', requireAuth, requirePermission('playlists.edit'), requireOwnership('playlist_items'), async (req: Request, res: Response) => {
    try {
      const item = await storage.updatePlaylistItem(Number(req.params.id), req.body);
      res.json(item);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/playlist-items/:id', requireAuth, requirePermission('playlists.edit'), requireOwnership('playlist_items'), async (req: Request, res: Response) => {
    try {
      await storage.removePlaylistItem(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  const reorderPlaylistHandler = async (req: Request, res: Response) => {
    try {
      await storage.reorderPlaylistItems(Number(req.params.id), req.body.itemIds || []);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  };
  app.post('/api/playlists/:id/reorder', requireAuth, requirePermission('playlists.edit'), requireOwnership('playlists'), reorderPlaylistHandler);
  app.put('/api/playlists/:id/reorder', requireAuth, requirePermission('playlists.edit'), requireOwnership('playlists'), reorderPlaylistHandler);

  app.get('/api/schedules', requireAuth, requirePermission('schedules.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const screenId = req.query.screenId ? Number(req.query.screenId) : null;
      if (screenId) {
        const schedules = await storage.getSchedules(screenId);
        res.json(schedules);
      } else {
        const db = getDb();
        const subscriberId = req.session.subscriberId;
        const teamIds = (db.prepare('SELECT team_id FROM team_members WHERE subscriber_id = ?').all(subscriberId) as any[]).map(r => r.team_id);
        let rows: any[];
        if (teamIds.length > 0) {
          const placeholders = teamIds.map(() => '?').join(',');
          rows = db.prepare(`SELECT s.* FROM schedules s JOIN screens sc ON s.screen_id = sc.id WHERE sc.owner_id = ? OR sc.team_id IN (${placeholders}) ORDER BY s.created_at DESC`).all(subscriberId, ...teamIds);
        } else {
          rows = db.prepare(`SELECT s.* FROM schedules s JOIN screens sc ON s.screen_id = sc.id WHERE sc.owner_id = ? ORDER BY s.created_at DESC`).all(subscriberId);
        }
        const normalized = rows.map((row: any) => {
          const out: any = {};
          for (const [k, v] of Object.entries(row)) {
            const camel = k.replace(/_([a-z])/g, (_: string, c: string) => c.toUpperCase());
            if ((k === 'days_of_week' || k === 'tags') && typeof v === 'string') {
              try { out[camel] = JSON.parse(v); } catch { out[camel] = []; }
            } else if ((k === 'days_of_week' || k === 'tags') && v === null) {
              out[camel] = [];
            } else {
              out[camel] = v;
            }
          }
          return out;
        });
        res.json(normalized);
      }
    } catch (e: any) { console.error('[route] GET /api/schedules error:', e); res.status(500).json({ message: e.message }); }
  });

  app.get('/api/schedules/:id', requireAuth, requirePermission('schedules.view'), requireOwnership('schedules'), async (req: Request, res: Response) => {
    try {
      const schedule = await storage.getSchedule(Number(req.params.id));
      if (!schedule) return res.status(404).json({ message: 'Schedule not found' });
      res.json(schedule);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/schedules', requireAuth, requirePermission('schedules.create'), validateBodyOwnership('screens', 'screenId'), async (req: Request, res: Response) => {
    try {
      const schedule = await storage.createSchedule(req.body);
      res.json(schedule);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/schedules/:id', requireAuth, requirePermission('schedules.edit'), requireOwnership('schedules'), async (req: Request, res: Response) => {
    try {
      const schedule = await storage.updateSchedule(Number(req.params.id), req.body);
      res.json(schedule);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/schedules/:id', requireAuth, requirePermission('schedules.delete'), requireOwnership('schedules'), async (req: Request, res: Response) => {
    try {
      await storage.deleteSchedule(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/schedules/bulk', requireAuth, requirePermission('schedules.create'), async (req: Request, res: Response) => {
    try {
      const { screenIds, name, contentId, playlistId, videoWallId, startTime, endTime, daysOfWeek, priority, enabled, repeatMode, startDate, endDate, timezone, intervalValue, intervalUnit, intervalDuration, useDefault } = req.body;
      if (!Array.isArray(screenIds) || screenIds.length === 0) return res.status(400).json({ message: 'At least one screen is required' });
      if (!name) return res.status(400).json({ message: 'Name is required' });
      if (startTime === undefined || endTime === undefined) return res.status(400).json({ message: 'Start and end time required' });
      if (!contentId && !playlistId && !videoWallId && !useDefault) return res.status(400).json({ message: 'Content, playlist, or video wall required' });
      const created = [];
      for (const screenId of screenIds) {
        const screen = await storage.getScreen(screenId);
        if (!screen) continue;
        const schedule = await storage.createSchedule({
          screenId, name, contentId: contentId || null, playlistId: playlistId || null,
          videoWallId: videoWallId || null, startTime, endTime,
          daysOfWeek: daysOfWeek || [0, 1, 2, 3, 4, 5, 6],
          priority: priority || 0, enabled: enabled !== false,
          repeatMode: repeatMode || 'weekly', startDate: startDate || null,
          endDate: endDate || null, timezone: timezone || 'UTC',
          intervalValue: intervalValue || null, intervalUnit: intervalUnit || null,
          intervalDuration: intervalDuration || null, useDefault: useDefault || false,
          ownerId: req.session.subscriberId,
        });
        created.push(schedule);
      }
      res.json({ created: created.length, schedules: created });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/video-walls', requireAuth, requirePermission('screens.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const walls = await storage.getVideoWallsByOwner(req.session.subscriberId, teamId);
      res.json(walls);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/video-walls/:id', requireAuth, requirePermission('screens.view'), requireOwnership('video_walls'), async (req: Request, res: Response) => {
    try {
      const wall = await storage.getVideoWall(Number(req.params.id));
      if (!wall) return res.status(404).json({ message: 'Video wall not found' });
      const screens = await storage.getVideoWallScreens(wall.id);
      res.json({ ...wall, screens });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/video-walls', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const wall = await storage.createVideoWall({ ...req.body, ownerId: req.session.subscriberId });
      res.json(wall);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/video-walls/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('video_walls'), async (req: Request, res: Response) => {
    try {
      const wall = await storage.updateVideoWall(Number(req.params.id), req.body);
      res.json(wall);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/video-walls/:id', requireAuth, requirePermission('screens.delete'), requireOwnership('video_walls'), async (req: Request, res: Response) => {
    try {
      await storage.deleteVideoWall(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.put('/api/video-walls/:id/screens', requireAuth, requirePermission('screens.edit'), requireOwnership('video_walls'), async (req: Request, res: Response) => {
    try {
      await storage.assignScreensToVideoWall(Number(req.params.id), req.body.assignments || []);
      const screens = await storage.getVideoWallScreens(Number(req.params.id));
      res.json(screens);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/video-walls/:id/resync', requireAuth, requirePermission('screens.edit'), requireOwnership('video_walls'), async (req: Request, res: Response) => {
    try {
      const wallScreens = await storage.getVideoWallScreens(Number(req.params.id));
      let synced = 0;
      for (const ws of wallScreens) {
        if (ws.pairingCode) {
          const players = getConnectedPlayers();
          const conn = players.get(ws.pairingCode);
          if (conn && conn.readyState === WebSocket.OPEN) {
            conn.send(JSON.stringify({ type: 'command', payload: { command: 'wall_resync' } }));
            synced++;
          }
        }
      }
      res.json({ synced });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/video-wall-screens/:id', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const db = getDb();
      const vws = db.prepare('SELECT * FROM video_wall_screens WHERE id = ?').get(Number(req.params.id)) as any;
      if (!vws) return res.status(404).json({ message: 'Video wall screen not found' });
      const { syncLocked } = req.body;
      if (syncLocked === undefined) return res.status(400).json({ message: 'No valid fields to update' });
      db.prepare('UPDATE video_wall_screens SET sync_locked = ? WHERE id = ?').run(syncLocked ? 1 : 0, Number(req.params.id));
      const updated = rowToCamel(db.prepare('SELECT * FROM video_wall_screens WHERE id = ?').get(Number(req.params.id)));
      res.json(updated);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/kiosks', requireAuth, requirePermission('design.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const kiosks = await storage.getKiosksByOwner(req.session.subscriberId, teamId);
      res.json(kiosks);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/kiosks/:id', requireAuth, requirePermission('design.view'), requireOwnership('kiosks'), async (req: Request, res: Response) => {
    try {
      const kiosk = await storage.getKiosk(Number(req.params.id));
      if (!kiosk) return res.status(404).json({ message: 'Kiosk not found' });
      res.json(kiosk);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/kiosks', requireAuth, requirePermission('design.create'), async (req: Request, res: Response) => {
    try {
      const kiosk = await storage.createKiosk({ ...req.body, ownerId: req.session.subscriberId });
      res.json(kiosk);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/kiosks/:id', requireAuth, requirePermission('design.edit'), requireOwnership('kiosks'), async (req: Request, res: Response) => {
    try {
      const kiosk = await storage.updateKiosk(Number(req.params.id), req.body);
      res.json(kiosk);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/kiosks/:id', requireAuth, requirePermission('design.delete'), requireOwnership('kiosks'), async (req: Request, res: Response) => {
    try {
      await storage.deleteKiosk(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/smart-triggers', requireAuth, requirePermission('screens.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const triggers = await storage.getSmartTriggersByOwner(req.session.subscriberId, teamId);
      res.json(triggers);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/smart-triggers/:id', requireAuth, requirePermission('screens.view'), requireOwnership('smart_triggers'), async (req: Request, res: Response) => {
    try {
      const trigger = await storage.getSmartTrigger(Number(req.params.id));
      if (!trigger) return res.status(404).json({ message: 'Trigger not found' });
      res.json(trigger);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/smart-triggers', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const trigger = await storage.createSmartTrigger({ ...req.body, ownerId: req.session.subscriberId });
      res.json(trigger);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/smart-triggers/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('smart_triggers'), async (req: Request, res: Response) => {
    try {
      const trigger = await storage.updateSmartTrigger(Number(req.params.id), req.body);
      res.json(trigger);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/smart-triggers/:id', requireAuth, requirePermission('screens.delete'), requireOwnership('smart_triggers'), async (req: Request, res: Response) => {
    try {
      await storage.deleteSmartTrigger(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/smart-triggers/:id/logs', requireAuth, requirePermission('screens.view'), requireOwnership('smart_triggers'), async (req: Request, res: Response) => {
    try {
      const logs = await storage.getTriggerLogs(Number(req.params.id));
      res.json(logs);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/broadcasts', requireAuth, requirePermission('content.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const broadcasts = await storage.getBroadcastsByOwner(req.session.subscriberId, teamId);
      res.json(broadcasts);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/broadcasts/:id', requireAuth, requirePermission('content.view'), requireOwnership('broadcasts'), async (req: Request, res: Response) => {
    try {
      const broadcast = await storage.getBroadcast(Number(req.params.id));
      if (!broadcast) return res.status(404).json({ message: 'Broadcast not found' });
      res.json(broadcast);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/broadcasts', requireAuth, requirePermission('content.create'), async (req: Request, res: Response) => {
    try {
      const broadcast = await storage.createBroadcast({ ...req.body, ownerId: req.session.subscriberId });
      broadcastToPlayers({ type: 'broadcast', payload: broadcast });
      res.json(broadcast);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/broadcasts/:id', requireAuth, requirePermission('content.edit'), requireOwnership('broadcasts'), async (req: Request, res: Response) => {
    try {
      const broadcast = await storage.updateBroadcast(Number(req.params.id), req.body);
      broadcastToPlayers({ type: 'broadcast_update', payload: broadcast });
      res.json(broadcast);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/broadcasts/:id/stop', requireAuth, requirePermission('content.edit'), requireOwnership('broadcasts'), async (req: Request, res: Response) => {
    try {
      const broadcast = await storage.stopBroadcast(Number(req.params.id));
      broadcastToPlayers({ type: 'broadcast_stop', payload: { id: Number(req.params.id) } });
      res.json(broadcast);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/broadcasts/:id', requireAuth, requirePermission('content.delete'), requireOwnership('broadcasts'), async (req: Request, res: Response) => {
    try {
      await storage.deleteBroadcast(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/notifications', requireAuth, async (req: Request, res: Response) => {
    try {
      const notifications = await storage.getNotifications({ targetType: 'customer', subscriberId: req.session.subscriberId, unreadOnly: req.query.unread === 'true' });
      res.json(notifications);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/notifications/unread-count', requireAuth, async (req: Request, res: Response) => {
    try {
      const count = await storage.getUnreadNotificationCount({ targetType: 'customer', subscriberId: req.session.subscriberId });
      res.json({ count });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/notifications/count', requireAuth, async (req: Request, res: Response) => {
    try {
      const count = await storage.getUnreadNotificationCount({ targetType: 'customer', subscriberId: req.session.subscriberId });
      res.json({ count });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/notifications/:id/read', requireAuth, requireOwnership('notifications'), async (req: Request, res: Response) => {
    try {
      await storage.markNotificationRead(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/notifications/read-all', requireAuth, async (req: Request, res: Response) => {
    try {
      await storage.markAllNotificationsRead({ targetType: 'customer', subscriberId: req.session.subscriberId });
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screen-groups', requireAuth, requirePermission('screens.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const groups = await storage.getScreenGroups(req.session.subscriberId, teamId);
      res.json(groups);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screen-groups', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const group = await storage.createScreenGroup({ ...req.body, ownerId: req.session.subscriberId });
      res.json(group);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/screen-groups/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('screen_groups'), async (req: Request, res: Response) => {
    try {
      const group = await storage.updateScreenGroup(Number(req.params.id), req.body);
      res.json(group);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/screen-groups/:id', requireAuth, requirePermission('screens.delete'), requireOwnership('screen_groups'), async (req: Request, res: Response) => {
    try {
      await storage.deleteScreenGroup(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screen-groups/:id/members', requireAuth, requirePermission('screens.edit'), requireOwnership('screen_groups'), async (req: Request, res: Response) => {
    try {
      await storage.addScreenToGroup(Number(req.params.id), req.body.screenId);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/screen-groups/:groupId/members/:screenId', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const db = getDb();
      const group = db.prepare('SELECT * FROM screen_groups WHERE id = ?').get(Number(req.params.groupId)) as OwnableRow | undefined;
      if (!group) return res.status(404).json({ message: 'Not found' });
      if (!assertOwnership(group, req.session.subscriberId)) return res.status(403).json({ message: 'Access denied' });
      await storage.removeScreenFromGroup(Number(req.params.groupId), Number(req.params.screenId));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/screen-groups/:id/push', requireAuth, requirePermission('screens.edit'), requireOwnership('screen_groups'), async (req: Request, res: Response) => {
    try {
      const { contentId, playlistId } = req.body;
      const isClear = contentId === null && playlistId === null;
      if (!contentId && !playlistId && !isClear) return res.status(400).json({ message: 'contentId or playlistId required' });
      const members = await storage.getScreenGroupMembers(Number(req.params.id));
      if (members.length === 0) return res.status(400).json({ message: 'No screens in this group' });
      let updated = 0;
      for (const member of members) {
        const screen = await storage.getScreen(member.screenId);
        if (!screen) continue;
        await storage.updateScreen(member.screenId, {
          contentId: isClear ? null : (playlistId ? null : (contentId || null)),
          playlistId: isClear ? null : (playlistId || null),
        });
        if (screen.pairingCode) {
          const players = getConnectedPlayers();
          const ws = players.get(screen.pairingCode);
          if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'refresh', payload: { contentId: contentId || null } }));
          }
        }
        updated++;
      }
      res.json({ updated });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/teams', requireAuth, requirePermission('teams.view'), async (req: Request, res: Response) => {
    try {
      const teams = await storage.getTeamsByOwner(req.session.subscriberId);
      const memberOf = await storage.getSubscriberTeams(req.session.subscriberId);
      const memberTeamIds = new Set(memberOf.map((m: any) => m.team.id));
      const ownerTeamIds = new Set(teams.map((t: any) => t.id));
      const allTeams = [...teams];
      for (const m of memberOf) {
        if (!ownerTeamIds.has(m.team.id)) allTeams.push(m.team);
      }
      res.json(allTeams);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/teams/:id', requireAuth, requirePermission('teams.view'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const team = await storage.getTeam(Number(req.params.id));
      if (!team) return res.status(404).json({ message: 'Team not found' });
      res.json(team);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/teams', requireAuth, requirePermission('teams.manage'), async (req: Request, res: Response) => {
    try {
      const team = await storage.createTeam({ ...req.body, ownerId: req.session.subscriberId });
      res.json(team);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/teams/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const team = await storage.updateTeam(Number(req.params.id), req.body);
      res.json(team);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/teams/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      await storage.deleteTeam(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/teams/:id/members', requireAuth, requirePermission('teams.view'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const members = await storage.getTeamMembers(Number(req.params.id));
      res.json(members);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/teams/:id/members', requireAuth, requirePermission('teams.manage'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const member = await storage.addTeamMember({ teamId: Number(req.params.id), ...req.body });
      res.json(member);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/teams/:id/members/invite', requireAuth, requirePermission('teams.manage'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const { email, name, roleId } = req.body;
      if (!email) return res.status(400).json({ message: 'Email is required' });
      let subscriber = await storage.getSubscriberByEmail(email);
      if (!subscriber) {
        return res.status(400).json({ message: 'User not found. On the local server, users must already exist to be invited to a team.' });
      }
      const existingMembers = await storage.getTeamMembers(Number(req.params.id));
      const alreadyMember = existingMembers.find((m: any) => m.subscriberId === subscriber!.id);
      if (alreadyMember) return res.status(400).json({ message: 'User is already a member of this team' });
      const member = await storage.addTeamMember({
        teamId: Number(req.params.id),
        subscriberId: subscriber.id,
        roleId: roleId || null,
        status: 'active',
      });
      res.json(member);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/team-members/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_members'), async (req: Request, res: Response) => {
    try {
      const member = await storage.updateTeamMember(Number(req.params.id), req.body);
      res.json(member);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/team-members/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_members'), async (req: Request, res: Response) => {
    try {
      await storage.removeTeamMember(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/teams/:id/roles', requireAuth, requirePermission('teams.view'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const roles = await storage.getTeamRoles(Number(req.params.id));
      res.json(roles);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/teams/:id/roles', requireAuth, requirePermission('teams.manage'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const role = await storage.createTeamRole({ teamId: Number(req.params.id), ...req.body });
      res.json(role);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/team-roles/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_roles'), async (req: Request, res: Response) => {
    try {
      const role = await storage.updateTeamRole(Number(req.params.id), req.body);
      res.json(role);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/team-roles/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_roles'), async (req: Request, res: Response) => {
    try {
      await storage.deleteTeamRole(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/teams/:id/screens', requireAuth, requirePermission('teams.view'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const screens = await storage.getTeamScreens(Number(req.params.id));
      res.json(screens);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/teams/:id/screens', requireAuth, requirePermission('teams.manage'), requireOwnership('teams'), async (req: Request, res: Response) => {
    try {
      const result = await storage.assignScreenToTeam({ teamId: Number(req.params.id), ...req.body });
      res.json(result);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/team-screens/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_screens'), async (req: Request, res: Response) => {
    try {
      const updated = await storage.updateTeamScreen(Number(req.params.id), req.body);
      res.json(updated);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/team-screens/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_screens'), async (req: Request, res: Response) => {
    try {
      await storage.removeScreenFromTeam(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/team-categories', requireAuth, requirePermission('teams.view'), async (req: Request, res: Response) => {
    try {
      const categories = await storage.getTeamCategoriesByOwner(req.session.subscriberId);
      res.json(categories);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/team-categories', requireAuth, requirePermission('teams.manage'), async (req: Request, res: Response) => {
    try {
      const cat = await storage.createTeamCategory({ ...req.body, ownerId: req.session.subscriberId });
      res.json(cat);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/team-categories/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_categories'), async (req: Request, res: Response) => {
    try {
      const cat = await storage.updateTeamCategory(Number(req.params.id), req.body);
      res.json(cat);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/team-categories/:id', requireAuth, requirePermission('teams.manage'), requireOwnership('team_categories'), async (req: Request, res: Response) => {
    try {
      await storage.deleteTeamCategory(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/dooh/campaigns', requireAuth, requirePermission('content.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const campaigns = await storage.getDoohCampaignsByOwner(req.session.subscriberId, teamId);
      res.json(campaigns);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/dooh/campaigns/:id', requireAuth, requirePermission('content.view'), requireOwnership('dooh_campaigns'), async (req: Request, res: Response) => {
    try {
      const campaign = await storage.getDoohCampaign(Number(req.params.id));
      if (!campaign) return res.status(404).json({ message: 'Campaign not found' });
      res.json(campaign);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/dooh/campaigns', requireAuth, requirePermission('content.create'), async (req: Request, res: Response) => {
    try {
      const campaign = await storage.createDoohCampaign({ ...req.body, ownerId: req.session.subscriberId });
      res.json(campaign);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/dooh/campaigns/:id', requireAuth, requirePermission('content.edit'), requireOwnership('dooh_campaigns'), async (req: Request, res: Response) => {
    try {
      const campaign = await storage.updateDoohCampaign(Number(req.params.id), req.body);
      res.json(campaign);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/dooh/campaigns/:id', requireAuth, requirePermission('content.delete'), requireOwnership('dooh_campaigns'), async (req: Request, res: Response) => {
    try {
      await storage.deleteDoohCampaign(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/dooh/ad-slots', requireAuth, requirePermission('screens.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const screenId = req.query.screenId ? Number(req.query.screenId) : null;
      if (screenId) {
        const slots = await storage.getDoohAdSlotsByScreen(screenId);
        res.json(slots);
      } else {
        const teamId = req.query.teamId ? Number(req.query.teamId) : null;
        const slots = await storage.getAdSlotsByOwner(req.session.subscriberId, teamId);
        res.json(slots);
      }
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/dooh/ad-slots', requireAuth, requirePermission('screens.edit'), validateBodyOwnership('screens', 'screenId'), async (req: Request, res: Response) => {
    try {
      const slot = await storage.createDoohAdSlot(req.body);
      res.json(slot);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/dooh/ad-slots/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('dooh_ad_slots'), async (req: Request, res: Response) => {
    try {
      const slot = await storage.updateDoohAdSlot(Number(req.params.id), req.body);
      res.json(slot);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/dooh/ad-slots/:id', requireAuth, requirePermission('screens.delete'), requireOwnership('dooh_ad_slots'), async (req: Request, res: Response) => {
    try {
      await storage.deleteDoohAdSlot(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/dooh/impressions', requireAuth, requirePermission('analytics.view'), validateBodyOwnership('dooh_campaigns', 'campaignId'), async (req: Request, res: Response) => {
    try {
      const impression = await storage.createDoohImpression(req.body);
      res.json(impression);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/dooh/impressions', requireAuth, requirePermission('analytics.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const campaignId = req.query.campaignId ? Number(req.query.campaignId) : undefined;
      const screenId = req.query.screenId ? Number(req.query.screenId) : undefined;
      const impressions = await storage.getDoohImpressions(campaignId, screenId);
      res.json(impressions);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/dooh/revenue', requireAuth, requirePermission('analytics.view'), async (_req: Request, res: Response) => {
    try {
      const stats = await storage.getDoohRevenueStats();
      res.json(stats);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/dooh/ad-requests', requireAuth, requirePermission('content.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const requests = await storage.getDoohAdRequests(req.session.subscriberId, teamId);
      res.json(requests);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/dooh/ad-requests/:id', requireAuth, requirePermission('content.edit'), requireOwnership('dooh_ad_requests'), async (req: Request, res: Response) => {
    try {
      const request = await storage.updateDoohAdRequest(Number(req.params.id), req.body);
      res.json(request);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/dooh/marketplace/listings', requireAuth, requirePermission('content.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const listings = await storage.getMarketplaceListingsByOwner(req.session.subscriberId, teamId);
      res.json(listings);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/licenses', requireAuth, requirePermission('billing.view'), async (req: Request, res: Response) => {
    try {
      const licenses = await storage.getLicensesBySubscriber(req.session.subscriberId);
      res.json(licenses);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/subscription-groups', requireAuth, requirePermission('billing.view'), async (req: Request, res: Response) => {
    try {
      const groups = await storage.getSubscriptionGroupsBySubscriber(req.session.subscriberId);
      res.json(groups);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/design-templates', requireAuth, requirePermission('design.view'), async (_req: Request, res: Response) => {
    try {
      const templates = await storage.getDesignTemplates();
      res.json(templates);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/design-templates/:id', requireAuth, requirePermission('design.view'), async (req: Request, res: Response) => {
    try {
      const template = await storage.getDesignTemplate(Number(req.params.id));
      if (!template) return res.status(404).json({ message: 'Template not found' });
      res.json(template);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/template-categories', requireAuth, requirePermission('design.view'), async (_req: Request, res: Response) => {
    try {
      const categories = await storage.getTemplateCategories();
      res.json(categories);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/widget-categories', requireAuth, requirePermission('design.view'), async (_req: Request, res: Response) => {
    try {
      const categories = await storage.getWidgetCategories();
      res.json(categories);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/widget-definitions', requireAuth, requirePermission('design.view'), async (_req: Request, res: Response) => {
    try {
      const definitions = await storage.getActiveWidgetDefinitions();
      res.json(definitions);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/onboarding', requireAuth, async (req: Request, res: Response) => {
    try {
      const progress = await storage.getOnboardingProgress({ targetType: 'customer', subscriberId: req.session.subscriberId });
      res.json(progress || { completedSteps: [], dismissed: false });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/onboarding/complete-step', requireAuth, async (req: Request, res: Response) => {
    try {
      const result = await storage.completeOnboardingStep({ targetType: 'customer', subscriberId: req.session.subscriberId, step: req.body.step });
      res.json(result);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/onboarding/dismiss', requireAuth, async (req: Request, res: Response) => {
    try {
      await storage.dismissOnboarding({ targetType: 'customer', subscriberId: req.session.subscriberId });
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/nav-order', requireAuth, (_req: Request, res: Response) => {
    res.json([]);
  });

  app.post('/api/nav-order', requireAuth, (_req: Request, res: Response) => {
    res.json({ ok: true });
  });

  app.get('/api/approvals', requireAuth, (_req: Request, res: Response) => {
    res.json([]);
  });

  app.get('/api/admin/hubs/subscriber/:id', requireAuth, (req: Request, res: Response) => {
    res.json({ id: Number(req.params.id), hubs: [] });
  });

  app.get('/api/layout-templates', requireAuth, requirePermission('design.view'), async (req: Request, res: Response) => {
    try {
      const templates = await storage.getLayoutTemplates(req.session.subscriberId);
      res.json(templates);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/layout-templates', requireAuth, requirePermission('design.create'), async (req: Request, res: Response) => {
    try {
      const template = await storage.createLayoutTemplate({ ...req.body, subscriberId: req.session.subscriberId });
      res.json(template);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/layout-templates/:id', requireAuth, requirePermission('design.delete'), requireOwnership('layout_templates'), async (req: Request, res: Response) => {
    try {
      await storage.deleteLayoutTemplate(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/smart-qr', requireAuth, requirePermission('content.view'), async (req: Request, res: Response) => {
    try {
      const codes = await storage.getSmartQrCodes(req.session.subscriberId);
      res.json(codes);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/smart-qr/:id', requireAuth, requirePermission('content.view'), requireOwnership('smart_qr_codes'), async (req: Request, res: Response) => {
    try {
      const code = await storage.getSmartQrCode(Number(req.params.id));
      if (!code) return res.status(404).json({ message: 'QR code not found' });
      res.json(code);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/smart-qr', requireAuth, requirePermission('content.create'), async (req: Request, res: Response) => {
    try {
      const shortCode = crypto.randomBytes(6).toString('base64url');
      const code = await storage.createSmartQrCode({ ...req.body, subscriberId: req.session.subscriberId, shortCode });
      res.json(code);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/smart-qr/:id', requireAuth, requirePermission('content.edit'), requireOwnership('smart_qr_codes'), async (req: Request, res: Response) => {
    try {
      const code = await storage.updateSmartQrCode(Number(req.params.id), req.body);
      res.json(code);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/smart-qr/:id', requireAuth, requirePermission('content.delete'), requireOwnership('smart_qr_codes'), async (req: Request, res: Response) => {
    try {
      await storage.deleteSmartQrCode(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/smart-qr/:id/image', requireAuth, requirePermission('content.view'), requireOwnership('smart_qr_codes'), async (req: Request, res: Response) => {
    try {
      const code = req.resource!;
      const baseUrl = process.env.CLOUD_URL || `http://localhost:${process.env.LOCAL_PORT || 8787}`;
      const url = `${baseUrl}/qr/${(code as any).shortCode || (code as any).short_code}`;
      const format = (req.query.format as string) || 'svg';
      const size = Number(req.query.size) || 256;
      if (format === 'png') {
        const buf = await generateQrBuffer(url, { size });
        res.set('Content-Type', 'image/png');
        res.send(buf);
      } else if (format === 'dataurl') {
        const dataUrl = await generateQrDataUrl(url, { size });
        res.json({ dataUrl });
      } else {
        const svg = await generateQrSvg(url, { size });
        res.set('Content-Type', 'image/svg+xml');
        res.send(svg);
      }
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/approval-logs', requireAuth, requirePermission('content.view'), async (req: Request, res: Response) => {
    try {
      const itemType = req.query.itemType as string;
      const itemId = Number(req.query.itemId);
      if (!itemType || !itemId) return res.status(400).json({ message: 'itemType and itemId required' });
      const logs = await storage.getApprovalLogs(itemType, itemId);
      res.json(logs);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/approval-logs', requireAuth, requirePermission('content.approve'), async (req: Request, res: Response) => {
    try {
      const sub = getSessionSubscriber(req.session.subscriberId);
      const log = await storage.createApprovalLog({ ...req.body, actorId: req.session.subscriberId, actorName: sub?.name });
      res.json(log);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/emergency-alerts/config', requireAuth, requirePermission('screens.view'), async (req: Request, res: Response) => {
    try {
      const config = await storage.getEmergencyAlertConfig(req.session.subscriberId);
      res.json(config || null);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.put('/api/emergency-alerts/config', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const config = await storage.upsertEmergencyAlertConfig({ ...req.body, subscriberId: req.session.subscriberId });
      res.json(config);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/emergency-alerts/config', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      await storage.deleteEmergencyAlertConfig(req.session.subscriberId);
      res.json({ success: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/emergency-alerts/custom-feeds', requireAuth, requirePermission('screens.view'), async (req: Request, res: Response) => {
    try {
      const feeds = await storage.getCustomAlertFeeds(req.session.subscriberId);
      res.json(feeds);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/emergency-alerts/custom-feeds', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const feed = await storage.createCustomAlertFeed({ ...req.body, subscriberId: req.session.subscriberId });
      res.json(feed);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.put('/api/emergency-alerts/custom-feeds/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('custom_alert_feeds'), async (req: Request, res: Response) => {
    try {
      const feed = await storage.updateCustomAlertFeed(Number(req.params.id), req.body);
      res.json(feed);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/emergency-alerts/custom-feeds/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('custom_alert_feeds'), async (req: Request, res: Response) => {
    try {
      await storage.deleteCustomAlertFeed(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/custom-alert-feeds', requireAuth, requirePermission('screens.view'), validateTeamAccess, async (req: Request, res: Response) => {
    try {
      const feeds = await storage.getCustomAlertFeeds(req.session.subscriberId);
      res.json(feeds);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/custom-alert-feeds', requireAuth, requirePermission('screens.edit'), async (req: Request, res: Response) => {
    try {
      const feed = await storage.createCustomAlertFeed({ ...req.body, subscriberId: req.session.subscriberId });
      res.json(feed);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/custom-alert-feeds/:id', requireAuth, requirePermission('screens.edit'), requireOwnership('custom_alert_feeds'), async (req: Request, res: Response) => {
    try {
      const feed = await storage.updateCustomAlertFeed(Number(req.params.id), req.body);
      res.json(feed);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/custom-alert-feeds/:id', requireAuth, requirePermission('screens.delete'), requireOwnership('custom_alert_feeds'), async (req: Request, res: Response) => {
    try {
      await storage.deleteCustomAlertFeed(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/directory/venues', requireAuth, requirePermission('content.view'), async (req: Request, res: Response) => {
    try {
      const teamId = req.query.teamId ? Number(req.query.teamId) : null;
      const venues = await storage.getDirectoryVenuesByOwner(req.session.subscriberId, teamId);
      res.json(venues);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/directory/venues/:id', requireAuth, requirePermission('content.view'), requireOwnership('directory_venues'), async (req: Request, res: Response) => {
    try {
      const venue = await storage.getDirectoryVenue(Number(req.params.id));
      if (!venue) return res.status(404).json({ message: 'Venue not found' });
      res.json(venue);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/directory/venues', requireAuth, requirePermission('content.create'), async (req: Request, res: Response) => {
    try {
      const venue = await storage.createDirectoryVenue({ ...req.body, ownerId: req.session.subscriberId });
      res.json(venue);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/directory/venues/:id', requireAuth, requirePermission('content.edit'), requireOwnership('directory_venues'), async (req: Request, res: Response) => {
    try {
      const venue = await storage.updateDirectoryVenue(Number(req.params.id), req.body);
      res.json(venue);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/directory/venues/:id', requireAuth, requirePermission('content.delete'), requireOwnership('directory_venues'), async (req: Request, res: Response) => {
    try {
      await storage.deleteDirectoryVenue(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/directory/venues/:venueId/floors', requireAuth, requirePermission('content.view'), requireParentOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const floors = await storage.getDirectoryFloors(Number(req.params.venueId));
      res.json(floors);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/directory/floors', requireAuth, requirePermission('content.create'), validateBodyOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const floor = await storage.createDirectoryFloor(req.body);
      res.json(floor);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/directory/floors/:id', requireAuth, requirePermission('content.edit'), requireOwnership('directory_floors'), async (req: Request, res: Response) => {
    try {
      const floor = await storage.updateDirectoryFloor(Number(req.params.id), req.body);
      res.json(floor);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/directory/floors/:id', requireAuth, requirePermission('content.delete'), requireOwnership('directory_floors'), async (req: Request, res: Response) => {
    try {
      await storage.deleteDirectoryFloor(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/directory/venues/:venueId/categories', requireAuth, requirePermission('content.view'), requireParentOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const categories = await storage.getDirectoryCategories(Number(req.params.venueId));
      res.json(categories);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/directory/categories', requireAuth, requirePermission('content.create'), validateBodyOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const cat = await storage.createDirectoryCategory(req.body);
      res.json(cat);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/directory/categories/:id', requireAuth, requirePermission('content.edit'), requireOwnership('directory_categories'), async (req: Request, res: Response) => {
    try {
      const cat = await storage.updateDirectoryCategory(Number(req.params.id), req.body);
      res.json(cat);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/directory/categories/:id', requireAuth, requirePermission('content.delete'), requireOwnership('directory_categories'), async (req: Request, res: Response) => {
    try {
      await storage.deleteDirectoryCategory(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/directory/venues/:venueId/stores', requireAuth, requirePermission('content.view'), requireParentOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const q = req.query.q as string;
      if (q) {
        const stores = await storage.searchDirectoryStores(Number(req.params.venueId), q);
        res.json(stores);
      } else {
        const stores = await storage.getDirectoryStores(Number(req.params.venueId));
        res.json(stores);
      }
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/directory/stores', requireAuth, requirePermission('content.create'), validateBodyOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const store = await storage.createDirectoryStore(req.body);
      res.json(store);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/directory/stores/:id', requireAuth, requirePermission('content.edit'), requireOwnership('directory_stores'), async (req: Request, res: Response) => {
    try {
      const store = await storage.updateDirectoryStore(Number(req.params.id), req.body);
      res.json(store);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/directory/stores/:id', requireAuth, requirePermission('content.delete'), requireOwnership('directory_stores'), async (req: Request, res: Response) => {
    try {
      await storage.deleteDirectoryStore(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/directory/venues/:venueId/promotions', requireAuth, requirePermission('content.view'), requireParentOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const active = req.query.active === 'true';
      if (active) {
        const promos = await storage.getActiveDirectoryPromotions(Number(req.params.venueId));
        res.json(promos);
      } else {
        const promos = await storage.getDirectoryPromotions(Number(req.params.venueId));
        res.json(promos);
      }
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.post('/api/directory/promotions', requireAuth, requirePermission('content.create'), validateBodyOwnership('directory_venues', 'venueId'), async (req: Request, res: Response) => {
    try {
      const promo = await storage.createDirectoryPromotion(req.body);
      res.json(promo);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/directory/promotions/:id', requireAuth, requirePermission('content.edit'), requireOwnership('directory_promotions'), async (req: Request, res: Response) => {
    try {
      const promo = await storage.updateDirectoryPromotion(Number(req.params.id), req.body);
      res.json(promo);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.delete('/api/directory/promotions/:id', requireAuth, requirePermission('content.delete'), requireOwnership('directory_promotions'), async (req: Request, res: Response) => {
    try {
      await storage.deleteDirectoryPromotion(Number(req.params.id));
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/pricing-cards', async (_req: Request, res: Response) => {
    try {
      const cards = await storage.getPricingCards(true);
      res.json(cards);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/events/recent', requireAuth, requirePermission('analytics.view'), async (req: Request, res: Response) => {
    try {
      const events = await storage.getRecentEvents(Number(req.query.limit) || 50);
      res.json(events);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/health-summary', requireAuth, requirePermission('analytics.view'), async (_req: Request, res: Response) => {
    try {
      const summary = await storage.getHealthSummary();
      res.json(summary);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/screen/:pairingCode/playlist', (req: Request, res: Response) => {
    const { allowed, reason } = isScreenAllowedToPlay(String(req.params.pairingCode));
    if (!allowed) {
      return res.json({
        playlist: null,
        enforcement: {
          blocked: true,
          reason,
          message: reason === 'cloud_disconnected'
            ? 'Subscription verification required — connect to internet'
            : reason === 'hub_revoked'
              ? 'Local server has been deactivated by administrator'
              : reason === 'license_expired'
                ? 'License expired — contact your administrator'
                : 'Screen not found',
        },
      });
    }

    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const now = new Date();
    const dayOfWeek = now.getDay();
    const minuteOfDay = now.getHours() * 60 + now.getMinutes();

    const schedules = db.prepare('SELECT * FROM schedules WHERE screen_id = ? AND enabled = 1 ORDER BY priority DESC').all(screen.id) as any[];
    let activeSchedule: any = null;
    for (const sched of schedules) {
      let days: number[] = [];
      try { days = JSON.parse(sched.days_of_week); } catch { days = []; }
      if (days.includes(dayOfWeek) && minuteOfDay >= sched.start_time && minuteOfDay <= sched.end_time) {
        activeSchedule = sched;
        break;
      }
    }

    if (!activeSchedule) {
      if (screen.playlist_id) {
        const playlist = rowToCamel(db.prepare('SELECT * FROM playlists WHERE id = ?').get(screen.playlist_id));
        return res.json({ playlist, schedule: null });
      }
      if (screen.content_id) {
        const content = rowToCamel(db.prepare('SELECT * FROM contents WHERE id = ?').get(screen.content_id));
        return res.json({ content, schedule: null });
      }
      return res.json({ playlist: null });
    }

    const playlist = activeSchedule.playlist_id ? rowToCamel(db.prepare('SELECT * FROM playlists WHERE id = ?').get(activeSchedule.playlist_id)) : null;
    const content = activeSchedule.content_id ? rowToCamel(db.prepare('SELECT * FROM contents WHERE id = ?').get(activeSchedule.content_id)) : null;
    res.json({ playlist, content, schedule: rowToCamel(activeSchedule) });
  });

  app.get('/api/screen/:pairingCode/content', (req: Request, res: Response) => {
    const { allowed, reason } = isScreenAllowedToPlay(String(req.params.pairingCode));
    if (!allowed) {
      return res.json({ content: null, enforcement: { blocked: true, reason } });
    }

    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    if (screen.content_id) {
      const content = rowToCamel(db.prepare('SELECT * FROM contents WHERE id = ?').get(screen.content_id));
      return res.json({ content });
    }

    if (screen.playlist_id) {
      const playlist = rowToCamel(db.prepare('SELECT * FROM playlists WHERE id = ?').get(screen.playlist_id));
      const items = rowsToCamel(db.prepare('SELECT pi.*, c.* FROM playlist_items pi JOIN contents c ON pi.content_id = c.id WHERE pi.playlist_id = ? ORDER BY pi.sort_order').all(screen.playlist_id) as any[]);
      return res.json({ playlist, items });
    }

    res.json({ content: null });
  });

  app.get('/api/screen/:pairingCode/broadcasts', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const broadcasts = db.prepare("SELECT * FROM broadcasts WHERE status = 'active'").all() as any[];
    const applicable = broadcasts.filter((b: any) => {
      if (!b.target_screen_ids) return true;
      try {
        const ids = JSON.parse(b.target_screen_ids);
        return !Array.isArray(ids) || ids.length === 0 || ids.includes(screen.id);
      } catch { return true; }
    });

    res.json(rowsToCamel(applicable));
  });

  app.get('/api/screen/:pairingCode/commands', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const commands = rowsToCamel(db.prepare("SELECT * FROM screen_commands WHERE screen_id = ? AND status = 'pending' ORDER BY issued_at ASC").all(screen.id) as any[]);
    res.json(commands);
  });

  app.post('/api/screen/:pairingCode/commands/:commandId/ack', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(String(req.params.pairingCode)) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });
    const result = db.prepare("UPDATE screen_commands SET status = 'acknowledged', acknowledged_at = datetime('now') WHERE id = ? AND screen_id = ?").run(req.params.commandId, screen.id);
    if (result.changes === 0) return res.status(404).json({ message: 'Command not found for this screen' });
    res.json({ ok: true });
  });

  app.post('/api/screen/:pairingCode/heartbeat', (req: Request, res: Response) => {
    const db = getDb();
    const { platform, model, osVersion, appVersion, resolution } = req.body;
    db.prepare(`
      UPDATE screens SET
        platform = COALESCE(?, platform),
        model = COALESCE(?, model),
        os_version = COALESCE(?, os_version),
        app_version = COALESCE(?, app_version),
        resolution = COALESCE(?, resolution),
        is_online = 1,
        last_seen_at = datetime('now'),
        last_heartbeat = datetime('now'),
        last_ping_at = datetime('now')
      WHERE pairing_code = ?
    `).run(platform, model, osVersion, appVersion, resolution, req.params.pairingCode);
    res.json({ status: 'ok' });
  });

  app.post('/api/screen/:pairingCode/snapshot', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const { imageData } = req.body;
    if (!imageData) return res.status(400).json({ message: 'imageData required' });

    db.prepare('INSERT INTO screen_snapshots (screen_id, image_data) VALUES (?, ?)').run(screen.id, imageData);
    db.prepare(`
      DELETE FROM screen_snapshots WHERE screen_id = ? AND id NOT IN (
        SELECT id FROM screen_snapshots WHERE screen_id = ? ORDER BY created_at DESC LIMIT 3
      )
    `).run(screen.id, screen.id);

    res.json({ ok: true });
  });

  app.post('/api/screen/:pairingCode/telemetry', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const { avgLoadTimeMs, maxLoadTimeMs, errorCount, errorDetails, connectionDrops, bytesLoaded, contentLoads } = req.body;
    db.prepare(`
      INSERT INTO screen_telemetry (screen_id, avg_load_time_ms, max_load_time_ms, error_count, error_details, connection_drops, bytes_loaded, content_loads)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(screen.id, avgLoadTimeMs, maxLoadTimeMs, errorCount || 0, errorDetails ? JSON.stringify(errorDetails) : null, connectionDrops || 0, bytesLoaded || 0, contentLoads || 0);

    res.json({ ok: true });
  });

  app.post('/api/screen/:pairingCode/event', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const { type, data } = req.body;
    db.prepare('INSERT INTO screen_events (screen_id, type, data) VALUES (?, ?, ?)').run(screen.id, type, data ? JSON.stringify(data) : null);
    res.json({ ok: true });
  });

  app.get('/api/screen/:pairingCode/kiosk', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });
    if (!screen.kiosk_mode) return res.json({ kiosk: null });

    const kiosks = db.prepare('SELECT * FROM kiosks WHERE owner_id = ? OR team_id IN (SELECT team_id FROM team_screens WHERE screen_id = ?)').all(screen.owner_id, screen.id);
    res.json({ kiosk: kiosks[0] ? rowToCamel(kiosks[0]) : null, screen: rowToCamel(screen) });
  });

  app.get('/api/screen/:pairingCode/wall', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });
    if (!screen.video_wall_id) return res.json({ wall: null });

    const wall = rowToCamel(db.prepare('SELECT * FROM video_walls WHERE id = ?').get(screen.video_wall_id));
    const wallScreens = rowsToCamel(db.prepare('SELECT * FROM video_wall_screens WHERE wall_id = ?').all(screen.video_wall_id) as any[]);
    res.json({ wall, wallScreens, screen: rowToCamel(screen) });
  });

  app.get('/api/screen/:pairingCode/triggers', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });
    const triggers = rowsToCamel(db.prepare('SELECT * FROM smart_triggers WHERE screen_id = ? AND enabled = 1').all(screen.id) as any[]);
    res.json(triggers);
  });

  app.get('/api/subscriber', requireAuth, async (req: Request, res: Response) => {
    try {
      const sub = await storage.getSubscriber(req.session.subscriberId);
      if (!sub) return res.status(404).json({ message: 'Subscriber not found' });
      const { passwordHash, ...safe } = sub;
      res.json(safe);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.patch('/api/subscriber', requireAuth, async (req: Request, res: Response) => {
    try {
      const allowedFields = ['name', 'firstName', 'lastName', 'avatarUrl', 'phone', 'company', 'timezone', 'language', 'promotionalEmails'];
      const safeUpdates: Record<string, unknown> = {};
      for (const key of allowedFields) {
        if (key in req.body) safeUpdates[key] = req.body[key];
      }
      if (Object.keys(safeUpdates).length === 0) {
        return res.status(400).json({ message: 'No valid fields to update' });
      }
      const sub = await storage.updateSubscriber(req.session.subscriberId, safeUpdates);
      const { passwordHash, ...safe } = sub as any;
      res.json(safe);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('/api/subscriber/teams', requireAuth, async (req: Request, res: Response) => {
    try {
      const teams = await storage.getSubscriberTeams(req.session.subscriberId);
      res.json(teams);
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

  app.get('*', (req: Request, res: Response) => {
    if (req.path.startsWith('/api/')) {
      return res.status(404).json({ message: 'Not found' });
    }
    res.sendFile(path.join(frontendPath, 'index.html'));
  });

  app.use((err: any, req: Request, res: Response, _next: NextFunction) => {
    console.error(`[global-error] ${req.method} ${req.originalUrl}:`, err.message, err.stack);
    insertErrorLog({
      level: 'error',
      source: 'global-error-handler',
      route: req.originalUrl,
      method: req.method,
      statusCode: 500,
      message: err.message || 'Unknown server error',
      stack: err.stack,
      context: { subscriberId: req.session?.subscriberId },
    });
    if (!res.headersSent) {
      res.status(500).json({ message: err.message, route: req.originalUrl, stack: err.stack?.split('\n').slice(0, 5) });
    }
  });

  server = http.createServer(app);

  wss = new WebSocketServer({ server });
  wss.on('connection', (ws, _req) => {
    let playerCode: string | null = null;

    ws.on('message', (raw) => {
      try {
        const data = JSON.parse(raw.toString());

        if (data.type === 'tvIdentify') {
          playerCode = data.payload?.pairingCode;
          if (playerCode) {
            registerPlayer(playerCode, ws);
            const db = getDb();
            const screen = db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(playerCode) as any;
            if (screen) { (ws as any)._screenId = screen.id; }
            ws.send(JSON.stringify({ type: 'tvConnected', payload: { pairingCode: playerCode } }));
          }
        }

        if (data.type === 'commandAck' && data.payload?.commandId) {
          const db = getDb();
          const screenId = (ws as any)._screenId;
          if (screenId) {
            db.prepare("UPDATE screen_commands SET status = 'acknowledged', acknowledged_at = datetime('now') WHERE id = ? AND screen_id = ?").run(data.payload.commandId, screenId);
          }
        }
      } catch (e) {
        console.error('WS parse error', e);
      }
    });

    ws.on('close', () => {
      if (playerCode) {
        unregisterPlayer(playerCode);
        const db = getDb();
        db.prepare("UPDATE screens SET is_online = 0 WHERE pairing_code = ?").run(playerCode);
      }
    });
  });

  return new Promise((resolve, reject) => {
    const onListening = () => {
      const actualPort = (server!.address() as { port: number }).port;
      boundPort = actualPort;
      console.log(`[digipal-local] Server running on port ${actualPort}`);

      const syncState = getSyncState();
      const hubName = syncState?.hub_name;

      startMdns(actualPort, hubName);

      if (syncState?.hub_token && syncState?.cloud_url && !isHubRevoked() && syncState?.sync_enabled !== 0) {
        cloudSync = new CloudSync(syncState.cloud_url, syncState.hub_token);
        cloudSync.start();
      } else if (isHubRevoked()) {
        console.log('[digipal-local] Hub is revoked — cloud sync disabled');
      } else if (syncState?.sync_enabled === 0) {
        console.log('[digipal-local] Cloud sync disabled by user preference');
      }

      autoSyncOnStartup().catch(e => console.error('[auto-sync] Error:', e.message));

      resolve(actualPort);
    };

    let attempts = 0;
    const maxAttempts = 10;

    const tryPort = (p: number) => {
      const onError = (err: NodeJS.ErrnoException) => {
        if (err.code === 'EADDRINUSE') {
          attempts++;
          if (attempts >= maxAttempts) {
            reject(new Error(`All ports ${port}-${port + maxAttempts - 1} are in use. Is another instance already running?`));
            return;
          }
          console.log(`[digipal-local] Port ${p} is in use, trying ${p + 1}...`);
          server!.removeListener('error', onError);
          tryPort(p + 1);
        } else {
          reject(err);
        }
      };

      server!.on('error', onError);
      server!.listen(p, '0.0.0.0', onListening);
    };

    tryPort(port);
  });
}

export async function stopServer(): Promise<void> {
  if (sessionCleanupInterval) { clearInterval(sessionCleanupInterval); sessionCleanupInterval = null; }
  stopMdns();
  cloudSync?.stop();

  const players = getConnectedPlayers();
  players.forEach((ws) => ws.close());
  players.clear();

  if (wss) {
    wss.close();
    wss = null;
  }

  return new Promise((resolve) => {
    if (server) {
      server.close(() => {
        server = null;
        resolve();
      });
    } else {
      resolve();
    }
  });
}

export { getConnectedPlayers, broadcastToPlayers } from './player-bus';
