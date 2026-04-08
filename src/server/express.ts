import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import http from 'http';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';
import { WebSocketServer, WebSocket } from 'ws';
import { getDb, getSyncState, isHubRevoked, isScreenAllowedToPlay, getUnpushedChangeCount } from '../db/sqlite';
import { startMdns, stopMdns, scanForExistingHubs } from './mdns';
import { CloudSync } from './cloud-sync';
import { getConnectedPlayers, registerPlayer, unregisterPlayer, broadcastToPlayers } from './player-bus';
import { SqliteStorage } from '../db/sqlite-storage';
import { authenticateUser, getSessionSubscriber, initSessionTable, createSession, getSession, deleteSession, cleanExpiredSessions } from './auth';
import { saveUploadedFile, getMediaDir, getMediaDiskUsage, deleteLocalFile } from './file-storage';
import { generateQrSvg, generateQrDataUrl, generateQrBuffer } from './qr-generator';

let server: http.Server | null = null;
let wss: WebSocketServer | null = null;
let cloudSync: CloudSync | null = null;
let hubBlocked = false;
let discoveredHubs: Array<{ name: string; host: string; port: number }> = [];
let sessionCleanupInterval: NodeJS.Timeout | null = null;
const storage = new SqliteStorage();

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
  const db = getDb();
  const sub = db.prepare('SELECT account_role FROM subscribers WHERE id = ?').get(subscriberId) as { account_role: string } | undefined;
  if (!sub) return [];
  if (sub.account_role === 'owner') return [...ALL_PERMISSIONS];

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
  const teamId = req.query.teamId ? Number(req.query.teamId) : (req.body?.teamId ? Number(req.body.teamId) : null);
  if (!teamId) return next();
  const db = getDb();
  const membership = db.prepare('SELECT id FROM team_members WHERE team_id = ? AND subscriber_id = ?').get(teamId, req.session.subscriberId) as { id: number } | undefined;
  const isOwner = db.prepare('SELECT id FROM teams WHERE id = ? AND owner_id = ?').get(teamId, req.session.subscriberId) as { id: number } | undefined;
  if (!membership && !isOwner) {
    return res.status(403).json({ message: 'You do not have access to this team' });
  }
  next();
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
    throw new Error(`requireOwnership: unknown table "${table}"`);
  }
  return async (req: Request, res: Response, next: NextFunction) => {
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
  initSessionTable();
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

  app.post('/api/customer/login', async (req: Request, res: Response) => {
    if (hubBlocked) return res.status(503).json({ message: 'Hub is blocked — another Digipal hub was detected on this network. Only one hub is allowed per network.' });
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email and password required' });
    const result = await authenticateUser(email, password);
    if (!result.success) return res.status(401).json({ message: result.error });
    const sessionId = createSession(result.subscriber!.id);
    res.setHeader('Set-Cookie', `session=${sessionId}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${7 * 24 * 60 * 60}`);
    res.json(result.subscriber);
  });

  app.post('/api/customer/logout', (req: Request, res: Response) => {
    const token = req.session?.token;
    if (token) deleteSession(token);
    res.setHeader('Set-Cookie', 'session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0');
    res.json({ ok: true });
  });

  app.get('/api/customer/me', requireAuth, async (req: Request, res: Response) => {
    const sub = getSessionSubscriber(req.session.subscriberId);
    if (!sub) return res.status(401).json({ message: 'Session expired' });
    const perms = resolvePermissions(sub.id);
    const role = (sub as any).account_role || (sub as any).accountRole || 'viewer';
    res.json({
      ...sub,
      accountRole: role,
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
    res.json({
      playlists: true,
      schedules: true,
      analytics: true,
      designStudio: true,
      smartTriggers: true,
      aiAnalytics: false,
      kioskDesigner: true,
      videoWalls: true,
      doohAds: false,
      teamManagement: true,
      broadcasts: true,
      knowledgeBase: false,
      directory: false,
      screenCast: true,
      smartQr: true,
    });
  });

  app.get('/api/customer/storage', requireAuth, async (_req: Request, res: Response) => {
    try {
      const { totalBytes } = getMediaDiskUsage();
      res.json({ usedMB: Math.round(totalBytes / 1024 / 1024), limitMB: 10240, percentage: Math.round(totalBytes / (10240 * 1024 * 1024) * 100) });
    } catch { res.json({ usedMB: 0, limitMB: 10240, percentage: 0 }); }
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
  };
  app.use((req: Request, _res: Response, next: NextFunction) => {
    for (const [prefix, target] of Object.entries(customerPathMap)) {
      if (req.path === prefix || req.path.startsWith(prefix + '/')) {
        req.url = req.url.replace(prefix, target);
        break;
      }
    }
    next();
  });

  app.post('/api/auth/login', async (req: Request, res: Response) => {
    if (hubBlocked) return res.status(503).json({ message: 'Hub is blocked — another Digipal hub was detected on this network. Only one hub is allowed per network.' });
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email and password required' });

    const result = await authenticateUser(email, password);
    if (!result.success) return res.status(401).json({ message: result.error });

    const sessionId = createSession(result.subscriber!.id);

    res.setHeader('Set-Cookie', `session=${sessionId}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${7 * 24 * 60 * 60}`);
    res.json({ subscriber: result.subscriber, token: sessionId });
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
    const cloudUrl = syncState?.cloud_url || process.env.CLOUD_URL || 'https://app.digipal.io';
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
      version: '1.2.2',
      mode: 'local',
      isLocalServer: true,
      cloudUrl,
    });
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
    } catch (e: any) { res.status(500).json({ message: e.message }); }
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
    } catch (e: any) { res.status(500).json({ message: e.message }); }
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

  app.post('/api/playlists/:id/reorder', requireAuth, requirePermission('playlists.edit'), requireOwnership('playlists'), async (req: Request, res: Response) => {
    try {
      await storage.reorderPlaylistItems(Number(req.params.id), req.body.itemIds || []);
      res.json({ ok: true });
    } catch (e: any) { res.status(500).json({ message: e.message }); }
  });

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
        const placeholders = teamIds.length > 0 ? teamIds.map(() => '?').join(',') : 'NULL';
        const schedules = db.prepare(`SELECT s.* FROM schedules s JOIN screens sc ON s.screen_id = sc.id WHERE sc.owner_id = ? OR sc.team_id IN (${placeholders}) ORDER BY s.created_at DESC`).all(subscriberId, ...teamIds);
        res.json(schedules);
      }
    } catch (e: any) { res.status(500).json({ message: e.message }); }
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
        const playlist = db.prepare('SELECT * FROM playlists WHERE id = ?').get(screen.playlist_id);
        return res.json({ playlist, schedule: null });
      }
      if (screen.content_id) {
        const content = db.prepare('SELECT * FROM contents WHERE id = ?').get(screen.content_id);
        return res.json({ content, schedule: null });
      }
      return res.json({ playlist: null });
    }

    const playlist = activeSchedule.playlist_id ? db.prepare('SELECT * FROM playlists WHERE id = ?').get(activeSchedule.playlist_id) : null;
    const content = activeSchedule.content_id ? db.prepare('SELECT * FROM contents WHERE id = ?').get(activeSchedule.content_id) : null;
    res.json({ playlist, content, schedule: activeSchedule });
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
      const content = db.prepare('SELECT * FROM contents WHERE id = ?').get(screen.content_id);
      return res.json({ content });
    }

    if (screen.playlist_id) {
      const playlist = db.prepare('SELECT * FROM playlists WHERE id = ?').get(screen.playlist_id);
      const items = db.prepare('SELECT pi.*, c.* FROM playlist_items pi JOIN contents c ON pi.content_id = c.id WHERE pi.playlist_id = ? ORDER BY pi.sort_order').all(screen.playlist_id);
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

    res.json(applicable);
  });

  app.get('/api/screen/:pairingCode/commands', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const commands = db.prepare("SELECT * FROM screen_commands WHERE screen_id = ? AND status = 'pending' ORDER BY issued_at ASC").all(screen.id);
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
    res.json({ kiosk: kiosks[0] || null, screen });
  });

  app.get('/api/screen/:pairingCode/wall', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });
    if (!screen.video_wall_id) return res.json({ wall: null });

    const wall = db.prepare('SELECT * FROM video_walls WHERE id = ?').get(screen.video_wall_id);
    const wallScreens = db.prepare('SELECT * FROM video_wall_screens WHERE wall_id = ?').all(screen.video_wall_id);
    res.json({ wall, wallScreens, screen });
  });

  app.get('/api/screen/:pairingCode/triggers', (req: Request, res: Response) => {
    const db = getDb();
    const screen = db.prepare('SELECT id FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });
    const triggers = db.prepare('SELECT * FROM smart_triggers WHERE screen_id = ? AND enabled = 1').all(screen.id);
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
      const sub = await storage.updateSubscriber(req.session.subscriberId, req.body);
      res.json(sub);
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
      console.log(`[digipal-local] Server running on port ${actualPort}`);

      const syncState = getSyncState();
      const hubName = syncState?.hub_name;

      startMdns(actualPort, hubName);

      if (syncState?.hub_token && syncState?.cloud_url && !isHubRevoked()) {
        cloudSync = new CloudSync(syncState.cloud_url, syncState.hub_token);
        cloudSync.start();
      } else if (isHubRevoked()) {
        console.log('[digipal-local] Hub is revoked — cloud sync disabled');
      }

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
