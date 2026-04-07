import express from 'express';
import cors from 'cors';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { getDb, getSyncState } from '../db/sqlite';
import { startMdns, stopMdns } from './mdns';
import { CloudSync } from './cloud-sync';

let server: http.Server | null = null;
let wss: WebSocketServer | null = null;
let cloudSync: CloudSync | null = null;
const connectedPlayers = new Map<string, WebSocket>();

export async function startServer(port: number): Promise<void> {
  const app = express();
  app.use(cors());
  app.use(express.json());

  app.get('/api/status', (_req, res) => {
    const syncState = getSyncState();
    res.json({
      status: 'running',
      connectedPlayers: connectedPlayers.size,
      lastSync: syncState?.last_sync_at,
      version: require('../../package.json').version,
    });
  });

  app.get('/api/screens', (_req, res) => {
    const db = getDb();
    const screens = db.prepare('SELECT * FROM screens ORDER BY name').all();
    res.json(screens);
  });

  app.get('/api/playlists', (_req, res) => {
    const db = getDb();
    const playlists = db.prepare('SELECT * FROM playlists ORDER BY name').all();
    res.json(playlists);
  });

  app.get('/api/contents', (_req, res) => {
    const db = getDb();
    const contents = db.prepare('SELECT * FROM contents ORDER BY created_at DESC').all();
    res.json(contents);
  });

  app.get('/api/schedules', (_req, res) => {
    const db = getDb();
    const schedules = db.prepare('SELECT * FROM schedules ORDER BY created_at DESC').all();
    res.json(schedules);
  });

  app.get('/api/screen/:pairingCode/playlist', (req, res) => {
    const db = getDb();
    const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(req.params.pairingCode) as any;
    if (!screen) return res.status(404).json({ message: 'Screen not found' });

    const schedule = db.prepare(
      'SELECT * FROM schedules WHERE screen_id = ? AND enabled = 1 ORDER BY start_time'
    ).get(screen.id) as any;

    if (!schedule) return res.json({ playlist: null });

    const playlist = db.prepare('SELECT * FROM playlists WHERE id = ?').get(schedule.playlist_id);
    res.json({ playlist, schedule });
  });

  app.post('/api/screen/:pairingCode/heartbeat', (req, res) => {
    const db = getDb();
    const { platform, model, osVersion, appVersion, resolution } = req.body;
    db.prepare(`
      INSERT INTO screens (pairing_code, platform, model, os_version, app_version, resolution, is_online, last_seen_at)
      VALUES (?, ?, ?, ?, ?, ?, 1, datetime('now'))
      ON CONFLICT(pairing_code) DO UPDATE SET
        platform = excluded.platform,
        model = excluded.model,
        os_version = excluded.os_version,
        app_version = excluded.app_version,
        resolution = excluded.resolution,
        is_online = 1,
        last_seen_at = datetime('now')
    `).run(req.params.pairingCode, platform, model, osVersion, appVersion, resolution);
    res.json({ status: 'ok' });
  });

  server = http.createServer(app);

  wss = new WebSocketServer({ server });
  wss.on('connection', (ws, req) => {
    let playerCode: string | null = null;

    ws.on('message', (raw) => {
      try {
        const data = JSON.parse(raw.toString());

        if (data.type === 'tvIdentify') {
          playerCode = data.payload?.pairingCode;
          if (playerCode) {
            connectedPlayers.set(playerCode, ws);
            ws.send(JSON.stringify({ type: 'tvConnected', payload: { pairingCode: playerCode } }));
          }
        }
      } catch (e) {
        console.error('WS parse error', e);
      }
    });

    ws.on('close', () => {
      if (playerCode) {
        connectedPlayers.delete(playerCode);
      }
    });
  });

  return new Promise((resolve) => {
    server!.listen(port, () => {
      console.log(`[digipal-local] Server running on port ${port}`);
      startMdns(port);

      const syncState = getSyncState();
      if (syncState?.hub_token && syncState?.cloud_url) {
        cloudSync = new CloudSync(syncState.cloud_url, syncState.hub_token);
        cloudSync.start();
      }

      resolve();
    });
  });
}

export async function stopServer(): Promise<void> {
  stopMdns();
  cloudSync?.stop();

  connectedPlayers.forEach((ws) => ws.close());
  connectedPlayers.clear();

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

export function getConnectedPlayers() {
  return connectedPlayers;
}

export function broadcastToPlayers(message: any) {
  const payload = JSON.stringify(message);
  connectedPlayers.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  });
}
