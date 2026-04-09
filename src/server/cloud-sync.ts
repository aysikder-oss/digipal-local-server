import WebSocket from 'ws';
import {
  getDb,
  updateSyncState,
  getUnpushedChanges,
  markChangesPushed,
  getFullRow,
  upsertRow,
  deleteRow,
  withoutTriggers,
  getSyncState,
  setHubRevoked,
  updateCloudContactTime,
  enforceLocalFreeScreenLimit,
  getUnsentErrorLogs,
  markErrorLogsSent,
  pruneOldErrorLogs,
  insertErrorLog,
} from '../db/sqlite';
import { broadcastToPlayers } from './player-bus';

const STANDARD_SYNC_INTERVAL = 60 * 60 * 1000;
const LAZY_SYNC_INTERVAL = 24 * 60 * 60 * 1000;

export class CloudSync {
  private ws: WebSocket | null = null;
  private cloudUrl: string;
  private hubToken: string;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private standardSyncInterval: NodeJS.Timeout | null = null;
  private lazySyncInterval: NodeJS.Timeout | null = null;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private errorReportInterval: NodeJS.Timeout | null = null;
  private authTimeout: NodeJS.Timeout | null = null;
  private isRunning = false;
  private isAuthenticated = false;
  private pushAckPending = new Set<number>();
  private onAuthFailure?: () => void;

  constructor(cloudUrl: string, hubToken: string, onAuthFailure?: () => void) {
    this.cloudUrl = cloudUrl;
    this.hubToken = hubToken;
    this.onAuthFailure = onAuthFailure;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.connect();
    if (!this.errorReportInterval) {
      this.reportErrorsToCloud();
      this.errorReportInterval = setInterval(() => {
        this.reportErrorsToCloud();
      }, 5 * 60 * 1000);
    }
  }

  stop() {
    this.isRunning = false;
    this.isAuthenticated = false;
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    if (this.standardSyncInterval) clearInterval(this.standardSyncInterval);
    if (this.lazySyncInterval) clearInterval(this.lazySyncInterval);
    if (this.reconnectTimeout) clearTimeout(this.reconnectTimeout);
    if (this.errorReportInterval) clearInterval(this.errorReportInterval);
    if (this.authTimeout) clearTimeout(this.authTimeout);
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  private connect() {
    if (!this.isRunning) return;

    this.isAuthenticated = false;
    const wsUrl = this.cloudUrl.replace(/^http/, 'ws');
    this.ws = new WebSocket(wsUrl);

    this.ws.on('open', () => {
      console.log('[cloud-sync] Connected to cloud');
      this.send({ type: 'hubIdentify', payload: { hubToken: this.hubToken } });

      if (this.authTimeout) clearTimeout(this.authTimeout);
      this.authTimeout = setTimeout(() => {
        if (!this.isAuthenticated && this.isRunning) {
          console.error('[cloud-sync] Auth timeout — no hubConnected received within 15s, treating as auth failure');
          insertErrorLog({
            level: 'error',
            source: 'cloud-sync',
            message: 'Hub authentication timed out — hub token may be invalid',
          });
          this.handleAuthFailure();
        }
      }, 15000);
    });

    this.ws.on('message', (raw) => {
      try {
        const data = JSON.parse(raw.toString());
        this.handleMessage(data);
      } catch (e) {
        console.error('[cloud-sync] Parse error:', e);
      }
    });

    this.ws.on('close', () => {
      console.log('[cloud-sync] Disconnected from cloud');
      this.stopHeartbeat();
      this.stopTieredSync();
      this.scheduleReconnect();
    });

    this.ws.on('error', (err) => {
      console.error('[cloud-sync] Connection error:', err.message);
      insertErrorLog({
        level: 'error',
        source: 'cloud-sync',
        message: `WebSocket connection error: ${err.message}`,
      });
    });
  }

  private handleAuthFailure() {
    console.log('[cloud-sync] Triggering auth failure handler — stopping sync');
    this.stop();
    if (this.onAuthFailure) {
      this.onAuthFailure();
    }
  }

  private handleMessage(data: any) {
    updateCloudContactTime();

    switch (data.type) {
      case 'hubConnected':
        console.log('[cloud-sync] Hub authenticated');
        this.isAuthenticated = true;
        if (this.authTimeout) {
          clearTimeout(this.authTimeout);
          this.authTimeout = null;
        }
        this.startHeartbeat();
        this.startTieredSync();
        this.pullChanges(['realtime', 'standard', 'lazy']);
        break;

      case 'error': {
        const msg = data.payload?.message || 'Unknown error';
        console.error(`[cloud-sync] Server error: ${msg}`);
        insertErrorLog({
          level: 'error',
          source: 'cloud-sync',
          message: `Cloud WebSocket error: ${msg}`,
        });
        if (msg.toLowerCase().includes('invalid hub token') || msg.toLowerCase().includes('unauthorized')) {
          this.handleAuthFailure();
        }
        break;
      }

      case 'hubSyncPullResponse':
        this.applyChanges(data.payload?.changes || []);
        break;

      case 'hubSyncPushAck': {
        const ackedIds = data.payload?.changeIds || [];
        if (ackedIds.length > 0) {
          markChangesPushed(ackedIds);
          console.log(`[cloud-sync] ${ackedIds.length} changes acknowledged by cloud`);
        }
        break;
      }

      case 'forceSyncNow':
        this.pullChanges(['realtime', 'standard', 'lazy']);
        this.pushChanges();
        break;

      case 'hubRevoked':
        console.log('[cloud-sync] Hub token revoked by admin — persisting to database');
        setHubRevoked(true);
        broadcastToPlayers({ type: 'hubRevoked', payload: {} });
        this.stop();
        break;

      case 'wipe_content':
        console.log('[cloud-sync] Received wipe_content — forwarding to all players');
        broadcastToPlayers({ type: 'wipe_content', payload: data.payload || {} });
        break;

      case 'subscriptionExpired':
        console.log('[cloud-sync] Subscription expired — enforcing free screen limit');
        enforceLocalFreeScreenLimit();
        broadcastToPlayers({ type: 'subscriptionExpired', payload: {} });
        break;

      default:
        break;
    }
  }

  private startHeartbeat() {
    this.heartbeatInterval = setInterval(() => {
      const db = getDb();
      const onlineScreens = (db.prepare('SELECT COUNT(*) as count FROM screens WHERE is_online = 1').get() as any)?.count || 0;

      this.send({
        type: 'hubHeartbeat',
        payload: {
          connectedScreenCount: onlineScreens,
          version: require('../../package.json').version,
          connectionMode: 'LOCAL',
        },
      });

      this.pullChanges(['realtime']);
      this.pushChanges();
    }, 5 * 60 * 1000);
  }

  private stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private startTieredSync() {
    this.standardSyncInterval = setInterval(() => {
      console.log('[cloud-sync] Standard tier sync pull');
      this.pullChanges(['standard']);
    }, STANDARD_SYNC_INTERVAL);

    this.lazySyncInterval = setInterval(() => {
      console.log('[cloud-sync] Lazy tier sync pull');
      this.pullChanges(['lazy']);
    }, LAZY_SYNC_INTERVAL);
  }

  private stopTieredSync() {
    if (this.standardSyncInterval) {
      clearInterval(this.standardSyncInterval);
      this.standardSyncInterval = null;
    }
    if (this.lazySyncInterval) {
      clearInterval(this.lazySyncInterval);
      this.lazySyncInterval = null;
    }
  }

  private scheduleReconnect() {
    if (!this.isRunning) return;
    this.reconnectTimeout = setTimeout(() => this.connect(), 10000);
  }

  private pullChanges(tiers: string[]) {
    const syncState = getSyncState();

    let since: string;
    if (tiers.length === 1 && tiers[0] === 'standard') {
      since = syncState?.last_standard_sync_at || new Date(0).toISOString();
    } else if (tiers.length === 1 && tiers[0] === 'lazy') {
      since = syncState?.last_lazy_sync_at || new Date(0).toISOString();
    } else {
      since = syncState?.last_sync_at || new Date(0).toISOString();
    }

    this.send({
      type: 'hubSyncPull',
      payload: { since, tiers },
    });
  }

  private pushChanges() {
    const unpushed = getUnpushedChanges();
    if (unpushed.length === 0) return;

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
      };
    });

    console.log(`[cloud-sync] Pushing ${changes.length} local changes to cloud`);
    this.send({
      type: 'hubSyncPush',
      payload: { changes },
    });
  }

  private applyChanges(changes: any[]) {
    if (changes.length === 0) return;

    console.log(`[cloud-sync] Applying ${changes.length} changes from cloud`);

    let applied = 0;
    let maxTimestamp = '';
    const tierMaxTimestamps: Record<string, string> = {};

    withoutTriggers(() => {
      for (const change of changes) {
        try {
          const tableName = change.tableName;
          const operation = change.operation;
          const data = change.data || change.payload;
          const tier = change.syncTier || 'realtime';
          const changeTs = change.createdAt || change.timestamp || '';

          if (!tableName || !operation) continue;

          if (operation === 'INSERT' || operation === 'UPDATE') {
            if (data && typeof data === 'object') {
              upsertRow(tableName, data);
              applied++;
            }
          } else if (operation === 'DELETE') {
            const recordId = change.recordId || data?.id;
            if (recordId) {
              deleteRow(tableName, recordId);
              applied++;
            }
          }

          if (changeTs && changeTs > maxTimestamp) maxTimestamp = changeTs;
          if (changeTs && (!tierMaxTimestamps[tier] || changeTs > tierMaxTimestamps[tier])) {
            tierMaxTimestamps[tier] = changeTs;
          }
        } catch (err: any) {
          console.error(`[cloud-sync] Failed to apply change to ${change.tableName}:`, err.message);
          insertErrorLog({
            level: 'error',
            source: 'cloud-sync',
            message: `Failed to apply change to ${change.tableName}: ${err.message}`,
            stack: err.stack,
            context: { tableName: change.tableName, operation: change.operation, recordId: change.recordId },
          });
        }
      }
    });

    console.log(`[cloud-sync] Applied ${applied}/${changes.length} changes`);

    if (applied > 0 && maxTimestamp) {
      const updates: Record<string, string> = { last_sync_at: maxTimestamp };
      if (tierMaxTimestamps['standard']) updates.last_standard_sync_at = tierMaxTimestamps['standard'];
      if (tierMaxTimestamps['lazy']) updates.last_lazy_sync_at = tierMaxTimestamps['lazy'];
      updateSyncState(updates);
    }

    const hasScreenLicenseChanges = changes.some(
      (c) => c.tableName === 'screens' && (c.data?.license_status || c.payload?.license_status)
    );
    if (hasScreenLicenseChanges) {
      console.log('[cloud-sync] Screen license status changed — enforcing free screen limit');
      enforceLocalFreeScreenLimit();
    }
  }

  isConnected(): boolean {
    return !!(this.ws && this.ws.readyState === WebSocket.OPEN && this.isAuthenticated);
  }

  sendMessage(message: any): boolean {
    if (!this.isConnected()) return false;
    this.send(message);
    return true;
  }

  private send(message: any) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }

  private async reportErrorsToCloud() {
    try {
      const unsent = getUnsentErrorLogs(100);
      if (unsent.length === 0) return;

      const httpUrl = this.cloudUrl.replace(/^ws/, 'http');
      const res = await fetch(`${httpUrl}/api/hub/report-errors`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Hub-Token': this.hubToken,
        },
        body: JSON.stringify({
          errors: unsent.map((e: any) => ({
            level: e.level,
            source: e.source,
            route: e.route,
            method: e.method,
            statusCode: e.status_code,
            message: e.message,
            stack: e.stack,
            context: e.context ? JSON.parse(e.context) : null,
            timestamp: e.timestamp,
          })),
        }),
      });

      if (res.ok) {
        const ids = unsent.map((e: any) => e.id);
        markErrorLogsSent(ids);
        console.log(`[cloud-sync] Reported ${ids.length} errors to cloud`);
      } else {
        console.log(`[cloud-sync] Error report failed: HTTP ${res.status}`);
      }

      pruneOldErrorLogs(1000);
    } catch (e: any) {
      console.log(`[cloud-sync] Error reporting failed: ${e.message}`);
    }
  }
}
