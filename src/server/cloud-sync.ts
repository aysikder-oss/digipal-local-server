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
} from '../db/sqlite';

export class CloudSync {
  private ws: WebSocket | null = null;
  private cloudUrl: string;
  private hubToken: string;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private isRunning = false;
  private pushAckPending = new Set<number>();

  constructor(cloudUrl: string, hubToken: string) {
    this.cloudUrl = cloudUrl;
    this.hubToken = hubToken;
  }

  start() {
    this.isRunning = true;
    this.connect();
  }

  stop() {
    this.isRunning = false;
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    if (this.reconnectTimeout) clearTimeout(this.reconnectTimeout);
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  private connect() {
    if (!this.isRunning) return;

    const wsUrl = this.cloudUrl.replace(/^http/, 'ws');
    this.ws = new WebSocket(wsUrl);

    this.ws.on('open', () => {
      console.log('[cloud-sync] Connected to cloud');
      this.send({ type: 'hubIdentify', payload: { hubToken: this.hubToken } });
      this.startHeartbeat();
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
      this.scheduleReconnect();
    });

    this.ws.on('error', (err) => {
      console.error('[cloud-sync] Connection error:', err.message);
    });
  }

  private handleMessage(data: any) {
    switch (data.type) {
      case 'hubConnected':
        console.log('[cloud-sync] Hub authenticated');
        this.pullChanges();
        break;

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
        this.pullChanges();
        this.pushChanges();
        break;

      case 'hubRevoked':
        console.log('[cloud-sync] Hub token revoked by admin');
        this.stop();
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
    }, 5 * 60 * 1000);
  }

  private stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private scheduleReconnect() {
    if (!this.isRunning) return;
    this.reconnectTimeout = setTimeout(() => this.connect(), 10000);
  }

  private pullChanges() {
    const db = getDb();
    const syncState = db.prepare('SELECT last_sync_at FROM sync_state WHERE id = 1').get() as any;
    const since = syncState?.last_sync_at || new Date(0).toISOString();

    this.send({
      type: 'hubSyncPull',
      payload: { since },
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

    withoutTriggers(() => {
      for (const change of changes) {
        try {
          const tableName = change.tableName;
          const operation = change.operation;
          const data = change.data || change.payload;

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
        } catch (err: any) {
          console.error(`[cloud-sync] Failed to apply change:`, err.message);
        }
      }
    });

    console.log(`[cloud-sync] Applied ${applied}/${changes.length} changes`);
    updateSyncState({ last_sync_at: new Date().toISOString() });
  }

  private send(message: any) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }
}
