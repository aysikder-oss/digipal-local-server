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
  reconcileScreenLicenseStatuses,
  getUnsentErrorLogs,
  markErrorLogsSent,
  pruneOldErrorLogs,
  insertErrorLog,
  setIdMapping,
  getCloudId,
  getLocalId,
  removeIdMapping,
  getUnmappedLocalRecords,
  logSyncConflict,
} from '../db/sqlite';
import { broadcastToPlayers } from './player-bus';
import { broadcastToDashboard } from './dashboard-bus';
import { queueContentMediaDownloads, queueDesignTemplateMediaDownloads, scanAndQueueAllCloudContent } from './media-downloader';

const STANDARD_SYNC_INTERVAL = 60 * 60 * 1000;
const LAZY_SYNC_INTERVAL = 24 * 60 * 60 * 1000;

const FK_REMAP_RULES: Record<string, Record<string, string>> = {
  playlist_items: { playlist_id: 'playlists', content_id: 'contents' },
  schedules: { screen_id: 'screens', content_id: 'contents', playlist_id: 'playlists', video_wall_id: 'video_walls' },
  video_wall_screens: { wall_id: 'video_walls', screen_id: 'screens' },
  screen_group_members: { group_id: 'screen_groups', screen_id: 'screens' },
  smart_triggers: { screen_id: 'screens', target_content_id: 'contents', target_playlist_id: 'playlists', fallback_content_id: 'contents' },
  team_screens: { team_id: 'teams', screen_id: 'screens' },
  team_members: { team_id: 'teams' },
  team_roles: { team_id: 'teams' },
  dooh_ad_slots: { screen_id: 'screens' },
  screens: { content_id: 'contents', playlist_id: 'playlists', video_wall_id: 'video_walls' },
};

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
  private consecutiveFailures = 0;
  private pendingNonInserts: any[] | null = null;
  private static readonly BASE_RECONNECT_MS = 10_000;
  private static readonly MAX_RECONNECT_MS = 5 * 60 * 1000;

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
    let didOpen = false;
    const base = this.cloudUrl.replace(/^http/, 'ws').replace(/\/ws\/?$/, '').replace(/\/$/, '');
    const wsUrl = `${base}/ws`;
    this.ws = new WebSocket(wsUrl);

    this.ws.on('open', () => {
      didOpen = true;
      console.log('[cloud-sync] Connected to cloud');
      let identifyIp: string | undefined;
      try {
        const os = require('os');
        const ifaces = os.networkInterfaces();
        for (const name of Object.keys(ifaces)) {
          for (const iface of ifaces[name] || []) {
            if (iface.family === 'IPv4' && !iface.internal) {
              identifyIp = iface.address;
              break;
            }
          }
          if (identifyIp) break;
        }
      } catch {}
      this.send({ type: 'hubIdentify', payload: { hubToken: this.hubToken, ipAddress: identifyIp } });

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
      if (this.authTimeout) {
        clearTimeout(this.authTimeout);
        this.authTimeout = null;
      }
      this.stopHeartbeat();
      this.stopTieredSync();

      if (!didOpen) {
        this.consecutiveFailures++;
        console.log(`[cloud-sync] Connection failed without opening (attempt ${this.consecutiveFailures})`);
        if (this.consecutiveFailures >= 5) {
          console.error('[cloud-sync] 5 consecutive connection failures — treating as auth failure');
          insertErrorLog({
            level: 'error',
            source: 'cloud-sync',
            message: `WebSocket failed to connect ${this.consecutiveFailures} times consecutively`,
          });
          this.handleAuthFailure();
          return;
        }
      }

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
        this.consecutiveFailures = 0;
        if (this.authTimeout) {
          clearTimeout(this.authTimeout);
          this.authTimeout = null;
        }
        this.syncSubscriptionFirst().catch(() => {}).then(() => {
          this.startHeartbeat();
          this.startTieredSync();
          this.pullChanges(['realtime', 'standard', 'lazy']);
        });
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
        const failedIds = data.payload?.failedIds || [];
        const errors = data.payload?.errors || [];
        if (failedIds.length > 0) {
          console.warn(`[cloud-sync] ${failedIds.length} changes failed on cloud — will retry on next sync`);
          for (const err of errors) {
            console.warn(`[cloud-sync]   Failed: ${err.tableName} (changeId=${err.changeId}): ${err.error}`);
            insertErrorLog({
              level: 'warn',
              source: 'cloud-sync',
              message: `Cloud rejected sync change for ${err.tableName}: ${err.error}`,
              context: { changeId: err.changeId, tableName: err.tableName },
            });
          }
        }
        const mappings = data.payload?.idMappings;
        if (Array.isArray(mappings) && mappings.length > 0) {
          for (const m of mappings) {
            if (m.tableName && m.localId && m.cloudId) {
              setIdMapping(m.tableName, m.localId, m.cloudId);
              console.log(`[cloud-sync] ID mapping: ${m.tableName} local=${m.localId} -> cloud=${m.cloudId}`);
            }
          }
        }
        if (this.pendingNonInserts && this.pendingNonInserts.length > 0) {
          const deferred = this.pendingNonInserts;
          this.pendingNonInserts = null;
          console.log(`[cloud-sync] INSERT ACK received, now pushing ${deferred.length} deferred UPDATE/DELETE changes`);
          const mapChange = (change: any) => {
            const row = change.operation !== 'DELETE'
              ? getFullRow(change.table_name, change.record_id)
              : null;
            let effectiveRecordId = change.record_id;
            if (change.operation === 'UPDATE' || change.operation === 'DELETE') {
              const cloudId = getCloudId(change.table_name, change.record_id);
              if (cloudId) effectiveRecordId = cloudId;
            }
            let changeData = row || JSON.parse(change.payload || '{}');
            if (change.operation !== 'DELETE' && changeData) {
              changeData = this.remapForeignKeys(change.table_name, changeData);
            }
            if (effectiveRecordId !== change.record_id && changeData) {
              changeData.id = effectiveRecordId;
            }
            return {
              id: change.id,
              tableName: change.table_name,
              recordId: effectiveRecordId,
              localRecordId: change.record_id,
              operation: change.operation,
              data: changeData,
            };
          };
          const changes = deferred.map(mapChange);
          this.send({
            type: 'hubSyncPush',
            payload: { changes },
          });
        } else {
          this.pendingNonInserts = null;
        }
        break;
      }

      case 'forceSyncNow':
        console.log('[cloud-sync] Received forceSyncNow — performing immediate full sync');
        this.syncSubscriptionFirst().catch(() => {}).then(() => {
          this.pullChanges(['realtime', 'standard', 'lazy']);
          this.pushChanges();
        });
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
        console.log('[cloud-sync] Subscription expired — re-syncing subscription state');
        this.syncSubscriptionFirst().catch(() => {}).then(() => {
          reconcileScreenLicenseStatuses();
          enforceLocalFreeScreenLimit();
          broadcastToPlayers({ type: 'subscriptionExpired', payload: {} });
        });
        break;

      default:
        break;
    }
  }

  private startHeartbeat() {
    this.heartbeatInterval = setInterval(() => {
      const db = getDb();
      const onlineScreens = (db.prepare('SELECT COUNT(*) as count FROM screens WHERE is_online = 1').get() as any)?.count || 0;

      let localIp: string | undefined;
      try {
        const os = require('os');
        const ifaces = os.networkInterfaces();
        for (const name of Object.keys(ifaces)) {
          for (const iface of ifaces[name] || []) {
            if (iface.family === 'IPv4' && !iface.internal) {
              localIp = iface.address;
              break;
            }
          }
          if (localIp) break;
        }
      } catch {}

      this.send({
        type: 'hubHeartbeat',
        payload: {
          connectedScreenCount: onlineScreens,
          version: require('../../package.json').version,
          connectionMode: 'LOCAL',
          ipAddress: localIp,
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
      console.log('[cloud-sync] Standard tier sync pull (with subscription refresh)');
      this.syncSubscriptionFirst().catch(() => {}).then(() => {
        this.pullChanges(['standard']);
        this.reconcileUnmappedRecords();
      });
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

  private reconcileUnmappedRecords() {
    try {
      const unmapped = getUnmappedLocalRecords();
      if (unmapped.length === 0) return;

      const alreadyPendingChanges = getUnpushedChanges();
      const pendingSet = new Set(
        alreadyPendingChanges.map((c: any) => `${c.table_name}:${c.record_id}`)
      );

      let enqueued = 0;
      for (const { tableName, localId } of unmapped) {
        const key = `${tableName}:${localId}`;
        if (pendingSet.has(key)) continue;

        const row = getFullRow(tableName, localId);
        if (!row) continue;

        const db = getDb();
        db.prepare(`
          INSERT INTO sync_change_log (table_name, record_id, operation, payload, pushed)
          VALUES (?, ?, 'INSERT', ?, 0)
        `).run(tableName, localId, JSON.stringify(row));
        enqueued++;
      }

      if (enqueued > 0) {
        console.log(`[cloud-sync] Reconciliation: enqueued ${enqueued} unmapped records for sync (of ${unmapped.length} total unmapped)`);
        this.pushChanges();
      }
    } catch (err: any) {
      console.error('[cloud-sync] Reconciliation failed:', err.message);
    }
  }

  private scheduleReconnect() {
    if (!this.isRunning) return;
    const exponent = Math.max(0, this.consecutiveFailures - 1);
    const delay = Math.min(
      CloudSync.BASE_RECONNECT_MS * Math.pow(2, exponent),
      CloudSync.MAX_RECONNECT_MS,
    );
    console.log(`[cloud-sync] Reconnecting in ${Math.round(delay / 1000)}s (failures: ${this.consecutiveFailures})`);
    this.reconnectTimeout = setTimeout(() => this.connect(), delay);
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

  private remapForeignKeys(tableName: string, data: Record<string, any>): Record<string, any> {
    const rules = FK_REMAP_RULES[tableName];
    if (!rules || !data) return data;
    const remapped = { ...data };
    for (const [fkCol, refTable] of Object.entries(rules)) {
      const localFkId = remapped[fkCol];
      if (localFkId && typeof localFkId === 'number') {
        const cloudFkId = getCloudId(refTable, localFkId);
        if (cloudFkId) {
          remapped[fkCol] = cloudFkId;
        }
      }
    }
    return remapped;
  }

  private unremapForeignKeys(tableName: string, data: Record<string, any>): Record<string, any> {
    const rules = FK_REMAP_RULES[tableName];
    if (!rules || !data) return data;
    const remapped = { ...data };
    for (const [fkCol, refTable] of Object.entries(rules)) {
      const cloudFkId = remapped[fkCol];
      if (cloudFkId && typeof cloudFkId === 'number') {
        const localFkId = getLocalId(refTable, cloudFkId);
        if (localFkId) {
          remapped[fkCol] = localFkId;
        }
      }
    }
    return remapped;
  }

  private pushChanges() {
    const unpushed = getUnpushedChanges();
    if (unpushed.length === 0) return;

    const inserts: any[] = [];
    const nonInserts: any[] = [];

    for (const change of unpushed) {
      if (change.operation === 'INSERT') {
        inserts.push(change);
      } else {
        nonInserts.push(change);
      }
    }

    const mapChange = (change: any) => {
      const row = change.operation !== 'DELETE'
        ? getFullRow(change.table_name, change.record_id)
        : null;

      let effectiveRecordId = change.record_id;
      if (change.operation === 'UPDATE' || change.operation === 'DELETE') {
        const cloudId = getCloudId(change.table_name, change.record_id);
        if (cloudId) {
          effectiveRecordId = cloudId;
        }
      }

      let data = row || JSON.parse(change.payload || '{}');
      if (change.operation !== 'DELETE' && data) {
        data = this.remapForeignKeys(change.table_name, data);
      }
      if (effectiveRecordId !== change.record_id && data) {
        data.id = effectiveRecordId;
      }

      return {
        id: change.id,
        tableName: change.table_name,
        recordId: effectiveRecordId,
        localRecordId: change.record_id,
        operation: change.operation,
        data,
      };
    };

    if (inserts.length > 0) {
      const insertChanges = inserts.map(mapChange);
      console.log(`[cloud-sync] Pushing ${insertChanges.length} INSERT changes first (${nonInserts.length} UPDATE/DELETE deferred until ACK)`);
      this.pendingNonInserts = nonInserts.length > 0 ? nonInserts : null;
      this.send({
        type: 'hubSyncPush',
        payload: { changes: insertChanges },
      });
    } else {
      const changes = nonInserts.map(mapChange);
      console.log(`[cloud-sync] Pushing ${changes.length} UPDATE/DELETE changes to cloud`);
      this.send({
        type: 'hubSyncPush',
        payload: { changes },
      });
    }
  }

  private applyChanges(changes: any[]) {
    if (changes.length === 0) return;

    console.log(`[cloud-sync] Applying ${changes.length} changes from cloud`);

    let applied = 0;
    let maxTimestamp = '';
    const tierMaxTimestamps: Record<string, string> = {};
    try {
      withoutTriggers(() => {
        for (const change of changes) {
          const tableName = change.tableName;
          const operation = change.operation;
          const data = change.data || change.payload;
          const tier = change.syncTier || 'realtime';
          const changeTs = change.createdAt || change.timestamp || '';

          if (!tableName || !operation) continue;

          if (operation === 'INSERT' || operation === 'UPDATE') {
            if (data && typeof data === 'object') {
              const cloudRecordId = data.id || change.recordId;
              if (cloudRecordId) {
                const existingLocalId = getLocalId(tableName, cloudRecordId);
                if (existingLocalId) {
                  data.id = existingLocalId;
                } else if (operation === 'INSERT') {
                  const db = getDb();
                  const existingRow = db.prepare(`SELECT id FROM ${tableName} WHERE id = ?`).get(cloudRecordId);
                  if (existingRow) {
                    const { id: _stripCloudId, ...insertFields } = data;
                    const remappedInsert = this.unremapForeignKeys(tableName, insertFields);
                    const cols = Object.keys(remappedInsert);
                    if (cols.length > 0) {
                      const placeholders = cols.map(() => '?').join(', ');
                      const values = cols.map(k => {
                        const v = remappedInsert[k];
                        if (v === null || v === undefined) return null;
                        if (typeof v === 'boolean') return v ? 1 : 0;
                        if (typeof v === 'object') return JSON.stringify(v);
                        return v;
                      });
                      const sql = `INSERT INTO ${tableName} (${cols.join(', ')}) VALUES (${placeholders})`;
                      const result = db.prepare(sql).run(...values);
                      const newLocalId = Number(result.lastInsertRowid);
                      setIdMapping(tableName, newLocalId, cloudRecordId);
                      console.log(`[cloud-sync] Cloud INSERT collision avoided: ${tableName} cloud=${cloudRecordId} -> local=${newLocalId}`);
                      applied++;
                      if (changeTs && changeTs > maxTimestamp) maxTimestamp = changeTs;
                      if (changeTs && (!tierMaxTimestamps[tier] || changeTs > tierMaxTimestamps[tier])) {
                        tierMaxTimestamps[tier] = changeTs;
                      }
                      continue;
                    }
                  }
                }
              }
              if (operation === 'UPDATE') {
                const effectiveLocalId = data.id || cloudRecordId;
                const localRow = getFullRow(tableName, effectiveLocalId);
                if (localRow) {
                  const localUpdatedAt = localRow.updated_at;
                  const incomingUpdatedAt = data.updated_at || data.updatedAt || changeTs;
                  if (localUpdatedAt && incomingUpdatedAt) {
                    const localTime = new Date(localUpdatedAt).getTime();
                    const incomingTime = new Date(incomingUpdatedAt).getTime();
                    if (!isNaN(localTime) && !isNaN(incomingTime) && localTime > incomingTime) {
                      console.log(`[cloud-sync] Conflict detected: ${tableName}/${effectiveLocalId} local updated_at=${localUpdatedAt} > incoming=${incomingUpdatedAt} — keeping local (newer-wins)`);
                      logSyncConflict({
                        tableName,
                        recordId: effectiveLocalId,
                        operation: 'UPDATE',
                        localVersion: localRow,
                        incomingVersion: data,
                        resolution: 'local_wins_newer',
                        localUpdatedAt,
                        incomingUpdatedAt: String(incomingUpdatedAt),
                      });
                      applied++;
                      if (changeTs && changeTs > maxTimestamp) maxTimestamp = changeTs;
                      if (changeTs && (!tierMaxTimestamps[tier] || changeTs > tierMaxTimestamps[tier])) {
                        tierMaxTimestamps[tier] = changeTs;
                      }
                      continue;
                    }
                  }
                }
              }
              const remappedData = this.unremapForeignKeys(tableName, data);
              upsertRow(tableName, remappedData);
              applied++;
            }
          } else if (operation === 'DELETE') {
            const cloudRecordId = change.recordId || data?.id;
            if (cloudRecordId) {
              const localId = getLocalId(tableName, cloudRecordId);
              const effectiveId = localId || cloudRecordId;
              deleteRow(tableName, effectiveId);
              if (localId) {
                removeIdMapping(tableName, localId);
              }
              applied++;
            }
          }

          if (changeTs && changeTs > maxTimestamp) maxTimestamp = changeTs;
          if (changeTs && (!tierMaxTimestamps[tier] || changeTs > tierMaxTimestamps[tier])) {
            tierMaxTimestamps[tier] = changeTs;
          }
        }

        if (applied > 0 && maxTimestamp) {
          const updates: Record<string, string> = { last_sync_at: maxTimestamp };
          if (tierMaxTimestamps['standard']) updates.last_standard_sync_at = tierMaxTimestamps['standard'];
          if (tierMaxTimestamps['lazy']) updates.last_lazy_sync_at = tierMaxTimestamps['lazy'];
          updateSyncState(updates);
        }
      });
    } catch (err: any) {
      const failedTable = err._syncTable || 'unknown';
      console.error(`[cloud-sync] Batch apply rolled back due to error in ${failedTable}:`, err.message);
      insertErrorLog({
        level: 'error',
        source: 'cloud-sync',
        message: `Batch apply rolled back: failed on ${failedTable}: ${err.message}`,
        stack: err.stack,
        context: { failedTable, totalChanges: changes.length },
      });
      return;
    }

    console.log(`[cloud-sync] Applied ${applied}/${changes.length} changes`);

    if (applied > 0) {
      const changedTables = new Set(changes.map((c) => c.tableName).filter(Boolean));

      const hasLicenseChanges = changedTables.has('licenses') || changedTables.has('subscription_groups');
      const hasScreenChanges = changedTables.has('screens');

      if (hasLicenseChanges || hasScreenChanges) {
        console.log('[cloud-sync] License/screen changes detected — reconciling and enforcing free screen limit');
        reconcileScreenLicenseStatuses();
        enforceLocalFreeScreenLimit();
      }

      if (hasLicenseChanges) {
        broadcastToDashboard({ type: 'licensesChanged', payload: {} });
      }
      if (hasScreenChanges) {
        broadcastToDashboard({ type: 'screensChanged', payload: {} });
      }

      if (changedTables.has('contents') || changedTables.has('design_templates')) {
        let queued = 0;
        const contentChanges = changes.filter(c => c.tableName === 'contents' && c.operation !== 'DELETE');
        for (const change of contentChanges) {
          const recordId = change.data?.id || change.recordId;
          if (recordId) {
            queued += queueContentMediaDownloads(recordId);
          }
        }
        const templateChanges = changes.filter(c => c.tableName === 'design_templates' && c.operation !== 'DELETE');
        for (const change of templateChanges) {
          const recordId = change.data?.id || change.recordId;
          if (recordId) {
            queued += queueDesignTemplateMediaDownloads(recordId);
          }
        }
        if (queued > 0) {
          console.log(`[cloud-sync] Queued ${queued} media downloads for synced content/templates`);
        }
      }
    }
  }

  private cachedFeatures: Record<string, boolean> | null = null;
  private cachedFeaturesAt: number = 0;
  private static readonly FEATURES_CACHE_TTL = 2 * 60 * 60 * 1000;

  getCachedFeatures(): Record<string, boolean> | null {
    if (this.cachedFeatures && (Date.now() - this.cachedFeaturesAt) < CloudSync.FEATURES_CACHE_TTL) {
      return this.cachedFeatures;
    }
    return null;
  }

  async syncSubscriptionFirst(): Promise<void> {
    try {
      const httpUrl = this.cloudUrl.replace(/^ws/, 'http');
      console.log('[cloud-sync] Priority sync: fetching subscription state...');
      const res = await fetch(`${httpUrl}/api/hub/subscription-state`, {
        headers: { 'x-hub-token': this.hubToken, 'Accept': 'application/json' },
      });
      if (!res.ok) {
        const msg = `Subscription state fetch failed: HTTP ${res.status}`;
        console.log(`[cloud-sync] ${msg}`);
        throw new Error(msg);
      }
      const state = await res.json() as {
        subscriber: { id: number; name: string; email: string; plan: string } | null;
        licenses: any[];
        subscriptionGroups: any[];
        features: Record<string, boolean>;
      };

      const db = getDb();
      withoutTriggers(() => {
        if (state.subscriber) {
          const existing = db.prepare('SELECT id FROM subscribers WHERE id = ?').get(state.subscriber.id);
          if (existing) {
            db.prepare('UPDATE subscribers SET plan = ? WHERE id = ?').run(state.subscriber.plan, state.subscriber.id);
          }
        }

        if (state.licenses && Array.isArray(state.licenses)) {
          const cloudLicenseIds = new Set(state.licenses.map((l: any) => l.id).filter((id: any) => id !== undefined));
          for (const lic of state.licenses) {
            if (lic && typeof lic.id !== 'undefined') {
              upsertRow('licenses', lic);
            }
          }
          if (cloudLicenseIds.size > 0) {
            const localLicenses = db.prepare('SELECT id FROM licenses').all() as any[];
            for (const local of localLicenses) {
              if (!cloudLicenseIds.has(local.id)) {
                try { deleteRow('licenses', local.id); } catch {}
              }
            }
          } else {
            db.prepare('DELETE FROM licenses').run();
          }
          console.log(`[cloud-sync] Synced ${state.licenses.length} licenses (authoritative)`);
        }

        if (state.subscriptionGroups && Array.isArray(state.subscriptionGroups)) {
          const cloudGroupIds = new Set(state.subscriptionGroups.map((sg: any) => sg.id).filter((id: any) => id !== undefined));
          for (const sg of state.subscriptionGroups) {
            if (sg && typeof sg.id !== 'undefined') {
              upsertRow('subscription_groups', sg);
            }
          }
          if (cloudGroupIds.size > 0) {
            const localGroups = db.prepare('SELECT id FROM subscription_groups').all() as any[];
            for (const local of localGroups) {
              if (!cloudGroupIds.has(local.id)) {
                try { deleteRow('subscription_groups', local.id); } catch {}
              }
            }
          } else {
            db.prepare('DELETE FROM subscription_groups').run();
          }
          console.log(`[cloud-sync] Synced ${state.subscriptionGroups.length} subscription groups (authoritative)`);
        }
      });

      if (state.features) {
        this.cachedFeatures = state.features;
        this.cachedFeaturesAt = Date.now();
        console.log('[cloud-sync] Features resolved from subscription:', Object.entries(state.features).filter(([, v]) => v).map(([k]) => k).join(', '));
      }

      reconcileScreenLicenseStatuses();
      enforceLocalFreeScreenLimit();
      broadcastToDashboard({ type: 'licensesChanged', payload: {} });
      console.log('[cloud-sync] Priority subscription sync complete');
    } catch (e: any) {
      console.error('[cloud-sync] Subscription sync failed:', e.message);
      insertErrorLog({
        level: 'warn',
        source: 'cloud-sync',
        message: `Subscription priority sync failed: ${e.message}`,
      });
      throw e;
    }
  }

  isConnected(): boolean {
    return !!(this.ws && this.ws.readyState === WebSocket.OPEN && this.isAuthenticated);
  }

  triggerForceSync() {
    if (!this.isConnected()) return;
    this.syncSubscriptionFirst().catch(() => {}).then(() => {
      this.pullChanges(['realtime', 'standard', 'lazy']);
      this.pushChanges();
    });
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
