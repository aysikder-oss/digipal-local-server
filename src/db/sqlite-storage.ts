import { getDb, getUnpushedChangeCount } from './sqlite';

type DataRecord = Record<string, unknown>;

interface PaginationParams {
  page?: number;
  limit?: number;
  search?: string;
  folderId?: number | null;
}

interface PaginatedResult<T> {
  data: T[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

interface NotificationQuery {
  targetType?: string;
  subscriberId?: number;
  unreadOnly?: boolean;
  limit?: number;
}

interface OnboardingQuery {
  targetType: string;
  subscriberId?: number;
  step?: string;
}

interface ScreenAnalytics {
  totalEvents: number;
  uptimeHours: number;
  contentChanges: number;
  errors: number;
  lastSeen: string | null;
}

interface DoohRevenueStats {
  totalRevenue: number;
  totalImpressions: number;
  activeCampaigns: number;
  optedInScreens: number;
  revenueByDay: unknown[];
  topScreens: unknown[];
  topCampaigns: unknown[];
}

interface WallAssignment {
  screenId: number;
  row: number;
  col: number;
  syncOffset?: number;
}

export interface ILocalStorage {
  getContentFolders(): Promise<DataRecord[]>;
  getContentFoldersByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  createContentFolder(folder: DataRecord): Promise<DataRecord>;
  updateContentFolder(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteContentFolder(id: number): Promise<void>;

  getContents(params?: PaginationParams): Promise<DataRecord[] | PaginatedResult<DataRecord>>;
  getContent(id: number): Promise<DataRecord | undefined>;
  createContent(content: DataRecord): Promise<DataRecord>;
  updateContent(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteContent(id: number): Promise<void>;
  getContentsByOwner(ownerId: number, params?: PaginationParams, teamId?: number | null): Promise<DataRecord[] | PaginatedResult<DataRecord>>;

  getScreens(params?: PaginationParams): Promise<DataRecord[] | PaginatedResult<DataRecord>>;
  getScreen(id: number): Promise<DataRecord | undefined>;
  getScreenByPairingCode(code: string): Promise<DataRecord | undefined>;
  createScreen(screen: DataRecord): Promise<DataRecord>;
  updateScreen(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteScreen(id: number): Promise<void>;
  getScreensByOwner(ownerId: number, params?: PaginationParams, teamId?: number | null): Promise<DataRecord[] | PaginatedResult<DataRecord>>;
  getAllScreensByOwner(ownerId: number): Promise<DataRecord[]>;

  getPlaylists(): Promise<DataRecord[]>;
  getPlaylist(id: number): Promise<DataRecord | undefined>;
  getPlaylistWithItems(id: number): Promise<DataRecord | undefined>;
  createPlaylist(playlist: DataRecord): Promise<DataRecord>;
  updatePlaylist(id: number, updates: DataRecord): Promise<DataRecord>;
  deletePlaylist(id: number): Promise<void>;
  getPlaylistsByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;

  getPlaylistItems(playlistId: number): Promise<DataRecord[]>;
  addPlaylistItem(item: DataRecord): Promise<DataRecord>;
  updatePlaylistItem(id: number, updates: DataRecord): Promise<DataRecord>;
  removePlaylistItem(id: number): Promise<void>;
  reorderPlaylistItems(playlistId: number, itemIds: number[]): Promise<void>;

  getSchedules(screenId: number): Promise<DataRecord[]>;
  getSchedule(id: number): Promise<DataRecord | undefined>;
  createSchedule(schedule: DataRecord): Promise<DataRecord>;
  updateSchedule(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteSchedule(id: number): Promise<void>;
  getActiveSchedule(screenId: number, dayOfWeek: number, minuteOfDay: number): Promise<DataRecord | undefined>;

  getSubscriber(id: number): Promise<DataRecord | undefined>;
  getSubscriberByEmail(email: string): Promise<DataRecord | undefined>;
  createSubscriber(subscriber: DataRecord): Promise<DataRecord>;
  updateSubscriber(id: number, updates: DataRecord): Promise<DataRecord>;

  getVideoWalls(): Promise<DataRecord[]>;
  getVideoWall(id: number): Promise<DataRecord | undefined>;
  getVideoWallsByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  createVideoWall(wall: DataRecord): Promise<DataRecord>;
  updateVideoWall(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteVideoWall(id: number): Promise<void>;
  getVideoWallScreens(wallId: number): Promise<DataRecord[]>;
  getWallAssignmentForScreen(screenId: number): Promise<DataRecord | undefined>;
  assignScreenToWall(assignment: DataRecord): Promise<DataRecord>;
  assignScreensToVideoWall(wallId: number, assignments: WallAssignment[]): Promise<void>;
  clearWallScreens(wallId: number): Promise<void>;
  removeScreenFromWall(id: number): Promise<void>;
  updateWallScreen(id: number, updates: DataRecord): Promise<DataRecord>;

  getKiosks(): Promise<DataRecord[]>;
  getKiosk(id: number): Promise<DataRecord | undefined>;
  getKiosksByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  createKiosk(kiosk: DataRecord): Promise<DataRecord>;
  updateKiosk(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteKiosk(id: number): Promise<void>;

  getSmartTriggers(): Promise<DataRecord[]>;
  getSmartTriggersByScreen(screenId: number): Promise<DataRecord[]>;
  getSmartTrigger(id: number): Promise<DataRecord | undefined>;
  getSmartTriggersByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  createSmartTrigger(trigger: DataRecord): Promise<DataRecord>;
  updateSmartTrigger(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteSmartTrigger(id: number): Promise<void>;
  createTriggerLog(log: DataRecord): Promise<DataRecord>;
  getTriggerLogs(triggerId: number, limit?: number): Promise<DataRecord[]>;
  getTriggerLogsByScreen(screenId: number, limit?: number): Promise<DataRecord[]>;

  getBroadcasts(): Promise<DataRecord[]>;
  getBroadcastsByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  getActiveBroadcasts(): Promise<DataRecord[]>;
  getActiveBroadcastsForScreen(screenId: number): Promise<DataRecord[]>;
  getBroadcast(id: number): Promise<DataRecord | undefined>;
  createBroadcast(broadcast: DataRecord): Promise<DataRecord>;
  updateBroadcast(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteBroadcast(id: number): Promise<void>;
  stopBroadcast(id: number): Promise<DataRecord>;

  getScreenEvents(screenId: number, limit?: number, type?: string): Promise<DataRecord[]>;
  createScreenEvent(event: DataRecord): Promise<DataRecord>;
  getRecentEvents(limit?: number): Promise<DataRecord[]>;
  getScreenCommands(screenId: number, status?: string): Promise<DataRecord[]>;
  getPendingCommands(screenId: number): Promise<DataRecord[]>;
  createScreenCommand(command: DataRecord): Promise<DataRecord>;
  updateCommandStatus(id: number, status: string): Promise<DataRecord>;
  getLatestSnapshot(screenId: number): Promise<DataRecord | undefined>;
  createSnapshot(snapshot: DataRecord): Promise<DataRecord>;
  deleteOldSnapshots(screenId: number, keepCount?: number): Promise<void>;
  recordTelemetry(data: DataRecord): Promise<DataRecord>;
  getScreenTelemetry(screenId: number, since?: Date): Promise<DataRecord[]>;
  getScreenAnalytics(screenId: number): Promise<ScreenAnalytics>;
  getHealthSummary(): Promise<DataRecord[]>;

  getNotifications(opts: NotificationQuery): Promise<DataRecord[]>;
  getUnreadNotificationCount(opts: NotificationQuery): Promise<number>;
  createNotification(notification: DataRecord): Promise<DataRecord>;
  markNotificationRead(id: number): Promise<void>;
  markAllNotificationsRead(opts: NotificationQuery): Promise<void>;
  deleteNotification(id: number): Promise<void>;
  deleteOldNotifications(daysOld: number): Promise<void>;

  getScreenGroups(ownerId?: number | null, teamId?: number | null): Promise<DataRecord[]>;
  getScreenGroup(id: number): Promise<DataRecord | undefined>;
  createScreenGroup(group: DataRecord): Promise<DataRecord>;
  updateScreenGroup(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteScreenGroup(id: number): Promise<void>;
  getScreenGroupMembers(groupId: number): Promise<DataRecord[]>;
  addScreenToGroup(groupId: number, screenId: number): Promise<DataRecord>;
  removeScreenFromGroup(groupId: number, screenId: number): Promise<void>;
  setScreenGroups(screenId: number, groupIds: number[]): Promise<void>;
  getGroupsForScreen(screenId: number): Promise<DataRecord[]>;

  getEmergencyAlertConfig(subscriberId: number): Promise<DataRecord | undefined>;
  upsertEmergencyAlertConfig(config: DataRecord): Promise<DataRecord>;
  getCustomAlertFeeds(subscriberId: number): Promise<DataRecord[]>;
  getCustomAlertFeed(id: number): Promise<DataRecord | undefined>;
  createCustomAlertFeed(feed: DataRecord): Promise<DataRecord>;
  updateCustomAlertFeed(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteCustomAlertFeed(id: number): Promise<void>;

  getTeams(): Promise<DataRecord[]>;
  getTeam(id: number): Promise<DataRecord | undefined>;
  getTeamsByOwner(ownerId: number): Promise<DataRecord[]>;
  createTeam(team: DataRecord): Promise<DataRecord>;
  updateTeam(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteTeam(id: number): Promise<void>;
  getTeamRoles(teamId: number): Promise<DataRecord[]>;
  getTeamRole(id: number): Promise<DataRecord | undefined>;
  createTeamRole(role: DataRecord): Promise<DataRecord>;
  updateTeamRole(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteTeamRole(id: number): Promise<void>;
  getTeamMembers(teamId: number): Promise<DataRecord[]>;
  addTeamMember(member: DataRecord): Promise<DataRecord>;
  updateTeamMember(id: number, updates: DataRecord): Promise<DataRecord>;
  removeTeamMember(id: number): Promise<void>;
  getSubscriberTeams(subscriberId: number): Promise<DataRecord[]>;
  getTeamMember(teamId: number, subscriberId: number): Promise<DataRecord | undefined>;

  getDoohCampaigns(): Promise<DataRecord[]>;
  getDoohCampaign(id: number): Promise<DataRecord | undefined>;
  getDoohCampaignsByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  createDoohCampaign(campaign: DataRecord): Promise<DataRecord>;
  updateDoohCampaign(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteDoohCampaign(id: number): Promise<void>;
  getDoohAdSlots(): Promise<DataRecord[]>;
  getDoohAdSlotsByScreen(screenId: number): Promise<DataRecord[]>;
  getDoohAdSlot(id: number): Promise<DataRecord | undefined>;
  getAdSlotsByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  createDoohAdSlot(slot: DataRecord): Promise<DataRecord>;
  updateDoohAdSlot(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteDoohAdSlot(id: number): Promise<void>;
  createDoohImpression(impression: DataRecord): Promise<DataRecord>;
  getDoohImpressions(campaignId?: number, screenId?: number, limit?: number): Promise<DataRecord[]>;
  getImpressionsByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  getDoohAdRequests(subscriberId: number, teamId?: number | null): Promise<DataRecord[]>;
  getDoohAdRequest(id: number): Promise<DataRecord | undefined>;
  updateDoohAdRequest(id: number, updates: DataRecord): Promise<DataRecord>;
  getDoohRevenueStats(): Promise<DoohRevenueStats>;

  getLicensesBySubscriber(subscriberId: number): Promise<DataRecord[]>;
  getLicenseByScreenId(screenId: number): Promise<DataRecord | undefined>;
  getAvailableLicenses(subscriberId: number): Promise<DataRecord[]>;
  getLicense(id: number): Promise<DataRecord | undefined>;
  assignLicenseToScreen(licenseId: number, screenId: number): Promise<DataRecord>;
  getSubscriptionGroupsBySubscriber(subscriberId: number): Promise<DataRecord[]>;

  getDesignTemplates(): Promise<DataRecord[]>;
  getDesignTemplate(id: number): Promise<DataRecord | undefined>;
  getDesignTemplatesByCategory(categoryId: number): Promise<DataRecord[]>;
  getTemplateCategories(): Promise<DataRecord[]>;
  getWidgetCategories(): Promise<DataRecord[]>;
  getWidgetDefinitions(): Promise<DataRecord[]>;
  getActiveWidgetDefinitions(): Promise<DataRecord[]>;

  getSetting(key: string): Promise<string | null>;
  setSetting(key: string, value: string): Promise<void>;

  getOnboardingProgress(opts: OnboardingQuery): Promise<DataRecord | undefined>;
  upsertOnboardingProgress(progress: DataRecord): Promise<DataRecord>;
  completeOnboardingStep(opts: OnboardingQuery): Promise<DataRecord>;
  dismissOnboarding(opts: OnboardingQuery): Promise<void>;

  getPricingCards(visibleOnly?: boolean): Promise<DataRecord[]>;

  getLayoutTemplates(subscriberId: number): Promise<DataRecord[]>;
  getLayoutTemplate(id: number): Promise<DataRecord | undefined>;
  createLayoutTemplate(template: DataRecord): Promise<DataRecord>;
  deleteLayoutTemplate(id: number): Promise<void>;

  getSmartQrCodes(subscriberId: number): Promise<DataRecord[]>;
  getSmartQrCode(id: number): Promise<DataRecord | undefined>;
  getSmartQrCodeByShortCode(shortCode: string): Promise<DataRecord | undefined>;
  createSmartQrCode(qr: DataRecord): Promise<DataRecord>;
  updateSmartQrCode(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteSmartQrCode(id: number): Promise<void>;
  incrementSmartQrScanCount(id: number): Promise<void>;

  getApprovalLogs(itemType: string, itemId: number): Promise<DataRecord[]>;
  createApprovalLog(log: DataRecord): Promise<DataRecord>;

  getDirectoryVenues(): Promise<DataRecord[]>;
  getDirectoryVenuesByOwner(ownerId: number, teamId?: number | null): Promise<DataRecord[]>;
  getDirectoryVenue(id: number): Promise<DataRecord | undefined>;
  createDirectoryVenue(venue: DataRecord): Promise<DataRecord>;
  updateDirectoryVenue(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteDirectoryVenue(id: number): Promise<void>;
  getDirectoryFloors(venueId: number): Promise<DataRecord[]>;
  getDirectoryFloor(id: number): Promise<DataRecord | undefined>;
  createDirectoryFloor(floor: DataRecord): Promise<DataRecord>;
  updateDirectoryFloor(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteDirectoryFloor(id: number): Promise<void>;
  getDirectoryCategories(venueId: number): Promise<DataRecord[]>;
  getDirectoryCategory(id: number): Promise<DataRecord | undefined>;
  createDirectoryCategory(category: DataRecord): Promise<DataRecord>;
  updateDirectoryCategory(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteDirectoryCategory(id: number): Promise<void>;
  getDirectoryStores(venueId: number): Promise<DataRecord[]>;
  getDirectoryStore(id: number): Promise<DataRecord | undefined>;
  createDirectoryStore(store: DataRecord): Promise<DataRecord>;
  updateDirectoryStore(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteDirectoryStore(id: number): Promise<void>;
  searchDirectoryStores(venueId: number, query: string): Promise<DataRecord[]>;
  getDirectoryPromotions(venueId: number): Promise<DataRecord[]>;
  getActiveDirectoryPromotions(venueId: number): Promise<DataRecord[]>;
  getDirectoryPromotion(id: number): Promise<DataRecord | undefined>;
  createDirectoryPromotion(promo: DataRecord): Promise<DataRecord>;
  updateDirectoryPromotion(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteDirectoryPromotion(id: number): Promise<void>;

  getUnsyncedChangeCount(): Promise<number>;
  getUptimeReport(screenId: number, days: number): Promise<DataRecord[]>;
  getFleetUptimeReport(ownerId?: number, days?: number): Promise<DataRecord[]>;

  getMarketplaceListingsByOwner(subscriberId: number, teamId?: number | null): Promise<DataRecord[]>;
  getMarketplaceListingByScreen(screenId: number): Promise<DataRecord | undefined>;
  createMarketplaceListing(listing: DataRecord): Promise<DataRecord>;
  updateMarketplaceListing(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteMarketplaceListing(id: number): Promise<void>;

  getTeamCategoriesByOwner(ownerId: number): Promise<DataRecord[]>;
  getTeamCategory(id: number): Promise<DataRecord | undefined>;
  createTeamCategory(category: DataRecord): Promise<DataRecord>;
  updateTeamCategory(id: number, updates: DataRecord): Promise<DataRecord>;
  deleteTeamCategory(id: number): Promise<void>;

  getTeamScreens(teamId: number): Promise<DataRecord[]>;
  assignScreenToTeam(assignment: DataRecord): Promise<DataRecord>;
  updateTeamScreen(id: number, updates: DataRecord): Promise<DataRecord>;
  removeScreenFromTeam(id: number): Promise<void>;
  deleteEmergencyAlertConfig(subscriberId: number): Promise<void>;
}

function parseJsonField(val: any): any {
  if (val === null || val === undefined) return null;
  if (typeof val === 'string') {
    try { return JSON.parse(val); } catch { return val; }
  }
  return val;
}

function parseArrayField(val: any): any[] {
  if (Array.isArray(val)) return val;
  if (val === null || val === undefined) return [];
  if (typeof val === 'string') {
    try { const parsed = JSON.parse(val); return Array.isArray(parsed) ? parsed : []; } catch { return []; }
  }
  return [];
}

function boolToInt(val: any): number {
  return val ? 1 : 0;
}

function intToBool(val: any): boolean {
  return val === 1 || val === true;
}

function toIso(val: any): string | null {
  if (!val) return null;
  if (val instanceof Date) return val.toISOString();
  return String(val);
}

function normalizeRow(row: any): any {
  if (!row) return row;
  const result = { ...row };
  for (const [key, value] of Object.entries(result)) {
    if (key === 'is_online' || key === 'is_paired' || key === 'kiosk_mode' || key === 'enabled' ||
        key === 'is_read' || key === 'dismissed' || key === 'email_verified' || key === 'is_active' ||
        key === 'is_published' || key === 'is_admin' || key === 'scrolling' || key === 'is_public' ||
        key === 'continuous' || key === 'featured' || key === 'active' || key === 'verified' ||
        key === 'cancel_at_period_end' || key === 'admin_granted' || key === 'require_approval' ||
        key === 'must_change_password' || key === 'totp_enabled' || key === 'target_all_screens' ||
        key === 'use_default' || key === 'sync_locked' || key === 'is_highlighted' ||
        key === 'is_visible' || key === 'show_branding_note' || key === 'show_trial_note' ||
        key === 'setup_fee_required' || key === 'promotional_emails' || key === 'tax_location_override' ||
        key === 'dooh_portal_enabled' || key === 'dooh_addon_active' || key === 'dooh_free_access' ||
        key === 'matched') {
      result[key] = intToBool(value);
    }
    if (key === 'device_info' || key === 'data' || key === 'settings' || key === 'conditions' ||
        key === 'device_fingerprint' || key === 'payload' || key === 'error_details' ||
        key === 'layout_zones' || key === 'custom_layout_config' || key === 'config' ||
        key === 'map_data' || key === 'location' || key === 'permissions' || key === 'features' ||
        key === 'customization' || key === 'input_data' || key === 'ai_analysis' ||
        key === 'route_geometry' || key === 'steps') {
      if (typeof value === 'string') {
        result[key] = parseJsonField(value);
      }
    }
    if (key === 'tags' || key === 'days_of_week' || key === 'target_screen_ids' ||
        key === 'target_locations' || key === 'enabled_alert_types' || key === 'filter_keywords' ||
        key === 'regions' || key === 'subscriber_ids' || key === 'screen_ids_delivered' ||
        key === 'completed_steps' || key === 'photo_urls') {
      if (typeof value === 'string') {
        result[key] = parseArrayField(value);
      }
    }
  }
  return result;
}

function snakeToCamel(str: string): string {
  return str.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
}

function camelToSnake(str: string): string {
  return str.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

function rowToCamel(row: any): any {
  if (!row) return row;
  const normalized = normalizeRow(row);
  const result: any = {};
  for (const [key, value] of Object.entries(normalized)) {
    result[snakeToCamel(key)] = value;
  }
  return result;
}

export { rowToCamel };

export function rowsToCamel(rows: any[]): any[] {
  return rows.map(rowToCamel);
}

function buildInsertSql(table: string, data: Record<string, any>): { sql: string; values: any[] } {
  const entries = Object.entries(data).filter(([_, v]) => v !== undefined);
  const cols = entries.map(([k]) => camelToSnake(k));
  const placeholders = cols.map(() => '?');
  const values = entries.map(([_, v]) => {
    if (v === null) return null;
    if (typeof v === 'boolean') return v ? 1 : 0;
    if (typeof v === 'object' && !(v instanceof Date)) return JSON.stringify(v);
    if (v instanceof Date) return v.toISOString();
    return v;
  });
  return {
    sql: `INSERT INTO ${table} (${cols.join(', ')}) VALUES (${placeholders.join(', ')})`,
    values,
  };
}

function buildUpdateSql(table: string, id: number, updates: Record<string, any>): { sql: string; values: any[] } {
  const entries = Object.entries(updates).filter(([k, v]) => v !== undefined && k !== 'id');
  const sets = entries.map(([k]) => `${camelToSnake(k)} = ?`);
  const values = entries.map(([_, v]) => {
    if (v === null) return null;
    if (typeof v === 'boolean') return v ? 1 : 0;
    if (typeof v === 'object' && !(v instanceof Date)) return JSON.stringify(v);
    if (v instanceof Date) return v.toISOString();
    return v;
  });
  return {
    sql: `UPDATE ${table} SET ${sets.join(', ')} WHERE id = ?`,
    values: [...values, id],
  };
}

export class SqliteStorage implements ILocalStorage {
  private get db() { return getDb(); }

  async getContentFolders() {
    return rowsToCamel(this.db.prepare('SELECT * FROM content_folders ORDER BY name').all());
  }

  async getContentFoldersByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) {
      return rowsToCamel(this.db.prepare('SELECT * FROM content_folders WHERE team_id = ? ORDER BY name').all(teamId));
    }
    return rowsToCamel(this.db.prepare('SELECT * FROM content_folders WHERE owner_id = ? AND team_id IS NULL ORDER BY name').all(ownerId));
  }

  async createContentFolder(folder: DataRecord) {
    const { sql, values } = buildInsertSql('content_folders', folder);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM content_folders WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateContentFolder(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('content_folders', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM content_folders WHERE id = ?').get(id));
  }

  async deleteContentFolder(id: number) {
    this.db.prepare('DELETE FROM content_folders WHERE id = ?').run(id);
  }

  async getContents(params?: PaginationParams) {
    if (params?.page && params?.limit) {
      const offset = (params.page - 1) * params.limit;
      const rows = rowsToCamel(this.db.prepare('SELECT * FROM contents ORDER BY created_at DESC LIMIT ? OFFSET ?').all(params.limit, offset));
      const total = (this.db.prepare('SELECT COUNT(*) as count FROM contents').get() as any).count;
      return { data: rows, total, page: params.page, limit: params.limit, totalPages: Math.ceil(total / params.limit) };
    }
    return rowsToCamel(this.db.prepare('SELECT * FROM contents ORDER BY created_at DESC').all());
  }

  async getContent(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM contents WHERE id = ?').get(id));
  }

  async createContent(content: DataRecord) {
    const { sql, values } = buildInsertSql('contents', content);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM contents WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateContent(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('contents', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM contents WHERE id = ?').get(id));
  }

  async deleteContent(id: number) {
    this.db.prepare('DELETE FROM contents WHERE id = ?').run(id);
  }

  async getContentsByOwner(ownerId: number, params?: PaginationParams, teamId?: number | null) {
    const where = teamId ? 'team_id = ?' : 'owner_id = ? AND team_id IS NULL';
    const bindVal = teamId || ownerId;
    if (params?.page && params?.limit) {
      const offset = (params.page - 1) * params.limit;
      const rows = rowsToCamel(this.db.prepare(`SELECT * FROM contents WHERE ${where} ORDER BY created_at DESC LIMIT ? OFFSET ?`).all(bindVal, params.limit, offset));
      const total = (this.db.prepare(`SELECT COUNT(*) as count FROM contents WHERE ${where}`).get(bindVal) as any).count;
      return { data: rows, total, page: params.page, limit: params.limit, totalPages: Math.ceil(total / params.limit) };
    }
    return rowsToCamel(this.db.prepare(`SELECT * FROM contents WHERE ${where} ORDER BY created_at DESC`).all(bindVal));
  }

  async getScreens(params?: PaginationParams) {
    if (params?.page && params?.limit) {
      const offset = (params.page - 1) * params.limit;
      const rows = rowsToCamel(this.db.prepare('SELECT * FROM screens ORDER BY name LIMIT ? OFFSET ?').all(params.limit, offset));
      const total = (this.db.prepare('SELECT COUNT(*) as count FROM screens').get() as any).count;
      return { data: rows, total, page: params.page, limit: params.limit, totalPages: Math.ceil(total / params.limit) };
    }
    return rowsToCamel(this.db.prepare('SELECT * FROM screens ORDER BY name').all());
  }

  async getScreen(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM screens WHERE id = ?').get(id));
  }

  async getScreenByPairingCode(code: string) {
    return rowToCamel(this.db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(code));
  }

  async createScreen(screen: DataRecord) {
    const { sql, values } = buildInsertSql('screens', screen);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screens WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateScreen(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('screens', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screens WHERE id = ?').get(id));
  }

  async deleteScreen(id: number) {
    this.db.prepare('DELETE FROM screens WHERE id = ?').run(id);
  }

  async getScreensByOwner(ownerId: number, params?: PaginationParams, teamId?: number | null) {
    const where = teamId ? 'team_id = ?' : 'owner_id = ? AND team_id IS NULL';
    const bindVal = teamId || ownerId;
    if (params?.page && params?.limit) {
      const offset = (params.page - 1) * params.limit;
      const rows = rowsToCamel(this.db.prepare(`SELECT * FROM screens WHERE ${where} ORDER BY name LIMIT ? OFFSET ?`).all(bindVal, params.limit, offset));
      const total = (this.db.prepare(`SELECT COUNT(*) as count FROM screens WHERE ${where}`).get(bindVal) as any).count;
      return { data: rows, total, page: params.page, limit: params.limit, totalPages: Math.ceil(total / params.limit) };
    }
    return rowsToCamel(this.db.prepare(`SELECT * FROM screens WHERE ${where} ORDER BY name`).all(bindVal));
  }

  async getAllScreensByOwner(ownerId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM screens WHERE owner_id = ? ORDER BY name').all(ownerId));
  }

  async getPlaylists() {
    return rowsToCamel(this.db.prepare('SELECT * FROM playlists ORDER BY name').all());
  }

  async getPlaylist(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM playlists WHERE id = ?').get(id));
  }

  async getPlaylistWithItems(id: number) {
    const playlist = rowToCamel(this.db.prepare('SELECT * FROM playlists WHERE id = ?').get(id));
    if (!playlist) return undefined;
    const items = this.db.prepare(`
      SELECT pi.*, c.name as content_name, c.type as content_type, c.data as content_data,
             c.mime_type as content_mime_type, c.file_size as content_file_size,
             c.url as content_url, c.local_path as content_local_path,
             c.thumbnail_url as content_thumbnail_url, c.canvas_data as content_canvas_data,
             c.settings as content_settings, c.folder_id as content_folder_id,
             c.owner_id as content_owner_id, c.team_id as content_team_id,
             c.approval_status as content_approval_status, c.created_at as content_created_at
      FROM playlist_items pi
      LEFT JOIN contents c ON pi.content_id = c.id
      WHERE pi.playlist_id = ?
      ORDER BY pi.sort_order ASC
    `).all(id);
    playlist.items = items.map((item: any) => {
      const pi = rowToCamel({
        id: item.id, playlist_id: item.playlist_id, content_id: item.content_id,
        duration: item.duration, sort_order: item.sort_order,
      });
      pi.content = rowToCamel({
        id: item.content_id, name: item.content_name, type: item.content_type,
        data: item.content_data, mime_type: item.content_mime_type, file_size: item.content_file_size,
        url: item.content_url, local_path: item.content_local_path, thumbnail_url: item.content_thumbnail_url,
        canvas_data: item.content_canvas_data, settings: item.content_settings,
        folder_id: item.content_folder_id, owner_id: item.content_owner_id, team_id: item.content_team_id,
        approval_status: item.content_approval_status, created_at: item.content_created_at,
      });
      return pi;
    });
    return playlist;
  }

  async createPlaylist(playlist: DataRecord) {
    const { sql, values } = buildInsertSql('playlists', playlist);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM playlists WHERE id = ?').get(info.lastInsertRowid));
  }

  async updatePlaylist(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('playlists', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM playlists WHERE id = ?').get(id));
  }

  async deletePlaylist(id: number) {
    this.db.prepare('DELETE FROM playlists WHERE id = ?').run(id);
  }

  async getPlaylistsByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) {
      return rowsToCamel(this.db.prepare('SELECT * FROM playlists WHERE team_id = ? ORDER BY name').all(teamId));
    }
    return rowsToCamel(this.db.prepare('SELECT * FROM playlists WHERE owner_id = ? AND team_id IS NULL ORDER BY name').all(ownerId));
  }

  async getPlaylistItems(playlistId: number) {
    const items = this.db.prepare(`
      SELECT pi.*, c.name as content_name, c.type as content_type, c.data as content_data,
             c.url as content_url, c.local_path as content_local_path,
             c.thumbnail_url as content_thumbnail_url, c.created_at as content_created_at,
             c.file_size as content_file_size, c.mime_type as content_mime_type,
             c.canvas_data as content_canvas_data, c.settings as content_settings,
             c.folder_id as content_folder_id, c.owner_id as content_owner_id,
             c.team_id as content_team_id, c.approval_status as content_approval_status
      FROM playlist_items pi
      LEFT JOIN contents c ON pi.content_id = c.id
      WHERE pi.playlist_id = ?
      ORDER BY pi.sort_order ASC
    `).all(playlistId);
    return items.map((item: any) => {
      const pi = rowToCamel({
        id: item.id, playlist_id: item.playlist_id, content_id: item.content_id,
        duration: item.duration, sort_order: item.sort_order,
      });
      pi.content = rowToCamel({
        id: item.content_id, name: item.content_name, type: item.content_type,
        data: item.content_data, url: item.content_url, local_path: item.content_local_path,
        thumbnail_url: item.content_thumbnail_url, created_at: item.content_created_at,
        file_size: item.content_file_size, mime_type: item.content_mime_type,
        canvas_data: item.content_canvas_data, settings: item.content_settings,
        folder_id: item.content_folder_id, owner_id: item.content_owner_id,
        team_id: item.content_team_id, approval_status: item.content_approval_status,
      });
      return pi;
    });
  }

  async addPlaylistItem(item: DataRecord) {
    const { sql, values } = buildInsertSql('playlist_items', item);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM playlist_items WHERE id = ?').get(info.lastInsertRowid));
  }

  async updatePlaylistItem(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('playlist_items', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM playlist_items WHERE id = ?').get(id));
  }

  async removePlaylistItem(id: number) {
    this.db.prepare('DELETE FROM playlist_items WHERE id = ?').run(id);
  }

  async reorderPlaylistItems(playlistId: number, itemIds: number[]) {
    const stmt = this.db.prepare('UPDATE playlist_items SET sort_order = ? WHERE id = ? AND playlist_id = ?');
    const txn = this.db.transaction(() => {
      itemIds.forEach((id, idx) => stmt.run(idx, id, playlistId));
    });
    txn();
  }

  async getSchedules(screenId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM schedules WHERE screen_id = ? ORDER BY priority DESC, start_time ASC').all(screenId));
  }

  async getSchedule(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM schedules WHERE id = ?').get(id));
  }

  async createSchedule(schedule: DataRecord) {
    const { sql, values } = buildInsertSql('schedules', schedule);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM schedules WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateSchedule(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('schedules', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM schedules WHERE id = ?').get(id));
  }

  async deleteSchedule(id: number) {
    this.db.prepare('DELETE FROM schedules WHERE id = ?').run(id);
  }

  async getActiveSchedule(screenId: number, dayOfWeek: number, minuteOfDay: number) {
    const schedules = this.db.prepare('SELECT * FROM schedules WHERE screen_id = ? AND enabled = 1 ORDER BY priority DESC').all(screenId) as any[];
    for (const sched of schedules) {
      const days = parseArrayField(sched.days_of_week);
      if (!days.includes(dayOfWeek)) continue;
      if (minuteOfDay >= sched.start_time && minuteOfDay <= sched.end_time) {
        return rowToCamel(sched);
      }
    }
    return undefined;
  }

  async getSubscriber(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM subscribers WHERE id = ?').get(id));
  }

  async getSubscriberByEmail(email: string) {
    return rowToCamel(this.db.prepare('SELECT * FROM subscribers WHERE email = ?').get(email));
  }

  async createSubscriber(subscriber: DataRecord) {
    const { sql, values } = buildInsertSql('subscribers', subscriber);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM subscribers WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateSubscriber(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('subscribers', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM subscribers WHERE id = ?').get(id));
  }

  async getVideoWalls() {
    return rowsToCamel(this.db.prepare('SELECT * FROM video_walls ORDER BY name').all());
  }

  async getVideoWall(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM video_walls WHERE id = ?').get(id));
  }

  async getVideoWallsByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM video_walls WHERE team_id = ?').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM video_walls WHERE owner_id = ? AND team_id IS NULL').all(ownerId));
  }

  async createVideoWall(wall: DataRecord) {
    const { sql, values } = buildInsertSql('video_walls', wall);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM video_walls WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateVideoWall(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('video_walls', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM video_walls WHERE id = ?').get(id));
  }

  async deleteVideoWall(id: number) {
    this.db.prepare('DELETE FROM video_walls WHERE id = ?').run(id);
  }

  async getVideoWallScreens(wallId: number) {
    const rows = this.db.prepare(`
      SELECT vws.*, s.name as screen_name, s.pairing_code, s.is_online, s.platform
      FROM video_wall_screens vws
      LEFT JOIN screens s ON vws.screen_id = s.id
      WHERE vws.wall_id = ?
    `).all(wallId);
    return rows.map((r: any) => {
      const vws = rowToCamel({
        id: r.id, wall_id: r.wall_id, screen_id: r.screen_id, row: r.row, col: r.col,
        sync_offset: r.sync_offset, sync_locked: r.sync_locked,
        pos_x: r.pos_x, pos_y: r.pos_y, size_w: r.size_w, size_h: r.size_h,
      });
      vws.screen = rowToCamel({
        id: r.screen_id, name: r.screen_name, pairingCode: r.pairing_code,
        isOnline: r.is_online, platform: r.platform,
      });
      return vws;
    });
  }

  async getWallAssignmentForScreen(screenId: number): Promise<DataRecord | undefined> {
    const row = this.db.prepare('SELECT * FROM video_wall_screens WHERE screen_id = ?').get(screenId) as any;
    if (!row) return undefined;
    return {
      wallId: row.wall_id, row: row.row, col: row.col, syncOffset: row.sync_offset,
      syncLocked: intToBool(row.sync_locked), posX: row.pos_x, posY: row.pos_y,
      sizeW: row.size_w, sizeH: row.size_h,
    };
  }

  async assignScreenToWall(assignment: DataRecord) {
    const { sql, values } = buildInsertSql('video_wall_screens', assignment);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM video_wall_screens WHERE id = ?').get(info.lastInsertRowid));
  }

  async assignScreensToVideoWall(wallId: number, assignments: WallAssignment[]) {
    this.db.prepare('DELETE FROM video_wall_screens WHERE wall_id = ?').run(wallId);
    for (const a of assignments) {
      const { sql, values } = buildInsertSql('video_wall_screens', { wallId, ...a });
      this.db.prepare(sql).run(...values);
    }
  }

  async clearWallScreens(wallId: number) {
    this.db.prepare('DELETE FROM video_wall_screens WHERE wall_id = ?').run(wallId);
  }

  async removeScreenFromWall(id: number) {
    this.db.prepare('DELETE FROM video_wall_screens WHERE id = ?').run(id);
  }

  async updateWallScreen(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('video_wall_screens', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM video_wall_screens WHERE id = ?').get(id));
  }

  async getKiosks() {
    return rowsToCamel(this.db.prepare('SELECT * FROM kiosks ORDER BY name').all());
  }

  async getKiosk(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM kiosks WHERE id = ?').get(id));
  }

  async getKiosksByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM kiosks WHERE team_id = ?').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM kiosks WHERE owner_id = ? AND team_id IS NULL').all(ownerId));
  }

  async createKiosk(kiosk: DataRecord) {
    const { sql, values } = buildInsertSql('kiosks', kiosk);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM kiosks WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateKiosk(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('kiosks', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM kiosks WHERE id = ?').get(id));
  }

  async deleteKiosk(id: number) {
    this.db.prepare('DELETE FROM kiosks WHERE id = ?').run(id);
  }

  async getSmartTriggers() {
    return rowsToCamel(this.db.prepare('SELECT * FROM smart_triggers ORDER BY name').all());
  }

  async getSmartTriggersByScreen(screenId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM smart_triggers WHERE screen_id = ?').all(screenId));
  }

  async getSmartTrigger(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM smart_triggers WHERE id = ?').get(id));
  }

  async getSmartTriggersByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM smart_triggers WHERE team_id = ?').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM smart_triggers WHERE owner_id = ? AND team_id IS NULL').all(ownerId));
  }

  async createSmartTrigger(trigger: DataRecord) {
    const { sql, values } = buildInsertSql('smart_triggers', trigger);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM smart_triggers WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateSmartTrigger(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('smart_triggers', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM smart_triggers WHERE id = ?').get(id));
  }

  async deleteSmartTrigger(id: number) {
    this.db.prepare('DELETE FROM smart_triggers WHERE id = ?').run(id);
  }

  async createTriggerLog(log: DataRecord) {
    const { sql, values } = buildInsertSql('trigger_logs', log);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM trigger_logs WHERE id = ?').get(info.lastInsertRowid));
  }

  async getTriggerLogs(triggerId: number, limit = 50) {
    return rowsToCamel(this.db.prepare('SELECT * FROM trigger_logs WHERE trigger_id = ? ORDER BY created_at DESC LIMIT ?').all(triggerId, limit));
  }

  async getTriggerLogsByScreen(screenId: number, limit = 50) {
    return rowsToCamel(this.db.prepare('SELECT * FROM trigger_logs WHERE screen_id = ? ORDER BY created_at DESC LIMIT ?').all(screenId, limit));
  }

  async getBroadcasts() {
    return rowsToCamel(this.db.prepare('SELECT * FROM broadcasts ORDER BY created_at DESC').all());
  }

  async getBroadcastsByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM broadcasts WHERE team_id = ?').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM broadcasts WHERE owner_id = ? AND team_id IS NULL').all(ownerId));
  }

  async getActiveBroadcasts() {
    return rowsToCamel(this.db.prepare("SELECT * FROM broadcasts WHERE status = 'active'").all());
  }

  async getActiveBroadcastsForScreen(screenId: number) {
    const all = this.db.prepare("SELECT * FROM broadcasts WHERE status = 'active'").all() as any[];
    return rowsToCamel(all.filter((b: any) => {
      if (!b.target_screen_ids) return true;
      const ids = parseArrayField(b.target_screen_ids);
      return ids.length === 0 || ids.includes(screenId);
    }));
  }

  async getBroadcast(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM broadcasts WHERE id = ?').get(id));
  }

  async createBroadcast(broadcast: DataRecord) {
    const { sql, values } = buildInsertSql('broadcasts', broadcast);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM broadcasts WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateBroadcast(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('broadcasts', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM broadcasts WHERE id = ?').get(id));
  }

  async deleteBroadcast(id: number) {
    this.db.prepare('DELETE FROM broadcasts WHERE id = ?').run(id);
  }

  async stopBroadcast(id: number) {
    this.db.prepare("UPDATE broadcasts SET status = 'stopped' WHERE id = ?").run(id);
    return rowToCamel(this.db.prepare('SELECT * FROM broadcasts WHERE id = ?').get(id));
  }

  async getScreenEvents(screenId: number, limit = 50, type?: string) {
    if (type) {
      return rowsToCamel(this.db.prepare('SELECT * FROM screen_events WHERE screen_id = ? AND type = ? ORDER BY created_at DESC LIMIT ?').all(screenId, type, limit));
    }
    return rowsToCamel(this.db.prepare('SELECT * FROM screen_events WHERE screen_id = ? ORDER BY created_at DESC LIMIT ?').all(screenId, limit));
  }

  async createScreenEvent(event: DataRecord) {
    const { sql, values } = buildInsertSql('screen_events', event);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_events WHERE id = ?').get(info.lastInsertRowid));
  }

  async getRecentEvents(limit = 50) {
    return rowsToCamel(this.db.prepare(`
      SELECT se.*, s.name as screen_name
      FROM screen_events se
      LEFT JOIN screens s ON se.screen_id = s.id
      ORDER BY se.created_at DESC LIMIT ?
    `).all(limit));
  }

  async getScreenCommands(screenId: number, status?: string) {
    if (status) {
      return rowsToCamel(this.db.prepare('SELECT * FROM screen_commands WHERE screen_id = ? AND status = ? ORDER BY issued_at DESC').all(screenId, status));
    }
    return rowsToCamel(this.db.prepare('SELECT * FROM screen_commands WHERE screen_id = ? ORDER BY issued_at DESC').all(screenId));
  }

  async getPendingCommands(screenId: number) {
    return rowsToCamel(this.db.prepare("SELECT * FROM screen_commands WHERE screen_id = ? AND status = 'pending' ORDER BY issued_at ASC").all(screenId));
  }

  async createScreenCommand(command: DataRecord) {
    const { sql, values } = buildInsertSql('screen_commands', command);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_commands WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateCommandStatus(id: number, status: string) {
    this.db.prepare('UPDATE screen_commands SET status = ?, acknowledged_at = datetime("now") WHERE id = ?').run(status, id);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_commands WHERE id = ?').get(id));
  }

  async getLatestSnapshot(screenId: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM screen_snapshots WHERE screen_id = ? ORDER BY created_at DESC LIMIT 1').get(screenId));
  }

  async createSnapshot(snapshot: DataRecord) {
    const { sql, values } = buildInsertSql('screen_snapshots', snapshot);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_snapshots WHERE id = ?').get(info.lastInsertRowid));
  }

  async deleteOldSnapshots(screenId: number, keepCount = 3) {
    this.db.prepare(`
      DELETE FROM screen_snapshots WHERE screen_id = ? AND id NOT IN (
        SELECT id FROM screen_snapshots WHERE screen_id = ? ORDER BY created_at DESC LIMIT ?
      )
    `).run(screenId, screenId, keepCount);
  }

  async recordTelemetry(data: DataRecord) {
    const { sql, values } = buildInsertSql('screen_telemetry', data);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_telemetry WHERE id = ?').get(info.lastInsertRowid));
  }

  async getScreenTelemetry(screenId: number, since?: Date) {
    if (since) {
      return rowsToCamel(this.db.prepare('SELECT * FROM screen_telemetry WHERE screen_id = ? AND reported_at >= ? ORDER BY reported_at DESC').all(screenId, since.toISOString()));
    }
    return rowsToCamel(this.db.prepare('SELECT * FROM screen_telemetry WHERE screen_id = ? ORDER BY reported_at DESC LIMIT 100').all(screenId));
  }

  async getScreenAnalytics(screenId: number) {
    const totalEvents = (this.db.prepare('SELECT COUNT(*) as count FROM screen_events WHERE screen_id = ?').get(screenId) as any)?.count || 0;
    const contentChanges = (this.db.prepare("SELECT COUNT(*) as count FROM screen_events WHERE screen_id = ? AND type = 'content_change'").get(screenId) as any)?.count || 0;
    const errors = (this.db.prepare("SELECT COUNT(*) as count FROM screen_events WHERE screen_id = ? AND type = 'error'").get(screenId) as any)?.count || 0;
    const lastEvent = this.db.prepare('SELECT created_at FROM screen_events WHERE screen_id = ? ORDER BY created_at DESC LIMIT 1').get(screenId) as any;
    return { totalEvents, uptimeHours: 0, contentChanges, errors, lastSeen: lastEvent?.created_at || null };
  }

  async getHealthSummary() {
    return rowsToCamel(this.db.prepare(`
      SELECT s.id, s.name, s.is_online, s.last_heartbeat,
        (SELECT COUNT(*) FROM screen_events WHERE screen_id = s.id AND type = 'error') as error_count
      FROM screens s ORDER BY s.name
    `).all());
  }

  async getNotifications(opts: NotificationQuery) {
    let where = '1=1';
    const params: any[] = [];
    if (opts.targetType) { where += ' AND target_type = ?'; params.push(opts.targetType); }
    if (opts.subscriberId) { where += ' AND subscriber_id = ?'; params.push(opts.subscriberId); }
    if (opts.unreadOnly) { where += ' AND is_read = 0'; }
    const limit = opts.limit || 50;
    return rowsToCamel(this.db.prepare(`SELECT * FROM notifications WHERE ${where} ORDER BY created_at DESC LIMIT ?`).all(...params, limit));
  }

  async getUnreadNotificationCount(opts: NotificationQuery) {
    let where = 'is_read = 0';
    const params: any[] = [];
    if (opts.targetType) { where += ' AND target_type = ?'; params.push(opts.targetType); }
    if (opts.subscriberId) { where += ' AND subscriber_id = ?'; params.push(opts.subscriberId); }
    return (this.db.prepare(`SELECT COUNT(*) as count FROM notifications WHERE ${where}`).get(...params) as any)?.count || 0;
  }

  async createNotification(notification: DataRecord) {
    const { sql, values } = buildInsertSql('notifications', notification);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM notifications WHERE id = ?').get(info.lastInsertRowid));
  }

  async markNotificationRead(id: number) {
    this.db.prepare('UPDATE notifications SET is_read = 1 WHERE id = ?').run(id);
  }

  async markAllNotificationsRead(opts: NotificationQuery) {
    let where = '1=1';
    const params: any[] = [];
    if (opts.targetType) { where += ' AND target_type = ?'; params.push(opts.targetType); }
    if (opts.subscriberId) { where += ' AND subscriber_id = ?'; params.push(opts.subscriberId); }
    this.db.prepare(`UPDATE notifications SET is_read = 1 WHERE ${where}`).run(...params);
  }

  async deleteNotification(id: number) {
    this.db.prepare('DELETE FROM notifications WHERE id = ?').run(id);
  }

  async deleteOldNotifications(daysOld: number) {
    this.db.prepare(`DELETE FROM notifications WHERE created_at < datetime('now', '-' || ? || ' days')`).run(daysOld);
  }

  async getScreenGroups(ownerId?: number | null, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM screen_groups WHERE team_id = ?').all(teamId));
    if (ownerId) return rowsToCamel(this.db.prepare('SELECT * FROM screen_groups WHERE owner_id = ? AND team_id IS NULL').all(ownerId));
    return rowsToCamel(this.db.prepare('SELECT * FROM screen_groups').all());
  }

  async getScreenGroup(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM screen_groups WHERE id = ?').get(id));
  }

  async createScreenGroup(group: DataRecord) {
    const { sql, values } = buildInsertSql('screen_groups', group);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_groups WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateScreenGroup(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('screen_groups', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_groups WHERE id = ?').get(id));
  }

  async deleteScreenGroup(id: number) {
    this.db.prepare('DELETE FROM screen_groups WHERE id = ?').run(id);
  }

  async getScreenGroupMembers(groupId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM screen_group_members WHERE group_id = ?').all(groupId));
  }

  async addScreenToGroup(groupId: number, screenId: number) {
    this.db.prepare('INSERT OR IGNORE INTO screen_group_members (group_id, screen_id) VALUES (?, ?)').run(groupId, screenId);
    return rowToCamel(this.db.prepare('SELECT * FROM screen_group_members WHERE group_id = ? AND screen_id = ?').get(groupId, screenId));
  }

  async removeScreenFromGroup(groupId: number, screenId: number) {
    this.db.prepare('DELETE FROM screen_group_members WHERE group_id = ? AND screen_id = ?').run(groupId, screenId);
  }

  async setScreenGroups(screenId: number, groupIds: number[]) {
    this.db.prepare('DELETE FROM screen_group_members WHERE screen_id = ?').run(screenId);
    for (const gid of groupIds) {
      this.db.prepare('INSERT INTO screen_group_members (group_id, screen_id) VALUES (?, ?)').run(gid, screenId);
    }
  }

  async getGroupsForScreen(screenId: number) {
    return rowsToCamel(this.db.prepare(`
      SELECT sg.* FROM screen_groups sg
      JOIN screen_group_members sgm ON sg.id = sgm.group_id
      WHERE sgm.screen_id = ?
    `).all(screenId));
  }

  async getEmergencyAlertConfig(subscriberId: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM emergency_alert_configs WHERE subscriber_id = ?').get(subscriberId));
  }

  async upsertEmergencyAlertConfig(config: DataRecord) {
    const existing = this.db.prepare('SELECT id FROM emergency_alert_configs WHERE subscriber_id = ?').get(config.subscriberId) as any;
    if (existing) {
      const { sql, values } = buildUpdateSql('emergency_alert_configs', existing.id, config);
      this.db.prepare(sql).run(...values);
      return rowToCamel(this.db.prepare('SELECT * FROM emergency_alert_configs WHERE id = ?').get(existing.id));
    }
    const { sql, values } = buildInsertSql('emergency_alert_configs', config);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM emergency_alert_configs WHERE id = ?').get(info.lastInsertRowid));
  }

  async getCustomAlertFeeds(subscriberId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM custom_alert_feeds WHERE subscriber_id = ?').all(subscriberId));
  }

  async getCustomAlertFeed(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM custom_alert_feeds WHERE id = ?').get(id));
  }

  async createCustomAlertFeed(feed: DataRecord) {
    const { sql, values } = buildInsertSql('custom_alert_feeds', feed);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM custom_alert_feeds WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateCustomAlertFeed(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('custom_alert_feeds', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM custom_alert_feeds WHERE id = ?').get(id));
  }

  async deleteCustomAlertFeed(id: number) {
    this.db.prepare('DELETE FROM custom_alert_feeds WHERE id = ?').run(id);
  }

  async getTeams() {
    return rowsToCamel(this.db.prepare('SELECT * FROM teams ORDER BY name').all());
  }

  async getTeam(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM teams WHERE id = ?').get(id));
  }

  async getTeamsByOwner(ownerId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM teams WHERE owner_id = ?').all(ownerId));
  }

  async createTeam(team: DataRecord) {
    const { sql, values } = buildInsertSql('teams', team);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM teams WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateTeam(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('teams', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM teams WHERE id = ?').get(id));
  }

  async deleteTeam(id: number) {
    this.db.prepare('DELETE FROM teams WHERE id = ?').run(id);
  }

  async getTeamRoles(teamId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM team_roles WHERE team_id = ?').all(teamId));
  }

  async getTeamRole(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM team_roles WHERE id = ?').get(id));
  }

  async createTeamRole(role: DataRecord) {
    const { sql, values } = buildInsertSql('team_roles', role);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_roles WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateTeamRole(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('team_roles', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_roles WHERE id = ?').get(id));
  }

  async deleteTeamRole(id: number) {
    this.db.prepare('DELETE FROM team_roles WHERE id = ?').run(id);
  }

  async getTeamMembers(teamId: number) {
    const rows = this.db.prepare(`
      SELECT tm.*, s.name as subscriber_name, s.email as subscriber_email,
             tr.name as role_name, tr.permissions as role_permissions
      FROM team_members tm
      LEFT JOIN subscribers s ON tm.subscriber_id = s.id
      LEFT JOIN team_roles tr ON tm.role_id = tr.id
      WHERE tm.team_id = ?
    `).all(teamId);
    return rows.map((r: any) => {
      const member = rowToCamel({ id: r.id, team_id: r.team_id, subscriber_id: r.subscriber_id, role_id: r.role_id, joined_at: r.joined_at });
      member.subscriber = rowToCamel({ id: r.subscriber_id, name: r.subscriber_name, email: r.subscriber_email });
      member.role = r.role_id ? rowToCamel({ id: r.role_id, name: r.role_name, permissions: r.role_permissions }) : null;
      return member;
    });
  }

  async addTeamMember(member: DataRecord) {
    const { sql, values } = buildInsertSql('team_members', member);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_members WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateTeamMember(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('team_members', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_members WHERE id = ?').get(id));
  }

  async removeTeamMember(id: number) {
    this.db.prepare('DELETE FROM team_members WHERE id = ?').run(id);
  }

  async getSubscriberTeams(subscriberId: number) {
    const rows = this.db.prepare(`
      SELECT tm.*, t.name as team_name, t.description as team_description, t.owner_id as team_owner_id,
             tr.name as role_name, tr.permissions as role_permissions
      FROM team_members tm
      JOIN teams t ON tm.team_id = t.id
      LEFT JOIN team_roles tr ON tm.role_id = tr.id
      WHERE tm.subscriber_id = ?
    `).all(subscriberId);
    return rows.map((r: any) => ({
      team: rowToCamel({ id: r.team_id, name: r.team_name, description: r.team_description, ownerId: r.team_owner_id }),
      role: r.role_id ? rowToCamel({ id: r.role_id, name: r.role_name, permissions: r.role_permissions }) : null,
      membership: rowToCamel({ id: r.id, teamId: r.team_id, subscriberId: r.subscriber_id, roleId: r.role_id, joinedAt: r.joined_at }),
    }));
  }

  async getTeamMember(teamId: number, subscriberId: number) {
    const row = this.db.prepare(`
      SELECT tm.*, tr.name as role_name, tr.permissions as role_permissions
      FROM team_members tm
      LEFT JOIN team_roles tr ON tm.role_id = tr.id
      WHERE tm.team_id = ? AND tm.subscriber_id = ?
    `).get(teamId, subscriberId) as any;
    if (!row) return undefined;
    const member = rowToCamel({ id: row.id, teamId: row.team_id, subscriberId: row.subscriber_id, roleId: row.role_id, joinedAt: row.joined_at });
    member.role = row.role_id ? rowToCamel({ id: row.role_id, name: row.role_name, permissions: row.role_permissions }) : null;
    return member;
  }

  async getDoohCampaigns() {
    return rowsToCamel(this.db.prepare('SELECT * FROM dooh_campaigns ORDER BY created_at DESC').all());
  }

  async getDoohCampaign(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_campaigns WHERE id = ?').get(id));
  }

  async getDoohCampaignsByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM dooh_campaigns WHERE team_id = ?').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM dooh_campaigns WHERE owner_id = ? AND team_id IS NULL').all(ownerId));
  }

  async createDoohCampaign(campaign: DataRecord) {
    const { sql, values } = buildInsertSql('dooh_campaigns', campaign);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_campaigns WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateDoohCampaign(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('dooh_campaigns', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_campaigns WHERE id = ?').get(id));
  }

  async deleteDoohCampaign(id: number) {
    this.db.prepare('DELETE FROM dooh_campaigns WHERE id = ?').run(id);
  }

  async getDoohAdSlots() {
    return rowsToCamel(this.db.prepare('SELECT * FROM dooh_ad_slots ORDER BY created_at DESC').all());
  }

  async getDoohAdSlotsByScreen(screenId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM dooh_ad_slots WHERE screen_id = ?').all(screenId));
  }

  async getDoohAdSlot(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_ad_slots WHERE id = ?').get(id));
  }

  async getAdSlotsByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) {
      return rowsToCamel(this.db.prepare(`
        SELECT das.* FROM dooh_ad_slots das
        JOIN screens s ON das.screen_id = s.id
        WHERE s.team_id = ?
      `).all(teamId));
    }
    return rowsToCamel(this.db.prepare(`
      SELECT das.* FROM dooh_ad_slots das
      JOIN screens s ON das.screen_id = s.id
      WHERE s.owner_id = ? AND s.team_id IS NULL
    `).all(ownerId));
  }

  async createDoohAdSlot(slot: DataRecord) {
    const { sql, values } = buildInsertSql('dooh_ad_slots', slot);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_ad_slots WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateDoohAdSlot(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('dooh_ad_slots', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_ad_slots WHERE id = ?').get(id));
  }

  async deleteDoohAdSlot(id: number) {
    this.db.prepare('DELETE FROM dooh_ad_slots WHERE id = ?').run(id);
  }

  async createDoohImpression(impression: DataRecord) {
    const { sql, values } = buildInsertSql('dooh_impressions', impression);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_impressions WHERE id = ?').get(info.lastInsertRowid));
  }

  async getDoohImpressions(campaignId?: number, screenId?: number, limit = 100) {
    let where = '1=1';
    const params: any[] = [];
    if (campaignId) { where += ' AND campaign_id = ?'; params.push(campaignId); }
    if (screenId) { where += ' AND screen_id = ?'; params.push(screenId); }
    return rowsToCamel(this.db.prepare(`SELECT * FROM dooh_impressions WHERE ${where} ORDER BY played_at DESC LIMIT ?`).all(...params, limit));
  }

  async getImpressionsByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) {
      return rowsToCamel(this.db.prepare(`
        SELECT di.* FROM dooh_impressions di
        JOIN dooh_campaigns dc ON di.campaign_id = dc.id
        WHERE dc.team_id = ?
      `).all(teamId));
    }
    return rowsToCamel(this.db.prepare(`
      SELECT di.* FROM dooh_impressions di
      JOIN dooh_campaigns dc ON di.campaign_id = dc.id
      WHERE dc.owner_id = ?
    `).all(ownerId));
  }

  async getDoohAdRequests(subscriberId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM dooh_ad_requests WHERE team_id = ? ORDER BY created_at DESC').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM dooh_ad_requests WHERE subscriber_id = ? AND team_id IS NULL ORDER BY created_at DESC').all(subscriberId));
  }

  async getDoohAdRequest(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_ad_requests WHERE id = ?').get(id));
  }

  async updateDoohAdRequest(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('dooh_ad_requests', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_ad_requests WHERE id = ?').get(id));
  }

  async getLicensesBySubscriber(subscriberId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM licenses WHERE subscriber_id = ?').all(subscriberId));
  }

  async getLicenseByScreenId(screenId: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM licenses WHERE screen_id = ?').get(screenId));
  }

  async getLicense(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM licenses WHERE id = ?').get(id));
  }

  async assignLicenseToScreen(licenseId: number, screenId: number) {
    this.db.prepare('UPDATE licenses SET screen_id = ? WHERE id = ?').run(screenId, licenseId);
    return rowToCamel(this.db.prepare('SELECT * FROM licenses WHERE id = ?').get(licenseId))!;
  }

  async getAvailableLicenses(subscriberId: number) {
    return rowsToCamel(this.db.prepare("SELECT * FROM licenses WHERE subscriber_id = ? AND screen_id IS NULL AND status = 'active'").all(subscriberId));
  }

  async getSubscriptionGroupsBySubscriber(subscriberId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM subscription_groups WHERE subscriber_id = ?').all(subscriberId));
  }

  async getDesignTemplates() {
    return rowsToCamel(this.db.prepare('SELECT * FROM design_templates WHERE is_published = 1 ORDER BY name').all());
  }

  async getDesignTemplate(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM design_templates WHERE id = ?').get(id));
  }

  async getDesignTemplatesByCategory(categoryId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM design_templates WHERE category_id = ? AND is_published = 1').all(categoryId));
  }

  async getTemplateCategories() {
    return rowsToCamel(this.db.prepare('SELECT * FROM template_categories ORDER BY sort_order').all());
  }

  async getWidgetCategories() {
    return rowsToCamel(this.db.prepare('SELECT * FROM widget_categories ORDER BY sort_order').all());
  }

  async getWidgetDefinitions() {
    return rowsToCamel(this.db.prepare('SELECT * FROM widget_definitions ORDER BY sort_order').all());
  }

  async getActiveWidgetDefinitions() {
    return rowsToCamel(this.db.prepare('SELECT * FROM widget_definitions WHERE is_active = 1 ORDER BY sort_order').all());
  }

  async getSetting(key: string) {
    const row = this.db.prepare('SELECT value FROM app_settings WHERE key = ?').get(key) as any;
    return row?.value || null;
  }

  async setSetting(key: string, value: string) {
    this.db.prepare('INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, datetime("now"))').run(key, value);
  }

  async getOnboardingProgress(opts: OnboardingQuery) {
    let where = 'target_type = ?';
    const params: any[] = [opts.targetType];
    if (opts.subscriberId) { where += ' AND subscriber_id = ?'; params.push(opts.subscriberId); }
    return rowToCamel(this.db.prepare(`SELECT * FROM onboarding_progress WHERE ${where}`).get(...params));
  }

  async upsertOnboardingProgress(progress: DataRecord) {
    const existing = await this.getOnboardingProgress(progress as unknown as OnboardingQuery);
    if (existing) {
      const { sql, values } = buildUpdateSql('onboarding_progress', existing.id as number, progress);
      this.db.prepare(sql).run(...values);
      return rowToCamel(this.db.prepare('SELECT * FROM onboarding_progress WHERE id = ?').get(existing.id));
    }
    const { sql, values } = buildInsertSql('onboarding_progress', progress);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM onboarding_progress WHERE id = ?').get(info.lastInsertRowid));
  }

  async completeOnboardingStep(opts: OnboardingQuery) {
    const progress = await this.getOnboardingProgress(opts);
    const steps = progress?.completedSteps || [];
    if (!steps.includes(opts.step)) steps.push(opts.step);
    return this.upsertOnboardingProgress({ ...opts, completedSteps: steps });
  }

  async dismissOnboarding(opts: OnboardingQuery) {
    const progress = await this.getOnboardingProgress(opts);
    if (progress) {
      this.db.prepare('UPDATE onboarding_progress SET dismissed = 1 WHERE id = ?').run(progress.id);
    }
  }

  async getPricingCards(visibleOnly = false) {
    if (visibleOnly) return rowsToCamel(this.db.prepare('SELECT * FROM pricing_cards WHERE is_visible = 1 ORDER BY display_order').all());
    return rowsToCamel(this.db.prepare('SELECT * FROM pricing_cards ORDER BY display_order').all());
  }

  async getLayoutTemplates(subscriberId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM layout_templates WHERE subscriber_id = ?').all(subscriberId));
  }

  async getLayoutTemplate(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM layout_templates WHERE id = ?').get(id));
  }

  async createLayoutTemplate(template: DataRecord) {
    const { sql, values } = buildInsertSql('layout_templates', template);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM layout_templates WHERE id = ?').get(info.lastInsertRowid));
  }

  async deleteLayoutTemplate(id: number) {
    this.db.prepare('DELETE FROM layout_templates WHERE id = ?').run(id);
  }

  async getSmartQrCodes(subscriberId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM smart_qr_codes WHERE subscriber_id = ? ORDER BY created_at DESC').all(subscriberId));
  }

  async getSmartQrCode(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM smart_qr_codes WHERE id = ?').get(id));
  }

  async getSmartQrCodeByShortCode(shortCode: string) {
    return rowToCamel(this.db.prepare('SELECT * FROM smart_qr_codes WHERE short_code = ?').get(shortCode));
  }

  async createSmartQrCode(qr: DataRecord) {
    const { sql, values } = buildInsertSql('smart_qr_codes', qr);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM smart_qr_codes WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateSmartQrCode(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('smart_qr_codes', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM smart_qr_codes WHERE id = ?').get(id));
  }

  async deleteSmartQrCode(id: number) {
    this.db.prepare('DELETE FROM smart_qr_codes WHERE id = ?').run(id);
  }

  async incrementSmartQrScanCount(id: number) {
    this.db.prepare('UPDATE smart_qr_codes SET scan_count = scan_count + 1 WHERE id = ?').run(id);
  }

  async getApprovalLogs(itemType: string, itemId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM approval_logs WHERE item_type = ? AND item_id = ? ORDER BY created_at DESC').all(itemType, itemId));
  }

  async createApprovalLog(log: DataRecord) {
    const { sql, values } = buildInsertSql('approval_logs', log);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM approval_logs WHERE id = ?').get(info.lastInsertRowid));
  }

  async getDirectoryVenues() {
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_venues ORDER BY name').all());
  }

  async getDirectoryVenuesByOwner(ownerId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM directory_venues WHERE team_id = ?').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_venues WHERE owner_id = ? AND team_id IS NULL').all(ownerId));
  }

  async getDirectoryVenue(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM directory_venues WHERE id = ?').get(id));
  }

  async createDirectoryVenue(venue: DataRecord) {
    const { sql, values } = buildInsertSql('directory_venues', venue);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_venues WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateDirectoryVenue(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('directory_venues', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_venues WHERE id = ?').get(id));
  }

  async deleteDirectoryVenue(id: number) {
    this.db.prepare('DELETE FROM directory_venues WHERE id = ?').run(id);
  }

  async getDirectoryFloors(venueId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_floors WHERE venue_id = ? ORDER BY level').all(venueId));
  }

  async getDirectoryFloor(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM directory_floors WHERE id = ?').get(id));
  }

  async createDirectoryFloor(floor: DataRecord) {
    const { sql, values } = buildInsertSql('directory_floors', floor);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_floors WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateDirectoryFloor(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('directory_floors', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_floors WHERE id = ?').get(id));
  }

  async deleteDirectoryFloor(id: number) {
    this.db.prepare('DELETE FROM directory_floors WHERE id = ?').run(id);
  }

  async getDirectoryCategories(venueId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_categories WHERE venue_id = ? ORDER BY sort_order').all(venueId));
  }

  async getDirectoryCategory(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM directory_categories WHERE id = ?').get(id));
  }

  async createDirectoryCategory(category: DataRecord) {
    const { sql, values } = buildInsertSql('directory_categories', category);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_categories WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateDirectoryCategory(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('directory_categories', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_categories WHERE id = ?').get(id));
  }

  async deleteDirectoryCategory(id: number) {
    this.db.prepare('DELETE FROM directory_categories WHERE id = ?').run(id);
  }

  async getDirectoryStores(venueId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_stores WHERE venue_id = ? ORDER BY name').all(venueId));
  }

  async getDirectoryStore(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM directory_stores WHERE id = ?').get(id));
  }

  async createDirectoryStore(store: DataRecord) {
    const { sql, values } = buildInsertSql('directory_stores', store);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_stores WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateDirectoryStore(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('directory_stores', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_stores WHERE id = ?').get(id));
  }

  async deleteDirectoryStore(id: number) {
    this.db.prepare('DELETE FROM directory_stores WHERE id = ?').run(id);
  }

  async searchDirectoryStores(venueId: number, query: string) {
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_stores WHERE venue_id = ? AND name LIKE ? ORDER BY name').all(venueId, `%${query}%`));
  }

  async getDirectoryPromotions(venueId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_promotions WHERE venue_id = ?').all(venueId));
  }

  async getActiveDirectoryPromotions(venueId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM directory_promotions WHERE venue_id = ? AND active = 1').all(venueId));
  }

  async getDirectoryPromotion(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM directory_promotions WHERE id = ?').get(id));
  }

  async createDirectoryPromotion(promo: DataRecord) {
    const { sql, values } = buildInsertSql('directory_promotions', promo);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_promotions WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateDirectoryPromotion(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('directory_promotions', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM directory_promotions WHERE id = ?').get(id));
  }

  async deleteDirectoryPromotion(id: number) {
    this.db.prepare('DELETE FROM directory_promotions WHERE id = ?').run(id);
  }

  async getUnsyncedChangeCount(): Promise<number> {
    return getUnpushedChangeCount();
  }

  async getDoohRevenueStats() {
    const totalRevenue = (this.db.prepare('SELECT COALESCE(SUM(revenue), 0) as total FROM dooh_impressions').get() as any)?.total || 0;
    const totalImpressions = (this.db.prepare('SELECT COUNT(*) as count FROM dooh_impressions').get() as any)?.count || 0;
    const activeCampaigns = (this.db.prepare("SELECT COUNT(*) as count FROM dooh_campaigns WHERE status = 'active'").get() as any)?.count || 0;
    const optedInScreens = (this.db.prepare('SELECT COUNT(DISTINCT screen_id) as count FROM dooh_ad_slots WHERE enabled = 1').get() as any)?.count || 0;
    return {
      totalRevenue, totalImpressions, activeCampaigns, optedInScreens,
      revenueByDay: [], topScreens: [], topCampaigns: [],
    };
  }

  async getUptimeReport(screenId: number, days: number) {
    return [];
  }

  async getFleetUptimeReport(ownerId?: number, days?: number) {
    return [];
  }

  async getMarketplaceListingsByOwner(subscriberId: number, teamId?: number | null) {
    if (teamId) return rowsToCamel(this.db.prepare('SELECT * FROM dooh_marketplace_listings WHERE team_id = ?').all(teamId));
    return rowsToCamel(this.db.prepare('SELECT * FROM dooh_marketplace_listings WHERE subscriber_id = ?').all(subscriberId));
  }

  async getMarketplaceListingByScreen(screenId: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_marketplace_listings WHERE screen_id = ?').get(screenId));
  }

  async createMarketplaceListing(listing: DataRecord) {
    const { sql, values } = buildInsertSql('dooh_marketplace_listings', listing);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_marketplace_listings WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateMarketplaceListing(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('dooh_marketplace_listings', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM dooh_marketplace_listings WHERE id = ?').get(id));
  }

  async deleteMarketplaceListing(id: number) {
    this.db.prepare('DELETE FROM dooh_marketplace_listings WHERE id = ?').run(id);
  }

  async getTeamCategoriesByOwner(ownerId: number) {
    return rowsToCamel(this.db.prepare('SELECT * FROM team_categories WHERE owner_id = ? ORDER BY sort_order').all(ownerId));
  }

  async getTeamCategory(id: number) {
    return rowToCamel(this.db.prepare('SELECT * FROM team_categories WHERE id = ?').get(id));
  }

  async createTeamCategory(category: DataRecord) {
    const { sql, values } = buildInsertSql('team_categories', category);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_categories WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateTeamCategory(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('team_categories', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_categories WHERE id = ?').get(id));
  }

  async deleteTeamCategory(id: number) {
    this.db.prepare('DELETE FROM team_categories WHERE id = ?').run(id);
  }

  async getTeamScreens(teamId: number) {
    const rows = this.db.prepare(`
      SELECT ts.*, s.name as screen_name, s.pairing_code, s.is_online, s.platform
      FROM team_screens ts
      LEFT JOIN screens s ON ts.screen_id = s.id
      WHERE ts.team_id = ?
    `).all(teamId);
    return rows.map((r: any) => {
      const ts = rowToCamel({ id: r.id, teamId: r.team_id, screenId: r.screen_id, accessType: r.access_type, assignedAt: r.assigned_at });
      ts.screen = rowToCamel({ id: r.screen_id, name: r.screen_name, pairingCode: r.pairing_code, isOnline: r.is_online, platform: r.platform });
      return ts;
    });
  }

  async assignScreenToTeam(assignment: DataRecord) {
    const { sql, values } = buildInsertSql('team_screens', assignment);
    const info = this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_screens WHERE id = ?').get(info.lastInsertRowid));
  }

  async updateTeamScreen(id: number, updates: DataRecord) {
    const { sql, values } = buildUpdateSql('team_screens', id, updates);
    this.db.prepare(sql).run(...values);
    return rowToCamel(this.db.prepare('SELECT * FROM team_screens WHERE id = ?').get(id));
  }

  async removeScreenFromTeam(id: number) {
    this.db.prepare('DELETE FROM team_screens WHERE id = ?').run(id);
  }

  async deleteEmergencyAlertConfig(subscriberId: number) {
    this.db.prepare('DELETE FROM emergency_alert_configs WHERE subscriber_id = ?').run(subscriberId);
  }
}
