import Database from 'better-sqlite3';
import path from 'path';
import { app } from 'electron';

let db: Database.Database;

const SYNCED_TABLES = [
  'screens',
  'playlists',
  'contents',
  'schedules',
  'playlist_items',
  'smart_triggers',
  'video_walls',
  'video_wall_screens',
  'content_folders',
  'layout_templates',
  'screen_groups',
  'screen_group_members',
  'emergency_alert_configs',
  'custom_alert_feeds',
  'dooh_campaigns',
  'dooh_ad_slots',
  'dooh_impressions',
  'kiosks',
  'broadcasts',
  'smart_qr_codes',
  'subscribers',
  'teams',
  'team_roles',
  'team_members',
  'team_screens',
  'team_categories',
  'licenses',
  'subscription_groups',
  'notifications',
  'template_categories',
  'design_templates',
  'widget_categories',
  'widget_definitions',
  'dooh_ad_requests',
  'dooh_marketplace_listings',
  'dooh_marketplace_listing_slots',
  'approval_logs',
  'directory_venues',
  'directory_floors',
  'directory_categories',
  'directory_stores',
  'directory_promotions',
  'directory_kiosk_positions',
  'directory_analytics',
];

function camelToSnake(str: string): string {
  return str.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

let tableColumnsCache: Map<string, Set<string>> | null = null;

function getTableColumns(tableName: string): Set<string> {
  if (!tableColumnsCache) {
    tableColumnsCache = new Map();
  }
  if (tableColumnsCache.has(tableName)) return tableColumnsCache.get(tableName)!;
  const cols = new Set<string>();
  try {
    const info = db.pragma(`table_info(${tableName})`) as Array<{ name: string }>;
    for (const row of info) cols.add(row.name);
  } catch { /* table may not exist yet */ }
  tableColumnsCache.set(tableName, cols);
  return cols;
}

function normalizePayloadForTable(tableName: string, data: Record<string, any>): Record<string, any> {
  const validCols = getTableColumns(tableName);
  const normalized: Record<string, any> = {};
  for (const [key, value] of Object.entries(data)) {
    const snakeKey = camelToSnake(key);
    const colName = validCols.has(key) ? key : validCols.has(snakeKey) ? snakeKey : null;
    if (!colName) continue;
    if (value === null || value === undefined) {
      normalized[colName] = null;
    } else if (typeof value === 'boolean') {
      normalized[colName] = value ? 1 : 0;
    } else if (typeof value === 'object') {
      normalized[colName] = JSON.stringify(value);
    } else {
      normalized[colName] = value;
    }
  }
  return normalized;
}

export function getDb(): Database.Database {
  return db;
}

export function initDatabase(overridePath?: string) {
  // overridePath lets tests / non-Electron callers pass an explicit DB path
  // (e.g. ':memory:') without depending on Electron's userData directory.
  const dbPath = overridePath ?? path.join(app.getPath('userData'), 'digipal-local.db');
  db = new Database(dbPath);

  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS hub_id_map (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      table_name TEXT NOT NULL,
      local_id INTEGER NOT NULL,
      cloud_id INTEGER NOT NULL,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(table_name, local_id),
      UNIQUE(table_name, cloud_id)
    );

    CREATE TABLE IF NOT EXISTS subscribers (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT,
      phone TEXT,
      company TEXT,
      status TEXT DEFAULT 'active',
      plan TEXT DEFAULT 'free',
      stripe_customer_id TEXT,
      notes TEXT,
      tags TEXT,
      email_verified INTEGER DEFAULT 0,
      promotional_emails INTEGER DEFAULT 0,
      consent_tos_at TEXT,
      consent_privacy_at TEXT,
      totp_secret TEXT,
      totp_enabled INTEGER DEFAULT 0,
      account_role TEXT DEFAULT 'owner',
      must_change_password INTEGER DEFAULT 0,
      storage_over_limit_since TEXT,
      deactivated_at TEXT,
      downgraded_to_free_at TEXT,
      dooh_portal_enabled INTEGER DEFAULT 0,
      dooh_portal_slug TEXT UNIQUE,
      dooh_ad_guidelines TEXT,
      dooh_tax_rate INTEGER DEFAULT 0,
      dooh_tax_label TEXT DEFAULT 'Tax',
      stripe_connect_account_id TEXT,
      stripe_connect_status TEXT DEFAULT 'none',
      platform_fee_model TEXT DEFAULT 'none',
      platform_fee_percent INTEGER DEFAULT 0,
      platform_fee_monthly INTEGER DEFAULT 0,
      dooh_addon_active INTEGER DEFAULT 0,
      dooh_addon_stripe_subscription_id TEXT,
      dooh_free_access INTEGER DEFAULT 0,
      dooh_addon_price_override INTEGER,
      country TEXT,
      province TEXT,
      tax_location_override INTEGER DEFAULT 0,
      avatar_url TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS screens (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      pairing_code TEXT UNIQUE NOT NULL,
      is_paired INTEGER DEFAULT 0,
      content_id INTEGER,
      playlist_id INTEGER,
      owner_id INTEGER,
      team_id INTEGER,
      plan TEXT DEFAULT 'free',
      subscription_status TEXT DEFAULT 'active',
      stripe_subscription_id TEXT,
      last_ping_at TEXT DEFAULT (datetime('now')),
      is_online INTEGER DEFAULT 0,
      kiosk_mode INTEGER DEFAULT 0,
      platform TEXT,
      app_version TEXT,
      device_info TEXT,
      last_heartbeat TEXT,
      uptime_seconds INTEGER DEFAULT 0,
      layout TEXT DEFAULT 'single',
      layout_zones TEXT,
      custom_layout_config TEXT,
      display_mode TEXT DEFAULT 'auto',
      display_width INTEGER,
      display_height INTEGER,
      rotation INTEGER DEFAULT 0,
      video_wall_id INTEGER,
      video_wall_row INTEGER,
      video_wall_col INTEGER,
      timezone TEXT,
      connection_mode TEXT,
      license_status TEXT DEFAULT 'none',
      last_seen_at TEXT,
      model TEXT,
      os_version TEXT,
      resolution TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS content_folders (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      parent_id INTEGER,
      owner_id INTEGER,
      team_id INTEGER,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS contents (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      type TEXT NOT NULL,
      data TEXT NOT NULL DEFAULT '',
      mime_type TEXT,
      file_size INTEGER,
      canvas_data TEXT,
      settings TEXT,
      folder_id INTEGER,
      owner_id INTEGER,
      team_id INTEGER,
      approval_status TEXT DEFAULT 'approved',
      approval_note TEXT,
      approved_by INTEGER,
      approved_at TEXT,
      submitted_by INTEGER,
      submitted_at TEXT,
      url TEXT,
      local_path TEXT,
      thumbnail_url TEXT,
      duration INTEGER,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS playlists (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      owner_id INTEGER,
      team_id INTEGER,
      approval_status TEXT DEFAULT 'approved',
      approval_note TEXT,
      approved_by INTEGER,
      approved_at TEXT,
      submitted_by INTEGER,
      submitted_at TEXT,
      subscriber_id INTEGER,
      data TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS playlist_items (
      id INTEGER PRIMARY KEY,
      playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
      content_id INTEGER NOT NULL REFERENCES contents(id) ON DELETE CASCADE,
      duration INTEGER NOT NULL DEFAULT 10,
      sort_order INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS schedules (
      id INTEGER PRIMARY KEY,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      name TEXT NOT NULL DEFAULT '',
      content_id INTEGER,
      playlist_id INTEGER,
      video_wall_id INTEGER,
      start_time INTEGER NOT NULL,
      end_time INTEGER NOT NULL,
      days_of_week TEXT NOT NULL,
      priority INTEGER DEFAULT 0,
      enabled INTEGER DEFAULT 1,
      repeat_mode TEXT DEFAULT 'weekly',
      start_date TEXT,
      end_date TEXT,
      timezone TEXT DEFAULT 'UTC',
      interval_value INTEGER,
      interval_unit TEXT,
      interval_duration INTEGER,
      use_default INTEGER DEFAULT 0,
      team_id INTEGER,
      approval_status TEXT DEFAULT 'approved',
      approval_note TEXT,
      approved_by INTEGER,
      approved_at TEXT,
      submitted_by INTEGER,
      submitted_at TEXT,
      subscriber_id INTEGER,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS screen_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      type TEXT NOT NULL,
      data TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS screen_commands (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      command TEXT NOT NULL,
      payload TEXT,
      status TEXT DEFAULT 'pending',
      issued_at TEXT DEFAULT (datetime('now')),
      acknowledged_at TEXT
    );

    CREATE TABLE IF NOT EXISTS screen_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      image_data TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS screen_telemetry (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      avg_load_time_ms INTEGER,
      max_load_time_ms INTEGER,
      error_count INTEGER DEFAULT 0,
      error_details TEXT,
      connection_drops INTEGER DEFAULT 0,
      bytes_loaded INTEGER DEFAULT 0,
      content_loads INTEGER DEFAULT 0,
      reported_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS smart_triggers (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      trigger_type TEXT NOT NULL,
      conditions TEXT NOT NULL,
      target_content_id INTEGER,
      target_playlist_id INTEGER,
      fallback_content_id INTEGER,
      enabled INTEGER NOT NULL DEFAULT 1,
      priority INTEGER NOT NULL DEFAULT 0,
      cooldown_seconds INTEGER NOT NULL DEFAULT 30,
      last_triggered TEXT,
      device_fingerprint TEXT,
      owner_id INTEGER,
      team_id INTEGER,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS trigger_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      trigger_id INTEGER NOT NULL REFERENCES smart_triggers(id) ON DELETE CASCADE,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      input_type TEXT NOT NULL,
      input_data TEXT,
      ai_analysis TEXT,
      matched INTEGER DEFAULT 0,
      action_taken TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS video_walls (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      rows INTEGER NOT NULL DEFAULT 2,
      cols INTEGER NOT NULL DEFAULT 2,
      content_id INTEGER,
      playlist_id INTEGER,
      owner_id INTEGER,
      team_id INTEGER,
      bezel_compensation INTEGER DEFAULT 0,
      playback_epoch TEXT,
      layout_mode TEXT NOT NULL DEFAULT 'grid',
      canvas_ratio TEXT DEFAULT '16:9',
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS video_wall_screens (
      id INTEGER PRIMARY KEY,
      wall_id INTEGER NOT NULL REFERENCES video_walls(id) ON DELETE CASCADE,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      row INTEGER NOT NULL,
      col INTEGER NOT NULL,
      sync_offset INTEGER NOT NULL DEFAULT 0,
      sync_locked INTEGER NOT NULL DEFAULT 0,
      pos_x REAL NOT NULL DEFAULT 0,
      pos_y REAL NOT NULL DEFAULT 0,
      size_w REAL NOT NULL DEFAULT 100,
      size_h REAL NOT NULL DEFAULT 100
    );

    CREATE TABLE IF NOT EXISTS team_categories (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      owner_id INTEGER,
      parent_id INTEGER,
      sort_order INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS teams (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      owner_id INTEGER,
      category_id INTEGER,
      require_approval INTEGER DEFAULT 1,
      storage_over_limit_since TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS team_roles (
      id INTEGER PRIMARY KEY,
      team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      permissions TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS team_members (
      id INTEGER PRIMARY KEY,
      team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
      subscriber_id INTEGER NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
      role_id INTEGER,
      joined_at TEXT DEFAULT (datetime('now')),
      UNIQUE(team_id, subscriber_id)
    );

    CREATE TABLE IF NOT EXISTS team_screens (
      id INTEGER PRIMARY KEY,
      team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      access_type TEXT DEFAULT 'individual',
      assigned_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS app_settings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      key TEXT NOT NULL UNIQUE,
      value TEXT NOT NULL,
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS licenses (
      id INTEGER PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      plan_tier TEXT NOT NULL,
      billing_interval TEXT DEFAULT 'monthly',
      status TEXT DEFAULT 'active',
      stripe_subscription_id TEXT,
      subscription_group_id INTEGER,
      current_period_end TEXT,
      cancel_at_period_end INTEGER DEFAULT 0,
      screen_id INTEGER,
      admin_granted INTEGER DEFAULT 0,
      admin_granted_until TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS subscription_groups (
      id INTEGER PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      plan_tier TEXT NOT NULL,
      billing_interval TEXT NOT NULL,
      stripe_subscription_id TEXT,
      stripe_price_id TEXT,
      quantity INTEGER DEFAULT 0,
      status TEXT DEFAULT 'active',
      current_period_end TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS kiosks (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      data TEXT NOT NULL,
      settings TEXT,
      owner_id INTEGER,
      team_id INTEGER,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_campaigns (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      advertiser_name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'draft',
      content_id INTEGER,
      budget INTEGER NOT NULL DEFAULT 0,
      spent INTEGER NOT NULL DEFAULT 0,
      pricing_model TEXT NOT NULL DEFAULT 'per_play',
      cpm_rate INTEGER NOT NULL DEFAULT 500,
      price_per_play INTEGER NOT NULL DEFAULT 25,
      flat_rate INTEGER NOT NULL DEFAULT 5000,
      target_screen_ids TEXT,
      target_locations TEXT,
      start_date TEXT,
      end_date TEXT,
      daily_budget INTEGER,
      play_duration INTEGER NOT NULL DEFAULT 15,
      frequency INTEGER NOT NULL DEFAULT 1,
      daily_impression_target INTEGER,
      owner_id INTEGER,
      team_id INTEGER,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_ad_slots (
      id INTEGER PRIMARY KEY,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      start_time INTEGER NOT NULL DEFAULT 0,
      end_time INTEGER NOT NULL DEFAULT 1439,
      days_of_week TEXT NOT NULL,
      max_ad_minutes_per_hour INTEGER NOT NULL DEFAULT 10,
      revenue_share INTEGER NOT NULL DEFAULT 70,
      location_tag TEXT,
      price_per_slot INTEGER,
      pricing_period TEXT DEFAULT 'flat',
      ad_layout TEXT DEFAULT 'fullscreen',
      is_public INTEGER DEFAULT 0,
      continuous INTEGER DEFAULT 0,
      fill_mode TEXT DEFAULT 'repeat',
      max_campaigns INTEGER DEFAULT 10,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_impressions (
      id INTEGER PRIMARY KEY,
      campaign_id INTEGER NOT NULL REFERENCES dooh_campaigns(id) ON DELETE CASCADE,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
      ad_slot_id INTEGER,
      played_at TEXT NOT NULL DEFAULT (datetime('now')),
      duration_played INTEGER NOT NULL,
      verified INTEGER NOT NULL DEFAULT 0,
      revenue INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS dooh_ad_requests (
      id INTEGER PRIMARY KEY,
      slug TEXT NOT NULL UNIQUE,
      subscriber_id INTEGER NOT NULL,
      team_id INTEGER,
      ad_slot_id INTEGER,
      advertiser_name TEXT NOT NULL,
      advertiser_email TEXT NOT NULL,
      advertiser_phone TEXT,
      business_name TEXT,
      content_type TEXT NOT NULL,
      file_name TEXT,
      file_url TEXT,
      requested_start_date TEXT,
      requested_end_date TEXT,
      play_duration INTEGER DEFAULT 15,
      pricing_model TEXT DEFAULT 'per_play',
      total_price INTEGER,
      message TEXT,
      status TEXT DEFAULT 'pending',
      payment_method TEXT DEFAULT 'online',
      stripe_payment_intent_id TEXT,
      stripe_payment_status TEXT,
      stripe_checkout_session_id TEXT,
      refund_status TEXT,
      refund_amount INTEGER,
      refund_reason TEXT,
      refunded_at TEXT,
      rejection_reason TEXT,
      campaign_id INTEGER,
      expires_at TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_marketplace_listings (
      id INTEGER PRIMARY KEY,
      screen_id INTEGER NOT NULL UNIQUE,
      subscriber_id INTEGER NOT NULL,
      team_id INTEGER,
      enabled INTEGER DEFAULT 1,
      featured INTEGER DEFAULT 0,
      venue_name TEXT NOT NULL,
      venue_type TEXT NOT NULL,
      description TEXT,
      address TEXT,
      city TEXT,
      province_state TEXT,
      country TEXT,
      latitude TEXT,
      longitude TEXT,
      photo_urls TEXT,
      estimated_daily_footfall INTEGER,
      operating_hours TEXT,
      screen_orientation TEXT DEFAULT 'landscape',
      screen_width INTEGER,
      screen_height INTEGER,
      price_per_slot_cents INTEGER NOT NULL,
      pricing_period TEXT DEFAULT 'daily',
      min_booking_days INTEGER DEFAULT 1,
      max_booking_days INTEGER DEFAULT 365,
      play_duration_seconds INTEGER DEFAULT 15,
      ad_slot_id INTEGER,
      avg_rating INTEGER DEFAULT 0,
      review_count INTEGER DEFAULT 0,
      reliability_score INTEGER DEFAULT 100,
      total_orders INTEGER DEFAULT 0,
      total_revenue_cents INTEGER DEFAULT 0,
      admin_note TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_marketplace_listing_slots (
      id INTEGER PRIMARY KEY,
      listing_id INTEGER NOT NULL,
      ad_slot_id INTEGER NOT NULL,
      custom_price_cents INTEGER,
      custom_pricing_period TEXT,
      enabled INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_marketplace_orders (
      id INTEGER PRIMARY KEY,
      slug TEXT NOT NULL UNIQUE,
      buyer_name TEXT NOT NULL,
      buyer_email TEXT NOT NULL,
      buyer_phone TEXT,
      buyer_business TEXT,
      subtotal_cents INTEGER NOT NULL,
      tax_total_cents INTEGER DEFAULT 0,
      total_cents INTEGER NOT NULL,
      platform_fee_cents INTEGER DEFAULT 0,
      currency TEXT DEFAULT 'usd',
      status TEXT DEFAULT 'pending',
      payment_status TEXT DEFAULT 'unpaid',
      stripe_checkout_session_id TEXT,
      stripe_payment_intent_id TEXT,
      refund_status TEXT,
      refund_amount_cents INTEGER,
      refund_reason TEXT,
      refunded_at TEXT,
      refund_history TEXT,
      notes TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_marketplace_order_items (
      id INTEGER PRIMARY KEY,
      order_id INTEGER NOT NULL,
      listing_id INTEGER,
      screen_id INTEGER,
      subscriber_id INTEGER,
      venue_name TEXT NOT NULL,
      price_cents INTEGER NOT NULL,
      tax_rate_percent INTEGER DEFAULT 0,
      tax_label TEXT,
      tax_amount_cents INTEGER DEFAULT 0,
      start_date TEXT NOT NULL,
      end_date TEXT NOT NULL,
      play_duration_seconds INTEGER DEFAULT 15,
      file_url TEXT,
      content_type TEXT DEFAULT 'image',
      status TEXT DEFAULT 'pending',
      ad_slot_id INTEGER,
      dooh_ad_request_id INTEGER,
      campaign_id INTEGER,
      expected_impressions INTEGER,
      actual_impressions INTEGER DEFAULT 0,
      delivery_percent INTEGER,
      auto_refund_cents INTEGER,
      payout_status TEXT DEFAULT 'pending',
      payout_amount_cents INTEGER,
      payout_released_at TEXT,
      payout_hold_until TEXT,
      stripe_transfer_id TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_marketplace_reviews (
      id INTEGER PRIMARY KEY,
      order_item_id INTEGER NOT NULL,
      listing_id INTEGER NOT NULL,
      order_id INTEGER NOT NULL,
      buyer_name TEXT NOT NULL,
      buyer_email TEXT NOT NULL,
      rating INTEGER NOT NULL,
      comment TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_marketplace_disputes (
      id INTEGER PRIMARY KEY,
      order_item_id INTEGER NOT NULL,
      order_id INTEGER NOT NULL,
      listing_id INTEGER,
      buyer_name TEXT NOT NULL,
      buyer_email TEXT NOT NULL,
      reason TEXT NOT NULL,
      status TEXT DEFAULT 'open',
      resolution_notes TEXT,
      resolution_type TEXT,
      refund_amount_cents INTEGER,
      resolved_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dooh_impressions_daily (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      date TEXT NOT NULL,
      campaign_id INTEGER NOT NULL,
      screen_id INTEGER NOT NULL,
      impression_count INTEGER DEFAULT 0,
      total_duration_seconds INTEGER DEFAULT 0,
      verified_count INTEGER DEFAULT 0,
      total_revenue_cents INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS screen_telemetry_daily (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      date TEXT NOT NULL,
      screen_id INTEGER NOT NULL,
      avg_load_time_ms INTEGER,
      max_load_time_ms INTEGER,
      total_error_count INTEGER DEFAULT 0,
      total_connection_drops INTEGER DEFAULT 0,
      total_bytes_loaded INTEGER DEFAULT 0,
      total_content_loads INTEGER DEFAULT 0,
      report_count INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS emergency_alert_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      external_alert_id TEXT NOT NULL,
      source TEXT NOT NULL,
      alert_type TEXT NOT NULL,
      severity TEXT NOT NULL,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      regions TEXT,
      started_at TEXT NOT NULL,
      expires_at TEXT,
      subscriber_ids TEXT,
      screen_ids_delivered TEXT,
      display_mode TEXT,
      status TEXT DEFAULT 'active',
      cleared_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS instagram_tokens (
      id INTEGER PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      ig_user_id TEXT NOT NULL,
      ig_username TEXT,
      access_token TEXT NOT NULL,
      expires_at TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS template_categories (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      sort_order INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS design_templates (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      category_id INTEGER,
      thumbnail_url TEXT,
      canvas_data TEXT NOT NULL,
      canvas_width INTEGER DEFAULT 1920,
      canvas_height INTEGER DEFAULT 1080,
      is_published INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS notifications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      severity TEXT DEFAULT 'info',
      target_type TEXT,
      subscriber_id INTEGER,
      screen_id INTEGER,
      is_read INTEGER DEFAULT 0,
      action_url TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS screen_groups (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      color TEXT DEFAULT '#0d9488',
      owner_id INTEGER,
      team_id INTEGER,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS screen_group_members (
      id INTEGER PRIMARY KEY,
      group_id INTEGER NOT NULL REFERENCES screen_groups(id) ON DELETE CASCADE,
      screen_id INTEGER NOT NULL REFERENCES screens(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS onboarding_progress (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      target_type TEXT NOT NULL,
      subscriber_id INTEGER,
      admin_user_id TEXT,
      completed_steps TEXT DEFAULT '[]',
      dismissed INTEGER DEFAULT 0,
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS broadcasts (
      id INTEGER PRIMARY KEY,
      message TEXT NOT NULL,
      display_mode TEXT NOT NULL DEFAULT 'fullscreen',
      text_color TEXT DEFAULT '#ffffff',
      background_color TEXT DEFAULT '#dc2626',
      font_size TEXT DEFAULT 'large',
      font_family TEXT DEFAULT 'Inter',
      scrolling INTEGER DEFAULT 0,
      target_screen_ids TEXT,
      status TEXT NOT NULL DEFAULT 'active',
      starts_at TEXT,
      expires_at TEXT,
      owner_id INTEGER,
      team_id INTEGER,
      is_admin INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS emergency_alert_configs (
      id INTEGER PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      country TEXT NOT NULL,
      region TEXT NOT NULL,
      enabled_alert_types TEXT NOT NULL,
      target_screen_ids TEXT,
      display_mode TEXT NOT NULL DEFAULT 'fullscreen',
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS custom_alert_feeds (
      id INTEGER PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      feed_url TEXT NOT NULL,
      feed_type TEXT NOT NULL DEFAULT 'auto',
      polling_interval_minutes INTEGER NOT NULL DEFAULT 5,
      enabled INTEGER NOT NULL DEFAULT 1,
      filter_keywords TEXT,
      display_mode TEXT NOT NULL DEFAULT 'fullscreen',
      target_all_screens INTEGER NOT NULL DEFAULT 1,
      target_screen_ids TEXT,
      last_polled_at TEXT,
      last_error TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS smart_qr_codes (
      id INTEGER PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      type TEXT NOT NULL,
      data TEXT NOT NULL,
      customization TEXT,
      short_code TEXT NOT NULL UNIQUE,
      scan_count INTEGER NOT NULL DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS layout_templates (
      id INTEGER PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      config TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS widget_categories (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      sort_order INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS widget_definitions (
      id INTEGER PRIMARY KEY,
      category_id INTEGER,
      value TEXT NOT NULL UNIQUE,
      label TEXT NOT NULL,
      description TEXT,
      placeholder TEXT,
      hint TEXT,
      input_type TEXT DEFAULT 'simple',
      icon_name TEXT,
      is_active INTEGER DEFAULT 1,
      sort_order INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS approval_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_type TEXT NOT NULL,
      item_id INTEGER NOT NULL,
      action TEXT NOT NULL,
      note TEXT,
      actor_id INTEGER,
      actor_name TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS directory_venues (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      welcome_message TEXT DEFAULT 'Welcome',
      logo_url TEXT,
      owner_id INTEGER,
      team_id INTEGER,
      settings TEXT DEFAULT '{}',
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS directory_floors (
      id INTEGER PRIMARY KEY,
      venue_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      level INTEGER NOT NULL DEFAULT 0,
      floor_plan_url TEXT,
      width INTEGER DEFAULT 1920,
      height INTEGER DEFAULT 1080,
      map_data TEXT DEFAULT '{}',
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS directory_categories (
      id INTEGER PRIMARY KEY,
      venue_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      icon TEXT DEFAULT 'Store',
      color TEXT DEFAULT '#3b82f6',
      sort_order INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS directory_stores (
      id INTEGER PRIMARY KEY,
      venue_id INTEGER NOT NULL,
      floor_id INTEGER,
      category_id INTEGER,
      name TEXT NOT NULL,
      logo_url TEXT,
      description TEXT,
      location TEXT DEFAULT '{}',
      unit TEXT,
      phone TEXT,
      website TEXT,
      hours TEXT,
      tags TEXT,
      featured INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS directory_promotions (
      id INTEGER PRIMARY KEY,
      venue_id INTEGER NOT NULL,
      store_id INTEGER,
      title TEXT NOT NULL,
      description TEXT,
      image_url TEXT,
      start_date TEXT,
      end_date TEXT,
      active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS directory_kiosk_positions (
      id INTEGER PRIMARY KEY,
      venue_id INTEGER NOT NULL,
      floor_id INTEGER NOT NULL,
      x INTEGER NOT NULL DEFAULT 0,
      y INTEGER NOT NULL DEFAULT 0,
      name TEXT DEFAULT 'Main Entrance'
    );

    CREATE TABLE IF NOT EXISTS directory_analytics (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      venue_id INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      store_id INTEGER,
      category_id INTEGER,
      search_query TEXT,
      session_id TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS pricing_cards (
      id INTEGER PRIMARY KEY,
      category TEXT NOT NULL,
      name TEXT NOT NULL,
      subtitle TEXT,
      plan_tier TEXT,
      price_usd_cents INTEGER,
      pricing_unit TEXT DEFAULT 'screen',
      pricing_interval TEXT DEFAULT 'month',
      setup_fee_usd_cents INTEGER,
      setup_fee_required INTEGER DEFAULT 0,
      setup_fee_label TEXT,
      extra_unit_fee_usd_cents INTEGER,
      extra_unit_label TEXT,
      features TEXT DEFAULT '[]',
      cta_type TEXT DEFAULT 'signup',
      cta_label TEXT,
      badge TEXT,
      is_highlighted INTEGER DEFAULT 0,
      show_branding_note INTEGER DEFAULT 0,
      show_trial_note INTEGER DEFAULT 0,
      display_order INTEGER DEFAULT 0,
      is_visible INTEGER DEFAULT 1,
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS sync_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_sync_at TEXT,
      last_standard_sync_at TEXT,
      last_lazy_sync_at TEXT,
      hub_token TEXT,
      cloud_url TEXT,
      subscriber_id INTEGER,
      last_cloud_contact_at TEXT,
      hub_revoked INTEGER DEFAULT 0,
      hub_name TEXT,
      sync_enabled INTEGER DEFAULT 1
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

    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      subscriber_id INTEGER NOT NULL,
      expires_at INTEGER NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_sessions_subscriber ON sessions(subscriber_id);
    CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

    CREATE TABLE IF NOT EXISTS local_hubs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      hub_id INTEGER,
      subscriber_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      ip_address TEXT,
      port INTEGER DEFAULT 8787,
      is_online INTEGER DEFAULT 0,
      version TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      last_seen_at TEXT
    );

    CREATE TABLE IF NOT EXISTS error_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT DEFAULT (datetime('now')),
      level TEXT NOT NULL DEFAULT 'error',
      source TEXT NOT NULL DEFAULT 'api',
      route TEXT,
      method TEXT,
      status_code INTEGER,
      message TEXT NOT NULL,
      stack TEXT,
      context TEXT,
      sent_to_cloud INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_error_logs_sent ON error_logs(sent_to_cloud) WHERE sent_to_cloud = 0;
    CREATE INDEX IF NOT EXISTS idx_error_logs_timestamp ON error_logs(timestamp);

    CREATE TABLE IF NOT EXISTS media_downloads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      object_path TEXT NOT NULL UNIQUE,
      local_filename TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      content_id INTEGER,
      field TEXT NOT NULL DEFAULT 'data',
      file_size INTEGER,
      retry_count INTEGER DEFAULT 0,
      error TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      completed_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_media_downloads_status ON media_downloads(status);
    CREATE INDEX IF NOT EXISTS idx_media_downloads_object_path ON media_downloads(object_path);

    CREATE TABLE IF NOT EXISTS sync_conflicts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      table_name TEXT NOT NULL,
      record_id INTEGER NOT NULL,
      operation TEXT NOT NULL,
      local_version TEXT,
      incoming_version TEXT,
      resolution TEXT NOT NULL,
      local_updated_at TEXT,
      incoming_updated_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_sync_conflicts_table ON sync_conflicts(table_name);
    CREATE INDEX IF NOT EXISTS idx_sync_conflicts_created ON sync_conflicts(created_at);
  `);

  runMigrations(db);
  setupChangeTriggers(db);

  return db;
}

function runMigrations(database: Database.Database) {
  const addColumnIfMissing = (table: string, column: string, definition: string) => {
    const cols = database.pragma(`table_info(${table})`) as Array<{ name: string }>;
    if (!cols.some((c) => c.name === column)) {
      database.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  };

  addColumnIfMissing('screens', 'license_status', "TEXT DEFAULT 'none'");
  addColumnIfMissing('screens', 'pairing_grace_until', 'TEXT');
  addColumnIfMissing('media_downloads', 'bytes_downloaded', 'INTEGER DEFAULT 0');
  addColumnIfMissing('media_downloads', 'expected_size', 'INTEGER');
  addColumnIfMissing('sync_state', 'last_cloud_contact_at', 'TEXT');
  addColumnIfMissing('sync_state', 'hub_revoked', 'INTEGER DEFAULT 0');
  addColumnIfMissing('sync_state', 'hub_name', 'TEXT');
  addColumnIfMissing('sync_state', 'sync_enabled', 'INTEGER DEFAULT 1');
  addColumnIfMissing('sync_state', 'cloud_session_cookie', 'TEXT');

  for (const table of SYNCED_TABLES) {
    database.exec(`DROP TRIGGER IF EXISTS trg_${table}_insert`);
    database.exec(`DROP TRIGGER IF EXISTS trg_${table}_update`);
    database.exec(`DROP TRIGGER IF EXISTS trg_${table}_delete`);
  }

  const { migratePlaintextSyncState } = require('../server/crypto-utils');
  migratePlaintextSyncState(db);
}

function setupChangeTriggers(database: Database.Database) {
  for (const table of SYNCED_TABLES) {
    const cols = database.pragma(`table_info(${table})`) as Array<{ name: string }>;
    const hasUpdatedAt = cols.some((c) => c.name === 'updated_at');

    const updatePayload = hasUpdatedAt
      ? `json_object('id', NEW.id, 'updated_at', NEW.updated_at)`
      : `json_object('id', NEW.id)`;

    database.exec(`
      CREATE TRIGGER IF NOT EXISTS trg_${table}_insert AFTER INSERT ON ${table}
      BEGIN
        INSERT INTO local_changes (table_name, record_id, operation, payload)
        VALUES ('${table}', NEW.id, 'INSERT', json_object('id', NEW.id));
      END;

      CREATE TRIGGER IF NOT EXISTS trg_${table}_update AFTER UPDATE ON ${table}
      BEGIN
        INSERT INTO local_changes (table_name, record_id, operation, payload)
        VALUES ('${table}', NEW.id, 'UPDATE', ${updatePayload});
      END;

      CREATE TRIGGER IF NOT EXISTS trg_${table}_delete AFTER DELETE ON ${table}
      BEGIN
        INSERT INTO local_changes (table_name, record_id, operation, payload)
        VALUES ('${table}', OLD.id, 'DELETE', json_object('id', OLD.id));
      END;
    `);
  }
}

export { SYNCED_TABLES };

export function getConfig(key: string): string | undefined {
  const row = db.prepare('SELECT value FROM config WHERE key = ?').get(key) as { value: string } | undefined;
  return row?.value;
}

export function setConfig(key: string, value: string): void {
  db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run(key, value);
}

export function getSyncState() {
  const { decryptSyncStateFields } = require('../server/crypto-utils');
  const row = db.prepare('SELECT * FROM sync_state WHERE id = 1').get() as any;
  return decryptSyncStateFields(row);
}

export function updateSyncState(updates: Record<string, any>) {
  const { encryptSyncStateFields } = require('../server/crypto-utils');
  const encrypted = encryptSyncStateFields(updates);
  const keys = Object.keys(encrypted);
  const sets = keys.map(k => `${k} = ?`).join(', ');
  db.prepare(`UPDATE sync_state SET ${sets} WHERE id = 1`).run(...keys.map(k => encrypted[k]));
}

export function getUnpushedChanges(): any[] {
  return db.prepare('SELECT * FROM local_changes WHERE pushed = 0 ORDER BY id ASC LIMIT 100').all();
}

export function getUnpushedChangeCount(): number {
  const row = db.prepare('SELECT COUNT(*) as count FROM local_changes WHERE pushed = 0').get() as any;
  return row?.count || 0;
}

export function markChangesPushed(ids: number[]): void {
  if (ids.length === 0) return;
  const placeholders = ids.map(() => '?').join(',');
  db.prepare(`UPDATE local_changes SET pushed = 1 WHERE id IN (${placeholders})`).run(...ids);
}

// Hub-local columns that must NEVER be pushed up to the cloud (the cloud
// schema does not have them). Keyed by table name.
const HUB_LOCAL_COLUMNS: Record<string, string[]> = {
  screens: ['pairing_grace_until'],
};

export function getFullRow(tableName: string, recordId: number): any {
  if (!SYNCED_TABLES.includes(tableName)) return null;
  const row = db.prepare(`SELECT * FROM ${tableName} WHERE id = ?`).get(recordId) as any;
  if (row && HUB_LOCAL_COLUMNS[tableName]) {
    for (const col of HUB_LOCAL_COLUMNS[tableName]) {
      delete row[col];
    }
  }
  return row;
}

export function upsertRow(tableName: string, data: Record<string, any>): void {
  if (!SYNCED_TABLES.includes(tableName)) return;
  const normalized = normalizePayloadForTable(tableName, data);
  const keys = Object.keys(normalized);
  if (keys.length === 0) return;
  const cols = keys.join(', ');
  const placeholders = keys.map(() => '?').join(', ');
  const updates = keys.filter(k => k !== 'id').map(k => `${k} = excluded.${k}`).join(', ');
  const values = keys.map(k => normalized[k]);
  const sql = `INSERT INTO ${tableName} (${cols}) VALUES (${placeholders}) ON CONFLICT(id) DO UPDATE SET ${updates}`;
  try {
    db.prepare(sql).run(...values);
  } catch (e: any) {
    if (e.message && e.message.includes('UNIQUE constraint failed')) {
      const match = e.message.match(/UNIQUE constraint failed: \w+\.(\w+)/);
      if (match) {
        const conflictCol = match[1];
        if (conflictCol !== 'id' && normalized[conflictCol] !== undefined) {
          db.prepare(`DELETE FROM ${tableName} WHERE ${conflictCol} = ? AND id != ?`).run(normalized[conflictCol], normalized['id']);
          db.prepare(sql).run(...values);
          return;
        }
      }
      if (normalized['id'] !== undefined) {
        db.prepare(`DELETE FROM ${tableName} WHERE id = ?`).run(normalized['id']);
        db.prepare(`INSERT INTO ${tableName} (${cols}) VALUES (${placeholders})`).run(...values);
        return;
      }
    }
    throw e;
  }
}

export function deleteRow(tableName: string, recordId: number): void {
  if (!SYNCED_TABLES.includes(tableName)) return;
  db.prepare(`DELETE FROM ${tableName} WHERE id = ?`).run(recordId);
}

export function isHubRevoked(): boolean {
  const state = getSyncState();
  return state?.hub_revoked === 1;
}

export function setHubRevoked(revoked: boolean): void {
  db.prepare('UPDATE sync_state SET hub_revoked = ? WHERE id = 1').run(revoked ? 1 : 0);
}

export function updateCloudContactTime(): void {
  const isoNow = new Date().toISOString();
  db.prepare('UPDATE sync_state SET last_cloud_contact_at = ? WHERE id = 1').run(isoNow);
}

const GRACE_PERIOD_DAYS = 3;

export function isCloudGracePeriodExceeded(): boolean {
  const state = getSyncState();
  if (!state?.last_cloud_contact_at) return true;
  const lastContact = new Date(state.last_cloud_contact_at).getTime();
  if (isNaN(lastContact)) return true;
  const now = Date.now();
  const gracePeriodMs = GRACE_PERIOD_DAYS * 24 * 60 * 60 * 1000;
  return (now - lastContact) > gracePeriodMs;
}

export function reconcileScreenLicenseStatuses(): void {
  const allScreens = db.prepare('SELECT id FROM screens').all() as any[];
  for (const screen of allScreens) {
    const activeLicense = db.prepare(
      "SELECT status FROM licenses WHERE screen_id = ? AND status IN ('active', 'canceling', 'trial') LIMIT 1"
    ).get(screen.id) as any;
    if (activeLicense) {
      db.prepare("UPDATE screens SET license_status = ? WHERE id = ?").run(activeLicense.status, screen.id);
    } else {
      const anyLicense = db.prepare("SELECT status FROM licenses WHERE screen_id = ? LIMIT 1").get(screen.id) as any;
      if (anyLicense) {
        db.prepare("UPDATE screens SET license_status = ? WHERE id = ?").run(anyLicense.status, screen.id);
      } else {
        db.prepare("UPDATE screens SET license_status = 'none' WHERE id = ?").run(screen.id);
      }
    }
  }
  console.log(`[reconcile] Reconciled license_status for ${allScreens.length} screens`);
}

export function isScreenAllowedToPlay(pairingCode: string): { allowed: boolean; reason?: string } {
  if (isHubRevoked()) {
    return { allowed: false, reason: 'hub_revoked' };
  }

  if (isCloudGracePeriodExceeded()) {
    return { allowed: false, reason: 'cloud_disconnected' };
  }

  const screen = db.prepare('SELECT * FROM screens WHERE pairing_code = ?').get(pairingCode) as any;
  if (!screen) {
    return { allowed: false, reason: 'screen_not_found' };
  }

  if (screen.license_status === 'expired') {
    const activeLicense = db.prepare(
      "SELECT status FROM licenses WHERE screen_id = ? AND status IN ('active', 'canceling', 'trial') LIMIT 1"
    ).get(screen.id) as any;
    if (activeLicense) {
      db.prepare("UPDATE screens SET license_status = ? WHERE id = ?").run(activeLicense.status, screen.id);
      return { allowed: true };
    }
    return { allowed: false, reason: 'license_expired' };
  }

  return { allowed: true };
}

// `pairing_grace_until` is a HUB-LOCAL field — it must never be pushed to
// the cloud (the cloud schema does not have it). Both write paths below run
// inside `withoutTriggers` so they don't enqueue local_changes rows for the
// sync engine. Additionally, getFullRow strips this column from outbound
// payloads as a belt-and-braces measure.
export function setScreenPairingGrace(pairingCode: string, durationMs: number): number {
  const until = new Date(Date.now() + durationMs).toISOString();
  let changes = 0;
  withoutTriggers(() => {
    const result = db.prepare(
      "UPDATE screens SET pairing_grace_until = ? WHERE pairing_code = ?"
    ).run(until, pairingCode);
    changes = Number(result.changes) || 0;
  });
  if (changes > 0) {
    console.log(`[pairing-grace] Marked screen ${pairingCode} as grace-protected until ${until}`);
  }
  return changes;
}

export function clearExpiredPairingGrace(): void {
  const now = new Date().toISOString();
  withoutTriggers(() => {
    db.prepare(
      "UPDATE screens SET pairing_grace_until = NULL WHERE pairing_grace_until IS NOT NULL AND pairing_grace_until <= ?"
    ).run(now);
  });
}

export function enforceLocalFreeScreenLimit(): void {
  // Sweep expired grace markers up front so they don't shield screens forever.
  clearExpiredPairingGrace();

  const allScreens = db.prepare(
    "SELECT * FROM screens ORDER BY created_at ASC"
  ).all() as any[];

  const licensedScreenIds = new Set<number>();
  const activeLicenses = db.prepare(
    "SELECT screen_id FROM licenses WHERE screen_id IS NOT NULL AND status IN ('active', 'canceling', 'trial')"
  ).all() as any[];
  for (const lic of activeLicenses) {
    licensedScreenIds.add(lic.screen_id);
  }

  for (const screen of allScreens) {
    if (licensedScreenIds.has(screen.id) && screen.license_status === 'expired') {
      const lic = db.prepare(
        "SELECT status FROM licenses WHERE screen_id = ? AND status IN ('active', 'canceling', 'trial') LIMIT 1"
      ).get(screen.id) as any;
      if (lic) {
        db.prepare("UPDATE screens SET license_status = ? WHERE id = ?").run(lic.status, screen.id);
      }
    }
  }

  const nowIso = new Date().toISOString();
  const isGraceProtected = (s: any) => s.pairing_grace_until && s.pairing_grace_until > nowIso;

  const freeScreens = allScreens.filter((s: any) => !licensedScreenIds.has(s.id) && s.license_status !== 'expired');

  if (licensedScreenIds.size > 0) return;

  if (freeScreens.length <= 1) return;

  // Sort: grace-protected screens first (they MUST stay), then by created_at.
  // This preserves freshly-paired screens while a license is still propagating.
  const sorted = [...freeScreens].sort((a, b) => {
    const aGrace = isGraceProtected(a) ? 0 : 1;
    const bGrace = isGraceProtected(b) ? 0 : 1;
    if (aGrace !== bGrace) return aGrace - bGrace;
    return String(a.created_at || '').localeCompare(String(b.created_at || ''));
  });

  const [keepScreen, ...rest] = sorted;
  // Never expire a grace-protected screen.
  const excessScreens = rest.filter((s: any) => !isGraceProtected(s));
  if (excessScreens.length === 0) return;

  for (const screen of excessScreens) {
    db.prepare("UPDATE screens SET license_status = 'expired' WHERE id = ?").run(screen.id);
  }

  console.log(`[enforcement] Free screen limit enforced: kept ${keepScreen.pairing_code}, expired ${excessScreens.length} excess screens`);
}

export function withoutTriggers(fn: () => void): void {
  const runInTransaction = db.transaction(() => {
    for (const table of SYNCED_TABLES) {
      db.exec(`DROP TRIGGER IF EXISTS trg_${table}_insert`);
      db.exec(`DROP TRIGGER IF EXISTS trg_${table}_update`);
      db.exec(`DROP TRIGGER IF EXISTS trg_${table}_delete`);
    }
    try {
      fn();
    } finally {
      setupChangeTriggers(db);
    }
  });
  runInTransaction();
}

export function invalidateColumnCache(): void {
  tableColumnsCache = null;
}

export function setIdMapping(tableName: string, localId: number, cloudId: number): void {
  db.prepare(
    'INSERT OR REPLACE INTO hub_id_map (table_name, local_id, cloud_id) VALUES (?, ?, ?)'
  ).run(tableName, localId, cloudId);
}

export function getCloudId(tableName: string, localId: number): number | null {
  const row = db.prepare(
    'SELECT cloud_id FROM hub_id_map WHERE table_name = ? AND local_id = ?'
  ).get(tableName, localId) as { cloud_id: number } | undefined;
  return row?.cloud_id ?? null;
}

export function getLocalId(tableName: string, cloudId: number): number | null {
  const row = db.prepare(
    'SELECT local_id FROM hub_id_map WHERE table_name = ? AND cloud_id = ?'
  ).get(tableName, cloudId) as { local_id: number } | undefined;
  return row?.local_id ?? null;
}

export function removeIdMapping(tableName: string, localId: number): void {
  db.prepare('DELETE FROM hub_id_map WHERE table_name = ? AND local_id = ?').run(tableName, localId);
}

/**
 * Remove hub_id_map rows whose local record no longer exists in the underlying table.
 * Both the local and cloud sides have effectively forgotten the record (cloud DELETE
 * already removes the mapping in applyChanges; this catches local-only deletions and
 * historical orphans accumulated before that path existed).
 */
export function pruneOrphanIdMappings(): number {
  let total = 0;
  // Allowlist of tables we ever interpolate into dynamic SQL. table_name in hub_id_map
  // can be set by remote (cloud-supplied) data, so we never pass user-controlled
  // identifiers through string concatenation.
  const allowlist = new Set<string>(SYNCED_TABLES);

  const tables = db.prepare(
    "SELECT DISTINCT table_name FROM hub_id_map"
  ).all() as Array<{ table_name: string }>;

  for (const { table_name } of tables) {
    try {
      if (!allowlist.has(table_name)) {
        // Unknown / non-synced table reference — drop the mapping rows outright.
        const result = db.prepare('DELETE FROM hub_id_map WHERE table_name = ?').run(table_name);
        total += Number(result.changes || 0);
        continue;
      }
      const tableExists = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
      ).get(table_name);
      if (!tableExists) {
        const result = db.prepare('DELETE FROM hub_id_map WHERE table_name = ?').run(table_name);
        total += Number(result.changes || 0);
        continue;
      }
      const result = db.prepare(`
        DELETE FROM hub_id_map
        WHERE table_name = ?
          AND local_id NOT IN (SELECT id FROM ${table_name})
      `).run(table_name);
      total += Number(result.changes || 0);
    } catch {
      continue;
    }
  }
  return total;
}

/**
 * Returns the age in milliseconds of the oldest unpushed local_changes entry,
 * or 0 if none exist.
 */
export function getOldestUnpushedAgeMs(): number {
  const row = db.prepare(
    "SELECT (julianday('now') - julianday(MIN(created_at))) * 86400000 AS age FROM local_changes WHERE pushed = 0"
  ).get() as { age: number | null } | undefined;
  return Math.max(0, Math.floor(row?.age ?? 0));
}

/**
 * Prune already-pushed local_changes rows older than the given age, while keeping
 * a configurable safety tail of the most-recent pushed entries for diagnostics.
 * Also enforces a hard cap on the table size by deleting the oldest pushed rows
 * when the table grows beyond `hardCap`.
 */
export function pruneAckedChanges(opts: {
  maxAgeMs?: number;
  keepTail?: number;
  hardCap?: number;
} = {}): number {
  const maxAgeMs = opts.maxAgeMs ?? 7 * 24 * 60 * 60 * 1000;
  const keepTail = opts.keepTail ?? 2000;
  const hardCap = opts.hardCap ?? 100_000;
  let total = 0;

  try {
    const tailRow = db.prepare(
      'SELECT id FROM local_changes WHERE pushed = 1 ORDER BY id DESC LIMIT 1 OFFSET ?'
    ).get(keepTail) as { id: number } | undefined;

    const cutoffSeconds = Math.floor(maxAgeMs / 1000);
    if (tailRow) {
      const result = db.prepare(`
        DELETE FROM local_changes
        WHERE pushed = 1
          AND id <= ?
          AND created_at < datetime('now', ?)
      `).run(tailRow.id, `-${cutoffSeconds} seconds`);
      total += Number(result.changes || 0);
    }

    const totalRow = db.prepare('SELECT COUNT(*) AS c FROM local_changes').get() as { c: number };
    if (totalRow.c > hardCap) {
      const excess = totalRow.c - hardCap;
      const result = db.prepare(`
        DELETE FROM local_changes
        WHERE id IN (
          SELECT id FROM local_changes WHERE pushed = 1 ORDER BY id ASC LIMIT ?
        )
      `).run(excess);
      total += Number(result.changes || 0);
    }
  } catch {
    /* best-effort */
  }

  return total;
}

/**
 * Recover from "stale unacked" change-log buildup — the failure mode where
 * `pushed=0` rows accumulate forever because acks were missed (cloud outage,
 * crash before commit, etc.) and no later activity re-derives them.
 *
 * Conservative rules — we only ever drop entries that are PROVABLY redundant:
 *   • Older than `minAgeMs` (default 7d) AND `pushed=0`
 *   • For INSERT/UPDATE: the underlying local row has since been deleted,
 *     so the change can never be replayed and the cloud either already
 *     synced it or will see a future DELETE.
 *   • For DELETE: a later (newer) INSERT/UPDATE for the same record exists,
 *     i.e. the row was recreated and the obsolete DELETE would corrupt state.
 *   • Or a strictly newer same-record INSERT/UPDATE exists, which means the
 *     newer entry will carry the correct state forward.
 *
 * Anything else is left alone — we'd rather keep the queue large than risk
 * losing a real change. Operates only on the synced-table allowlist so
 * arbitrary cloud-supplied table names can't drive the SQL path.
 */
export function pruneStaleUnackedChanges(opts: { minAgeMs?: number } = {}): number {
  const minAgeMs = opts.minAgeMs ?? 7 * 24 * 60 * 60 * 1000;
  const cutoffSeconds = Math.floor(minAgeMs / 1000);
  let total = 0;
  const allowlist = new Set<string>(SYNCED_TABLES);

  type Candidate = { id: number; table_name: string; record_id: number; operation: string };
  let candidates: Candidate[];
  try {
    candidates = db.prepare(`
      SELECT id, table_name, record_id, operation
      FROM local_changes
      WHERE pushed = 0
        AND created_at < datetime('now', ?)
      ORDER BY id ASC
      LIMIT 5000
    `).all(`-${cutoffSeconds} seconds`) as Candidate[];
  } catch (err) {
    console.warn('[sqlite] pruneStaleUnackedChanges: candidate scan failed:', (err as Error)?.message);
    return 0;
  }

  const deleteStmt = db.prepare('DELETE FROM local_changes WHERE id = ?');
  const tableExistsStmt = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
  const newerSameRecordStmt = db.prepare(
    'SELECT 1 FROM local_changes WHERE table_name=? AND record_id=? AND id>? LIMIT 1'
  );
  const mappingStmt = db.prepare(
    'SELECT 1 FROM hub_id_map WHERE table_name=? AND local_id=? LIMIT 1'
  );

  for (const c of candidates) {
    if (!allowlist.has(c.table_name)) {
      // Non-synced table reference — safe to drop the stranded change row.
      total += Number(deleteStmt.run(c.id).changes || 0);
      continue;
    }
    try {
      const tableExists = tableExistsStmt.get(c.table_name);
      if (!tableExists) {
        total += Number(deleteStmt.run(c.id).changes || 0);
        continue;
      }

      // DELETE tombstones are NEVER unilaterally dropped — the cloud has to
      // acknowledge them, otherwise the record will resurrect on reconciliation.
      if (c.operation === 'DELETE') continue;

      const stillExists = db.prepare(
        `SELECT 1 FROM ${c.table_name} WHERE id = ? LIMIT 1`
      ).get(c.record_id);
      const hasMapping = !!mappingStmt.get(c.table_name, c.record_id);
      const newer = !!newerSameRecordStmt.get(c.table_name, c.record_id, c.id);

      // Safety rule #1: if the underlying local row is gone, the INSERT/UPDATE
      // can't be replayed regardless. Drop it.
      if (!stillExists) {
        total += Number(deleteStmt.run(c.id).changes || 0);
        continue;
      }

      // Safety rule #2: only drop a redundant INSERT/UPDATE when the cloud
      // ALREADY HAS A MAPPING for this record (i.e. the cloud has a valid
      // target for the newer same-record op). Without a mapping, dropping
      // the only INSERT would strand the later UPDATE/DELETE — they'd be
      // sent against a local id with no cloud counterpart and reconciliation
      // can't recover because the pending non-INSERT suppresses re-enqueue.
      if (newer && hasMapping) {
        total += Number(deleteStmt.run(c.id).changes || 0);
        continue;
      }
      // Otherwise leave it alone — better to keep a row than risk losing the
      // only create-side change for an unmapped record.
    } catch {
      continue;
    }
  }
  return total;
}

/**
 * Collapse duplicate unacked DELETE tombstones for the same (table, record_id):
 * keep only the newest DELETE, drop the older duplicates. Safe under all
 * conditions — the cloud only needs one tombstone per logical record. Bounds
 * the queue's growth contribution from misbehaving triggers or reentrant
 * delete paths even when the cloud never acks.
 */
export function dedupeUnackedDeleteTombstones(): number {
  try {
    const result = db.prepare(`
      DELETE FROM local_changes
      WHERE pushed = 0
        AND operation = 'DELETE'
        AND id NOT IN (
          SELECT MAX(id) FROM local_changes
          WHERE pushed = 0 AND operation = 'DELETE'
          GROUP BY table_name, record_id
        )
    `).run();
    return Number(result.changes || 0);
  } catch (err) {
    console.warn('[sqlite] dedupeUnackedDeleteTombstones failed:', (err as Error)?.message);
    return 0;
  }
}

/**
 * Emergency last-resort safety valve for `local_changes`: when total row count
 * exceeds `targetCap`, drop the OLDEST entries to bring it back under cap.
 * This intentionally accepts data loss as the lesser evil vs. unbounded disk
 * growth. Caller is expected to log critically before invoking.
 *
 * Skips entries with operation=DELETE preferentially when possible, but if the
 * queue is dominated by tombstones it will drop them too — disk integrity wins.
 *
 * Returns the number of rows dropped.
 */
export function emergencyDropOldestChanges(targetCap: number): number {
  try {
    const totalRow = db.prepare('SELECT COUNT(*) AS n FROM local_changes').get() as { n: number };
    const total = Number(totalRow?.n ?? 0);
    if (total <= targetCap) return 0;
    const excess = total - targetCap;
    // Prefer to drop pushed=1 (acked) rows first; then non-DELETE unacked;
    // then anything left. Single statement using a sort key.
    const result = db.prepare(`
      DELETE FROM local_changes WHERE id IN (
        SELECT id FROM local_changes
        ORDER BY pushed DESC, (operation = 'DELETE') ASC, id ASC
        LIMIT ?
      )
    `).run(excess);
    return Number(result.changes || 0);
  } catch (err) {
    console.warn('[sqlite] emergencyDropOldestChanges failed:', (err as Error)?.message);
    return 0;
  }
}

/**
 * Returns true if there is a pending (unpushed) INSERT for the given (tableName, recordId).
 * Used by the push pipeline to defer dependent UPDATE/DELETE operations until the
 * INSERT has been acknowledged and an id mapping has been established.
 */
export function hasPendingInsert(tableName: string, recordId: number): boolean {
  const row = db.prepare(
    "SELECT 1 FROM local_changes WHERE pushed = 0 AND table_name = ? AND record_id = ? AND operation = 'INSERT' LIMIT 1"
  ).get(tableName, recordId);
  return !!row;
}

const RECONCILABLE_TABLES = [
  'screens', 'playlists', 'contents', 'schedules', 'playlist_items',
  'smart_triggers', 'video_walls', 'video_wall_screens', 'content_folders',
  'layout_templates', 'screen_groups', 'screen_group_members',
  'emergency_alert_configs', 'custom_alert_feeds', 'dooh_campaigns',
  'dooh_ad_slots', 'kiosks', 'broadcasts', 'smart_qr_codes',
  'teams', 'team_roles', 'team_members', 'team_screens', 'team_categories',
  'directory_venues', 'directory_floors', 'directory_categories',
  'directory_stores', 'directory_promotions', 'directory_kiosk_positions',
];

export function getUnmappedLocalRecords(): Array<{ tableName: string; localId: number }> {
  const results: Array<{ tableName: string; localId: number }> = [];
  for (const table of RECONCILABLE_TABLES) {
    try {
      const tableExists = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
      ).get(table);
      if (!tableExists) continue;

      const rows = db.prepare(`
        SELECT t.id FROM ${table} t
        LEFT JOIN hub_id_map m ON m.table_name = ? AND m.local_id = t.id
        WHERE m.id IS NULL
      `).all(table) as Array<{ id: number }>;

      for (const row of rows) {
        results.push({ tableName: table, localId: row.id });
      }
    } catch {
      continue;
    }
  }
  return results;
}

export interface ErrorLogEntry {
  level?: string;
  source: string;
  route?: string;
  method?: string;
  statusCode?: number;
  message: string;
  stack?: string;
  context?: Record<string, unknown>;
}

export function insertErrorLog(entry: ErrorLogEntry): void {
  try {
    db.prepare(`
      INSERT INTO error_logs (level, source, route, method, status_code, message, stack, context)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      entry.level || 'error',
      entry.source,
      entry.route || null,
      entry.method || null,
      entry.statusCode || null,
      entry.message.slice(0, 2000),
      entry.stack?.slice(0, 4000) || null,
      entry.context ? JSON.stringify(entry.context) : null
    );
  } catch {
  }
}

export function getRecentErrorLogs(limit = 50, offset = 0): any[] {
  return db.prepare('SELECT * FROM error_logs ORDER BY id DESC LIMIT ? OFFSET ?').all(limit, offset);
}

export function getErrorLogCount(): number {
  const row = db.prepare('SELECT COUNT(*) as count FROM error_logs').get() as any;
  return row?.count || 0;
}

export function getUnsentErrorLogs(limit = 100): any[] {
  return db.prepare('SELECT * FROM error_logs WHERE sent_to_cloud = 0 ORDER BY id ASC LIMIT ?').all(limit);
}

export function markErrorLogsSent(ids: number[]): void {
  if (ids.length === 0) return;
  const placeholders = ids.map(() => '?').join(',');
  db.prepare(`UPDATE error_logs SET sent_to_cloud = 1 WHERE id IN (${placeholders})`).run(...ids);
}

export function pruneOldErrorLogs(keepCount = 1000): void {
  try {
    const total = getErrorLogCount();
    if (total > keepCount) {
      db.prepare(`DELETE FROM error_logs WHERE id NOT IN (SELECT id FROM error_logs ORDER BY id DESC LIMIT ?)`).run(keepCount);
    }
  } catch {
  }
}

export function logSyncConflict(entry: {
  tableName: string;
  recordId: number;
  operation: string;
  localVersion: any;
  incomingVersion: any;
  resolution: string;
  localUpdatedAt?: string;
  incomingUpdatedAt?: string;
}): void {
  try {
    db.prepare(`
      INSERT INTO sync_conflicts (table_name, record_id, operation, local_version, incoming_version, resolution, local_updated_at, incoming_updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      entry.tableName,
      entry.recordId,
      entry.operation,
      entry.localVersion ? JSON.stringify(entry.localVersion) : null,
      entry.incomingVersion ? JSON.stringify(entry.incomingVersion) : null,
      entry.resolution,
      entry.localUpdatedAt || null,
      entry.incomingUpdatedAt || null
    );
  } catch {
  }
}

export function getSyncConflicts(limit = 50, offset = 0): any[] {
  return db.prepare('SELECT * FROM sync_conflicts ORDER BY id DESC LIMIT ? OFFSET ?').all(limit, offset);
}

export function getSyncConflictCount(): number {
  const row = db.prepare('SELECT COUNT(*) as count FROM sync_conflicts').get() as any;
  return row?.count || 0;
}

export function pruneOldSyncConflicts(keepCount = 500): void {
  try {
    const total = getSyncConflictCount();
    if (total > keepCount) {
      db.prepare(`DELETE FROM sync_conflicts WHERE id NOT IN (SELECT id FROM sync_conflicts ORDER BY id DESC LIMIT ?)`).run(keepCount);
    }
  } catch {
  }
}
