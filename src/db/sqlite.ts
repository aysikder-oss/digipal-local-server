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
  `);

  setupChangeTriggers(db);
  runMigrations(db);

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
  addColumnIfMissing('sync_state', 'last_cloud_contact_at', 'TEXT');
  addColumnIfMissing('sync_state', 'hub_revoked', 'INTEGER DEFAULT 0');
  addColumnIfMissing('sync_state', 'hub_name', 'TEXT');
  addColumnIfMissing('sync_state', 'sync_enabled', 'INTEGER DEFAULT 1');
}

function setupChangeTriggers(database: Database.Database) {
  for (const table of SYNCED_TABLES) {
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

export { SYNCED_TABLES };

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

export function getUnpushedChangeCount(): number {
  const row = db.prepare('SELECT COUNT(*) as count FROM local_changes WHERE pushed = 0').get() as any;
  return row?.count || 0;
}

export function markChangesPushed(ids: number[]): void {
  if (ids.length === 0) return;
  const placeholders = ids.map(() => '?').join(',');
  db.prepare(`UPDATE local_changes SET pushed = 1 WHERE id IN (${placeholders})`).run(...ids);
}

export function getFullRow(tableName: string, recordId: number): any {
  if (!SYNCED_TABLES.includes(tableName)) return null;
  return db.prepare(`SELECT * FROM ${tableName} WHERE id = ?`).get(recordId);
}

export function upsertRow(tableName: string, data: Record<string, any>): void {
  if (!SYNCED_TABLES.includes(tableName)) return;
  const normalized = normalizePayloadForTable(tableName, data);
  const keys = Object.keys(normalized);
  if (keys.length === 0) return;
  const cols = keys.join(', ');
  const placeholders = keys.map(() => '?').join(', ');
  const updates = keys.filter(k => k !== 'id').map(k => `${k} = excluded.${k}`).join(', ');
  db.prepare(`INSERT INTO ${tableName} (${cols}) VALUES (${placeholders}) ON CONFLICT(id) DO UPDATE SET ${updates}`)
    .run(...keys.map(k => normalized[k]));
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
    return { allowed: false, reason: 'license_expired' };
  }

  return { allowed: true };
}

export function enforceLocalFreeScreenLimit(): void {
  const allScreens = db.prepare(
    "SELECT * FROM screens WHERE license_status != 'expired' ORDER BY created_at ASC"
  ).all() as any[];

  const activeScreens = allScreens.filter((s: any) => s.license_status === 'active');

  if (activeScreens.length > 0) return;

  const freeScreens = allScreens.filter((s: any) => s.license_status === 'none' || s.license_status === 'free');
  if (freeScreens.length <= 1) return;

  const [keepScreen, ...excessScreens] = freeScreens;
  for (const screen of excessScreens) {
    db.prepare("UPDATE screens SET license_status = 'expired' WHERE id = ?").run(screen.id);
  }

  console.log(`[enforcement] Free screen limit enforced: kept ${keepScreen.pairing_code}, expired ${excessScreens.length} excess screens`);
}

export function withoutTriggers(fn: () => void): void {
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
}

export function invalidateColumnCache(): void {
  tableColumnsCache = null;
}
