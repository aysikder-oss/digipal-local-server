import crypto from 'crypto';
import os from 'os';
import path from 'path';
import fs from 'fs';

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;
const ENCRYPTED_PREFIX = 'enc:';
const SECRET_SALT_FILENAME = '.digipal-key-salt';

let cachedKey: Buffer | null = null;

function getSecretSaltPath(): string {
  try {
    const { app } = require('electron');
    return path.join(app.getPath('userData'), SECRET_SALT_FILENAME);
  } catch {
    return path.join(os.homedir(), SECRET_SALT_FILENAME);
  }
}

function getOrCreateSecretSalt(): string {
  const saltPath = getSecretSaltPath();
  try {
    if (fs.existsSync(saltPath)) {
      return fs.readFileSync(saltPath, 'utf8').trim();
    }
  } catch {}

  const salt = crypto.randomBytes(32).toString('hex');
  try {
    fs.writeFileSync(saltPath, salt, { mode: 0o600 });
  } catch {}
  return salt;
}

function getDeviceKey(): Buffer {
  if (cachedKey) return cachedKey;

  if (process.env.DIGIPAL_ENCRYPTION_KEY) {
    cachedKey = crypto.createHash('sha256').update(process.env.DIGIPAL_ENCRYPTION_KEY).digest();
    return cachedKey;
  }

  const hostname = os.hostname();
  const platform = os.platform();
  const arch = os.arch();

  let macAddress = '00:00:00:00:00:00';
  try {
    const ifaces = os.networkInterfaces();
    for (const name of Object.keys(ifaces)) {
      for (const iface of ifaces[name] || []) {
        if (!iface.internal && iface.mac && iface.mac !== '00:00:00:00:00:00') {
          macAddress = iface.mac;
          break;
        }
      }
      if (macAddress !== '00:00:00:00:00:00') break;
    }
  } catch {}

  const secretSalt = getOrCreateSecretSalt();
  const material = `digipal-local:${hostname}:${platform}:${arch}:${macAddress}:${secretSalt}`;
  cachedKey = crypto.createHash('sha256').update(material).digest();
  return cachedKey;
}

export function encryptField(plaintext: string): string {
  if (!plaintext) return plaintext;

  const key = getDeviceKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(plaintext, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  const authTag = cipher.getAuthTag();

  return ENCRYPTED_PREFIX + iv.toString('hex') + ':' + authTag.toString('hex') + ':' + encrypted;
}

export function decryptField(ciphertext: string): string {
  if (!ciphertext) return ciphertext;
  if (!ciphertext.startsWith(ENCRYPTED_PREFIX)) return ciphertext;

  const key = getDeviceKey();
  const parts = ciphertext.slice(ENCRYPTED_PREFIX.length).split(':');
  if (parts.length !== 3) return ciphertext;

  const [ivHex, authTagHex, encrypted] = parts;
  const iv = Buffer.from(ivHex, 'hex');
  const authTag = Buffer.from(authTagHex, 'hex');

  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(authTag);

  let decrypted = decipher.update(encrypted, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  return decrypted;
}

function isEncrypted(value: string): boolean {
  return typeof value === 'string' && value.startsWith(ENCRYPTED_PREFIX);
}

const SENSITIVE_SYNC_FIELDS = ['hub_token', 'cloud_session_cookie'];

export function encryptSyncStateFields(updates: Record<string, any>): Record<string, any> {
  const result = { ...updates };
  for (const field of SENSITIVE_SYNC_FIELDS) {
    if (field in result && result[field] && typeof result[field] === 'string') {
      result[field] = encryptField(result[field]);
    }
  }
  return result;
}

export function decryptSyncStateFields(row: Record<string, any>): Record<string, any> {
  if (!row) return row;
  const result = { ...row };
  for (const field of SENSITIVE_SYNC_FIELDS) {
    if (field in result && result[field] && typeof result[field] === 'string') {
      try {
        result[field] = decryptField(result[field]);
      } catch {
        result[field] = result[field];
      }
    }
  }
  return result;
}

export function migratePlaintextSyncState(db: any): void {
  try {
    const row = db.prepare('SELECT hub_token, cloud_session_cookie FROM sync_state WHERE id = 1').get() as any;
    if (!row) return;

    const updates: Record<string, string> = {};

    if (row.hub_token && typeof row.hub_token === 'string' && !isEncrypted(row.hub_token)) {
      updates.hub_token = encryptField(row.hub_token);
    }
    if (row.cloud_session_cookie && typeof row.cloud_session_cookie === 'string' && !isEncrypted(row.cloud_session_cookie)) {
      updates.cloud_session_cookie = encryptField(row.cloud_session_cookie);
    }

    if (Object.keys(updates).length > 0) {
      const keys = Object.keys(updates);
      const sets = keys.map(k => `${k} = ?`).join(', ');
      db.prepare(`UPDATE sync_state SET ${sets} WHERE id = 1`).run(...keys.map(k => updates[k]));
      console.log(`[crypto] Migrated ${keys.length} plaintext sync_state field(s) to encrypted storage`);
    }
  } catch (e: any) {
    console.error('[crypto] Failed to migrate plaintext sync_state fields:', e.message);
  }
}

export function buildSecureCookie(name: string, value: string, options: {
  path?: string;
  httpOnly?: boolean;
  sameSite?: string;
  maxAge?: number;
  isSecure?: boolean;
}): string {
  const parts = [`${name}=${value}`];
  if (options.path) parts.push(`Path=${options.path}`);
  if (options.httpOnly) parts.push('HttpOnly');
  if (options.sameSite) parts.push(`SameSite=${options.sameSite}`);
  if (options.maxAge !== undefined) parts.push(`Max-Age=${options.maxAge}`);
  if (options.isSecure) parts.push('Secure');
  return parts.join('; ');
}

export function isRequestSecure(req: { secure?: boolean; headers?: Record<string, any> }): boolean {
  if (req.secure) return true;
  const proto = req.headers?.['x-forwarded-proto'];
  return proto === 'https';
}
