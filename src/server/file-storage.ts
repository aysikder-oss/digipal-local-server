import path from 'path';
import fs from 'fs';
import { app } from 'electron';
import crypto from 'crypto';

let mediaDir: string;

export function getMediaDir(): string {
  if (!mediaDir) {
    mediaDir = path.join(app.getPath('home'), 'DigipalMedia');
    fs.mkdirSync(mediaDir, { recursive: true });
    fs.mkdirSync(path.join(mediaDir, 'uploads'), { recursive: true });
    fs.mkdirSync(path.join(mediaDir, 'thumbnails'), { recursive: true });
  }
  return mediaDir;
}

export function saveUploadedFile(buffer: Buffer, originalName: string): { filePath: string; fileName: string } {
  const dir = getMediaDir();
  const ext = path.extname(originalName);
  const uniqueName = `${Date.now()}-${crypto.randomBytes(8).toString('hex')}${ext}`;
  const filePath = path.join(dir, 'uploads', uniqueName);
  fs.writeFileSync(filePath, buffer);
  return { filePath, fileName: uniqueName };
}

export function getLocalFilePath(fileName: string): string {
  return path.join(getMediaDir(), 'uploads', fileName);
}

export function deleteLocalFile(fileName: string): void {
  const filePath = path.join(getMediaDir(), 'uploads', fileName);
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch (e) {
    console.error('[file-storage] Failed to delete file:', e);
  }
}

export function getMediaDiskUsage(): { totalBytes: number; fileCount: number } {
  const uploadsDir = path.join(getMediaDir(), 'uploads');
  let totalBytes = 0;
  let fileCount = 0;
  try {
    const files = fs.readdirSync(uploadsDir);
    for (const file of files) {
      const stat = fs.statSync(path.join(uploadsDir, file));
      totalBytes += stat.size;
      fileCount++;
    }
  } catch { }
  return { totalBytes, fileCount };
}
