import path from 'path';
import fs from 'fs';
import { app, nativeImage } from 'electron';
import crypto from 'crypto';
import { execFile } from 'child_process';

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

const IMAGE_EXTENSIONS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.tif']);
const VIDEO_EXTENSIONS = new Set(['.mp4', '.webm', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.m4v']);

export function generateThumbnailBase64(filePath: string, mimeType?: string): string | null {
  try {
    const ext = path.extname(filePath).toLowerCase();
    if (!IMAGE_EXTENSIONS.has(ext)) return null;

    const fileBuffer = fs.readFileSync(filePath);
    const img = nativeImage.createFromBuffer(fileBuffer);
    if (img.isEmpty()) return null;

    const size = img.getSize();
    const maxWidth = 400;
    if (size.width <= maxWidth) {
      const jpegBuffer = img.toJPEG(70);
      return `data:image/jpeg;base64,${jpegBuffer.toString('base64')}`;
    }

    const scale = maxWidth / size.width;
    const resized = img.resize({
      width: maxWidth,
      height: Math.round(size.height * scale),
      quality: 'good',
    });
    const jpegBuffer = resized.toJPEG(70);
    return `data:image/jpeg;base64,${jpegBuffer.toString('base64')}`;
  } catch (e) {
    console.error('[file-storage] Failed to generate thumbnail:', e);
    return null;
  }
}

function findFfmpeg(): string | null {
  const { execFileSync } = require('child_process');
  const candidates = process.platform === 'win32'
    ? ['ffmpeg.exe', 'C:\\ffmpeg\\bin\\ffmpeg.exe']
    : ['ffmpeg', '/usr/bin/ffmpeg', '/usr/local/bin/ffmpeg'];
  for (const cmd of candidates) {
    try {
      execFileSync(cmd, ['-version'], { stdio: 'ignore', timeout: 5000 });
      return cmd;
    } catch {}
  }
  return null;
}

let ffmpegPath: string | null | undefined;

export function generateVideoThumbnailBase64(filePath: string): Promise<string | null> {
  return new Promise((resolve) => {
    try {
      if (ffmpegPath === undefined) {
        ffmpegPath = findFfmpeg();
      }
      if (!ffmpegPath) {
        resolve(null);
        return;
      }

      const tmpDir = path.join(getMediaDir(), 'thumbnails');
      const tmpFile = path.join(tmpDir, `thumb_${crypto.randomBytes(8).toString('hex')}.jpg`);

      execFile(ffmpegPath, [
        '-i', filePath,
        '-ss', '00:00:01',
        '-vframes', '1',
        '-vf', 'scale=400:-1',
        '-q:v', '5',
        '-y',
        tmpFile,
      ], { timeout: 15000 }, (err) => {
        if (err || !fs.existsSync(tmpFile)) {
          resolve(null);
          return;
        }
        try {
          const frameBuffer = fs.readFileSync(tmpFile);
          fs.unlinkSync(tmpFile);
          if (frameBuffer.length === 0) {
            resolve(null);
            return;
          }
          resolve(`data:image/jpeg;base64,${frameBuffer.toString('base64')}`);
        } catch {
          resolve(null);
        }
      });
    } catch {
      resolve(null);
    }
  });
}

export function isVideoFile(filePath: string): boolean {
  return VIDEO_EXTENSIONS.has(path.extname(filePath).toLowerCase());
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
