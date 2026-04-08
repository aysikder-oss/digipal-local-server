import { app, BrowserWindow, Tray, Menu, nativeImage, ipcMain, shell } from 'electron';
import path from 'path';
import { startServer, stopServer, setHubBlocked, setDiscoveredHubs } from '../server/express';
import { initDatabase, getSyncState } from '../db/sqlite';
import { scanForExistingHubs } from '../server/mdns';

let mainWindow: BrowserWindow | null = null;
let tray: Tray | null = null;
let isQuitting = false;

let SERVER_PORT = 8787;
const CLOUD_URL = process.env.CLOUD_URL || 'https://digipalsignage.com';

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 800,
    minWidth: 800,
    minHeight: 600,
    title: 'Digipal Local Server',
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js'),
    },
    show: false,
  });

  mainWindow.on('ready-to-show', () => {
    mainWindow?.show();
  });

  mainWindow.on('close', (event) => {
    if (!isQuitting) {
      event.preventDefault();
      mainWindow?.hide();
    }
  });

  mainWindow.loadURL(`http://localhost:${SERVER_PORT}`);

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith('http://') || url.startsWith('https://')) {
      const billingPatterns = ['/billing', '/subscription', '/payment', '/verify', '/reset-password', '/stripe'];
      const isExternalLink = billingPatterns.some(p => url.includes(p)) || !url.includes(`localhost:${SERVER_PORT}`);
      if (isExternalLink) {
        shell.openExternal(url);
        return { action: 'deny' };
      }
    }
    return { action: 'allow' };
  });
}

function createTray() {
  const icon = nativeImage.createFromDataURL(
    'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABGdBTUEAALGPC/xhBQAAAAlwSFlzAAALEwAACxMBAJqcGAAAAVlJREFUWIXtl7FKA0EQhr+9I4UWFoKF2PgCPoCVjY/gC1hYWfkIFj6AjZWFhYWFICKIiIhYiIiIiIhY6HfcLje3e5ecF/CLf2Bv9p//n9md2YMaNWrUKJlakdqLwAIwDYwDo8AI0ALawC/wDTwDj8A1cA28JvVXJJkHloFFYBaYAsYwCS3gBwPiHbgFroAL4DSpX0VkZB4DPhBYApaBFWAemMYkjADDwC/wBbwBD8AF8ATcAPdJ/T0rkixGZGQOuMOALANrwAYwB0xhEkaBIeAHaAP3wCVwBpwm9dcoSrKPAfEaA3IFk7AJTGMS2sBP9v8zcAOcAidJ/SUqyUgJES/A85gZXwA2gS1MyijQwh74Aa6BY+AkqZ+jkiwnRNyC+VfW7WID2MZIWAKAN2MZIWAMAN2MZIWAMAN2MZIWAMAN2MZIWAMAN2MZIWAMAN2MZIWAKAN2MZIWAMANWrUqFGjRo3/j99Av5vFwRYHKwAAAABJRU5ErkJggg=='
  );
  tray = new Tray(icon);
  tray.setToolTip('Digipal Local Server');

  const syncState = getSyncState();
  const hubName = syncState?.hub_name || 'Local Server';

  const contextMenu = Menu.buildFromTemplate([
    { label: hubName, enabled: false },
    { type: 'separator' },
    { label: 'Open Dashboard', click: () => mainWindow?.show() },
    { label: 'Open in Browser', click: () => shell.openExternal(`http://localhost:${SERVER_PORT}`) },
    { type: 'separator' },
    { label: 'Cloud Dashboard', click: () => shell.openExternal(CLOUD_URL) },
    { label: 'Billing & Subscriptions', click: () => shell.openExternal(`${CLOUD_URL}/billing`) },
    { type: 'separator' },
    { label: 'Quit', click: () => { isQuitting = true; app.quit(); } },
  ]);

  tray.setContextMenu(contextMenu);
  tray.on('double-click', () => mainWindow?.show());
}

async function performFirstLaunchCheck(): Promise<boolean> {
  const syncState = getSyncState();
  if (syncState?.hub_token && syncState?.cloud_url) {
    return true;
  }

  console.log('[startup] First launch detected — scanning for existing hubs on network...');
  const existingHubs = await scanForExistingHubs(5000);

  if (existingHubs.length > 0) {
    console.log(`[startup] Found ${existingHubs.length} existing hub(s) on network:`);
    existingHubs.forEach(h => console.log(`  - ${h.name} at ${h.host}:${h.port}`));
    return false;
  }

  console.log('[startup] No existing hubs found — proceeding with setup');
  return true;
}

const gotTheLock = app.requestSingleInstanceLock();
if (!gotTheLock) {
  app.quit();
} else {
  app.on('second-instance', () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.show();
      mainWindow.focus();
    }
  });

  app.whenReady().then(async () => {
    initDatabase();

    const canProceed = await performFirstLaunchCheck();

    if (!canProceed) {
      const hubs = await scanForExistingHubs(5000);
      setDiscoveredHubs(hubs);
      setHubBlocked(true);
      console.log('[startup] Hub blocked — existing hub found on network. Dashboard will show blocked state.');
    }

    const actualPort = await startServer(SERVER_PORT);
    SERVER_PORT = actualPort;
    createWindow();
    createTray();

    app.on('activate', () => {
      if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
      } else {
        mainWindow?.show();
      }
    });
  });

  app.on('before-quit', async () => {
    isQuitting = true;
    await stopServer();
  });

  app.on('window-all-closed', () => {
    if (process.platform !== 'darwin') {
      app.quit();
    }
  });

  ipcMain.handle('get-server-port', () => SERVER_PORT);
  ipcMain.handle('get-app-version', () => app.getVersion());
  ipcMain.handle('get-cloud-url', () => CLOUD_URL);
}
