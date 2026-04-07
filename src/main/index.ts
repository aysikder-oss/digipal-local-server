import { app, BrowserWindow, Tray, Menu, nativeImage, ipcMain } from 'electron';
import path from 'path';
import { startServer, stopServer } from '../server/express';
import { initDatabase } from '../db/sqlite';

let mainWindow: BrowserWindow | null = null;
let tray: Tray | null = null;
let isQuitting = false;

const SERVER_PORT = 8787;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 800,
    height: 600,
    minWidth: 600,
    minHeight: 400,
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

  const rendererPath = app.isPackaged
    ? path.join(process.resourcesPath, 'renderer', 'index.html')
    : path.join(__dirname, '../../src/renderer/index.html');
  mainWindow.loadFile(rendererPath);
}

function createTray() {
  const icon = nativeImage.createFromDataURL(
    'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAABGdBTUEAALGPC/xhBQAAAAlwSFlzAAALEwAACxMBAJqcGAAAAVlJREFUWIXtl7FKA0EQhr+9I4UWFoKF2PgCPoCVjY/gC1hYWfkIFj6AjZWFhYWFICKIiIhYiIiIiIhY6HfcLje3e5ecF/CLf2Bv9p//n9md2YMaNWrUKJlakdqLwAIwDYwDo8AI0ALawC/wDTwDj8A1cA28JvVXJJkHloFFYBaYAsYwCS3gBwPiHbgFroAL4DSpX0VkZB4DPhBYApaBFWAemMYkjADDwC/wBbwBD8AF8ATcAPdJ/T0rkixGZGQOuMOALANrwAYwB0xhEkaBIeAHaAP3wCVwBpwm9dcoSrKPAfEaA3IFk7AJTGMS2sBP9v8zcAOcAidJ/SUqyUgJES/A85gZXwA2gS1MyijQwh74Aa6BY+AkqZ+jkiwnRNyC+VfW7WID2MZIWMAX/IN/4Bw4BY6S+hOUJDkhIqL9AjxhkjaBbUzCBCZhCDMyLUzCJXCW1K9QqMfVqFGjRo3/j99Av5vFwRYHKwAAAABJRU5ErkJggg=='
  );
  tray = new Tray(icon);
  tray.setToolTip('Digipal Local Server');

  const contextMenu = Menu.buildFromTemplate([
    { label: 'Show Dashboard', click: () => mainWindow?.show() },
    { type: 'separator' },
    { label: 'Quit', click: () => { isQuitting = true; app.quit(); } },
  ]);

  tray.setContextMenu(contextMenu);
  tray.on('double-click', () => mainWindow?.show());
}

app.whenReady().then(async () => {
  initDatabase();
  await startServer(SERVER_PORT);
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
