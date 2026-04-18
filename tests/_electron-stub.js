// Test-only stub: stand-in for the `electron` module when running tests
// outside an Electron runtime. Only `app.getPath` is referenced by the
// modules under test (sqlite.ts), and only when initDatabase() is called
// without an explicit override path.
'use strict';
const path = require('node:path');
const baseDir = path.join(__dirname, '.tmp-electron-userdata');
module.exports = {
  app: {
    getPath: (name) => {
      if (name === 'home') return path.join(baseDir, 'home');
      return baseDir;
    },
  },
};
