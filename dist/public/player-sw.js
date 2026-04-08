const CACHE_NAME = 'digipal-player-v23';
const TILE_CACHE_NAME = 'digipal-tiles-v1';
const TILE_MAX_ENTRIES = 2000;
const TILE_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

const TILE_HOSTS = [
  'a.basemaps.cartocdn.com',
  'b.basemaps.cartocdn.com',
  'c.basemaps.cartocdn.com',
  'a.tile.openstreetmap.org',
  'b.tile.openstreetmap.org',
  'c.tile.openstreetmap.org',
];

function isTileRequest(url) {
  return TILE_HOSTS.includes(url.hostname) ||
    url.hostname.endsWith('.basemaps.cartocdn.com') ||
    url.hostname.endsWith('.tile.openstreetmap.org') ||
    (url.hostname === 'api.mapbox.com' && url.pathname.includes('/tiles/')) ||
    /\/api\/directory\/public\/\d+\/mapbox-tile\//.test(url.pathname);
}

async function trimTileCache() {
  try {
    const cache = await caches.open(TILE_CACHE_NAME);
    const keys = await cache.keys();
    if (keys.length > TILE_MAX_ENTRIES) {
      const excess = keys.length - TILE_MAX_ENTRIES;
      for (let i = 0; i < excess; i++) {
        await cache.delete(keys[i]);
      }
    }
  } catch (e) {}
}

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll([
        '/player.html',
        '/player-manifest.json'
      ]);
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keyList) => {
      return Promise.all(
        keyList.map((key) => {
          if (key !== CACHE_NAME && key !== 'digipal-content-v1' && key !== TILE_CACHE_NAME) {
            return caches.delete(key);
          }
        })
      );
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  if (isTileRequest(url) && event.request.method === 'GET') {
    event.respondWith(
      caches.open(TILE_CACHE_NAME).then((cache) => {
        return cache.match(event.request).then((cached) => {
          var validCached = null;
          if (cached) {
            var dateHeader = cached.headers.get('sw-cached-at');
            if (!dateHeader || (Date.now() - parseInt(dateHeader, 10)) > TILE_MAX_AGE_MS) {
              cache.delete(event.request);
            } else {
              validCached = cached;
            }
          }
          if (validCached) return validCached;
          return fetch(event.request).then((response) => {
            if (response.ok || response.type === 'opaque') {
              var cloned = response.clone();
              cloned.blob().then((blob) => {
                if (blob.size > 0) {
                  var headers = new Headers();
                  if (response.ok) {
                    response.headers.forEach(function(v, k) { headers.set(k, v); });
                  }
                  headers.set('sw-cached-at', String(Date.now()));
                  cache.put(event.request, new Response(blob, {
                    status: response.status || 200,
                    statusText: response.statusText || 'OK',
                    headers: headers,
                  }));
                  trimTileCache();
                }
              });
            }
            return response;
          }).catch(() => {
            return new Response('', { status: 503 });
          });
        });
      })
    );
    return;
  }

  if (url.pathname.startsWith('/api/directory/public/') && event.request.method === 'GET') {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          }
          return response;
        })
        .catch(() => {
          return caches.match(event.request).then((cached) => {
            return cached || new Response(JSON.stringify({ error: 'Offline' }), {
              status: 503,
              headers: { 'Content-Type': 'application/json' }
            });
          });
        })
    );
    return;
  }

  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws')) {
    return;
  }

  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          }
          return response;
        })
        .catch(() => {
          return caches.match(event.request).then((cached) => {
            return cached || caches.match('/player.html');
          });
        })
    );
    return;
  }

  if (url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
    event.respondWith(
      caches.match(event.request).then((cached) => {
        if (cached) return cached;
        return fetch(event.request).then((response) => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          }
          return response;
        }).catch(() => new Response('', { status: 503 }));
      })
    );
    return;
  }

  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response.ok && event.request.method === 'GET') {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => {
        return caches.match(event.request).then((cached) => {
          return cached || new Response('', { status: 503 });
        });
      })
  );
});
