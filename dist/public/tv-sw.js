const BUILD_TS = '__BUILD_TIMESTAMP__';
const CACHE_NAME = 'digipal-tv-v' + BUILD_TS;
const STATIC_CACHE = 'digipal-tv-static-v' + BUILD_TS;

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keyList) => {
      return Promise.all(
        keyList.map((key) => {
          if (key !== CACHE_NAME && key !== STATIC_CACHE) {
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

  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws')) {
    return;
  }

  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => {
              cache.put(event.request, clone);
            });
          }
          return response;
        })
        .catch(() => {
          return caches.match(event.request).then((cached) => {
            if (cached) return cached;
            return caches.match('/tv').then((tvCached) => {
              if (tvCached) return tvCached;
              return new Response(
                '<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Offline</title></head><body style="background:#0a0e1a;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;font-family:sans-serif"><div style="text-align:center"><h2>Loading offline content...</h2><p>Please wait while the player starts.</p></div></body></html>',
                { headers: { 'Content-Type': 'text/html' } }
              );
            });
          });
        })
    );
    return;
  }

  if (
    url.pathname.match(/\.(js|css)$/) ||
    url.pathname.startsWith('/assets/')
  ) {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(STATIC_CACHE).then((cache) => {
              cache.put(event.request, clone);
            });
          }
          return response;
        })
        .catch(() => {
          return caches.match(event.request).then((cached) => {
            return cached || new Response('', { status: 503 });
          });
        })
    );
    return;
  }

  if (url.pathname.match(/\.(woff2?|ttf|eot|svg|png|jpg|ico)$/)) {
    event.respondWith(
      caches.match(event.request).then((cached) => {
        if (cached) {
          fetch(event.request)
            .then((response) => {
              if (response.ok) {
                caches.open(STATIC_CACHE).then((cache) => {
                  cache.put(event.request, response);
                });
              }
            })
            .catch(() => {});
          return cached;
        }
        return fetch(event.request)
          .then((response) => {
            if (response.ok) {
              const clone = response.clone();
              caches.open(STATIC_CACHE).then((cache) => {
                cache.put(event.request, clone);
              });
            }
            return response;
          })
          .catch(() => new Response('', { status: 503 }));
      })
    );
    return;
  }

  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response.ok && event.request.method === 'GET') {
          const clone = response.clone();
          caches.open(STATIC_CACHE).then((cache) => {
            cache.put(event.request, clone);
          });
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
