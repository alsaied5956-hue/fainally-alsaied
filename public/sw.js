// Service Worker for Offline & Online PWA Caching
const CACHE_NAME = "math-center-v3.5";
const ASSETS_TO_CACHE = [
  "/",
  "/index.html"
];

// Install Event: Cache critical app shell
self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(ASSETS_TO_CACHE).catch((err) => {
        console.warn("Service Worker pre-cache partial warning:", err);
      });
    }).then(() => self.skipWaiting())
  );
});

// Activate Event: Cleanup older caches
self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((name) => {
          if (name !== CACHE_NAME) {
            return caches.delete(name);
          }
        })
      );
    }).then(() => self.clients.claim())
  );
});

// Fetch Event: Network-First with Cache Fallback for dynamic, Stale-While-Revalidate for assets
self.addEventListener("fetch", (event) => {
  const request = event.request;

  // Don't intercept non-GET requests, API routes, or Firebase/Google API cloud calls
  if (
    request.method !== "GET" ||
    request.url.includes("/api/") ||
    request.url.includes("firestore.googleapis.com") ||
    request.url.includes("firebaseapp.com") ||
    request.url.includes("supabase.co") ||
    request.url.includes("identitytoolkit.googleapis.com") ||
    request.url.includes("chrome-extension")
  ) {
    return;
  }

  event.respondWith(
    fetch(request)
      .then((networkResponse) => {
        // If response is valid, update cache in background
        if (networkResponse && networkResponse.status === 200) {
          const responseToCache = networkResponse.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(request, responseToCache).catch(() => {});
          });
        }
        return networkResponse;
      })
      .catch(() => {
        // Network failed (Offline) -> Return cached response
        return caches.match(request).then((cachedResponse) => {
          if (cachedResponse) {
            return cachedResponse;
          }
          // If navigation request (HTML), fallback to root index.html
          if (request.mode === "navigate") {
            return caches.match("/index.html") || caches.match("/");
          }
          return new Response("Offline resource unavailable", {
            status: 503,
            statusText: "Offline",
          });
        });
      })
  );
});
