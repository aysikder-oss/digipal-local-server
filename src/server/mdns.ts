import { Bonjour, Browser } from 'bonjour-service';
import os from 'os';

let bonjour: Bonjour | null = null;
let published = false;

export interface DiscoveredHub {
  name: string;
  host: string;
  port: number;
  addresses: string[];
}

export function scanForExistingHubs(timeoutMs = 5000): Promise<DiscoveredHub[]> {
  return new Promise((resolve) => {
    const discovered: DiscoveredHub[] = [];
    let scanBonjour: Bonjour | null = null;
    let browser: Browser | null = null;

    try {
      scanBonjour = new Bonjour();
      browser = scanBonjour.find({ type: 'digipal', protocol: 'tcp' });

      browser.on('up', (service: any) => {
        discovered.push({
          name: service.name || 'Unknown Hub',
          host: service.host || service.referer?.address || '',
          port: service.port || 8787,
          addresses: service.addresses || [],
        });
      });

      setTimeout(() => {
        try {
          browser?.stop();
          scanBonjour?.destroy();
        } catch { }
        resolve(discovered);
      }, timeoutMs);
    } catch (err) {
      console.error('[mdns] Failed to scan for existing hubs:', err);
      resolve([]);
    }
  });
}

export function startMdns(port: number, hubName?: string) {
  try {
    bonjour = new Bonjour();

    const hostname = os.hostname();
    const displayName = hubName || `Digipal Hub - ${hostname}`;

    bonjour.publish({
      name: displayName,
      type: 'digipal',
      protocol: 'tcp',
      port,
      txt: {
        version: '1.0.0',
        hostname,
        hubName: hubName || hostname,
      },
    });

    published = true;
    console.log(`[mdns] Advertising _digipal._tcp as "${displayName}" on port ${port}`);
  } catch (err) {
    console.error('[mdns] Failed to start mDNS advertisement:', err);
  }
}

export function stopMdns() {
  if (bonjour && published) {
    bonjour.unpublishAll();
    bonjour.destroy();
    bonjour = null;
    published = false;
    console.log('[mdns] Stopped mDNS advertisement');
  }
}
