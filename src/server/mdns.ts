import { Bonjour } from 'bonjour-service';
import os from 'os';

let bonjour: Bonjour | null = null;
let published = false;

export function startMdns(port: number) {
  try {
    bonjour = new Bonjour();

    const hostname = os.hostname();

    bonjour.publish({
      name: `Digipal Hub - ${hostname}`,
      type: 'digipal',
      protocol: 'tcp',
      port,
      txt: {
        version: '1.0.0',
        hostname,
      },
    });

    published = true;
    console.log(`[mdns] Advertising _digipal._tcp on port ${port}`);
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
