import { Bonjour, Browser } from 'bonjour-service';
import os from 'os';

let bonjour: Bonjour | null = null;
let published = false;

interface MdnsStatus {
  advertising: boolean;
  selfTestPassed: boolean | null;
  port: number;
  displayName: string;
  error: string | null;
}

let mdnsStatus: MdnsStatus = {
  advertising: false,
  selfTestPassed: null,
  port: 0,
  displayName: '',
  error: null,
};

export interface DiscoveredHub {
  name: string;
  host: string;
  port: number;
  addresses: string[];
}

export function getMdnsStatus(): MdnsStatus {
  return { ...mdnsStatus };
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

function runSelfTest(displayName: string, port: number) {
  let testBonjour: Bonjour | null = null;
  try {
    testBonjour = new Bonjour();
    const browser = testBonjour.find({ type: 'digipal', protocol: 'tcp' });
    let found = false;

    browser.on('up', (service: any) => {
      if (service.port === port && (service.name === displayName || service.txt?.hostname === os.hostname())) {
        found = true;
        console.log(`[mdns] Self-test PASSED: found own service "${service.name}" on port ${port}`);
        mdnsStatus.selfTestPassed = true;
        try {
          browser.stop();
          testBonjour?.destroy();
          testBonjour = null;
        } catch { }
      }
    });

    setTimeout(() => {
      if (!found) {
        console.warn(`[mdns] Self-test FAILED: could not discover own service "${displayName}" on port ${port} within 5s`);
        console.warn('[mdns] This may indicate mDNS multicast is blocked on this network');
        mdnsStatus.selfTestPassed = false;
      }
      try {
        browser.stop();
        testBonjour?.destroy();
        testBonjour = null;
      } catch { }
    }, 5000);
  } catch (err) {
    console.error('[mdns] Self-test error:', err);
    mdnsStatus.selfTestPassed = false;
    try { testBonjour?.destroy(); } catch { }
  }
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
    mdnsStatus = {
      advertising: true,
      selfTestPassed: null,
      port,
      displayName,
      error: null,
    };
    console.log(`[mdns] Advertising _digipal._tcp as "${displayName}" on port ${port}`);

    setTimeout(() => runSelfTest(displayName, port), 1000);
  } catch (err: any) {
    mdnsStatus = {
      advertising: false,
      selfTestPassed: false,
      port,
      displayName: hubName || `Digipal Hub - ${os.hostname()}`,
      error: err?.message || String(err),
    };
    console.error('[mdns] Failed to start mDNS advertisement:', err);
  }
}

export function stopMdns() {
  if (bonjour && published) {
    bonjour.unpublishAll();
    bonjour.destroy();
    bonjour = null;
    published = false;
    mdnsStatus = {
      advertising: false,
      selfTestPassed: null,
      port: 0,
      displayName: '',
      error: null,
    };
    console.log('[mdns] Stopped mDNS advertisement');
  }
}
