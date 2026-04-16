import { WebSocket } from 'ws';

const dashboardClients = new Set<WebSocket>();

export function registerDashboardClient(ws: WebSocket) {
  dashboardClients.add(ws);
}

export function unregisterDashboardClient(ws: WebSocket) {
  dashboardClients.delete(ws);
}

export function broadcastToDashboard(message: any) {
  const payload = JSON.stringify(message);
  dashboardClients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  });
}

export function getDashboardClientCount() {
  return dashboardClients.size;
}
