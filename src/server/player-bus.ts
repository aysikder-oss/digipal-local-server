import { WebSocket } from 'ws';

const connectedPlayers = new Map<string, WebSocket>();

export function getConnectedPlayers() {
  return connectedPlayers;
}

export function registerPlayer(pairingCode: string, ws: WebSocket) {
  connectedPlayers.set(pairingCode, ws);
}

export function unregisterPlayer(pairingCode: string) {
  connectedPlayers.delete(pairingCode);
}

export function broadcastToPlayers(message: any) {
  const payload = JSON.stringify(message);
  connectedPlayers.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  });
}
