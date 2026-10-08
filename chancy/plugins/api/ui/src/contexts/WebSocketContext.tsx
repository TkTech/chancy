import { createContext, useContext } from 'react';

export interface WebSocketContextValue {
  ws: WebSocket | null;
  connected: boolean;
}

export const WebSocketContext = createContext<WebSocketContextValue | null>(null);

export function useWebSocket() {
  const context = useContext(WebSocketContext);
  if (!context) {
    throw new Error('useWebSocket must be used within WebSocketProvider');
  }
  return context;
}
