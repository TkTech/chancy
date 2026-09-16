import { createContext, useContext, useState, useEffect } from 'react';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { getToken } from '../services/auth';

interface WebSocketContextValue {
  ws: WebSocket | null;
  connected: boolean;
}

const WebSocketContext = createContext<WebSocketContextValue | null>(null);

export function WebSocketProvider({ children }: { children: React.ReactNode }) {
  const { url } = useServerConfiguration();
  const [ws, setWs] = useState<WebSocket | null>(null);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    if (!url) return;

    const t = getToken();
    const tokenQs = t ? `?token=${encodeURIComponent(t)}` : '';
    const wsProto = url.startsWith('https') ? 'wss' : 'ws';
    const wsUrl = `${wsProto}://${url.replace(/^https?:\/\//, '')}/api/v1/ws${tokenQs}`;

    const websocket = new WebSocket(wsUrl);
    setWs(websocket);

    websocket.onopen = () => setConnected(true);
    websocket.onerror = () => setConnected(false);
    websocket.onclose = () => setConnected(false);

    return () => {
      websocket.close();
      setWs(null);
      setConnected(false);
    };
  }, [url]);

  const value = {
    ws,
    connected,
  };

  return (
    <WebSocketContext.Provider value={value}>
      {children}
    </WebSocketContext.Provider>
  );
}

export function useWebSocket() {
  const context = useContext(WebSocketContext);
  if (!context) {
    throw new Error('useWebSocket must be used within WebSocketProvider');
  }
  return context;
}
