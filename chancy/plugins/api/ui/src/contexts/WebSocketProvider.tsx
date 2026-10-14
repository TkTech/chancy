import { useMemo, useSyncExternalStore, ReactNode } from 'react';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { getToken } from '../services/auth';
import { createWebSocketStore } from '../services/websocket';
import { WebSocketContext } from './WebSocketContext';

export function WebSocketProvider({ children }: { children: ReactNode }) {
  const { url, configuration } = useServerConfiguration();
  let wsUrl: string | null = null;
  if (url && configuration) {
    const token = getToken(url);
    const tokenQs = token ? `?token=${encodeURIComponent(token)}` : '';
    const protocol = url.startsWith('https') ? 'wss' : 'ws';
    wsUrl = `${protocol}://${url.replace(/^https?:\/\//, '')}/api/v1/ws${tokenQs}`;
  }

  const store = useMemo(() => createWebSocketStore(wsUrl), [wsUrl]);
  const connection = useSyncExternalStore(store.subscribe, store.getSnapshot);

  return (
    <WebSocketContext.Provider value={connection}>
      {children}
    </WebSocketContext.Provider>
  );
}
