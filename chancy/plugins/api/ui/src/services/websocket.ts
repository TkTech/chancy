interface Connection {
  ws: WebSocket | null;
  connected: boolean;
}

export function createWebSocketStore(url: string | null) {
  let snapshot: Connection = { ws: null, connected: false };
  const listeners = new Set<() => void>();
  let disconnect: (() => void) | undefined;

  const publish = (connection: Connection) => {
    snapshot = connection;
    listeners.forEach(listener => listener());
  };

  return {
    getSnapshot: () => snapshot,
    subscribe(listener: () => void) {
      listeners.add(listener);
      if (listeners.size === 1 && url) {
        const ws = new WebSocket(url);
        ws.onopen = () => publish({ ws, connected: true });
        ws.onerror = ws.onclose = () => publish({ ws, connected: false });
        disconnect = () => {
          ws.onopen = ws.onerror = ws.onclose = null;
          ws.close();
          snapshot = { ws: null, connected: false };
        };
        publish({ ws, connected: false });
      }
      return () => {
        listeners.delete(listener);
        if (listeners.size === 0) {
          disconnect?.();
          disconnect = undefined;
        }
      };
    },
  };
}
