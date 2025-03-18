import { useState, useEffect, useRef } from 'react';
import { getWebSocketUrl } from './useServerConfiguration';

interface EventData {
  name: string;
  body: Record<string, unknown>;
  timestamp: number;
}

export const useEvents = (url: string | null) => {
  const [events, setEvents] = useState<EventData[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const maxEvents = 1000;

  useEffect(() => {
    if (!url) {
      return;
    }

    const wsUrl = `${getWebSocketUrl(url)}/api/v1/events`;
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;
    
    ws.onopen = () => {
      setIsConnected(true);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        if (data.type === 'ping') {
          return;
        }
        
        setEvents(prev => [data, ...prev].slice(0, maxEvents));
      } catch (e) {
        console.error('Error parsing event data:', e);
      }
    };

    ws.onclose = () => {
      setIsConnected(false);
    };

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [url]);

  const clearEvents = () => {
    setEvents([]);
  };

  return { events, isConnected, clearEvents };
};