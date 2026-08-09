import React, { useEffect, useRef, useState } from 'react';
import { PageHeader } from '../components/common/PageHeader';
import { DataTable } from '../components/common/DataTable';
import { JsonViewer } from '../components/JsonViewer';
import { useTheme } from '../contexts/ThemeContext';
import { useWebSocket } from '../contexts/WebSocketContext';

interface GossipEvent {
  timestamp: Date;
  firstSeen: Date;
  event: string;
  data: any;
  count: number;
  isAggregated: boolean;
}

const AGGREGATION_WINDOW_MS = 1000;
const FLUSH_INTERVAL_MS = 100;
const MAX_GOSSIP_EVENTS = 50;

export function Gossip() {
  const { ws, connected } = useWebSocket();
  const { theme } = useTheme();
  const [events, setEvents] = useState<GossipEvent[]>([]);
  const bufferRef = useRef<Map<string, {event: GossipEvent, lastSeen: Date}>>(new Map());
  const flushTimerRef = useRef<number | null>(null);

  useEffect(() => {
    if (!ws) return;

    const flushBuffer = () => {
      const now = Date.now();
      const toFlush: GossipEvent[] = [];

      for (const [key, {event, lastSeen}] of bufferRef.current.entries()) {
        if (now - lastSeen.getTime() >= AGGREGATION_WINDOW_MS) {
          toFlush.push(event);
          bufferRef.current.delete(key);
        }
      }

      if (toFlush.length > 0) {
        setEvents((prev) => {
          const newEvents = [...toFlush, ...prev];
          return newEvents.slice(0, MAX_GOSSIP_EVENTS);
        });
      }

      if (bufferRef.current.size > 0) {
        flushTimerRef.current = setTimeout(flushBuffer, FLUSH_INTERVAL_MS);
      } else {
        flushTimerRef.current = null;
      }
    };

    const handleMessage = (ev: MessageEvent) => {
      try {
        const msg = JSON.parse(ev.data);
        const evt = msg?.event as string;
        const data = msg?.data;

        if (!evt) return;

        const aggKey = `${evt}::${JSON.stringify(data)}`;
        const now = new Date();

        const existing = bufferRef.current.get(aggKey);
        if (existing) {
          existing.event.count++;
          existing.event.timestamp = now;
          existing.event.isAggregated = existing.event.count > 5;
          existing.lastSeen = now;
        } else {
          bufferRef.current.set(aggKey, {
            event: {
              timestamp: now,
              firstSeen: now,
              event: evt,
              data: data,
              count: 1,
              isAggregated: false,
            },
            lastSeen: now,
          });
        }

        if (!flushTimerRef.current) {
          flushTimerRef.current = setTimeout(flushBuffer, FLUSH_INTERVAL_MS);
        }
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    };

    ws.addEventListener('message', handleMessage);

    return () => {
      ws.removeEventListener('message', handleMessage);
      if (flushTimerRef.current) {
        clearTimeout(flushTimerRef.current);
        flushTimerRef.current = null;
      }
      bufferRef.current.clear();
      setEvents([]);
    };
  }, [ws]);

  return (
    <div className="container-fluid">
      <PageHeader
        title="Gossip"
        description={
          <>
            Live stream of all hub events •
            <span className={`ms-2 badge ${connected ? 'bg-success' : 'bg-danger'}`}>
              {connected ? 'Connected' : 'Disconnected'}
            </span>
          </>
        }
      />

      {!connected && (
        <div className="alert alert-warning mb-3">
          WebSocket connection is not active. Events will appear when connected.
        </div>
      )}

      <DataTable className="table mb-0">
        <thead>
          <tr>
            <th className="text-nowrap">Timestamp</th>
            <th className="text-nowrap">Event</th>
            <th className="w-100">Data</th>
          </tr>
        </thead>
        <tbody>
          {events.length === 0 && (
            <tr>
              <td colSpan={3} className="text-center text-muted">
                {connected ? 'Waiting for events...' : 'Not connected to event stream'}
              </td>
            </tr>
          )}
          {events.map((event, idx) => {
            const borderClass = event.count >= 50
              ? 'border-start border-danger border-4'
              : event.isAggregated
                ? 'border-start border-warning border-3'
                : '';
            const isQueuePushedStorm = event.event === 'queue.pushed' && event.count >= 50;

            return (
              <React.Fragment key={idx}>
                <tr className={borderClass}>
                  <td className="text-nowrap" style={{ fontFamily: 'monospace', fontSize: '0.9em' }}>
                    <div>{event.firstSeen.toLocaleTimeString()}.{event.firstSeen.getMilliseconds().toString().padStart(3, '0')}</div>
                    {event.count > 1 && (
                      <div className="text-muted" style={{ fontSize: '0.85em' }}>
                        → {event.timestamp.toLocaleTimeString()}.{event.timestamp.getMilliseconds().toString().padStart(3, '0')}
                      </div>
                    )}
                  </td>
                  <td className="text-nowrap">
                    <code>{event.event}</code>
                    {event.count > 1 && (
                      <span className={`ms-2 badge ${event.count >= 50 ? 'bg-danger' : 'bg-warning'}`}>
                        ×{event.count}
                      </span>
                    )}
                  </td>
                  <td>
                    {event.data && Object.keys(event.data).length > 0 ? (
                      <div className="json-viewer-container">
                        <JsonViewer value={event.data} theme={theme} />
                      </div>
                    ) : (
                      <span className="text-muted">-</span>
                    )}
                  </td>
                </tr>
                {isQueuePushedStorm && (
                  <tr>
                    <td colSpan={3} className="p-0">
                      <div className="alert alert-warning mb-0 border-0 border-top rounded-0" style={{ fontSize: '0.9em' }}>
                        <strong>Performance Tip:</strong> This many individual <code>queue.pushed</code> events can impact performance.
                        Consider using <code>push_ex(..., notify=False)</code> or bulk push methods like <code>push_many_ex()</code> to reduce notification overhead.
                      </div>
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </DataTable>
    </div>
  );
}
