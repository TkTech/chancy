import { useState } from 'react';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { useEvents } from '../hooks/useEvents';

const formatTimestamp = (timestamp: number): string => {
  const date = new Date(timestamp * 1000);
  return date.toLocaleTimeString();
};

export function Events() {
  const { url } = useServerConfiguration();
  const { events, clearEvents } = useEvents(url);
  const [filter, setFilter] = useState('');
  
  const filteredEvents = events.filter(event => {
    if (!filter) return true;
    return (
      event.name.toLowerCase().includes(filter.toLowerCase()) ||
      JSON.stringify(event.body).toLowerCase().includes(filter.toLowerCase())
    );
  });

  return (
    <div className="container-fluid">
      <div className="row mb-3 align-items-center">
        <div className="col">
          <h1>Events</h1>
          <p className="text-muted small">
            Realtime look at internal, cluster-wide events used for coordination.
            This feed can be useful for debugging, monitoring, and optimization - for example, if you consistently see
            &nbsp;<code>queue.pushed</code> events for the same queue come in large bursts together, you may want to consider using a
            &nbsp;<code>Chancy.push_many</code> call instead.
          </p>
        </div>
        <div className="col-auto">
          <div className="input-group input-group-sm">
            <input
              type="text"
              className="form-control"
              placeholder="Filter events..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
            />
            <button
              className="btn btn-outline-secondary"
              onClick={clearEvents}
              title="Clear all events"
            >
              Clear
            </button>
          </div>
        </div>
      </div>

      {filteredEvents.length === 0 ? (
        <div className="alert alert-info">
          {events.length === 0 
            ? "No events received yet. Events will appear here when they occur."
            : "No events match your filter criteria."}
        </div>
      ) : (
        <div className="event-list">
          {filteredEvents.map((event, index) => (
            <div key={index} className="card mb-2">
              <div className="card-header py-2 d-flex justify-content-between align-items-center">
                <span className="fw-bold">{event.name}</span>
                <span className="text-muted small">{formatTimestamp(event.timestamp)}</span>
              </div>
              <div className="card-body py-2">
                <code>
                <pre className={"mb-0"}>
                    {JSON.stringify(event.body, null, 2)}
                </pre>
                </code>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}