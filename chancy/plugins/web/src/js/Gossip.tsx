import { useEffect, useState } from 'react';
import { TimeBlock, TimeSinceBlock } from './components/Time';

export function Gossip() {
  // Contains up to the last 100 gossip messages
  const [rows, setRows] = useState([]);

  useEffect(() => {
    let url = new URL("/ws-gossip", window.location.href);
    url.protocol = url.protocol.replace("http", "ws");
    const websocket = new WebSocket(url.href);

    websocket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      data.when = new Date();

      setRows((rows) => {
        if (rows.length > 100) {
          return rows.slice(1).concat(data);
        } else {
          return rows.concat(data);
        }
      });
    }

    return () => {
      websocket.close();
    }
  }, []);

  return (
    <div className="row">
      <div className="col">
        <div className="table-responsive">
          <table className="table table-bordered table-striped">
            <thead>
              <tr>
                <th>When</th>
                <th>Event</th>
                <th>Payload</th>
              </tr>
            </thead>
            <tbody>
              {rows.toReversed().map((row, index) => (
                <tr key={index}>
                  <td><code><TimeBlock datetime={row.when}/></code></td>
                  <td>{row.event}</td>
                  <td><code>{JSON.stringify(row.body)}</code></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}