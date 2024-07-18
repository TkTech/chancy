import {useQuery, keepPreviousData} from '@tanstack/react-query';
import {TimeSinceBlock} from './components/Time';
import {Link} from 'react-router-dom';

export function Workers () {
  const { data, isLoading } = useQuery({
    queryKey: ['workers'],
    queryFn: async () => {
      const response = await fetch('/api/workers');
      return response.json();
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData
  });

  return (
    <div className={"row"}>
      <div className={"col"}>
        <table className={"table table-bordered table-striped"}>
          <thead>
            <tr>
              <th>Worker Tags</th>
              <th>Queues</th>
              <th className={"text-center"}>
                Last Seen
              </th>
            </tr>
          </thead>
          <tbody>
            {data && data.map((worker: any) => {
              const expired = new Date(worker.last_seen) < new Date(worker.expires);
              return (
                <tr key={worker.worker_id}>
                  <td>
                    <ul className={"list-inline mb-0"}>
                      {worker.tags.sort().map((tag: string) => (
                        <li className={"list-inline-item"} key={tag}>
                          <span className={"badge text-bg-secondary"}>{tag}</span>
                        </li>
                      ))}
                      {worker.is_leader && (
                        <li className={"list-inline-item"}>
                          <span className={"badge text-bg-primary"}>Cluster Leader</span>
                        </li>
                      )}
                    </ul>
                  </td>
                  <td>
                    <ul className={"list-inline mb-0"}>
                      {worker.queues.map((queue: string) => (
                        <Link
                          to={`/queues/${queue}`}
                          className={"badge text-bg-success text-decoration-none"} key={queue}
                        >{queue}</Link>
                      ))}
                    </ul>
                  </td>
                  <td
                    className={[
                      "text-center",
                      expired ? "text-warning" : "text-success"
                    ].join(" ")}>
                    <TimeSinceBlock datetime={worker.last_seen} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
