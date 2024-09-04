import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Link, useParams} from 'react-router-dom';
import {Loading} from '../components/Loading.tsx';
import {useQueues} from '../hooks/useQueues.tsx';
import {useWorkers} from '../hooks/useWorkers.tsx';

export function Queue() {
  const { name } = useParams<{name: string}>();
  const { url } = useServerConfiguration();
  const { data: queues, isLoading } = useQueues(url);
  const { data: workers, isLoading: workersLoading } = useWorkers(url);

  if (isLoading || workersLoading) return <Loading />;
  const queue = queues?.find(queue => queue.name === name);

  if (!queue) {
    return (
      <div className={"container"}>
        <h2 className={"mb-4"}>Queue - {name}</h2>
        <div className={"alert alert-danger"}>Queue not found.</div>
      </div>
    );
  }

  return (
    <div className={"container"}>
      <div className={"card mb-4"}>
        <div className={"card-header"}>
          Queue - {queue.name}
        </div>
        {!queue ? (
          <div className={"alert alert-info"}>Queue not found.</div>
        ) : (
          <table className={"table table-hover table-striped mb-0"}>
            <tbody>
            <tr>
              <th>Name</th>
              <td>{queue.name}</td>
            </tr>
            <tr>
              <th>Concurrency</th>
              <td>{queue.concurrency}</td>
            </tr>
            <tr>
              <th>Tags</th>
              <td>
                {queue.tags.map(tag => (
                  <span key={tag} className={"badge bg-primary me-1"}>{tag}</span>
                ))}
              </td>
            </tr>
            <tr>
              <th>State</th>
              <td>
                <span className={`badge bg-${queue.state === 'active' ? 'success' : 'danger'}`}>{queue.state}</span>
              </td>
            </tr>
            <tr>
              <th>Executor</th>
              <td><code>{queue.executor}</code></td>
            </tr>
            <tr>
              <th>Executor Options</th>
              <td>
                <code>
                  <pre>{JSON.stringify(queue.executor_options, null, 2)}</pre>
                </code>
              </td>
            </tr>
            <tr>
              <th>Polling Interval</th>
              <td>{queue.polling_interval}</td>
            </tr>
            <tr>
              <th>Rate Limit</th>
              <td>
                {queue.rate_limit ? `${queue.rate_limit} requests per ${queue.rate_limit_window} seconds` : 'N/A'}
              </td>
            </tr>
            </tbody>
          </table>
        )}
      </div>
      <div className={"card"}>
        <div className={"card-header"}>
          Workers
        </div>
        {!workers ? (
          <div className={"alert alert-info"}>
            No workers are actively processing this queue.
          </div>
        ) : (
          <table className={"table table-hover table-striped mb-0"}>
            <thead>
            <tr>
              <th>Worker ID</th>
              <th>Tags</th>
            </tr>
            </thead>
            <tbody>
            {workers.filter(worker => worker.queues.includes(queue.name)).map(worker => (
              <tr key={worker.worker_id}>
                <td>
                  <Link to={`/workers/${worker.worker_id}`}>{worker.worker_id}</Link>
                </td>
                <td>
                  {worker.tags.map(tag => (
                    <span key={tag} className={"badge bg-secondary me-1"}>{tag}</span>
                  ))}
                </td>
              </tr>
            ))}
            </tbody>
          </table>
        )}
        <div className={"card-footer d-flex align-content-center align-items-center"}>
          <div className={"text-muted"}>
            These workers have announced that they're actively consuming this queue.
          </div>
          <div className={"ms-auto"}>
            <Link to={`/workers`} className={"btn btn-primary btn-sm"}>
              View All Workers
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}

export function Queues() {
  const { url } = useServerConfiguration();
  const { data: queues, isLoading } = useQueues(url);

  if (isLoading) return <Loading />;

  return (
    <div className={"container"}>
      <div className={"card"}>
        <div className={"card-header"}>
          Queues
        </div>
        <table className={"table mb-0"}>
          <thead>
          <tr>
            <th>Name</th>
            <th>Tags</th>
            <th>State</th>
          </tr>
          </thead>
          <tbody>
          {queues?.map(queue => (
            <tr key={queue.name}>
              <td>
                <Link to={`/queues/${queue.name}`}>{queue.name}</Link>
              </td>
              <td>
                {queue.tags.map(tag => (
                  <span key={tag} className={"badge bg-primary me-1"}>{tag}</span>
                ))}
              </td>
              <td>
                <span className={`badge bg-${queue.state === 'active' ? 'success' : 'danger'}`}>{queue.state}</span>
              </td>
            </tr>
          ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}