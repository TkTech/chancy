import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {useWorkers} from '../hooks/useWorkers.tsx';

export function Worker () {
  const { worker_id } = useParams<{worker_id: string}>();
  const { url } = useServerConfiguration();
  const { data: workers, isLoading } = useWorkers(url);

  if (isLoading) return <Loading />;

  const worker = workers?.find(worker => worker.worker_id === worker_id);

  if (!worker) {
    return (
      <div className={"container"}>
        <h2 className={"mb-4"}>Worker - {worker_id}</h2>
        <div className={"alert alert-danger"}>Worker not found.</div>
      </div>
    );
  }

  return (
    <div className={"container"}>
      <div className={'card'}>
        <div className={'card-header'}>
          Worker - {worker.worker_id}
        </div>
        <table className={"table mb-0"}>
          <tbody>
          <tr>
            <th>Worker ID</th>
            <td>{worker.worker_id}</td>
          </tr>
          <tr>
            <th>Tags</th>
            <td>
              {worker.tags.map((tag) => (
                <span key={tag} className={'badge bg-secondary me-1'}>{tag}</span>
              ))}
            </td>
          </tr>
          <tr>
            <th>Queues</th>
            <td>
              <div>
                {worker.queues.map((queue) => (
                  <span key={queue} className={'badge bg-primary me-1'}>
                    <a href={`/queues/${queue}`} className={'text-white'}>
                      {queue}
                    </a>
                  </span>
                ))}
              </div>
              <small className={'form-text text-muted'}>
                The worker has declared that it's processing these queues.
              </small>
            </td>
          </tr>
          <tr>
            <th>Last Seen</th>
            <td>{worker.last_seen}</td>
          </tr>
          <tr>
            <th>Expires At</th>
            <td>{worker.expires_at}</td>
          </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function Workers() {
  const {url} = useServerConfiguration();
  const {data: workers, isLoading} = useWorkers(url);

  if (isLoading) return <Loading/>;

  if (!workers) {
    return (
      <div className={'container'}>
        <h2 className={'mb-4'}>Workers</h2>
        <div className={'alert alert-danger'}>Workers not found.</div>
      </div>
    );
  }

  return (
    <div className={'container'}>
      <div className={"card"}>
        <div className={"card-header"}>
          Workers
        </div>
        <table className={'table mb-0'}>
          <thead>
          <tr>
            <th>Worker ID</th>
            <th>Tags</th>
            <th>Queues</th>
          </tr>
          </thead>
          <tbody>
          {workers.map((worker) => (
            <tr key={worker.worker_id}>
              <td className={'text-nowrap'}>
                <Link to={`/workers/${worker.worker_id}`}>{worker.worker_id}</Link>
              </td>
              <td>
                {worker.tags.map((tag) => (
                  <span key={tag} className={'badge bg-secondary me-1'}>{tag}</span>
                ))}
              </td>
              <td>
                {worker.queues.map((queue) => (
                  <span key={queue} className={'badge bg-primary me-1'}>
                    <a href={`/queues/${queue}`} className={"text-white"}>
                      {queue}
                    </a>
                  </span>
                ))}
              </td>
            </tr>
          ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}