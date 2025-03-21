import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {useWorkers, Worker} from '../hooks/useWorkers.tsx';
import {UpdatingTime} from '../components/UpdatingTime.tsx';
import {QueueMetrics, ResolutionSelector} from '../components/MetricCharts.tsx';
import {useState} from 'react';

function WorkerInfoTable({ worker } : { worker: Worker }) {
  return (
    <table className={"table table-hover border mb-4"}>
      <tbody>
      <tr>
        <th className={"text-nowrap"}>Worker ID</th>
        <td>
          {worker.worker_id}
        </td>
      </tr>
      <tr>
        <th>Tags</th>
        <td>
          {worker.is_leader && (
            <span className={'badge bg-success me-1'}>Leader Node</span>
          )}
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
        </td>
      </tr>
      <tr>
        <th className={"text-nowrap"}>Last Seen</th>
        <td><UpdatingTime date={worker.last_seen} /></td>
      </tr>
      <tr>
        <th className={"text-nowrap"}>Expires At</th>
        <td><UpdatingTime date={worker.expires_at} /></td>
      </tr>
      </tbody>
    </table>
  );
}

export function WorkerDetails () {
  const { worker_id } = useParams<{worker_id: string}>();
  const { url } = useServerConfiguration();
  const { data: workers, isLoading } = useWorkers(url);
  const [resolution, setResolution] = useState<string>('5min');

  if (isLoading) return <Loading />;

  const worker = workers?.find(worker => worker.worker_id === worker_id);

  if (!worker) {
    return (
      <div className={"container-fuid"}>
        <h2 className={"mb-4"}>Worker - {worker_id}</h2>
        <div className={"alert alert-danger"}>Worker not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Worker - {worker.worker_id}</h2>
      <h3 className="mb-3">Details</h3>
      <WorkerInfoTable worker={worker} />
      {worker.queues.length > 0 && (
        <>
          <h3 className="mb-3">Queue Metrics</h3>
          <p>Per-queue metrics are for jobs processed by <strong>this</strong> worker only.</p>
          <ResolutionSelector resolution={resolution} setResolution={setResolution} />
          
          {worker.queues.map(queueName => (
            <QueueMetrics
              key={queueName}
              apiUrl={url}
              queueName={queueName}
              resolution={resolution}
              workerId={worker.worker_id}
            />
          ))}
        </>
      )}
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
    <div className={'container-fluid'}>
      <h2 className={'mb-4'}>Workers</h2>
      {workers.sort((a, b) => a.worker_id.localeCompare(b.worker_id)).map(worker => (
        <div key={worker.worker_id}>
          <h3>
            <Link to={`/workers/${worker.worker_id}`}>{worker.worker_id}</Link>
          </h3>
          <WorkerInfoTable worker={worker} />
        </div>
      ))}
    </div>
  );
}