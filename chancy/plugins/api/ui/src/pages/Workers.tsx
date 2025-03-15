import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {useWorkers} from '../hooks/useWorkers.tsx';
import {CountdownTimer, UpdatingTime} from '../components/UpdatingTime.tsx';
import {QueueMetrics, ResolutionSelector} from '../components/MetricCharts.tsx';
import {useState} from 'react';

export function Worker () {
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
      <table className={"table table-hover border mb-4"}>
        <tbody>
        <tr>
          <th className={"text-nowrap"}>Worker ID</th>
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
      <table className={'table mb-0'}>
        <thead>
        <tr>
          <th className={"text-nowrap"}>Worker ID</th>
          <th className={"text-nowrap"}>Active Queues</th>
          <th className={"text-nowrap text-center"}>Last Seen</th>
        </tr>
        </thead>
        <tbody>
        {workers.map((worker) => (
          <tr key={worker.worker_id}>
            <td className={'text-nowrap'}>
              <Link to={`/workers/${worker.worker_id}`}>{worker.worker_id}</Link>
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
            <td className={"text-center"}>
              <CountdownTimer date={worker.last_seen}/>
            </td>
          </tr>
        ))}
        </tbody>
      </table>
    </div>
  );
}