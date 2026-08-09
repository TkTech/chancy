import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {useWorkers, Worker} from '../hooks/useWorkers.tsx';
import {CountdownTimer} from '../components/UpdatingTime.tsx';
import {PageHeader} from '../components/common/PageHeader.tsx';
import { MetricStatCard } from '../components/dashboard/MetricStatCard';
import { MetricSuccessRateCard } from '../components/dashboard/MetricSuccessRateCard';
import { MetricHistogramCard } from '../components/dashboard/MetricHistogramCard';

function WorkerInfoTable({ worker } : { worker: Worker }) {
  return (
    <div className="card">
      <div className="card-header">Details</div>
      <table className={"table table-hover mb-0"}>
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
          <td><CountdownTimer date={worker.last_seen} /></td>
        </tr>
        <tr>
          <th className={"text-nowrap"}>Expires At</th>
          <td><CountdownTimer date={worker.expires_at} /></td>
        </tr>
        </tbody>
      </table>
    </div>
  );
}

export function WorkerDetails () {
  const { worker_id } = useParams<{worker_id: string}>();
  const { url } = useServerConfiguration();
  const { data: workers, isLoading } = useWorkers(url);
  const resolution = '5min';

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
      <PageHeader
        title={`Worker - ${worker.worker_id}`}
      />
      <WorkerInfoTable worker={worker} />
      {worker.queues.length > 0 && (
        <>
          <div className="alert alert-info mt-4">
            Per-queue metrics are for jobs processed by <strong>this</strong> worker only. See the queue details pages for overall queue metrics.
          </div>
          {worker.queues.map(queueName => (
            <div key={queueName} className="mb-4">
              <h5 className="mb-3">
                <Link to={`/queues/${queueName}`}>{queueName}</Link>
              </h5>
              <div className="row g-3">
                <div className="col-12 col-md-6 col-xl-3">
                  <MetricSuccessRateCard
                    title="Success Rate"
                    succeededKey={`queue:${queueName}:succeeded`}
                    failedKey={`queue:${queueName}:failed`}
                    url={url!}
                    resolution={resolution}
                    workerId={worker.worker_id}
                  />
                </div>
                <div className="col-12 col-md-6 col-xl-3">
                  <MetricStatCard
                    title="Tasks Succeeded"
                    metricKey={`queue:${queueName}:succeeded`}
                    url={url!}
                    resolution={resolution}
                    subtitle="last 24 hours"
                    showSparkline={true}
                    sparklineColor="#10b981"
                    workerId={worker.worker_id}
                  />
                </div>
                <div className="col-12 col-md-6 col-xl-3">
                  <MetricStatCard
                    title="Tasks Failed"
                    metricKey={`queue:${queueName}:failed`}
                    url={url!}
                    resolution={resolution}
                    subtitle="last 24 hours"
                    showSparkline={true}
                    sparklineColor="#ef4444"
                    workerId={worker.worker_id}
                  />
                </div>
                <div className="col-12 col-md-6 col-xl-3">
                  <MetricHistogramCard
                    title="Avg Execution Time"
                    metricKey={`queue:${queueName}:execution_time`}
                    url={url!}
                    resolution={resolution}
                    stat="avg"
                    formatValue={(v) => `${v.toFixed(0)}ms`}
                    sparklineColor="#8b5cf6"
                    workerId={worker.worker_id}
                  />
                </div>
              </div>
            </div>
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
      <div className={'container-fluid'}>
        <PageHeader
          title="Workers"
          description="Active workers processing jobs across queues"
        />
        <div className={'alert alert-danger'}>Workers not found.</div>
      </div>
    );
  }

  return (
    <div className={'container-fluid'}>
      <PageHeader
        title="Workers"
        description="Active workers processing jobs across queues"
      />

      <div className="card">
        <table className="table table-hover mb-0">
          <thead>
            <tr>
              <th>Worker ID</th>
              <th>Tags</th>
              <th>Queues</th>
              <th>Last Seen</th>
              <th>Expires At</th>
            </tr>
          </thead>
          <tbody>
            {workers.sort((a, b) => a.worker_id.localeCompare(b.worker_id)).map(worker => (
              <tr key={worker.worker_id}>
                <td>
                  <Link to={`/workers/${worker.worker_id}`} className="fw-medium">
                    {worker.worker_id}
                  </Link>
                </td>
                <td>
                  {worker.is_leader && (
                    <span className={'badge bg-success me-1'}>Leader</span>
                  )}
                  {worker.tags.map((tag) => (
                    <span key={tag} className={'badge bg-secondary me-1'}>{tag}</span>
                  ))}
                </td>
                <td>
                  {worker.queues.map((queue) => (
                    <span key={queue} className={'badge bg-primary me-1'}>
                      <Link to={`/queues/${queue}`} className={'text-white text-decoration-none'}>
                        {queue}
                      </Link>
                    </span>
                  ))}
                </td>
                <td className="text-nowrap font-monospace">
                  <CountdownTimer date={worker.last_seen} />
                </td>
                <td className="text-nowrap font-monospace">
                  <CountdownTimer date={worker.expires_at} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
