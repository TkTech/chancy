import { useState } from 'react';
import {useApp} from '../hooks/useServerConfiguration.tsx';
import {Link, useParams } from 'react-router-dom';
import {Loading} from '../components/Loading.tsx';
import {useQueues} from '../hooks/useQueues.tsx';
import {useWorkers} from '../hooks/useWorkers.tsx';
import {QueueMetrics, ResolutionSelector, SparklineChart} from '../components/MetricCharts';
import {useMetricDetail} from '../hooks/useMetrics.tsx';
import React from 'react';
import {useSlidePanels} from '../components/SlidePanelContext.tsx';
import {WorkerDetails} from './Workers.tsx';

function QueueThroughputSpark({ queueName, apiUrl }: { queueName: string, apiUrl: string | null }) {

  const key = `queue:${queueName}:throughput`;
  const { data, isLoading } = useMetricDetail({
    url: apiUrl,
    key: key,
    resolution: '5min',
    limit: 20,
    enabled: !!apiUrl
  })

  if (isLoading || !data || !data[key]) {
    return <div style={{ width: 80, height: 30 }} />;
  }

  return <SparklineChart points={data[key].data} resolution="5min" />;
}

interface QueueProps {
  queueName?: string;
  inPanel?: boolean;
}

export function Queue({ queueName, inPanel = false }: QueueProps) {
  const params = useParams<{name: string}>();
  const name = queueName || params.name;
  const { serverUrl, configuration } = useApp();
  const { data: queues, isLoading } = useQueues(serverUrl);
  const { data: workers, isLoading: workersLoading } = useWorkers(serverUrl);
  const [resolution, setResolution] = useState<string>('5min');
  const { openPanel } = useSlidePanels();
  
  // Check if metrics plugin is available
  const hasMetricsPlugin = configuration?.plugins.includes('Metrics');

  if (isLoading || workersLoading) return <Loading />;
  const queue = queues?.find(queue => queue.name === name);

  if (!queue) {
    return (
      <div className={inPanel ? "" : "container-fluid"}>
        {!inPanel && <h2 className={"mb-4"}>Queue - {name}</h2>}
        <div className={"alert alert-danger"}>Queue not found.</div>
      </div>
    );
  }

  return (
    <div className={inPanel ? "" : "container-fluid"}>
      {!inPanel && <h2 className={"mb-4"}>Queue - {queue.name}</h2>}
      <table className={"table table-hover mb-0 border"}>
        <tbody>
        <tr>
          <th>Name</th>
          <td>{queue.name}</td>
        </tr>
        <tr>
          <th>Concurrency</th>
          <td>{queue.concurrency || <em>Executor default</em>}</td>
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
            {(queue.state === 'paused' && queue.resume_at) && (
              <span className={"text-info ms-2"}>
                Automatically resuming at <code>{queue.resume_at}</code>
              </span>
            )}
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
            {queue.rate_limit ?`${queue.rate_limit} requests per ${queue.rate_limit_window} seconds` : <em>No rate limit applied to this queue.</em>}
          </td>
        </tr>
        </tbody>
      </table>

      <h3 className={"mt-4"}>Active Workers</h3>
      <p>
        These workers have announced that they are actively accepting jobs from the <code>{queue.name}</code> queue.
      </p>
      {!workers ? (
        <div className={"alert alert-info"}>
          No workers are actively processing this queue.
        </div>
      ) : (
        <table className={"table table-hover border mb-0"}>
          <thead>
          <tr>
            <th>Worker ID</th>
          </tr>
          </thead>
          <tbody>
          {workers.filter(worker => worker.queues.includes(queue.name)).map(worker => {
            const handleClick = (e: React.MouseEvent) => {
              e.preventDefault();
              openPanel({
                title: "Worker Details",
                content: <WorkerDetails workerId={worker.worker_id} inPanel={true} />
              });
            };
            return (
              <tr key={worker.worker_id}>
                <td>
                  <Link to={`/workers/${worker.worker_id}`} onClick={handleClick}>{worker.worker_id}</Link>
                </td>
              </tr>
            );
          })}
          </tbody>
        </table>
      )}

      {hasMetricsPlugin && (
        <div className="mt-4">
          <h3 className={"mb-3"}>Queue Metrics</h3>
          <ResolutionSelector resolution={resolution} setResolution={setResolution} />

          <QueueMetrics
            apiUrl={serverUrl}
            queueName={queue.name}
            resolution={resolution}
          />
        </div>
      )}
    </div>
  );
}


export function Queues() {
  const { serverUrl, configuration } = useApp();
  const { data: queues, isLoading } = useQueues(serverUrl);
  const hasMetricsPlugin = configuration?.plugins.includes('Metrics');
  const { openPanel } = useSlidePanels();

  if (isLoading) return <Loading />;

  const handleQueueClick = (queueName: string, e: React.MouseEvent) => {
    e.preventDefault();
    openPanel({
      title: "Queue Details",
      content: <Queue queueName={queueName} inPanel={true} />
    });
  };

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Queues</h2>
      <table className={"table table-hover"}>
        <thead>
        <tr>
          <th>Name</th>
          <th className={"w-100"}>Tags</th>
          {hasMetricsPlugin && <th className="text-center">Throughput</th>}
          <th className={"text-center"}>State</th>
        </tr>
        </thead>
        <tbody>
        {queues?.map(queue => (
          <tr key={queue.name}>
            <td>
              <Link to={`/queues/${queue.name}`} onClick={(e) => handleQueueClick(queue.name, e)}>{queue.name}</Link>
            </td>
            <td>
              {queue.tags.map(tag => (
                <span key={tag} className={"badge bg-primary me-1"}>{tag}</span>
              ))}
            </td>
            {hasMetricsPlugin && (
              <td className="text-center">
                <QueueThroughputSpark queueName={queue.name} apiUrl={serverUrl} />
              </td>
            )}
            <td className={"text-center"}>
              <span className={`badge bg-${queue.state === 'active' ? 'success' : 'danger'}`}>{queue.state}</span>
            </td>
          </tr>
        ))}
        </tbody>
      </table>
    </div>
  )
}