import { useState } from 'react';
import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Link, useParams} from 'react-router-dom';
import {Loading} from '../components/Loading.tsx';
import {useQueues} from '../hooks/useQueues.tsx';
import {useWorkers} from '../hooks/useWorkers.tsx';
import {QueueMetrics, ResolutionSelector, SparklineChart} from '../components/MetricCharts';
import {useMetricDetail} from '../hooks/useMetrics.tsx';

function QueueThroughputSpark({ queueName, apiUrl }: { queueName: string, apiUrl: string | null }) {

  const key = `queue:${queueName}:throughput`;
  const { data, isLoading } = useMetricDetail({
    url: apiUrl,
    key: key,
    resolution: '5min',
    limit: 20,
    enabled: !!apiUrl
  })

  if (isLoading || !data) {
    return <div style={{ width: 80, height: 30 }} />;
  }

  return <SparklineChart points={data[key].data} resolution="5min" />;
}

export function Queue() {
  const { name } = useParams<{name: string}>();
  const { url } = useServerConfiguration();
  const { data: queues, isLoading } = useQueues(url);
  const { data: workers, isLoading: workersLoading } = useWorkers(url);
  const [resolution, setResolution] = useState<string>('5min');
  
  // Check if metrics plugin is available
  const hasMetricsPlugin = useServerConfiguration().configuration?.plugins?.includes('Metrics');

  if (isLoading || workersLoading) return <Loading />;
  const queue = queues?.find(queue => queue.name === name);

  if (!queue) {
    return (
      <div className={"container-fluid"}>
        <h2 className={"mb-4"}>Queue - {name}</h2>
        <div className={"alert alert-danger"}>Queue not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Queue - {queue.name}</h2>
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
      
      {hasMetricsPlugin && (
        <div className="mt-4">
          <h3 className={"mb-3"}>Queue Metrics</h3>
          <ResolutionSelector resolution={resolution} setResolution={setResolution} />
          
          <QueueMetrics
            apiUrl={url}
            queueName={queue.name}
            resolution={resolution}
          />
        </div>
      )}
      
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
          {workers.filter(worker => worker.queues.includes(queue.name)).map(worker => (
            <tr key={worker.worker_id}>
              <td>
                <Link to={`/workers/${worker.worker_id}`}>{worker.worker_id}</Link>
              </td>
            </tr>
          ))}
          </tbody>
        </table>
      )}
    </div>
  );
}


export function Queues() {
  const { url } = useServerConfiguration();
  const { data: queues, isLoading } = useQueues(url);
  const hasMetricsPlugin = useServerConfiguration().configuration?.plugins?.includes('Metrics');

  if (isLoading) return <Loading />;

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
              <Link to={`/queues/${queue.name}`}>{queue.name}</Link>
            </td>
            <td>
              {queue.tags.map(tag => (
                <span key={tag} className={"badge bg-primary me-1"}>{tag}</span>
              ))}
            </td>
            {hasMetricsPlugin && (
              <td className="text-center">
                <QueueThroughputSpark queueName={queue.name} apiUrl={url} />
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