import { useState } from 'react';
import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Link, useParams} from 'react-router-dom';
import {Loading} from '../components/Loading.tsx';
import {useQueues} from '../hooks/useQueues.tsx';
import {useWorkers} from '../hooks/useWorkers.tsx';
import {useMetricDetail} from '../hooks/useMetrics.tsx';
import {MetricChart, ResolutionSelector} from '../components/MetricCharts';

export function Queue() {
  const { name } = useParams<{name: string}>();
  const { url } = useServerConfiguration();
  const { data: queues, isLoading } = useQueues(url);
  const { data: workers, isLoading: workersLoading } = useWorkers(url);
  const [resolution, setResolution] = useState<string>('5min');
  
  // Fetch throughput metrics for this queue
  const { data: throughputData, isLoading: throughputLoading } = useMetricDetail({
    url,
    type: 'queue',
    name: `${name}:throughput`,
    resolution
  });
  
  // Fetch execution time metrics for this queue
  const { data: executionTimeData, isLoading: executionTimeLoading } = useMetricDetail({
    url,
    type: 'queue',
    name: `${name}:execution_time`,
    resolution
  });

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
      
      {hasMetricsPlugin && (
        <div className="mt-4">
          <h3>Queue Metrics</h3>
          <ResolutionSelector resolution={resolution} setResolution={setResolution} />
          
          <div className="row">
            {/* Throughput Chart */}
            <div className="col-md-6 mb-4">
              <div className="card h-100">
                <div className="card-header">
                  <h5 className="mb-0">Throughput</h5>
                </div>
                <div className="card-body">
                  {throughputLoading ? (
                    <Loading />
                  ) : (
                    <>
                      {throughputData?.default && throughputData.default.length > 0 ? (
                        <MetricChart points={throughputData.default} height={250} />
                      ) : (
                        <div className="alert alert-info">No throughput data available.</div>
                      )}
                    </>
                  )}
                </div>
              </div>
            </div>
            
            {/* Execution Time Chart */}
            <div className="col-md-6 mb-4">
              <div className="card h-100">
                <div className="card-header">
                  <h5 className="mb-0">Execution Time</h5>
                </div>
                <div className="card-body">
                  {executionTimeLoading ? (
                    <Loading />
                  ) : (
                    <>
                      {executionTimeData?.default && executionTimeData.default.length > 0 ? (
                        <MetricChart points={executionTimeData.default} height={250} isHistogram={true} />
                      ) : (
                        <div className="alert alert-info">No execution time data available.</div>
                      )}
                    </>
                  )}
                </div>
              </div>
            </div>
          </div>
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

  if (isLoading) return <Loading />;

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Queues</h2>
      <table className={"table table-hover"}>
        <thead>
        <tr>
          <th>Name</th>
          <th className={"w-100"}>Tags</th>
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