import { DetailCard } from '../components/common/DetailCard';
import { StatusBadge } from '../components/common/StatusBadge';
import type { Queue as QueueData } from '../services/chancy';
import { useState } from 'react';
import { formatExecutionTime } from '../utils';
import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Link, useParams} from 'react-router';
import {Loading} from '../components/Loading.tsx';
import {useQueues} from '../hooks/useQueues.tsx';
import {useWorkers} from '../hooks/useWorkers.tsx';
import {SparklineChart} from '../components/MetricCharts';
import {useMetricDetail} from '../hooks/useMetrics.tsx';
import { useQueueActions } from '../hooks/useQueueActions.tsx';
import { useConfirm } from '../components/common/ConfirmDialog.tsx';
import { QueueForm } from '../features/queues/QueueForm.tsx';
import { JsonViewer } from '../components/JsonViewer';
import { useTheme } from '../contexts/ThemeContext';
import { QueueStateCard } from '../components/queue/QueueStateCard';
import { PageHeader } from '../components/common/PageHeader';
import { DataTable } from '../components/common/DataTable';
import { MetricStatCard } from '../components/dashboard/MetricStatCard';
import { MetricSuccessRateCard } from '../components/dashboard/MetricSuccessRateCard';
import { MetricHistogramCard } from '../components/dashboard/MetricHistogramCard';

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
    return <div className="sparkline-placeholder" />;
  }

  return <SparklineChart points={data[key].data} resolution="5min" />;
}

export function Queue() {
  const { name } = useParams<{name: string}>();
  const { url } = useServerConfiguration();
  const { data: queues, isLoading } = useQueues(url);
  const { data: workers, isLoading: workersLoading } = useWorkers(url);
  const resolution = '5min';
  const { pause, resume, remove, update } = useQueueActions();
  const { confirm, dialog } = useConfirm();
  const { theme } = useTheme();
  const [editing, setEditing] = useState<{ url: string | null; queue: QueueData } | null>(null);
  if (editing && (editing.url !== url || editing.queue.name !== name)) setEditing(null);

  const hasMetricsPlugin = useServerConfiguration().configuration?.plugins?.includes('Metrics');

  const queue = queues?.find(q => q.name === name);

  if (isLoading || workersLoading) return <Loading />;

  if (!queue) {
    return (
      <div className={"container-fluid"}>
        <PageHeader title={`Queue - ${name}`} />
        <div className={"alert alert-danger"}>Queue not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <PageHeader title={`Queue - ${queue.name}`} actions={!editing && <div className="d-flex gap-2">
        <button className="btn btn-sm btn-primary" onClick={() => { update.reset(); setEditing({ url, queue }); }}>Edit</button>
        <button className="btn btn-sm btn-outline-danger" disabled={remove.isPending} onClick={async () => {
          const ok = await confirm({ title: 'Delete Queue', message: 'Delete queue? You can choose to also purge all jobs in this queue in the next step.' });
          if (!ok) return;
          const purge = await confirm({ title: 'Purge Jobs', message: 'Also purge jobs in this queue?' });
          remove.mutate({ name: queue.name, purge_jobs: !!purge });
        }}>Delete</button>
      </div>} />

      {hasMetricsPlugin && (
        <div className="row g-3 mb-3">
          <div className="col-12 col-md-6 col-xl-3">
            <MetricSuccessRateCard
              title="Success Rate"
              succeededKey={`queue:${queue.name}:succeeded`}
              failedKey={`queue:${queue.name}:failed`}
              url={url!}
              resolution={resolution}
            />
          </div>
          <div className="col-12 col-md-6 col-xl-3">
            <MetricStatCard
              title="Tasks Succeeded"
              metricKey={`queue:${queue.name}:succeeded`}
              url={url!}
              resolution={resolution}
              subtitle="last 24 hours"
              showSparkline={true}
              sparklineColor="#10b981"
            />
          </div>
          <div className="col-12 col-md-6 col-xl-3">
            <MetricStatCard
              title="Tasks Failed"
              metricKey={`queue:${queue.name}:failed`}
              url={url!}
              resolution={resolution}
              subtitle="last 24 hours"
              showSparkline={true}
              sparklineColor="#ef4444"
            />
          </div>
          <div className="col-12 col-md-6 col-xl-3">
            <MetricHistogramCard
              title="Avg Execution Time"
              metricKey={`queue:${queue.name}:execution_time`}
              url={url!}
              resolution={resolution}
              stat="avg"
              formatValue={formatExecutionTime}
              sparklineColor="#8b5cf6"
            />
          </div>
        </div>
      )}

      {editing ? <QueueForm key={`${url}:${queue.name}`} mode="edit" layout="inline" initial={editing.queue}
        onClose={() => setEditing(null)} mutation={{
          mutateAsync: payload => update.mutateAsync({ name: queue.name, payload }),
          isPending: update.isPending,
          error: update.error,
        }} /> : <>
      <DetailCard title="General Details" flush>
        <table className={"table border mb-0"}>
          <tbody>
            <tr>
              <th className="text-nowrap">State</th>
              <td className="w-100">
                <div className="d-flex align-items-center gap-2">
                  <StatusBadge status={queue.state} />
                  {queue.resume_at && (
                    <small className="text-muted">
                      (Resuming at {new Date(queue.resume_at).toLocaleString()})
                    </small>
                  )}
                  <QueueStateCard
                    state={queue.state}
                    resumeAt={queue.resume_at}
                    onPause={(resumeAt) => pause.mutate({ name: queue.name, resume_at: resumeAt })}
                    onResume={() => resume.mutate(queue.name)}
                    isPending={pause.isPending || resume.isPending}
                  />
                </div>
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Concurrency</th>
              <td className="w-100">
                <span>{queue.concurrency || <em className="text-muted">Executor default</em>}</span>
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Polling Interval</th>
              <td className="w-100">
                <span>{queue.polling_interval}s</span>
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Eager Polling</th>
              <td className="w-100">
                <span>{queue.eager_polling ? 'Enabled' : 'Disabled'}</span>
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Tags</th>
              <td className="w-100">
                <div>
                    {queue.tags.length === 0 ? (
                      <span className="text-muted">Unassigned (no tags)</span>
                    ) : (
                      queue.tags.map(tag => (
                        <span key={tag} className="badge bg-primary me-1 mb-1">{tag}</span>
                      ))
                    )}
                  </div>
              </td>
            </tr>
          </tbody>
        </table>
      </DetailCard>

      <DetailCard title="Rate Limiting" flush>
        <table className={"table border mb-0"}>
          <tbody>
            <tr>
              <th className="text-nowrap">Rate Limit</th>
              <td className="w-100">
                <span>{queue.rate_limit || <em className="text-muted">No limit</em>}</span>
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Rate Limit Window</th>
              <td className="w-100">
                <span>{queue.rate_limit_window ? `${queue.rate_limit_window}s` : <em className="text-muted">-</em>}</span>
              </td>
            </tr>
          </tbody>
        </table>
      </DetailCard>

      <DetailCard title="Executor Configuration" flush>
        <table className={"table border mb-0"}>
          <tbody>
            <tr>
              <th className="text-nowrap">Executor</th>
              <td className="w-100"><code>{queue.executor}</code></td>
            </tr>
            <tr>
              <th className="text-nowrap">Executor Options</th>
              <td className="w-100">
                <div className="json-viewer-container">
                    <JsonViewer value={queue.executor_options} theme={theme} />
                  </div>
              </td>
            </tr>
          </tbody>
        </table>
      </DetailCard>

      </>}

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
      {dialog}
    </div>
  );
}


export function Queues() {
  const { url } = useServerConfiguration();
  const { data: queues, isLoading } = useQueues(url);
  const hasMetricsPlugin = useServerConfiguration().configuration?.plugins?.includes('Metrics');
  const { create } = useQueueActions();
  const [showCreate, setShowCreate] = useState(false);

  if (isLoading) return <Loading />;

  return (
    <div className={"container-fluid"}>
      <PageHeader
        title="Queues"
        description="Manage job queues and worker assignments"
        actions={
          <button className="btn btn-primary btn-sm" onClick={() => { create.reset(); setShowCreate(true); }}>
            + New Queue
          </button>
        }
      />
      <DataTable>
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
              <StatusBadge status={queue.state} />
            </td>
          </tr>
        ))}
        </tbody>
      </DataTable>
      {showCreate && (
        <QueueForm mode="create" onClose={() => setShowCreate(false)} mutation={create} />
      )}
    </div>
  )
}
