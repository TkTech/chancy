import { useState } from 'react';
import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Link, useParams} from 'react-router-dom';
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
import { useEntityForm } from '../hooks/useEntityForm';
import { QueueFormSchema, queueToFormValues } from '../schemas/queue';
import { FormInput } from '../components/forms/FormInput';
import { FormCheckbox } from '../components/forms/FormCheckbox';
import { FormTagInput } from '../components/forms/FormTagInput';
import { FormJsonEditor } from '../components/forms/FormJsonEditor';
import { UseMutationResult } from '@tanstack/react-query';
import { z } from 'zod';
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
  const [isEditing, setIsEditing] = useState(false);

  const hasMetricsPlugin = useServerConfiguration().configuration?.plugins?.includes('Metrics');

  const queue = queues?.find(q => q.name === name);

  // Create a custom mutation for the form that wraps the update mutation
  const formMutation: Pick<UseMutationResult<unknown, Error, z.output<typeof QueueFormSchema>>, 'mutateAsync' | 'isPending' | 'error'> = {
    mutateAsync: async (payload: z.output<typeof QueueFormSchema>) => {
      if (!queue) throw new Error('Queue not found');
      return update.mutateAsync({ name: queue.name, payload });
    },
    isPending: update.isPending,
    error: update.error,
  };

  const { form, isSubmitting } = useEntityForm({
    schema: QueueFormSchema,
    defaultValues: queue ? queueToFormValues(queue) : { name: '', concurrency: '', polling_interval: '5', eager_polling: false, rate_limit: '', rate_limit_window: '', tags: [], executor_options: '{}' },
    mutation: formMutation as UseMutationResult<unknown, Error, z.output<typeof QueueFormSchema>>,
    onSuccess: () => setIsEditing(false),
  });

  if (isLoading || workersLoading) return <Loading />;

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
      <div className="d-flex align-items-center justify-content-between mb-3">
        <div>
          <h2 className={"mb-1"}>Queue - {queue.name}</h2>
        </div>
        <div className="d-flex gap-2">
          {!isEditing ? (
            <>
              <button className="btn btn-sm btn-primary" onClick={() => setIsEditing(true)}>Edit</button>
              <button className="btn btn-sm btn-outline-danger" onClick={async () => {
                const ok = await confirm({ title: 'Delete Queue', message: 'Delete queue? You can choose to also purge all jobs in this queue in the next step.' });
                if (!ok) return;
                const purge = await confirm({ title: 'Purge Jobs', message: 'Also purge jobs in this queue?' });
                remove.mutate({ name: queue.name, purge_jobs: !!purge });
              }}>Delete</button>
            </>
          ) : (
            <>
              <button
                className="btn btn-sm btn-success"
                onClick={() => form.handleSubmit()}
                disabled={isSubmitting}
              >
                {isSubmitting ? 'Saving...' : 'Save Changes'}
              </button>
              <button
                className="btn btn-sm btn-secondary"
                onClick={() => {
                  form.reset();
                  setIsEditing(false);
                }}
                disabled={isSubmitting}
              >
                Cancel
              </button>
            </>
          )}
        </div>
      </div>

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
              formatValue={(v) => `${v.toFixed(0)}ms`}
              sparklineColor="#8b5cf6"
            />
          </div>
        </div>
      )}

      <div className="card mb-3">
        <div className="card-header">General Details</div>
        <table className={"table border mb-0"}>
          <tbody>
            <tr>
              <th className="text-nowrap">State</th>
              <td className="w-100">
                <div className="d-flex align-items-center gap-2">
                  <span className={queue.state === 'active' ? 'text-success fw-semibold' : 'text-danger fw-semibold'}>
                    {queue.state === 'active' ? 'Active' : 'Paused'}
                  </span>
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
                {isEditing ? (
                  <form.Field name="concurrency">
                    {(field) => (
                      <div>
                        <FormInput field={field} type="number" placeholder="Executor default" />
                        {field.state.meta.errors.length > 0 && (
                          <div className="text-danger small mt-1">{field.state.meta.errors.join(', ')}</div>
                        )}
                      </div>
                    )}
                  </form.Field>
                ) : (
                  <span>{queue.concurrency || <em className="text-muted">Executor default</em>}</span>
                )}
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Polling Interval</th>
              <td className="w-100">
                {isEditing ? (
                  <form.Field name="polling_interval">
                    {(field) => (
                      <div>
                        <FormInput field={field} type="number" unit="s" />
                        {field.state.meta.errors.length > 0 && (
                          <div className="text-danger small mt-1">{field.state.meta.errors.join(', ')}</div>
                        )}
                      </div>
                    )}
                  </form.Field>
                ) : (
                  <span>{queue.polling_interval}s</span>
                )}
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Eager Polling</th>
              <td className="w-100">
                {isEditing ? (
                  <form.Field name="eager_polling">
                    {(field) => (
                      <FormCheckbox field={field} label="Enabled" id="eagerPollingEdit" />
                    )}
                  </form.Field>
                ) : (
                  <div className="form-check form-switch">
                    <input className="form-check-input" type="checkbox" checked={queue.eager_polling} disabled />
                    <label className="form-check-label">{queue.eager_polling ? 'Enabled' : 'Disabled'}</label>
                  </div>
                )}
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Tags</th>
              <td className="w-100">
                {isEditing ? (
                  <form.Field name="tags">
                    {(field) => (
                      <FormTagInput field={field} />
                    )}
                  </form.Field>
                ) : (
                  <div>
                    {queue.tags.length === 0 ? (
                      <span className="text-muted">No tags</span>
                    ) : (
                      queue.tags.map(tag => (
                        <span key={tag} className="badge bg-primary me-1 mb-1">{tag}</span>
                      ))
                    )}
                  </div>
                )}
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="card mb-3">
        <div className="card-header">Rate Limiting</div>
        <table className={"table border mb-0"}>
          <tbody>
            <tr>
              <th className="text-nowrap">Rate Limit</th>
              <td className="w-100">
                {isEditing ? (
                  <form.Field name="rate_limit">
                    {(field) => (
                      <div>
                        <FormInput field={field} type="number" unit="req" placeholder="No limit" />
                        {field.state.meta.errors.length > 0 && (
                          <div className="text-danger small mt-1">{field.state.meta.errors.join(', ')}</div>
                        )}
                      </div>
                    )}
                  </form.Field>
                ) : (
                  <span>{queue.rate_limit || <em className="text-muted">No limit</em>}</span>
                )}
              </td>
            </tr>
            <tr>
              <th className="text-nowrap">Rate Limit Window</th>
              <td className="w-100">
                {isEditing ? (
                  <form.Field name="rate_limit_window">
                    {(field) => (
                      <div>
                        <FormInput field={field} type="number" unit="s" />
                        {field.state.meta.errors.length > 0 && (
                          <div className="text-danger small mt-1">{field.state.meta.errors.join(', ')}</div>
                        )}
                      </div>
                    )}
                  </form.Field>
                ) : (
                  <span>{queue.rate_limit_window ? `${queue.rate_limit_window}s` : <em className="text-muted">-</em>}</span>
                )}
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="card mb-4">
        <div className="card-header">Executor Configuration</div>
        <table className={"table border mb-0"}>
          <tbody>
            <tr>
              <th className="text-nowrap">Executor</th>
              <td className="w-100"><code>{queue.executor}</code></td>
            </tr>
            <tr>
              <th className="text-nowrap">Executor Options</th>
              <td className="w-100">
                {isEditing ? (
                  <form.Field name="executor_options">
                    {(field) => (
                      <div>
                        <FormJsonEditor field={field} rows={10} />
                        {field.state.meta.errors.length > 0 && (
                          <div className="text-danger small mt-1">{field.state.meta.errors.join(', ')}</div>
                        )}
                      </div>
                    )}
                  </form.Field>
                ) : (
                  <div className="json-viewer-container">
                    <JsonViewer value={queue.executor_options} theme={theme} />
                  </div>
                )}
              </td>
            </tr>
          </tbody>
        </table>
      </div>

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
          <button className="btn btn-primary btn-sm" onClick={() => setShowCreate(true)}>
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
              <span className={`badge bg-${queue.state === 'active' ? 'success' : 'danger'}`}>{queue.state}</span>
            </td>
          </tr>
        ))}
        </tbody>
      </DataTable>
      {showCreate && (
        <QueueForm mode={'create'} onCancel={() => setShowCreate(false)} onSubmit={async (payload) => {
          await create.mutateAsync(payload);
          setShowCreate(false);
        }} />
      )}
    </div>
  )
}
