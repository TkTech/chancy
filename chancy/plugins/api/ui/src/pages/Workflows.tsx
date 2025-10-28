import React from 'react';
import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {useWorkflow, useWorkflows, FilterTriple} from '../hooks/useWorkflows.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams, useSearchParams} from 'react-router-dom';
import { useDrawer } from '../components/common/DrawerProvider';
import { JobDetailsView } from '../features/jobs/JobDetailsView';
// Drawer and JobDetailsView are not directly used here; navigation state opens drawer in Layout
import {CountdownTimer} from '../components/UpdatingTime.tsx';
import {statusToColor, extractFunctionName} from '../utils.tsx';
import WorkflowChart from './WorkflowChart.tsx';
import {ReactFlowProvider} from '@xyflow/react';
import { PageHeader } from '../components/common/PageHeader';
import { StatusBadge } from '../components/common/StatusBadge';
import { DataTable } from '../components/common/DataTable';
import { SearchFilter, FieldConfig } from '../components/common/SearchFilter';
import { PackedJobDetails } from '../components/PackedJobDetails';
import { MetricStatCard } from '../components/dashboard/MetricStatCard';
import { MetricSuccessRateCard } from '../components/dashboard/MetricSuccessRateCard';
import { MetricHistogramCard } from '../components/dashboard/MetricHistogramCard';


export function Workflow() {
  const { url } = useServerConfiguration();
  const { workflow_id } = useParams<{workflow_id: string}>();
  const resolution = '5min';
  // const location = useLocation();
  const { data: workflow, isLoading } = useWorkflow({ url, workflow_id, options: {refetchInterval: 5000 } });
  const drawer = useDrawer();

  const handleStepClick = (step: any, step_id: string) => {
    if (step.job_id) {
      // Job has started, show the running job details
      drawer.open(<JobDetailsView job_id={step.job_id} />, { title: `Job Details - ${step_id}` });
    } else if (step.job) {
      // Step is pending, show the packed job definition
      drawer.open(
        <>
          <div className="alert alert-info">
            This workflow step has not yet been reached. Once its dependencies are satisfied and it's ready for execution, this job will be pushed onto the queue.
          </div>
          <div className="card">
            <div className="card-header">Job Definition</div>
            <PackedJobDetails job={step.job} />
          </div>
        </>,
        { title: `Step Details - ${step_id}` }
      );
    }
  };

  if (isLoading) return <Loading />;

  if (!workflow) {
    return (
      <div className={"container-fluid"}>
        <h2 className={"mb-4"}>Workflow - {workflow_id}</h2>
        <div className={"alert alert-danger"}>Workflow not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Workflow - {workflow_id}</h2>

      {/* Per-Workflow Type Metrics */}
      <div className="alert alert-info mb-3">
        Statistics for all <strong>{workflow.name}</strong> workflows
      </div>
      <div className="row g-3 mb-4">
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Started"
            metricKey={`workflow:${workflow.name}:started`}
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            showSparkline={true}
            sparklineColor="#3b82f6"
            formatValue={(v) => v.toString()}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Completed"
            metricKey={`workflow:${workflow.name}:completed`}
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            showSparkline={true}
            sparklineColor="#10b981"
            formatValue={(v) => v.toString()}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Failed"
            metricKey={`workflow:${workflow.name}:failed`}
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            showSparkline={true}
            sparklineColor="#ef4444"
            formatValue={(v) => v.toString()}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricHistogramCard
            title="Avg Execution Time"
            metricKey={`workflow:${workflow.name}:execution_time`}
            url={url!}
            resolution={resolution}
            stat="avg"
            formatValue={(v) => `${v.toFixed(1)}s`}
            sparklineColor="#8b5cf6"
          />
        </div>
      </div>

      <div className="card mb-3">
        <div className="card-header">Details</div>
        <table className={"table border mb-0"}>
          <tbody>
          <tr>
            <th className="text-nowrap">Name</th>
            <td className="w-100">{workflow.name}</td>
          </tr>
          <tr>
            <th className="text-nowrap">State</th>
            <td className="w-100">
              <span className={`badge bg-${statusToColor(workflow.state)}`}>{workflow.state}</span>
            </td>
          </tr>
          <tr>
            <th className="text-nowrap">Created</th>
            <td className="w-100 font-monospace">
              <CountdownTimer date={workflow.created_at} />
            </td>
          </tr>
          <tr>
            <th className="text-nowrap">Updated</th>
            <td className="w-100 font-monospace">
              <CountdownTimer date={workflow.updated_at} />
            </td>
          </tr>
          </tbody>
        </table>
      </div>
      {workflow.steps && (
        <>
          <div className="card mt-4">
            <div className="card-header">
              Workflow Visualization
            </div>
            <div className="card-body">
              <ReactFlowProvider>
                <WorkflowChart workflow={workflow}/>
              </ReactFlowProvider>
            </div>
          </div>
          <h3 className="mt-4">Steps</h3>
          <table className={'table table-hover border mb-0'}>
            <thead>
            <tr>
              <th>Step ID</th>
              <th>Function</th>
              <th>Queue</th>
              <th>Dependencies</th>
              <th>State</th>
              <th>Job ID</th>
            </tr>
            </thead>
            <tbody>
            {Object.entries(workflow.steps).map(([step_id, step]) => (
              <tr
                key={step_id}
                onClick={() => handleStepClick(step, step_id)}
                style={{ cursor: 'pointer' }}
              >
                <td className="fw-medium">{step_id}</td>
                <td>
                  <code className="text-primary" title={step.job?.func}>
                    {extractFunctionName(step.job?.func || '')}
                  </code>
                </td>
                <td>
                  <span className="badge bg-secondary">{step.job?.queue || 'default'}</span>
                </td>
                <td>
                  {step.dependencies && step.dependencies.length > 0 ? (
                    <div className="d-flex flex-wrap gap-1">
                      {step.dependencies.map(dep => (
                        <span key={dep} className="badge bg-light text-dark border">
                          {dep}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <span className="text-muted">None</span>
                  )}
                </td>
                <td>
                  {step.state ? (
                    <span className={`badge bg-${statusToColor(step.state)}`}>
                      {step.state}
                    </span>
                  ) : (
                    <span className="badge bg-secondary">Waiting</span>
                  )}
                </td>
                <td className="text-break">
                  {step.job_id ? (
                    <span className="text-break">{step.job_id}</span>
                  ) : (
                    <span className="text-muted">Pending</span>
                  )}
                </td>
              </tr>
            ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

export function Workflows() {
  const {url} = useServerConfiguration();
  const [searchParams, setSearchParams] = useSearchParams();
  const resolution = '5min';

  // Parse filters from URL or use default
  const filtersFromUrl = React.useMemo(() => {
    const filtersParam = searchParams.get('filters');
    if (!filtersParam) return [];
    try {
      const parsed = JSON.parse(filtersParam);
      return Array.isArray(parsed) ? parsed as FilterTriple[] : [];
    } catch {
      return [];
    }
  }, [searchParams]);

  const [filters, setFilters] = React.useState<FilterTriple[]>(filtersFromUrl);

  // Sync filters to URL
  React.useEffect(() => {
    const newParams = new URLSearchParams(searchParams);
    if (filters.length > 0) {
      newParams.set('filters', JSON.stringify(filters));
    } else {
      newParams.delete('filters');
    }
    setSearchParams(newParams, { replace: true });
  }, [filters]);

  // Define filter field configuration
  const workflowFilterFields: Record<string, FieldConfig> = React.useMemo(() => ({
    state: {
      label: 'State',
      description: 'Filter workflows by their current state',
      type: 'autocomplete',
      operators: ['='],
      getSuggestions: async (query) => {
        const states = ['pending', 'running', 'completed', 'failed'];
        if (!query) return states;
        return states.filter(s => s.toLowerCase().includes(query.toLowerCase()));
      }
    },
    name: {
      label: 'Name',
      description: 'Filter workflows by name',
      type: 'autocomplete',
      operators: ['=', '~'],
      getSuggestions: async () => []
    }
  }), []);

  const {data: workflows, dataUpdatedAt, isLoading} = useWorkflows({url, filters});

  if (isLoading) return <Loading />;

  return (
    <div className={'container-fluid'}>
      <PageHeader
        title="Workflows"
        description={`Multi-step job workflows with dependencies • Last updated: ${dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : 'Never'}`}
      />

      {/* Workflow Metrics */}
      <div className="row g-3 mb-4">
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Workflows Completed"
            metricKey="workflows:state:completed"
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            showSparkline={true}
            sparklineColor="#10b981"
            formatValue={(v) => v.toString()}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Workflows Failed"
            metricKey="workflows:state:failed"
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            showSparkline={true}
            sparklineColor="#ef4444"
            formatValue={(v) => v.toString()}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricSuccessRateCard
            title="Success Rate"
            succeededKey="workflows:state:completed"
            failedKey="workflows:state:failed"
            url={url!}
            resolution={resolution}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Steps Queued"
            metricKey="workflows:steps:queued"
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            showSparkline={true}
            sparklineColor="#3b82f6"
            formatValue={(v) => v.toString()}
          />
        </div>
      </div>

      <SearchFilter
        fields={workflowFilterFields}
        value={filters}
        onChange={setFilters}
        placeholder="Add filter... (state, name)"
      />

      <DataTable>
        <thead>
        <tr>
          <th className={"w-100"}>Name</th>
          <th className={"text-center text-nowrap"}>Progress</th>
          <th className={"text-center text-nowrap"}>State</th>
          <th className={"text-center text-nowrap"}>Created</th>
        </tr>
        </thead>
        <tbody>
        {workflows?.length === 0 && (
          <tr>
            <td colSpan={4} className={'text-center'}>
              No matching workflows found.
            </td>
          </tr>
        )}
        {workflows?.map(workflow => {
          const pendingSteps = workflow.pending_steps || 0;
          const runningSteps = workflow.running_steps || 0;
          const succeededSteps = workflow.succeeded_steps || 0;
          const failedSteps = workflow.failed_steps || 0;
          const retryingSteps = workflow.retrying_steps || 0;
          const totalSteps = pendingSteps + runningSteps + succeededSteps + failedSteps + retryingSteps;

          return (
            <tr key={workflow.id}>
              <td>
                <Link to={`/workflows/${workflow.id}`}>
                  {workflow.name}
                </Link>
              </td>
              <td className={"text-center"} style={{minWidth: '200px'}}>
                {totalSteps > 0 ? (
                  <div className="progress" style={{height: '24px'}}>
                    {succeededSteps > 0 && (
                      <div
                        className="progress-bar bg-success"
                        role="progressbar"
                        style={{width: `${(succeededSteps / totalSteps) * 100}%`}}
                        title={`${succeededSteps} succeeded`}
                      >
                        {succeededSteps}
                      </div>
                    )}
                    {runningSteps > 0 && (
                      <div
                        className="progress-bar bg-primary"
                        role="progressbar"
                        style={{width: `${(runningSteps / totalSteps) * 100}%`}}
                        title={`${runningSteps} running`}
                      >
                        {runningSteps}
                      </div>
                    )}
                    {retryingSteps > 0 && (
                      <div
                        className="progress-bar bg-warning"
                        role="progressbar"
                        style={{width: `${(retryingSteps / totalSteps) * 100}%`}}
                        title={`${retryingSteps} retrying`}
                      >
                        {retryingSteps}
                      </div>
                    )}
                    {failedSteps > 0 && (
                      <div
                        className="progress-bar bg-danger"
                        role="progressbar"
                        style={{width: `${(failedSteps / totalSteps) * 100}%`}}
                        title={`${failedSteps} failed`}
                      >
                        {failedSteps}
                      </div>
                    )}
                    {pendingSteps > 0 && (
                      <div
                        className="progress-bar bg-secondary"
                        role="progressbar"
                        style={{width: `${(pendingSteps / totalSteps) * 100}%`}}
                        title={`${pendingSteps} pending`}
                      >
                        {pendingSteps}
                      </div>
                    )}
                  </div>
                ) : (
                  <span className="text-muted">-</span>
                )}
              </td>
              <td className={"text-center"}>
                <StatusBadge status={workflow.state} />
              </td>
              <td className={"text-center text-nowrap font-monospace"}>
                <CountdownTimer date={workflow.created_at} />
              </td>
            </tr>
          );
        })}
        </tbody>
      </DataTable>
      {workflows && workflows.length >= 100 && (
        <div className="text-muted text-center mt-2" style={{fontSize: '0.875rem'}}>
          Only showing the first 100 results...
        </div>
      )}
    </div>
  );
}
