import { WorkflowSteps } from '../features/workflows/WorkflowSteps';
import { DetailCard } from '../components/common/DetailCard';
import React from 'react';
import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {useWorkflow, useWorkflows, FilterTriple} from '../hooks/useWorkflows.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams, useSearchParams} from 'react-router';
import {CountdownTimer} from '../components/UpdatingTime.tsx';
import {formatExecutionTime} from '../utils.tsx';
import { PageHeader } from '../components/common/PageHeader';
import { StatusBadge } from '../components/common/StatusBadge';
import { DataTable } from '../components/common/DataTable';
import { SearchFilter, FieldConfig } from '../components/common/SearchFilter';
import { MetricStatCard } from '../components/dashboard/MetricStatCard';
import { MetricSuccessRateCard } from '../components/dashboard/MetricSuccessRateCard';
import { MetricHistogramCard } from '../components/dashboard/MetricHistogramCard';


export function Workflow() {
  const { url } = useServerConfiguration();
  const { workflow_id } = useParams<{workflow_id: string}>();
  const resolution = '5min';
  const { data: workflow, isLoading } = useWorkflow({ url, workflow_id, options: {refetchInterval: 5000 } });

  if (isLoading) return <Loading />;

  if (!workflow) {
    return (
      <div className={"container-fluid"}>
        <PageHeader title={`Workflow - ${workflow_id}`} />
        <div className={"alert alert-danger"}>Workflow not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <PageHeader title={`Workflow - ${workflow_id}`} />

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
            title="Avg workflow duration"
            metricKey={`workflow:${workflow.name}:execution_time`}
            url={url!}
            resolution={resolution}
            stat="avg"
            formatValue={formatExecutionTime}
            sparklineColor="#8b5cf6"
          />
        </div>
      </div>

      <DetailCard title="Details" flush>
        <table className={"table border mb-0"}>
          <tbody>
          <tr>
            <th className="text-nowrap">Name</th>
            <td className="w-100">{workflow.name}</td>
          </tr>
          <tr>
            <th className="text-nowrap">State</th>
            <td className="w-100">
              <StatusBadge status={workflow.state} />
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
      </DetailCard>
      <WorkflowSteps key={workflow.id} workflow={workflow} />
    </div>
  );
}

export function Workflows() {
  const {url} = useServerConfiguration();
  const [searchParams, setSearchParams] = useSearchParams();
  const resolution = '5min';

  // Parse filters from URL or use default
  const filters = React.useMemo(() => {
    const filtersParam = searchParams.get('filters');
    if (!filtersParam) return [];
    try {
      const parsed = JSON.parse(filtersParam);
      return Array.isArray(parsed) ? parsed as FilterTriple[] : [];
    } catch {
      return [];
    }
  }, [searchParams]);

  const setFilters = (nextFilters: FilterTriple[]) => {
    setSearchParams(previous => {
      const params = new URLSearchParams(previous);
      if (nextFilters.length > 0) {
        params.set('filters', JSON.stringify(nextFilters));
      } else {
        params.delete('filters');
      }
      return params;
    }, { replace: true });
  };

  const scope = JSON.stringify([url, filters]);
  const [view, setView] = React.useState<{
    scope: string;
    cursors: (string | undefined)[];
  }>({ scope, cursors: [undefined] });
  // Match Jobs: filter/server changes, including Back/Forward, start at page one.
  const currentView = view.scope === scope ? view : { scope, cursors: [undefined] };
  if (view.scope !== scope) setView(currentView);
  const { cursors } = currentView;
  const before = cursors[cursors.length - 1];
  const navigate = (cursors: (string | undefined)[]) => setView({ scope, cursors });

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

  const {data: page, dataUpdatedAt, isFetching, isPlaceholderData, error, refetch} = useWorkflows({url, filters, before});
  const workflows = page?.items;

  if (!workflows && !error) return <Loading />;

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

      {error && (
        <div className="alert alert-danger" role="alert">
          Could not load workflows: {error.message}{' '}
          <button className="btn btn-sm btn-outline-danger" disabled={isFetching} onClick={() => void refetch()}>Try again</button>
        </div>
      )}

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
          const totalSteps = workflow.total_steps;
          const segments = [
            { count: workflow.succeeded_steps, label: 'succeeded', color: 'bg-success' },
            { count: workflow.running_steps, label: 'running', color: 'bg-primary' },
            { count: workflow.retrying_steps, label: 'retrying', color: 'bg-warning' },
            { count: workflow.failed_steps, label: 'failed', color: 'bg-danger' },
            { count: workflow.pending_steps, label: 'pending in queue', color: 'bg-secondary' },
            { count: workflow.waiting_steps, label: 'waiting to be queued', color: 'bg-body-secondary text-body progress-bar-striped' },
          ];

          return (
            <tr key={workflow.id}>
              <td>
                <Link to={`/workflows/${workflow.id}`}>
                  {workflow.name}
                </Link>
              </td>
              <td className={"text-center"} style={{minWidth: '200px'}}>
                {totalSteps > 0 ? (
                  <>
                    <div className="progress" style={{height: '24px'}}>
                      {segments.filter(segment => segment.count > 0).map(segment => (
                        <div
                          key={segment.label}
                          className={`progress-bar ${segment.color}`}
                          role="progressbar"
                          aria-label={segment.label}
                          aria-valuenow={segment.count}
                          aria-valuemin={0}
                          aria-valuemax={totalSteps}
                          style={{width: `${(segment.count / totalSteps) * 100}%`}}
                          title={`${segment.count} ${segment.label}`}
                        >
                          {segment.count}
                        </div>
                      ))}
                    </div>
                    <div className="small text-muted mt-1">
                      {workflow.succeeded_steps}/{totalSteps} succeeded
                      {workflow.waiting_steps > 0 && ` · ${workflow.waiting_steps} waiting`}
                      {workflow.pending_steps > 0 && ` · ${workflow.pending_steps} queued`}
                    </div>
                  </>
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
      <nav aria-label="Workflows pagination" className="d-flex flex-wrap justify-content-between align-items-center gap-2 mt-3">
        <div className="text-muted small" role="status">
          {before ? 'Updates paused while browsing older workflows.' : 'Showing latest workflows · Updates every 5 seconds.'}
          {isFetching && ' Loading…'}
        </div>
        <div className="d-flex align-items-center gap-2">
          <span className="small text-muted">Page {cursors.length}</span>
          <button className="btn btn-sm btn-outline-secondary" disabled={isFetching || cursors.length === 1} onClick={() => navigate(cursors.slice(0, -1))}>Previous</button>
          <button className="btn btn-sm btn-outline-secondary" disabled={isFetching || isPlaceholderData || !!error || !page?.has_more || !page.next_cursor} onClick={() => {
            if (page?.next_cursor) navigate([...cursors, page.next_cursor]);
          }}>Next</button>
        </div>
      </nav>
    </div>
  );
}
