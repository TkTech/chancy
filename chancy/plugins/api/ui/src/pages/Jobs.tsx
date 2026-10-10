import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {useJobs, FilterTriple} from '../hooks/useJobs.tsx';
import {CountdownTimer} from '../components/UpdatingTime.tsx';
import React from 'react';
import { Link, useParams, useSearchParams } from 'react-router';
import { useJobActions } from '../hooks/useJobActions.tsx';
import { useConfirm } from '../components/common/ConfirmDialog.tsx';
import { JobDetailsView } from '../features/jobs/JobDetailsView';
import { useDrawer } from '../components/common/DrawerContext';
import { PageHeader } from '../components/common/PageHeader';
import { StatusBadge } from '../components/common/StatusBadge';
import { DataTable } from '../components/common/DataTable';
import { SearchFilter, FieldConfig } from '../components/common/SearchFilter';
import { useQueues } from '../hooks/useQueues';
import { useFunctions } from '../hooks/useFunctions';
import { JobStateBarGraph } from '../components/JobStateBarGraph';
import { BatchJobAction, BatchJobResponse } from '../services/chancy';

export function Job() {
  const { job_id } = useParams<{job_id: string}>();
  return (
    <div className={"container-fluid"}>
      <PageHeader title={`Job - ${job_id}`} />
      {job_id && <JobDetailsView job_id={job_id} />}
    </div>
  );
}

export function Jobs() {
  const {url} = useServerConfiguration();
  const [searchParams, setSearchParams] = useSearchParams();

  // Parse filters from URL
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

  const setFilters = (filters: FilterTriple[]) => {
    const newParams = new URLSearchParams(searchParams);
    if (filters.length > 0) {
      newParams.set('filters', JSON.stringify(filters));
    } else {
      newParams.delete('filters');
    }
    setSearchParams(newParams, { replace: true });
  };

  const scope = JSON.stringify([url, filters]);
  const [view, setView] = React.useState<{
    scope: string;
    cursors: (string | undefined)[];
    selected: Record<string, boolean>;
    summary?: { action: BatchJobAction } & ({ response: BatchJobResponse } | { error: string });
  }>({ scope, cursors: [undefined], selected: {} });
  // A new server or filter set starts at the first page immediately, including
  // URL changes made with the browser's Back/Forward buttons.
  const currentView: typeof view = view.scope === scope
    ? view : { scope, cursors: [undefined], selected: {} };
  if (view.scope !== scope) setView(currentView);
  const { cursors, selected } = currentView;
  const before = cursors[cursors.length - 1];
  const freezeUpdates = Object.values(selected).some(Boolean);
  const actionLock = React.useRef(false);
  const [actionPending, setActionPending] = React.useState(false);
  const setSelected = (selected: Record<string, boolean>) => {
    setView({ ...currentView, selected });
  };
  const navigate = (cursors: (string | undefined)[]) => {
    setView({ scope, cursors, selected: {} });
  };

  // Fetch queues and functions for autocomplete
  const { data: queues } = useQueues(url);
  const { data: functions } = useFunctions(url);

  // Define filter field configuration
  const jobFilterFields: Record<string, FieldConfig> = React.useMemo(() => ({
    state: {
      label: 'State',
      description: 'Filter jobs by their current state',
      type: 'autocomplete',
      operators: ['='],
      getSuggestions: async (query) => {
        const states = ['pending', 'running', 'succeeded', 'failed', 'retrying'];
        if (!query) return states;
        return states.filter(s => s.toLowerCase().includes(query.toLowerCase()));
      }
    },
    queue: {
      label: 'Queue',
      description: 'Filter jobs by the queue they belong to',
      type: 'autocomplete',
      operators: ['='],
      getSuggestions: async (query) => {
        const queueNames = queues?.map(q => q.name) || [];
        if (!query) return queueNames;
        return queueNames.filter(name => name.toLowerCase().includes(query.toLowerCase()));
      }
    },
    func: {
      label: 'Function',
      description: 'Filter jobs by their function name',
      type: 'autocomplete',
      operators: ['=', '~'],
      getSuggestions: async (query) => {
        const funcNames = functions || [];
        if (!query) return funcNames;
        return funcNames.filter(name => name.toLowerCase().includes(query.toLowerCase()));
      }
    },
    priority: {
      label: 'Priority',
      description: 'Filter jobs by priority level (higher = more important)',
      type: 'numeric',
      operators: ['=', '>', '<', '>=', '<=']
    },
    attempts: {
      label: 'Attempts',
      description: 'Filter jobs by number of execution attempts',
      type: 'numeric',
      operators: ['=', '>', '<', '>=', '<=']
    }
  }), [queues, functions]);

  const { data: page, dataUpdatedAt, isFetching, isPlaceholderData, error, refetch } = useJobs({
    url: url,
    state: undefined,
    filters: filters,
    pausePolling: freezeUpdates || actionPending,
    before,
  });
  const jobs = page?.items;
  // Only visible, current rows may be acted on. This also reconciles rows
  // removed by a mutation or by another operator while polling is paused.
  const selectedIds = isPlaceholderData ? [] : (jobs ?? []).filter(j => selected[j.id]).map(j => j.id);
  if (jobs && !isPlaceholderData && Object.keys(selected).some(id => selected[id] && !jobs.some(j => j.id === id))) {
    setSelected(Object.fromEntries(selectedIds.map(id => [id, true])));
  }

  const allSelected = jobs && jobs.length > 0 && jobs.every(j => selected[j.id]);
  const toggleAll = () => {
    if (!jobs) return;
    const next: Record<string, boolean> = {};
    if (!allSelected) jobs.forEach(j => next[j.id] = true);
    setSelected(next);
  }

  const { batch } = useJobActions();
  const { confirm, dialog } = useConfirm();
  const drawer = useDrawer();
  const actionsDisabled = actionPending || isFetching || isPlaceholderData || !!error;

  const runAction = async (action: BatchJobAction) => {
    if (actionLock.current || actionsDisabled || !selectedIds.length) return;
    actionLock.current = true;
    setActionPending(true);
    const ids = selectedIds;
    try {
      if (action !== 'retry') {
        const ok = await confirm(action === 'purge'
          ? { title: 'Purge Jobs', message: `Permanently delete ${ids.length} job(s)?` }
          : { title: 'Cancel Jobs', message: `Cancel ${ids.length} job(s)? Only pending, running, or retrying jobs can be cancelled.` });
        if (!ok) return;
      }
      const response = await batch.mutateAsync({ ids, action });
      const completed = new Set(response.results.filter(r => r.status === 'completed').map(r => r.id));
      setView(view => view.scope === scope && view.cursors === cursors ? {
        ...view,
        selected: Object.fromEntries(Object.entries(view.selected).filter(([id, checked]) => checked && !completed.has(id))),
        summary: { action, response },
      } : view);
    } catch (error) {
      setView(view => view.scope === scope && view.cursors === cursors ? {
        ...view,
        summary: { action, error: error instanceof Error ? error.message : 'Request failed' },
      } : view);
    } finally {
      actionLock.current = false;
      setActionPending(false);
    }
  };
  const summary = currentView.summary;

  // Avoid showing the global loader during background refetches
  // which causes visible flicker. Only show it before first data.
  if (!jobs && !error) return <Loading />;

  return (
    <div className={"container-fluid"}>
      <PageHeader
        title="Jobs"
        description={`Last updated: ${dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : 'Never'}`}
      />

      <JobStateBarGraph />

      <SearchFilter
        fields={jobFilterFields}
        value={filters}
        onChange={setFilters}
        placeholder="Add filter... (state, queue, func, priority, attempts)"
      />

      {error && (
        <div className="alert alert-danger" role="alert">
          Could not load jobs: {error.message}{' '}
          <button className="btn btn-sm btn-outline-danger" disabled={isFetching} onClick={() => void refetch()}>Try again</button>
        </div>
      )}

      {summary && (
        <div className={`alert ${'error' in summary || !summary.response.ok ? 'alert-warning' : 'alert-success'}`} role="status">
          <strong>{{ retry: 'Retry', purge: 'Purge', cancel: 'Cancel' }[summary.action]}: </strong>
          {'error' in summary ? <>Could not confirm the outcome: {summary.error}. Review the refreshed list before trying again.</> : <>
            {summary.response.results.filter(r => r.status === 'completed').length} completed,{' '}
            {summary.response.results.filter(r => r.status === 'skipped').length} skipped,{' '}
            {summary.response.results.filter(r => r.status === 'failed').length} failed.
            {!summary.response.ok && (
              <details className="mt-2">
                <summary>View unresolved items</summary>
                <ul className="mb-0">
                  {summary.response.results.filter(r => r.status !== 'completed').map(r => (
                    <li key={r.id}><code>{r.id}</code>: {r.status} — {r.message}</li>
                  ))}
                </ul>
              </details>
            )}
          </>}
        </div>
      )}

      {selectedIds.length > 0 && (
        <div className="alert alert-primary d-flex justify-content-between align-items-center py-2 mb-3">
          <div className="fw-medium">
            <span className="badge bg-primary me-2">{selectedIds.length}</span>
            {selectedIds.length === 1 ? 'job' : 'jobs'} selected
          </div>
          <div className="btn-group btn-group-sm">
            <button className="btn btn-primary" disabled={actionsDisabled} onClick={() => void runAction('retry')}>Retry</button>
            <button className="btn btn-danger" disabled={actionsDisabled} onClick={() => void runAction('purge')}>Purge</button>
            <button className="btn btn-warning" disabled={actionsDisabled} onClick={() => void runAction('cancel')}>Cancel</button>
          </div>
        </div>
      )}

      <DataTable>
        <thead>
        <tr>
          <th style={{width: '1%'}}>
            <input type="checkbox" aria-label="Select all jobs" disabled={actionsDisabled} checked={!!allSelected} onChange={toggleAll} />
          </th>
          <th className={"w-100"}>Job</th>
          <th className={'text-center'}>State</th>
          <th className={'text-center'}>Queue</th>
          <th className={"text-center"}>Attempts</th>
          <th className={"text-center"}>Time</th>
        </tr>
        </thead>
        <tbody>
        {jobs?.length === 0 && (
          <tr>
            <td colSpan={6} className={'text-center'}>
              No matching jobs found.
            </td>
          </tr>
        )}
        {jobs?.map((job) => (
          <tr key={job.id}>
            <td>
              <input type="checkbox" aria-label={`Select job ${job.id}`} disabled={actionsDisabled} checked={!!selected[job.id]} onChange={e => setSelected({...selected, [job.id]: e.target.checked})} />
            </td>
            <td className={"text-break"}>
              <Link to={`/jobs/${job.id}`}
                onClick={(e) => {
                  if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
                  e.preventDefault();
                  drawer.open(<JobDetailsView job_id={job.id} />, { title: 'Job Details' });
                }}
              >
                {job.func}
              </Link>
            </td>
            <td className={"text-center"}>
              <StatusBadge status={job.state} />
            </td>
            <td className={"text-center"}>
              <Link to={`/queues/${job.queue}`}>
                {job.queue}
              </Link>
            </td>
            <td className={"text-center"}>
              {job.attempts} / {job.max_attempts}
            </td>
            <td className={"text-center"} style={{fontFamily: 'monospace'}}>
              <CountdownTimer date={{
                "pending": job.created_at,
                "running": job.started_at,
                "succeeded": job.completed_at,
                "failed": job.completed_at,
                "retrying": job.started_at,
              }[job.state]} />
            </td>
          </tr>
        ))}
        </tbody>
      </DataTable>
      <nav aria-label="Jobs pagination" className="d-flex flex-wrap justify-content-between align-items-center gap-2 mt-3">
        <div className="text-muted small" role="status">
          {before ? 'Updates paused while browsing older jobs.'
            : freezeUpdates ? 'Updates paused while jobs are selected.'
            : 'Showing latest jobs · Updates every 5 seconds.'}
          {isFetching && ' Loading…'}
        </div>
        <div className="d-flex align-items-center gap-2">
          <span className="small text-muted">Page {cursors.length}</span>
          <button className="btn btn-sm btn-outline-secondary" disabled={actionPending || isFetching || cursors.length === 1} onClick={() => navigate(cursors.slice(0, -1))}>Previous</button>
          <button className="btn btn-sm btn-outline-secondary" disabled={actionsDisabled || !page?.has_more || !page.next_cursor} onClick={() => {
            if (page?.next_cursor) navigate([...cursors, page.next_cursor]);
          }}>Next</button>
        </div>
      </nav>
      {dialog}
    </div>
  )
}
