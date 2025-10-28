import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {useJobs, FilterTriple} from '../hooks/useJobs.tsx';
import {CountdownTimer} from '../components/UpdatingTime.tsx';
import React from 'react';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import { useJobActions } from '../hooks/useJobActions.tsx';
import { useConfirm } from '../components/common/ConfirmDialog.tsx';
import { JobDetailsView } from '../features/jobs/JobDetailsView';
import { useDrawer } from '../components/common/DrawerProvider';
import { PageHeader } from '../components/common/PageHeader';
import { StatusBadge } from '../components/common/StatusBadge';
import { DataTable } from '../components/common/DataTable';
import { SearchFilter, FieldConfig } from '../components/common/SearchFilter';
import { useQueues } from '../hooks/useQueues';
import { useFunctions } from '../hooks/useFunctions';
import { JobStateBarGraph } from '../components/JobStateBarGraph';

export function Job() {
  const { job_id } = useParams<{job_id: string}>();
  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Job - {job_id}</h2>
      {job_id && <JobDetailsView job_id={job_id} />}
    </div>
  );
}

export function Jobs() {
  const {url} = useServerConfiguration();
  const [searchParams, setSearchParams] = useSearchParams();

  const [selected, setSelected] = React.useState<Record<string, boolean>>({});
  const selectedIds = Object.keys(selected).filter(k => selected[k]);
  const freezeUpdates = selectedIds.length > 0;

  // Parse filters from URL
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

  const { data: jobs, dataUpdatedAt } = useJobs({
    url: url,
    state: undefined,
    filters: filters,
    enabled: url !== null && !freezeUpdates,
  });

  const allSelected = jobs && jobs.length > 0 && jobs.every(j => selected[j.id]);
  const toggleAll = () => {
    if (!jobs) return;
    const next: Record<string, boolean> = {};
    if (!allSelected) jobs.forEach(j => next[j.id] = true);
    setSelected(next);
  }

  const { retry, cancel, purge } = useJobActions();
  const { confirm, dialog } = useConfirm();
  const drawer = useDrawer();

  // Avoid showing the global loader during background refetches
  // which causes visible flicker. Only show it before first data.
  if (!jobs) return <Loading />;

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

      {selectedIds.length > 0 && (
        <div className="alert alert-primary d-flex justify-content-between align-items-center py-2 mb-3">
          <div className="fw-medium">
            <span className="badge bg-primary me-2">{selectedIds.length}</span>
            {selectedIds.length === 1 ? 'job' : 'jobs'} selected
          </div>
          <div className="btn-group btn-group-sm">
            <button className="btn btn-primary" onClick={() => retry.mutate(selectedIds)}>Retry</button>
            <button className="btn btn-danger" onClick={async () => {
              const ok = await confirm({ title: 'Purge Jobs', message: `Permanently delete ${selectedIds.length} job(s)?` });
              if (ok) purge.mutate(selectedIds);
            }}>Purge</button>
            <button className="btn btn-warning" onClick={async () => {
              const ok = await confirm({ title: 'Cancel Jobs', message: `Cancel ${selectedIds.length} job(s)? They must be pending or running.` });
              if (!ok) return;
              for (const id of selectedIds) await cancel.mutateAsync(id);
            }}>Cancel</button>
          </div>
        </div>
      )}

      <DataTable>
        <thead>
        <tr>
          <th style={{width: '1%'}}>
            <input type="checkbox" checked={!!allSelected} onChange={toggleAll} />
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
              <input type="checkbox" checked={!!selected[job.id]} onChange={e => setSelected(s => ({...s, [job.id]: e.target.checked}))} />
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
      {jobs && jobs.length >= 100 && (
        <div className="text-muted text-center mt-2" style={{fontSize: '0.875rem'}}>
          Only showing the first 100 results...
        </div>
      )}
      {dialog}
    </div>
  )
}
