import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {useJob, useJobs} from '../hooks/useJobs.tsx';
import {Link, useLocation, useParams, useSearchParams} from 'react-router-dom';
import {formattedTimeDelta, relativeTime, statusToColor} from '../utils.tsx';
import {CountdownTimer} from '../components/UpdatingTime.tsx';
import React from 'react';
import {SlidePanel} from '../components/SlidePanel.tsx';
import {CopyText} from '../components/Copy.jsx';

interface JobProps {
  jobId?: string;
  inPanel?: boolean;
}

export function Job({ jobId, inPanel = false }: JobProps) {
  const { url } = useServerConfiguration();
  const params = useParams<{job_id: string}>();
  const job_id = jobId || params.job_id;

  const { data: job, isLoading } = useJob({
    url: url,
    job_id: job_id,
    refetchInterval: 5000
  });

  if (isLoading) return <Loading />;

  if (!job || job.id === undefined) {
    return (
      <div className={inPanel ? "" : "container-fluid"}>
        {!inPanel && <h2 className={"mb-4"}>Job - {job_id}</h2>}
        <div className={"alert alert-danger"}>Job not found.</div>
      </div>
    );
  }

  return (
    <div className={inPanel ? "" : "container-fluid"}>
      {!inPanel && <h2 className={"mb-4"}>Job - {job_id}</h2>}
      <div className={"row row-cols-5 text-center mb-4"}>
        <div className={"col"}>
          <strong>Created</strong>
          <div>
            {relativeTime(job.created_at)}
          </div>
        </div>
        <div className={"col"}>
          <strong>Scheduled</strong>
          <div>
            {relativeTime(job.scheduled_at)}
          </div>
        </div>
        <div className={"col"}>
          <strong>Wait</strong>
          <div>
            {formattedTimeDelta(
              job.created_at,
              job.started_at ? job.started_at : new Date().toISOString()
            )}
          </div>
        </div>
        <div className={"col"}>
          <strong>Running</strong>
          <div>
            {job.started_at === null ? "-" : relativeTime(job.started_at)}
          </div>
        </div>
        <div className={"col"}>
          <strong>Completed</strong>
          <div>
            {job.completed_at === null ? "-" : relativeTime(job.completed_at)}
          </div>
        </div>
      </div>
      <table className={"table table-hover border mb-0"}>
        <tbody>
        <tr>
          <th>UUID</th>
          <td>
            <CopyText text={job.id}>
              <code className={"text-break"}>{job.id}</code>
            </CopyText>
          </td>
        </tr>
        <tr>
          <th>Function</th>
          <td>
            <CopyText text={job.func}>
              <code className={"text-break"}>{job.func}</code>
            </CopyText>
          </td>
        </tr>
        <tr>
          <th>Queue</th>
          <td>
            <Link to={`/queues/${job.queue}`}>
              {job.queue}
            </Link>
          </td>
        </tr>
        <tr>
          <th>State</th>
          <td>
              <span className={`badge bg-${statusToColor(job.state)}`}>
                {job.state}
              </span>
          </td>
        </tr>
        <tr>
          <th>Attempts</th>
          <td>
            {job.attempts} / {job.max_attempts}
          </td>
        </tr>
        <tr>
          <th>Created At</th>
          <td>
            {job.created_at}
          </td>
        </tr>
        <tr>
          <th>Scheduled At</th>
          <td>
            {job.scheduled_at}
          </td>
        </tr>
        <tr>
          <th>Started At</th>
          <td>
            {job.started_at}
          </td>
        </tr>
        <tr>
          <th>Completed At</th>
          <td>
            {job.completed_at}
          </td>
        </tr>
        {job.unique_key && (
          <tr>
            <th>Unique Key</th>
            <td>
              <code className={"text-break"}>{job.unique_key}</code>
            </td>
          </tr>
        )}
        <tr>
          <th>Priority</th>
          <td>
            {job.priority}
            <small className={'text-muted d-block'}>
              Higher values run first.
            </small>
          </td>
        </tr>
        <tr>
          <th>Limits</th>
          <td>
            {job.limits.length === 0 ? (
              <div className={'alert alert-info mb-0'}>
                No resource limits defined.
              </div>
            ) : (
              <table className={'table table-sm mb-0'}>
                <thead>
                <tr>
                  <th>Key</th>
                  <th>Value</th>
                </tr>
                </thead>
                <tbody>
                {job.limits.map(limit => (
                  <tr key={limit.key}>
                    <td>{limit.key}</td>
                    <td>{limit.value}</td>
                  </tr>
                ))}
                </tbody>
              </table>
            )}
          </td>
        </tr>
        </tbody>
      </table>
      <h3 className={"mt-4"}>Arguments</h3>
      <div className={"border p-4"}>
        <pre className={"mb-0"}><code>{JSON.stringify(job.kwargs, null, 2)}</code></pre>
      </div>
      <h3 className={"mt-4"}>Meta</h3>
      <div className={"border p-4"}>
        <pre className={"mb-0"}><code>{JSON.stringify(job.meta, null, 2)}</code></pre>
      </div>
      {job.errors.length !== 0 && (
        <>
          <h3 className={'mt-4 text-danger'}>Errors</h3>
          <p>
            The job encountered the following errors during execution.
          </p>
          {job.errors.map((error) => (
            <div className={'card mt-4 border-danger-subtle'} key={error.attempt}>
              <div className={'card-header bg-danger-subtle'}>
                <strong>Error on attempt #{error.attempt}</strong>
              </div>
              <div className={'card-body'}>
                <pre
                  style={{
                    maxHeight: '300px',
                  }}
                ><code>{error.traceback}</code></pre>
              </div>
            </div>
          ))}
        </>
      )}
    </div>
  );
}


export function Jobs() {
  const {url} = useServerConfiguration();
  const [searchParams, setSearchParams] = useSearchParams();
  const location = useLocation();
  const { pathname } = location;
  const state = pathname.split("/")[2];

  const func = searchParams.get('func') || undefined;
  const [funcInput, setFuncInput] = React.useState(func || '');
  
  const [selectedJobId, setSelectedJobId] = React.useState<string | null>(null);
  const [isPanelOpen, setIsPanelOpen] = React.useState(false);

  const { data: jobs, isLoading, dataUpdatedAt } = useJobs({
    url: url,
    state: state,
    func: func
  });
  
  const updateFuncFilter = (value: string) => {
    setFuncInput(value);
    const newParams = {...Object.fromEntries(searchParams.entries())};
    if (value) {
      newParams.func = value;
    } else {
      delete newParams.func;
    }
    setSearchParams(newParams);
  }
  
  const handleJobClick = (jobId: string, e: React.MouseEvent) => {
    e.preventDefault();
    setSelectedJobId(jobId);
    setIsPanelOpen(true);
  };

  const handleClosePanel = () => {
    setIsPanelOpen(false);
  };

  if (isLoading) return <Loading />;

  return (
    <div className={"container-fluid"}>
      <div className="d-flex justify-content-between align-items-center mb-3">
        <h2 className="mb-0">
          <span className={`text-${statusToColor(state)}`}>
            {state.charAt(0).toUpperCase() + state.slice(1)} Jobs
          </span>
        </h2>
        <div className="d-flex align-items-center">
          <small className="text-muted me-2">
            Last updated: {dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : 'Never'}
          </small>
          <div className="input-group input-group-sm me-2" style={{ width: "250px" }}>
            <input
              type="text"
              className="form-control"
              placeholder="Search by function name"
              value={funcInput}
              onChange={(e) => updateFuncFilter(e.target.value)}
            />
            {func && (
              <button 
                className="btn btn-outline-secondary" 
                type="button"
                onClick={() => updateFuncFilter('')}
              >
                ×
              </button>
            )}
          </div>
        </div>
      </div>

      <table className={'table table-hover mb-0'}>
        <thead>
        <tr>
          <th className={"w-100"}>Job</th>
          <th className={'text-center'}>Queue</th>
          <th className={"text-center"}>Attempts</th>
          <th className={"text-center"}>
            {{
              "pending": "Created",
              "running": "Started",
              "succeeded": "Completed",
              "failed": "Completed",
              "expired": "Completed",
              "retrying": "Started",
            }[state]}
          </th>
        </tr>
        </thead>
        <tbody>
        {jobs?.length === 0 && (
          <tr>
            <td colSpan={4} className={'text-center'}>
              No matching jobs found.
            </td>
          </tr>
        )}
        {jobs?.map((job) => (
          <tr key={job.id}>
            <td className={"text-break"}>
              <Link 
                to={`/jobs/${job.id}`} 
                onClick={(e) => handleJobClick(job.id, e)}
              >
                {job.func}
              </Link>
            </td>
            <td className={"text-center"}>
              <span onClick={(e) => { e.stopPropagation(); }}>
                <Link to={`/queues/${job.queue}`}>
                  {job.queue}
                </Link>
              </span>
            </td>
            <td className={"text-center"}>
              {job.attempts} / {job.max_attempts}
            </td>
            <td className={"text-center"}>
              <CountdownTimer date={{
                "pending": job.created_at,
                "running": job.started_at,
                "succeeded": job.completed_at,
                "failed": job.completed_at,
                "expired": job.completed_at,
                "retrying": job.started_at,
              }[job.state]} />
            </td>
          </tr>
        ))}
        </tbody>
      </table>
      
      <SlidePanel
        isOpen={isPanelOpen} 
        onClose={handleClosePanel}
        title={"Job Details"}
      >
        {selectedJobId && <Job jobId={selectedJobId} inPanel={true} />}
      </SlidePanel>
    </div>
  )
}