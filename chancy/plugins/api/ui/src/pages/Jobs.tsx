import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {useJob, useJobs} from '../hooks/useJobs.tsx';
import {Link, useParams, useSearchParams} from 'react-router-dom';
import {statusToColor} from '../utils.tsx';
import {CountdownTimer} from '../components/UpdatingTime.tsx';

export function Job() {
  const { url } = useServerConfiguration();
  const { job_id } = useParams<{job_id: string}>();

  const { data: job, isLoading } = useJob({
    url: url,
    job_id: job_id
  });

  if (isLoading) return <Loading />;

  if (!job || job.id === undefined) {
    return (
      <div className={"container-fluid"}>
        <h2 className={"mb-4"}>Job - {job_id}</h2>
        <div className={"alert alert-danger"}>Job not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Job - {job_id}</h2>
      <table className={"table table-hover border mb-0"}>
        <tbody>
        <tr>
          <th>Function</th>
          <td>
            <code>{job.func}</code>
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
              <code>{job.unique_key}</code>
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
            <div className={'card mt-4 border-danger-subtle'}>
              <div className={'card-header bg-danger-subtle'}>
                <strong>Attempt #{error.attempt}</strong>
              </div>
              <div className={'card-body'}>
                <pre><code>{error.traceback}</code></pre>
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
  const state = searchParams.get('state') || 'pending';

  const { data: jobs, isLoading, dataUpdatedAt } = useJobs({
    url: url,
    state: state
  });

  const setState = (state: string) => {
    setSearchParams({
      ...searchParams,
      state: state
    });
  }

  if (isLoading) return <Loading />;

  function stateFilter({ stateName, stateLabel }: { stateName: string, stateLabel: string }) {
    return (
      <button
        key={stateName}
        className={`btn btn-${statusToColor(stateName)} btn-sm me-2 ${state === stateName ? 'active' : ''}`}
        onClick={() => setState(stateName)}
      >
        {stateLabel}
      </button>
    );
  }

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-3"}>
        Jobs - <span className={`text-${statusToColor(state)}`}>{state}</span>
      </h2>
      <div className={'mb-1 w-100'}>
        {stateFilter({stateName: 'pending', stateLabel: 'Pending'})}
        {stateFilter({stateName: 'running', stateLabel: 'Running'})}
        {stateFilter({stateName: 'succeeded', stateLabel: 'Succeeded'})}
        {stateFilter({stateName: 'failed', stateLabel: 'Failed'})}
        {stateFilter({stateName: 'retrying', stateLabel: 'Retrying'})}
        <small className={'text-muted'}>
          Last fetched: {dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : 'Never'}
        </small>
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
              "retrying": "Started",
            }[state]}
          </th>
        </tr>
        </thead>
        <tbody>
        {jobs?.length === 0 && (
          <tr>
            <td colSpan={4} className={'text-center table-info'}>
              No matching jobs found.
            </td>
          </tr>
        )}
        {jobs?.map((job) => (
          <tr key={job.id}>
            <td className={"text-break"}>
              <span className={`text-${statusToColor(job.state)} me-2`} title={job.state}>⬤</span>
              <Link to={`/jobs/${job.id}`}>
                {job.func}
              </Link>
            </td>
            <td className={"text-center"}>
              <Link to={`/queues/${job.queue}`}>
                {job.queue}
              </Link>
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
                "retrying": job.started_at,
              }[job.state]} />
            </td>
          </tr>
        ))}
        </tbody>
      </table>
    </div>
  )
}