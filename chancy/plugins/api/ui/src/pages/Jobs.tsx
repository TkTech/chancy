import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading} from '../components/Loading.tsx';
import {useJob, useJobs} from '../hooks/useJobs.tsx';
import {Link, useParams, useSearchParams} from 'react-router-dom';
import {statusToColor} from '../utils.tsx';

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
      <div className={"container"}>
        <h2 className={"mb-4"}>Job - {job_id}</h2>
        <div className={"alert alert-danger"}>Job not found.</div>
      </div>
    );
  }

  return (
    <div className={"container"}>
      <div className={'card'}>
        <div className={'card-header'}>
          Job - {job_id}
        </div>
        <table className={"table mb-0"}>
          <tbody>
          <tr>
            <th>Function</th>
            <td>
              <code>{job.payload.func}</code>
            </td>
          </tr>
          <tr>
            <th>Arguments</th>
            <td>
              <pre className={"mb-0"}><code>{JSON.stringify(job.payload.kwargs)}</code></pre>
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
          <tr>
            <th>Scheduled At</th>
            <td>
              {job.scheduled_at}
            </td>
          </tr>
          </tbody>
        </table>
      </div>
      {job.errors.length !== 0 && (
        <div className={'card mt-4 border-danger-subtle'}>
          <div className={'card-header bg-danger-subtle'}>
            Errors
          </div>
          <ul className={"list-group list-group-flush"}>
            {job.errors.map((error, index) => (
              <li key={index} className={"list-group-item text-danger"}>
                <div className={"mb-3 py-1 border-bottom"}>
                  <strong>Attempt #{error.attempt}</strong>
                </div>
                <pre><code>{error.traceback}</code></pre>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

export function Jobs () {
  const { url } = useServerConfiguration();
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
    <div className={"container"}>
      <div className={'mb-3'}>
        {stateFilter({ stateName: 'pending', stateLabel: 'Pending' })}
        {stateFilter({ stateName: 'running', stateLabel: 'Running' })}
        {stateFilter({ stateName: 'succeeded', stateLabel: 'Succeeded' })}
        {stateFilter({ stateName: 'failed', stateLabel: 'Failed' })}
        {stateFilter({ stateName: 'retrying', stateLabel: 'Retrying' })}
      </div>

      <div className={'card'}>
        <div className={'card-header d-flex'}>
          Jobs
          <div className={'ms-auto'}>
            Last fetched: {dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : 'Never'}
          </div>
        </div>
        <table className={"table table-sm table-striped table-hover mb-0"}>
          <thead>
          <tr>
            <th>Job</th>
            <th className={"text-center"}>State</th>
            <th className={"text-center"}>Queue</th>
            <th className={"text-end"}>Attempts</th>
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
              <td>
                <Link to={`/jobs/${job.id}`}>
                  <code>{job.payload.func}</code>
                </Link>
              </td>
              <td className={"text-center"}>
                <span className={`badge bg-${statusToColor(job.state)}`}>
                  {job.state}
                </span>
              </td>
              <td className={"text-center"}>
                <Link to={`/queues/${job.queue}`}>
                  {job.queue}
                </Link>
              </td>
              <td className={"text-end"}>
                {job.attempts} / {job.max_attempts}
              </td>
            </tr>
          ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}