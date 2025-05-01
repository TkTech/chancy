import React, {useState} from 'react';
import {useApp} from '../hooks/useServerConfiguration.tsx';
import {Loading, LoadingWithMessage} from '../components/Loading.tsx';
import {useJob, useJobs} from '../hooks/useJobs.tsx';
import {Link, useParams, useSearchParams} from 'react-router-dom';
import {formattedTimeDelta, relativeTime, statusToColorCode} from '../utils.tsx';
import {useSlidePanels} from '../components/SlidePanelContext.tsx';
import {CopyText} from '../components/Copy.tsx';
import {Queue} from './Queues.tsx';
import {WorkerDetails} from './Workers.tsx';
import {useMetricDetail} from '../hooks/useMetrics.tsx';
import {MetricChart, ResolutionSelector} from '../components/MetricCharts.tsx';
import {MetricsWrapper} from './Metrics.tsx';
import {StateBadge, states, StatusIcons} from '../components/StateBadge.tsx';

interface JobProps {
  jobId?: string;
  inPanel?: boolean;
}

function JobMetrics({ func }: { func: string; }) {
  const [resolution, setResolution] = useState<string>('5min');
  const { serverUrl } = useApp();
  const { data: metrics, isLoading } = useMetricDetail({
    url: serverUrl,
    key: `job:${func}`,
    resolution
  });

  return (
    <MetricsWrapper
      isLoading={isLoading}
      data={metrics}
      errorMessage={`No metrics data available for ${func}`}
    >
      <h3 className={"mt-4"}>Metrics</h3>
      <p>Metrics for all executions of this function across all workers and queues.</p>
      <ResolutionSelector resolution={resolution} setResolution={setResolution} />
      {metrics && Object.entries(metrics).map(([subtype, metricData]) => {
        return (
          <div key={subtype} className="card mb-4">
            <div className="card-header">
              <h5 className="mb-0">{subtype}</h5>
            </div>
            <div className="card-body">
              <MetricChart
                points={metricData.data}
                metricType={metricData.type}
                resolution={resolution}
              />
            </div>
          </div>
        );
      })}
    </MetricsWrapper>
  );
}

/**
 * Detailed view of a job.
 *
 * @param jobId Job ID to display. If not provided, it will be taken from the URL.
 * @param inPanel Use embedded panel styling
 * @constructor
 */
export function Job({ jobId, inPanel = false }: JobProps) {
  const { serverUrl } = useApp();
  const params = useParams<{job_id: string}>();
  const job_id = jobId || params.job_id;
  const { openPanel } = useSlidePanels();

  const { data: job, isLoading } = useJob({
    url: serverUrl,
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
          <div className="mb-1">
            {job.created_at ? (
              <span className="position-relative d-inline-block text-success">
                ✓
              </span>
            ) : (
              <span className="position-relative d-inline-block">
                <div className="spinner-border spinner-border-sm" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
              </span>
            )}
          </div>
          <strong>Created</strong>
          <div>
            {relativeTime(job.created_at)}
          </div>
        </div>
        <div className={"col"}>
          <div className="mb-1">
            {job.scheduled_at ? (
              <span className="position-relative d-inline-block text-success">
                ✓
              </span>
            ) : (
              <span className="position-relative d-inline-block">
                <div className="spinner-border spinner-border-sm" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
              </span>
            )}
          </div>
          <strong>Scheduled</strong>
          <div>
            {relativeTime(job.scheduled_at)}
          </div>
        </div>
        <div className={"col"}>
          <div className="mb-1">
            {job.started_at ? (
              <span className="position-relative d-inline-block text-success">
                ✓
              </span>
            ) : (
              <span className="position-relative d-inline-block">
                {job.state === "pending" && (
                  <div className="spinner-border spinner-border-sm" role="status">
                    <span className="visually-hidden">Loading...</span>
                  </div>
                )}
                {job.state !== "pending" && "-"}
              </span>
            )}
          </div>
          <strong>Waiting</strong>
          <div>
            {formattedTimeDelta(
              job.created_at,
              job.started_at ? job.started_at : new Date().toISOString()
            )}
          </div>
        </div>
        <div className={"col"}>
          <div className="mb-1">
            {job.started_at ? (
              <span className="position-relative d-inline-block">
                {job.completed_at ? (
                  <span className="text-success">✓</span>
                ) : (
                  <div className="spinner-border spinner-border-sm" role="status">
                    <span className="visually-hidden">Loading...</span>
                  </div>
                )}
              </span>
            ) : (
              <span className="position-relative d-inline-block">-</span>
            )}
          </div>
          <strong>Running</strong>
          <div>
            {job.started_at === null ? "-" : relativeTime(job.started_at)}
          </div>
        </div>
        <div className={"col"}>
          <div className="mb-1">
            {job.completed_at ? (
              <span className="position-relative d-inline-block">
                {job.state === "succeeded" && (
                  <span className="text-success">✓</span>
                )}
                {(job.state === "failed" || job.state === "expired") && (
                  <span className="text-danger">✗</span>
                )}
              </span>
            ) : (
              <span className="position-relative d-inline-block">-</span>
            )}
          </div>
          <strong>
            {{
              "succeeded": "Completed",
              "failed": "Failed",
              "expired": "Expired",
            }[job.state] || "Completed"}
          </strong>
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
            <Link to={`/queues/${job.queue}`} onClick={(e) => {
              e.preventDefault();
              openPanel({
                title: "Queue Details",
                content: <Queue queueName={job.queue} inPanel={true} />
              });
            }}>
              {job.queue}
            </Link>
          </td>
        </tr>
        <tr>
          <th>State</th>
          <td>
            <StateBadge state={job.state} />
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
            <span className={'text-muted ms-2'}>
              - Higher values run first.
            </span>
          </td>
        </tr>
        <tr>
          <th>Limits</th>
          <td>
            {job.limits.length === 0 ? (
              <span className={'text-muted'}>No resource limits set.</span>
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
        <tr>
          <th>Taken By</th>
          <td>
            {job.taken_by ? (
              <Link to={`/workers/${job.taken_by}`} onClick={(e) => {
                e.preventDefault();
                openPanel({
                  title: "Worker Details",
                  content: <WorkerDetails workerId={job.taken_by} inPanel={true} />
                });
              }}>
                {job.taken_by}
              </Link>
            ) : (
              <span className={'text-muted'}>Not yet claimed by a worker</span>
            )}
          </td>
        </tr>
        </tbody>
      </table>
      <h3 className={"mt-4"}>Arguments</h3>
      <p>Arguments passed to the function. This is a JSON-encoded string.</p>
      <div className={"border p-4"}>
        <pre className={"mb-0"}><code>{JSON.stringify(job.kwargs, null, 2)}</code></pre>
      </div>
      <h3 className={"mt-4"}>Meta</h3>
      <p>Arbitrary persistent metadata attached to the job by a user or plugin.</p>
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
      <JobMetrics func={job.func} />
    </div>
  );
}


export function Jobs() {
  const { serverUrl } = useApp();
  const { openPanel } = useSlidePanels();
  const [searchParams, setSearchParams] = useSearchParams();

  const currentState = searchParams.get('state') || undefined;
  const currentFunc = searchParams.get('func') || undefined;

  const { data: jobs, isLoading, isRefetching } = useJobs({
    url: serverUrl,
    state: currentState,
    func: currentFunc
  });

  const handleJobClick = (jobId: string, e: React.MouseEvent) => {
    e.preventDefault();
    openPanel({
      title: "Job Details",
      content: <Job jobId={jobId} inPanel={true} />
    });
  };

  if (isLoading) return <LoadingWithMessage message={"Loading jobs..."} />

  return (
    <div className={"container-fluid"}>
      <div className={"d-flex mb-4 align-items-center align-content-center"}>
        <div className={"d-flex flex-grow-1 align-content-center align-items-center"}>
          <ul className={"nav nav-pills"}>
            {states.map((state) => {
              const Icon = StatusIcons[state];
              return (
                <li className={"nav-item"} key={state}>
                  <Link
                    to={`/jobs?state=${state}`}
                    style={{
                      color: statusToColorCode(state),
                    }}
                    className={`nav-link ${currentState === state ? 'active' : ''}`}
                    onClick={(e) => {
                      e.preventDefault();
                      setSearchParams((prev) => {
                          const params = new URLSearchParams(prev);
                          params.set('state', state);
                          return params;
                        }
                      )
                    }}
                  >
                    <Icon
                      width={"1em"}
                      height={"1em"}
                      className={'me-2'}
                      style={{
                        color: statusToColorCode(state),
                      }}
                    />
                    {state}
                  </Link>
                </li>
              )
            })}
          </ul>
          {isRefetching && (
            <div className={"spinner-border spinner-border-sm ms-2"} role="status">
              <span className="visually-hidden">Updating...</span>
            </div>
          )}
        </div>
        <div>
          <input
            type="text"
            className={"form-control form-control-sm"}
            placeholder={"Search..."}
            onChange={(e) => {
              const value = e.target.value;
              if (value === "") {
                setSearchParams((prev) => {
                  const params = new URLSearchParams(prev);
                  params.delete('func');
                  return params;
                });
              } else {
                setSearchParams((prev) => {
                  const params = new URLSearchParams(prev);
                  params.set('func', value);
                  return params;
                });
              }
            }}
          />
        </div>
      </div>
      <table className={'table table-hover mb-0'}>
        <thead>
        <tr>
          <th className={"w-100"}>Job</th>
          <th className={'text-center'}>Queue</th>
          <th className={"text-center"}>Attempts</th>
          <th className={"text-center"}>State</th>
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
                <code title={job.func}>{job.func.split('.').pop()}</code>
              </Link>
            </td>
            <td className={"text-center"}>
              <span onClick={(e) => { e.stopPropagation(); }}>
                <Link to={`/queues/${job.queue}`} onClick={(e) => {
                  e.preventDefault();
                  openPanel({
                    title: "Queue Details",
                    content: <Queue queueName={job.queue} inPanel={true} />
                  });
                }}>
                  {job.queue}
                </Link>
              </span>
            </td>
            <td className={"text-center"}>
              {job.attempts} / {job.max_attempts}
            </td>
            <td>
              <StateBadge state={job.state} />
            </td>
          </tr>
        ))}
        </tbody>
      </table>
    </div>
  )
}