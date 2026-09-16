import {useJob} from '../../hooks/useJobs';
import {useServerConfiguration} from '../../hooks/useServerConfiguration';
import {Loading} from '../../components/Loading';
import {statusToColor} from '../../utils';
import { Link } from 'react-router-dom';
import { useJobActions } from '../../hooks/useJobActions';
import { useConfirm } from '../../components/common/ConfirmDialog';
import { JobTimers } from './JobTimers';
import { JsonViewer } from '../../components/JsonViewer';
import { useTheme } from '../../contexts/ThemeContext';

export function JobDetailsView({ job_id }: { job_id: string, compact?: boolean }) {
  const { url } = useServerConfiguration();
  const { data: job, isLoading } = useJob({ url, job_id });
  const { retry, cancel, purge } = useJobActions();
  const { confirm, dialog } = useConfirm();
  const { theme } = useTheme();

  if (isLoading) return <Loading/>;
  if (!job) return <div className={'alert alert-danger'}>Job not found.</div>;

  return (
    <div>
      <JobTimers job={job} />
      <div className="mb-3 d-flex gap-2 justify-content-end">
        {['failed','retrying','succeeded'].includes(job.state) && (
          <button className="btn btn-sm btn-primary" onClick={() => retry.mutate(job.id)}>Retry Job</button>
        )}
        {['pending','running'].includes(job.state) && (
          <button className="btn btn-sm btn-warning" onClick={() => cancel.mutate(job.id)}>Cancel Job</button>
        )}
        {['succeeded','failed'].includes(job.state) && (
          <button className="btn btn-sm btn-danger" onClick={async () => {
            const ok = await confirm({ title: 'Purge Job', message: 'This will permanently delete the job record. Continue?' });
            if (ok) purge.mutate(job.id);
          }}>Purge Job</button>
        )}
      </div>
      <div className="card mb-3">
        <div className="card-header">Details</div>
        <table className="table border mb-0">
          <tbody>
          <tr>
            <th className="text-nowrap">ID</th>
            <td className="w-100 text-break"><code className="text-break">{job.id}</code></td>
          </tr>
          <tr>
            <th className="text-nowrap">Function</th>
            <td className="w-100 text-break"><code className="text-break">{job.func}</code></td>
          </tr>
          <tr>
            <th className="text-nowrap">Queue</th>
            <td className="w-100"><Link to={`/queues/${job.queue}`}>{job.queue}</Link></td>
          </tr>
          <tr>
            <th className="text-nowrap">State</th>
            <td className="w-100"><span className={`badge bg-${statusToColor(job.state)}`}>{job.state}</span></td>
          </tr>
          <tr>
            <th className="text-nowrap">Attempts</th>
            <td className="w-100">{job.attempts} / {job.max_attempts}</td>
          </tr>
          {job.unique_key && (
            <tr>
              <th className="text-nowrap">Unique Key</th>
              <td className="w-100"><code>{job.unique_key}</code></td>
            </tr>
          )}
          <tr>
            <th className="text-nowrap">Priority</th>
            <td className="w-100">{job.priority}</td>
          </tr>
          </tbody>
        </table>
      </div>
      <div className="card mb-3">
        <div className="card-header">
          <h6 className="mb-0">Arguments</h6>
        </div>
        <div className="card-body p-0">
          <JsonViewer value={job.kwargs} theme={theme} />
        </div>
      </div>
      <div className="card mb-3">
        <div className="card-header">
          <h6 className="mb-0">Metadata</h6>
        </div>
        <div className="card-body p-0">
          <JsonViewer value={job.meta} theme={theme} />
        </div>
      </div>
      {job.errors.length > 0 && (
        <>
          <h6 className="mt-3 text-danger">Errors</h6>
          {job.errors.map((e, idx) => (
            <div key={idx} className="card mt-2 border-danger-subtle">
              <div className="card-header bg-danger-subtle"><strong>Attempt #{e.attempt}</strong></div>
              <div className="card-body"><pre className="mb-0"><code>{e.traceback}</code></pre></div>
            </div>
          ))}
        </>
      )}
      {dialog}
    </div>
  );
}
