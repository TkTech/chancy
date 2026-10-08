import { DetailCard } from '../../components/common/DetailCard';
import {useJob} from '../../hooks/useJobs';
import {useServerConfiguration} from '../../hooks/useServerConfiguration';
import {Loading} from '../../components/Loading';
import { StatusBadge } from '../../components/common/StatusBadge';
import { Link } from 'react-router';
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
          <button className="btn btn-sm btn-primary" disabled={retry.isPending} onClick={() => retry.mutate(job.id)}>Retry Job</button>
        )}
        {['pending','running'].includes(job.state) && (
          <button className="btn btn-sm btn-warning" disabled={cancel.isPending} onClick={() => cancel.mutate(job.id)}>Cancel Job</button>
        )}
        {['succeeded','failed'].includes(job.state) && (
          <button className="btn btn-sm btn-danger" onClick={async () => {
            const ok = await confirm({ title: 'Purge Job', message: 'This will permanently delete the job record. Continue?' });
            if (ok) purge.mutate(job.id);
          }}>Purge Job</button>
        )}
      </div>
      <DetailCard title="Details" flush>
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
            <td className="w-100"><StatusBadge status={job.state} /></td>
          </tr>
          <tr>
            <th className="text-nowrap">Worker</th>
            <td className="w-100 text-break">
              {job.taken_by ? (
                <Link to={`/workers/${encodeURIComponent(job.taken_by)}`}>{job.taken_by}</Link>
              ) : 'Unassigned'}
            </td>
          </tr>
          <tr>
            <th className="text-nowrap">Claim ID</th>
            <td className="w-100 text-break">
              {job.claim_id === undefined ? 'Unavailable' : (
                job.claim_id ? <code className="text-break">{job.claim_id}</code> : 'None'
              )}
            </td>
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
          <tr>
            <th className="text-nowrap">Limits</th>
            <td className="w-100">
              {job.limits.length > 0 ? job.limits.map((limit, idx) => (
                <div key={idx}>
                  {limit.type === 'time'
                    ? `Time: ${limit.value.toLocaleString()} s`
                    : `Memory: ${limit.value.toLocaleString()} bytes`}
                </div>
              )) : 'None'}
            </td>
          </tr>
          </tbody>
        </table>
      </DetailCard>
      <DetailCard title="Arguments" flush>
        <JsonViewer value={job.kwargs} theme={theme} />
      </DetailCard>
      <DetailCard title="Metadata" flush>
        <JsonViewer value={job.meta} theme={theme} />
      </DetailCard>
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
