import { Link } from 'react-router-dom';
import { JsonViewer } from './JsonViewer';
import { useTheme } from '../contexts/ThemeContext';

interface PackedJob {
  func: string;
  queue: string;
  kwargs?: unknown;
  priority?: number;
  max_attempts?: number;
  unique_key?: string;
  limits?: Array<{
    key: string;
    value: number;
  }>;
  meta?: unknown;
  scheduled_at?: string;
}

interface PackedJobDetailsProps {
  job: PackedJob;
}

export function PackedJobDetails({
  job
}: PackedJobDetailsProps) {
  const { theme } = useTheme();

  return (
    <table className="table mb-0">
      <tbody>
        <tr>
          <th className="text-nowrap">Function</th>
          <td className="w-100 text-break"><code className="text-break">{job.func}</code></td>
        </tr>
        <tr>
          <th className="text-nowrap">Queue</th>
          <td className="w-100"><Link to={`/queues/${job.queue}`}>{job.queue}</Link></td>
        </tr>
        <tr>
          <th className="text-nowrap">Priority</th>
          <td className="w-100">{job.priority ?? 0}</td>
        </tr>
        <tr>
          <th className="text-nowrap">Max Attempts</th>
          <td className="w-100">{job.max_attempts ?? 1}</td>
        </tr>
        {job.unique_key && (
          <tr>
            <th className="text-nowrap">Unique Key</th>
            <td className="w-100"><code>{job.unique_key}</code></td>
          </tr>
        )}
        {job.scheduled_at && (
          <tr>
            <th className="text-nowrap">Scheduled At</th>
            <td className="w-100">{new Date(job.scheduled_at).toLocaleString()}</td>
          </tr>
        )}
        {job.limits && job.limits.length > 0 && (
          <tr>
            <th className="text-nowrap">Limits</th>
            <td className="w-100">
              {job.limits.map((limit, idx) => (
                <div key={idx}>
                  <span className="badge bg-secondary me-1">{limit.key}</span>
                  {limit.value}
                </div>
              ))}
            </td>
          </tr>
        )}
        {job.kwargs && Object.keys(job.kwargs as object).length > 0 ? (
          <tr>
            <th className="text-nowrap align-top">Arguments</th>
            <td className="w-100">
              <JsonViewer value={job.kwargs} theme={theme} />
            </td>
          </tr>
        ) : null}
        {job.meta && Object.keys(job.meta as object).length > 0 ? (
          <tr>
            <th className="text-nowrap align-top">Metadata</th>
            <td className="w-100">
              <JsonViewer value={job.meta} theme={theme} />
            </td>
          </tr>
        ) : null}
      </tbody>
    </table>
  );
}
