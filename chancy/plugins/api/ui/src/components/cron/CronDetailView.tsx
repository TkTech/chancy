import { CountdownTimer } from '../UpdatingTime';
import { PackedJobDetails } from '../PackedJobDetails';

interface CronDetailViewProps {
  cron: {
    unique_key: string;
    cron: string;
    next_run: string;
    last_run: string;
    job: {
      func: string;
      queue: string;
      kwargs: unknown;
      priority: number;
      max_attempts: number;
      limits: Array<{ key: string; value: number }>;
    };
  };
}

export function CronDetailView({ cron }: CronDetailViewProps) {
  return (
    <div>
      <div className={'card'}>
        <div className={'card-header'}>
          Scheduled Job - {cron.unique_key}
        </div>
        <table className={"table mb-0"}>
          <tbody>
          <tr>
            <th>Unique Key</th>
            <td>{cron.unique_key}</td>
          </tr>
          <tr>
            <th>Expression</th>
            <td><code>{cron.cron}</code></td>
          </tr>
          <tr>
            <th>Next Run</th>
            <td><CountdownTimer date={cron.next_run} /></td>
          </tr>
          <tr>
            <th>Last Run</th>
            <td><CountdownTimer date={cron.last_run} /></td>
          </tr>
          </tbody>
        </table>
      </div>
      <div className="alert alert-info mt-4">
        Each time this cron schedule triggers, a job matching this definition will be pushed onto the queue.
      </div>
      <div className="card">
        <div className="card-header">
          Job Definition
        </div>
        <PackedJobDetails job={cron.job} />
      </div>
    </div>
  );
}
