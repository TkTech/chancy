import { DetailCard } from '../common/DetailCard';
import type { Cron } from '../../services/schemas';
import { CountdownTimer } from '../UpdatingTime';
import { PackedJobDetails } from '../PackedJobDetails';

interface CronDetailViewProps { cron: Cron; }

export function CronDetailView({ cron }: CronDetailViewProps) {
  return (
    <div>
      <DetailCard title={`Scheduled Job - ${cron.unique_key}`} flush>
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
            <th>Timezone</th>
            <td>{cron.timezone}</td>
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
      </DetailCard>
      <div className="alert alert-info mt-4">
        Each time this cron schedule triggers, a job matching this definition will be pushed onto the queue.
      </div>
      <DetailCard title="Job Definition" flush>
        <PackedJobDetails job={cron.job} />
      </DetailCard>
    </div>
  );
}
