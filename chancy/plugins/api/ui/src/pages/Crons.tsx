import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {useCrons} from '../hooks/useCrons.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {CountdownTimer} from '../components/UpdatingTime.tsx';
import { PageHeader } from '../components/common/PageHeader';
import { CronTimeline } from '../components/cron/CronTimeline';
import { useDrawer } from '../components/common/DrawerProvider';
import { CronDetailView } from '../components/cron/CronDetailView';

export function Cron() {
  const { url } = useServerConfiguration();
  const { data: crons, isLoading } = useCrons({ url });
  const { cron_id } = useParams<{cron_id: string}>();

  if (isLoading) return <Loading />;

  const cron = crons?.find(cron => cron.unique_key === cron_id);

  if (!cron) {
    return (
      <div className={"container-fluid"}>
        <h2 className={"mb-4"}>Scheduled Job - {cron_id}</h2>
        <div className={"alert alert-danger"}>Scheduled job not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <CronDetailView cron={cron} />
    </div>
  );
}

export function Crons() {
  const { url } = useServerConfiguration();
  const { data: crons, isLoading } = useCrons({ url });
  const drawer = useDrawer();

  if (isLoading) return <Loading />;

  const handleCronClick = (cron: any) => {
    drawer.open(
      <CronDetailView cron={cron} />,
      { title: `Scheduled Job` }
    );
  };

  return (
    <div className={"container-fluid"}>
      <PageHeader
        title="Scheduled Jobs"
        description="Jobs scheduled to run on a recurring basis using cron expressions"
      />

      {/* Timeline view */}
      <div className="mb-4">
        <CronTimeline crons={crons || []} />
      </div>

      {/* Table view */}
      <table className={"table mb-0"}>
        <thead>
        <tr>
          <th>Key</th>
          <th className={"w-100"}>Function</th>
          <th>Expression</th>
          <th className={"text-nowrap text-center"}>Next Run</th>
          <th className={"text-nowrap text-center"}>Last Run</th>
        </tr>
        </thead>
        <tbody>
        {crons?.length === 0 && (
          <tr>
            <td colSpan={5} className={"text-center table-info"}>
              No scheduled jobs found.
            </td>
          </tr>
        )}
        {crons?.map(cron => (
          <tr key={cron.unique_key}>
            <td>
              <Link
                to={`/crons/${cron.unique_key}`}
                onClick={(e) => {
                  if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
                  e.preventDefault();
                  handleCronClick(cron);
                }}
              >
                {cron.unique_key}
              </Link>
            </td>
            <td><code className={"text-break"}>{cron.job.func}</code></td>
            <td className={"text-nowrap"}><code>{cron.cron}</code></td>
            <td className={"text-nowrap text-center font-monospace"}>
              <CountdownTimer date={cron.next_run} />
            </td>
            <td className={"text-nowrap text-center font-monospace"}>
              <CountdownTimer date={cron.last_run}/>
            </td>
          </tr>
        ))}
        </tbody>
      </table>
    </div>
  );
}