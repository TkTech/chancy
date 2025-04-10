import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {useCrons} from '../hooks/useCrons.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {CountdownTimer} from '../components/UpdatingTime.tsx';

export function Cron() {
  const { url } = useServerConfiguration();
  const { data: crons, isLoading } = useCrons({ url });
  const { cron_id } = useParams<{cron_id: string}>();

  if (isLoading) return <Loading />;

  const cron = crons?.find(cron => cron.unique_key === cron_id);

  if (!cron) {
    return (
      <div className={"container-fluid"}>
        <h2 className={"mb-4"}>Cron - {cron_id}</h2>
        <div className={"alert alert-danger"}>Cron not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <div className={'card'}>
        <div className={'card-header'}>
          Cron - {cron_id}
        </div>
        <table className={"table mb-0"}>
          <tbody>
          <tr>
            <th>Unique Key</th>
            <td>
              {cron.unique_key}
            </td>
          </tr>
          <tr>
            <th>Expression</th>
            <td>
              <code>{cron.cron}</code>
            </td>
          </tr>
          <tr>
            <th>Next Run</th>
            <td>
              {cron.next_run}
            </td>
          </tr>
          <tr>
            <th>Last Run</th>
            <td>
              {cron.last_run}
            </td>
          </tr>
          </tbody>
        </table>
      </div>
      <div className={"card mt-4"}>
        <div className={"card-header"}>
          Job Definition
        </div>
        <table className={"table mb-0"}>
          <tbody>
          <tr>
            <th>Function</th>
            <td>
              <code>{cron.job.func}</code>
            </td>
          </tr>
          <tr>
            <th>Arguments</th>
            <td>
              <pre className={"mb-0"}><code>{JSON.stringify(cron.job.kwargs)}</code></pre>
            </td>
          </tr>
          <tr>
            <th>Queue</th>
            <td>
              <Link to={`/queues/${cron.job.queue}`}>
                {cron.job.queue}
              </Link>
            </td>
          </tr>
          <tr>
            <th>Priority</th>
            <td>
              {cron.job.priority}
            </td>
          </tr>
          <tr>
            <th>Max Attempts</th>
            <td>
              {cron.job.max_attempts}
            </td>
          </tr>
          <tr>
            <th>Limits</th>
            <td>
              {cron.job.limits.length === 0 ? (
                <div className={"alert alert-info mb-0"}>
                  No resource limits defined.
                </div>
              ) : (
                <table className={"table table-sm mb-0"}>
                  <thead>
                  <tr>
                    <th>Key</th>
                    <th>Value</th>
                  </tr>
                  </thead>
                  <tbody>
                  {cron.job.limits.map(limit => (
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
        <div className={"card-footer text-muted fst-italic"}>
          This is the job that will be run when the cron expression is satisfied.
        </div>
      </div>
    </div>
  );
}

export function Crons() {
  const { url } = useServerConfiguration();
  const { data: crons, isLoading } = useCrons({ url });

  if (isLoading) return <Loading />;

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Crons</h2>
      <p>
        Crons are scheduled jobs that run at specified intervals using
        a <a href={'https://crontab.guru/'} target={'_blank'} rel={'noreferrer'}>cron
        expression</a>.
      </p>
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
              No crons found.
            </td>
          </tr>
        )}
        {crons?.map(cron => (
          <tr key={cron.unique_key}>
            <td>
              <Link to={`/crons/${cron.unique_key}`}>
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