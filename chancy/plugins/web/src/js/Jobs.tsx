import {useEffect, useState} from 'react';
import {useQuery} from '@tanstack/react-query';
import {Link, useSearchParams} from 'react-router-dom';
import {TimeBlock, TimeSinceBlock} from './components/Time';


/**
 * Implements a pseudo-table listing of jobs, optionally matching
 * specific conditions.
 */
export function JobTable({ state }) {
  const [params, setParams] = useState(() => new URLSearchParams({
    state: state
  }));

  useEffect(() => {
    if (state) {
      setParams((prevParams) => {
        const newParams = new URLSearchParams(prevParams);
        newParams.set('state', state);
        return newParams;
      });
    }
  }, [state]);

  const { data, isLoading} = useQuery({
    queryKey: ['jobs', params.toString()],
    queryFn: async () => {
      const response = await fetch(`/api/jobs?${params.toString()}`);
      return response.json();
    },
    refetchInterval: 1000
  })

  if (isLoading) {
    return <p>Loading...</p>;
  }

  return (
    <article className={'panel is-primary'}>
      <p className={'panel-heading'}>
        Jobs - {state}
      </p>
      {data && data.length === 0 && (
        <div className={'panel-block is-info'}>
          No matching jobs found.
        </div>
      )}
      {data && data.map((job) => (
        <div key={job.id} className={'panel-block level mb-2'}>
          <div className={'level-left'}>
            <div className={'level-item'}>
              <div className={"is-flex-direction-column is-align-content-start"}>
                <div className={"has-text-weight-bold"}>
                  <Link to={`/jobs/${job.id}`}>
                    {job.payload.func}
                  </Link>
                </div>
                <div>
                  {job.attempts} / {job.max_attempts} <code>{JSON.stringify(job.payload.kwargs)}</code>
                </div>
              </div>
            </div>
          </div>
          <div className={'level-right'}>
            <div className={'level-item'}>
              <span className={"tag"}>
                {job.queue}
              </span>
            </div>
            <div className={'level-item'}>
              <span className={"tag"}>
                {job.state === "running" ? (
                  <TimeSinceBlock datetime={job.started_at}/>
                ) : (
                  <TimeBlock datetime={job.completed_at}/>
                )}
              </span>
            </div>
          </div>
        </div>
      ))}
    </article>
  );
}

export function Jobs () {
  const [searchParams, setSearchParams] = useSearchParams();

  useEffect(() => {
    if (!searchParams.has('state')) {
      setSearchParams((prevParams) => {
        const newParams = new URLSearchParams(prevParams);
        newParams.set('state', 'running');
        return newParams;
      });
    }
  }, [searchParams, setSearchParams]);

  const { data: stateData, isLoading: statesIsLoading } = useQuery({
    queryKey: ['states'],
    queryFn: async () => {
      const response = await fetch('/api/states');
      return response.json();
    }
  });

  return (
    <>
      <div className={"mb-4"}>
        <h1 className={'title'}>Jobs</h1>
        <p className={"subtitle"}>Search waiting, running, and finished jobs.</p>
      </div>
      <div className={"columns"}>
        <div className={'column is-3'}>
          <aside className={"menu"}>
            <p className={"menu-label"}>
              States
            </p>
            <ul className={"menu-list"}>
              {stateData && stateData.map((state: any) => (
                <li key={state.name}>
                  <Link
                    to={`/jobs/?state=${state.name}`}
                    className={state.name === searchParams.get('state') ? 'is-active' : ''}
                  >
                    {state.name}
                  </Link>
                </li>
              ))}
            </ul>
          </aside>
        </div>
        <div className={'column'}>
          <JobTable state={searchParams.get('state')}/>
        </div>
      </div>
    </>
  );
}