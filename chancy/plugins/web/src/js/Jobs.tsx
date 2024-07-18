import {useEffect, useState} from 'react';
import {useQuery, keepPreviousData} from '@tanstack/react-query';
import {Link, useSearchParams} from 'react-router-dom';
import {TimeBlock, TimeSinceBlock} from './components/Time';


/**
 * Implements a pseudo-table listing of jobs, optionally matching
 * specific conditions.
 */
export function JobTable({ state }) {
  const [params, setParams] = useState(() => new URLSearchParams({
    state: state,
    limit: "20"
  }));

  const [limit, setLimit] = useState(20);

  useEffect(() => {
    if (state) {
      if (state !== params.get('state')) {
        setLimit(20);
      }

      setParams((prevParams) => {
        const newParams = new URLSearchParams(prevParams);
        newParams.set('state', state);
        return newParams;
      });
    }
    if (limit) {
      setParams((prevParams) => {
        const newParams = new URLSearchParams(prevParams);
        newParams.set('limit', limit.toString());
        return newParams;
      });
    }
  }, [state, limit]);

  const { data, isLoading} = useQuery({
    queryKey: ['jobs', params.toString()],
    queryFn: async () => {
      const response = await fetch(`/api/jobs?${params.toString()}`);
      return response.json();
    },
    refetchInterval: 1000,
    placeholderData: keepPreviousData
  });

  if (isLoading) {
    return <p>Loading...</p>;
  }

  return (
    <div className={'card mb-4'}>
      <div className={'card-header text-bg-info'}>
        Showing <strong>{state}</strong> jobs
      </div>
      {data && data.length === 0 && (
        <div className={"card-body"}>
          <div className={"text-muted"}>
            No matching jobs found.
          </div>
        </div>
      )}
      <ul className={"list-group list-group-flush"}>
        {data && data.map((job) => {
          return (
            <li key={job.id} className={'list-group-item'}>
              <div className={'d-flex w-100 justify-content-between'}>
                <Link
                  className={"text-decoration-none"}
                  to={`/jobs/${job.id}`}>{job.payload.func}</Link>
                <div className={'d-inline-block'}>
                  <div className={'badge text-bg-info mr-2'}>
                    <abbr title={"Queue Name"}>{job.queue}</abbr>
                  </div>
                </div>
              </div>
              <div className={'d-flex w-100 justify-content-between mt-2'}>
                <code>{JSON.stringify(job.payload.kwargs)}</code>
                <div className={"text-muted small"}>
                  <TimeSinceBlock datetime={job.created_at} />
                </div>
              </div>
            </li>
          );
        })}
      </ul>
      {data && data.length > 20 && (
        <div className={"card-footer"}>
          <div className={"d-grid gap-2"}>
            <button
              className={"btn btn-primary"}
              type={"button"}
              onClick={() => setLimit(limit + 20)}
              disabled={limit >= 200}
            >Load More</button>
          </div>
        </div>
      )}
    </div>
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

  if (statesIsLoading) {
    return (
      <div className="d-flex justify-content-center">
        <div className="spinner-border" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
      </div>
    );
  }

  return (
    <div className={"row"}>
      <div className={"col col-3"}>
        <h5>States</h5>
        <div className={"list-group"}>
          {stateData && stateData.map((state) => (
            <Link
              key={state.name}
              to={`/jobs?state=${state.name}`}
              className={`list-group-item ${searchParams.get('state') === state.name ? 'active' : ''}`}
            >
              {state.name}
            </Link>
          ))}
        </div>
      </div>
      <div className={'col'}>
        <JobTable state={searchParams.get('state')}/>
      </div>
    </div>
  );
}