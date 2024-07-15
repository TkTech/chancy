import {useQuery, keepPreviousData} from '@tanstack/react-query';

export function Workers () {
  const { data, isLoading } = useQuery({
    queryKey: ['workers'],
    queryFn: async () => {
      const response = await fetch('/api/workers');
      return response.json();
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData
  });

  return (
    <>
      <div className={"mb-4"}>
        <h1 className={'title'}>Workers</h1>
        <p className={"subtitle"}>All currently active workers.</p>
      </div>
      <div className={"columns"}>
        <div className={'column'}>
          <table className={'table is-fullwidth is-striped is-bordered'}>
            <thead>
              <tr>
                <th>Worker ID</th>
                <th>Last Seen</th>
                <th>Expires At</th>
              </tr>
            </thead>
            <tbody>
              {data && data.map((worker: any) => (
                <tr key={worker.id}>
                  <td>
                    {worker.worker_id}
                    {worker.is_leader && <span className={'tag is-primary ml-2'}>Cluster Leader</span>}
                  </td>
                  <td>{worker.last_seen}</td>
                  <td className={
                    new Date(worker.expires_at) < new Date() ? "has-text-danger" : ""
                  }>{worker.expires_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
