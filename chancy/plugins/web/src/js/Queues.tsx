import { useQuery, keepPreviousData } from '@tanstack/react-query';
import { Link } from 'react-router-dom';

export function Queues() {
  const { data: workers, isLoading: workersLoading } = useQuery({
    queryKey: ['workers'],
    queryFn: async () => {
      const response = await fetch('/api/workers');
      return response.json();
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData
  });

  const { data, isLoading } = useQuery({
    queryKey: ['queues'],
    queryFn: async () => {
      const response = await fetch('/api/queues');
      const queueData = await response.json();
      if (workers) {
        for (let queue of queueData) {
          queue.worker_count = workers.filter((worker) => worker.queues.includes(queue.name)).length;
        }
      }
      return queueData;
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
    enabled: !workersLoading
  });

  if (isLoading || workersLoading) {
    return (
      <div className="d-flex justify-content-center">
        <div className="spinner-border" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
      </div>
    );
  }

  return (
    <>
      <div className="row">
        <div className="col">
          <table className="table table-striped table-bordered">
            <thead>
              <tr>
                <th>Queue</th>
                <th className="text-center">Workers</th>
                <th className="text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {data && data.map((queue) => (
                <tr key={queue.name}>
                  <td>
                    <Link to={`/queues/${queue.name}`}>{queue.name}</Link>
                  </td>
                  <td className="text-center">
                    {queue.worker_count}
                  </td>
                  <td className="text-center">
                    {queue.state}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}