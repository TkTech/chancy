import { useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {PrettyDateAndTime} from './components/Time';

export function Job() {
  const { jobId } = useParams();

  const { data: jobData, isLoading: jobIsLoading } = useQuery({
    queryKey: ['job', jobId],
    queryFn: async () => {
      const response = await fetch(`/api/jobs/${jobId}`);
      return response.json();
    }
  });

  if (jobIsLoading) {
    return <div className="spinner-border" role="status">
      <span className="visually-hidden">Loading...</span>
    </div>;
  }

  return (
    <div className="row">
      <div className={"col"}>
        <div className="mb-4">
          <h1 className="display-6">Job: {jobData.payload.func}</h1>
        </div>
        <div className="card mb-4">
          <div className="card-header text-bg-info">
            General
          </div>
          <table className="table table-striped mb-0">
            <tbody>
              <tr>
                <th>ID</th>
                <td>{jobData.id}</td>
              </tr>
              <tr>
                <th>Target</th>
                <td>{jobData.payload.func}</td>
              </tr>
              <tr>
                <th>State</th>
                <td>{jobData.state}</td>
              </tr>
              <tr>
                <th>Created</th>
                <td><PrettyDateAndTime datetime={jobData.created_at} /></td>
              </tr>
              {jobData.started_at && (
                <tr>
                  <th>Started</th>
                  <td><PrettyDateAndTime datetime={jobData.started_at} /></td>
                </tr>
              )}
              {jobData.finished_at && (
                <tr>
                  <th>Finished</th>
                  <td><PrettyDateAndTime datetime={jobData.finished_at} /></td>
                </tr>
              )}
              {jobData.scheduled_at && (
                <tr>
                  <th>Scheduled</th>
                  <td><PrettyDateAndTime datetime={jobData.scheduled_at}/></td>
                </tr>
              )}
              <tr>
                <th>Attempts</th>
                <td>{jobData.attempts}</td>
              </tr>
              <tr>
                <th>Max Attempts</th>
                <td>{jobData.max_attempts}</td>
              </tr>
              <tr>
                <th>Priority</th>
                <td>{jobData.priority}</td>
              </tr>
              <tr>
                <th>Queue</th>
                <td>{jobData.queue}</td>
              </tr>
              {jobData.taken_by && (
                <tr>
                  <th>Run By</th>
                  <td>
                    <code>{jobData.taken_by}</code>
                    <small className="d-block text-muted">
                      The ID of the last worker to attempt to run this job.
                    </small>
                  </td>
                </tr>
              )}
              {jobData.unique_key && (
                <tr>
                  <th>Unique Key</th>
                  <td>
                    <code>{jobData.unique_key}</code>
                    <small className="d-block text-muted">
                      A unique key that can be used to prevent duplicate jobs.
                    </small>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="card">
          <div className="card-header text-bg-info">
            Limits
          </div>
          <div className="card-body">
            {jobData.payload.limits.length === 0 ? (
              <div className="text-muted">
                No limits have been set for this job.
              </div>
            ) : (
              <table className="table table-striped mb-0">
                <thead>
                  <tr>
                    <th>Limit</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {jobData.payload.limits.map((limit, index) => (
                    <tr key={index}>
                      <td>{limit.name}</td>
                      <td>{limit.value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}