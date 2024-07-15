import {useParams} from 'react-router-dom';
import {useQuery} from '@tanstack/react-query';

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
    return <p>Loading...</p>;
  }

  return (
    <>
      <div className={"mb-4"}>
        <h1 className={'title'}>Job: {jobData.payload.func}</h1>
        <p className={'subtitle'}>Details for job {jobData.id}</p>
      </div>
      <article className={'panel is-primary mt-2'}>
        <p className={'panel-heading'}>
          General
        </p>
        <div className={'panel-block'}>
          <table className={'table is-fullwidth'}>
            <tbody>
            <tr>
              <td>State</td>
              <td>{jobData.state}</td>
            </tr>
            <tr>
              <td>Created</td>
              <td>{jobData.created_at}</td>
            </tr>
            <tr>
              <td>Started</td>
              <td>{jobData.started_at}</td>
            </tr>
            <tr>
              <td>Finished</td>
              <td>{jobData.completed_at}</td>
            </tr>
            <tr>
              <td>Scheduled</td>
              <td>{jobData.scheduled_at}</td>
            </tr>
            <tr>
              <td>Attempts</td>
              <td>{jobData.attempts} <em>of</em> {jobData.max_attempts}</td>
            </tr>
            <tr>
              <td>Priority</td>
              <td>{jobData.priority}</td>
            </tr>
            <tr>
              <td>Queue</td>
              <td>{jobData.queue}</td>
            </tr>
            {jobData.taken_by && (
              <tr>
                <td>Run By</td>
                <td>
                  <code>{jobData.taken_by}</code>
                  <div className={'has-text-info is-size-7'}>
                    The ID of the last worker to attempt to run this job.
                  </div>
                </td>
              </tr>
            )}
            {jobData.unique_key && (
              <tr>
                <td>Unique Key</td>
                <td>
                  <code>{jobData.unique_key}</code>
                  <div className={'has-text-info is-size-7'}>
                    A unique key that can be used to prevent duplicate jobs.
                  </div>
                </td>
              </tr>
            )}
            </tbody>
          </table>
        </div>
      </article>
      <article className={'panel is-primary mt-2'}>
        <p className={'panel-heading'}>
          Payload
        </p>
        <pre className={'panel-block'}>
          <code>
            {JSON.stringify(jobData.payload, null, 2)}
          </code>
        </pre>
      </article>
    </>
  );

}