import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {useWorkflow, useWorkflows} from '../hooks/useWorkflows.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {UpdatingTime} from '../components/UpdatingTime.tsx';
import {statusToColor} from '../utils.tsx';

export function Workflow() {
  const { url } = useServerConfiguration();
  const { workflow_id } = useParams<{workflow_id: string}>();
  const { data: workflow, isLoading } = useWorkflow({ url, workflow_id });

  if (isLoading) return <Loading />;

  if (!workflow) {
    return (
      <div className={"container"}>
        <h2 className={"mb-4"}>Workflow - {workflow_id}</h2>
        <div className={"alert alert-danger"}>Workflow not found.</div>
      </div>
    );
  }

  return (
    <div className={"container"}>
      <div className={"card"}>
        <div className={"card-header"}>
          Workflow - {workflow_id}
        </div>
        <table className={"table mb-0"}>
          <tbody>
          <tr>
            <th>Name</th>
            <td>{workflow.name}</td>
          </tr>
          <tr>
            <th>State</th>
            <td>
              <span className={`badge bg-${statusToColor(workflow.state)}`}>{workflow.state}</span>
            </td>
          </tr>
          <tr>
            <th>Created</th>
            <td>
              <UpdatingTime date={workflow.created_at} />
            </td>
          </tr>
          <tr>
            <th>Updated</th>
            <td>
              <UpdatingTime date={workflow.updated_at} />
            </td>
          </tr>
          </tbody>
        </table>
      </div>
      {workflow.steps && (
        <div className={"card mt-4"}>
          <div className={"card-header"}>
            Steps
          </div>
          <table className={"table mb-0"}>
            <thead>
            <tr>
              <th>Step ID</th>
              <th>State</th>
              <th>Job ID</th>
            </tr>
            </thead>
            <tbody>
            {Object.entries(workflow.steps).map(([step_id, step]) => (
              <tr key={step_id}>
                <td>{step_id}</td>
                <td>
                  <span className={`badge bg-${statusToColor(step.state)}`}>{step.state}</span>
                </td>
                <td>
                  <Link to={`/jobs/${step.job_id}`}>
                    {step.job_id}
                  </Link>
                </td>
              </tr>
            ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export function Workflows() {
  const { url } = useServerConfiguration();
  const { data: workflows, isLoading } = useWorkflows({ url });

  if (isLoading) return <Loading />;

  return (
    <div className={"container"}>
      <div className={"card"}>
        <div className={"card-header"}>
          Workflows
        </div>
        <table className={"table table-sm table-striped table-hover mb-0"}>
          <thead>
          <tr>
            <th>Name</th>
            <th className={"text-center"}>State</th>
            <th>Created</th>
          </tr>
          </thead>
          <tbody>
          {workflows?.map(workflow => (
            <tr key={workflow.id}>
              <td>
                <Link to={`/workflows/${workflow.id}`}>
                  {workflow.name}
                </Link>
              </td>
              <td className={"text-center"}>
                <span className={`badge bg-${statusToColor(workflow.state)}`}>{workflow.state}</span>
              </td>
              <td>
                <UpdatingTime date={workflow.created_at} />
              </td>
            </tr>
          ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}