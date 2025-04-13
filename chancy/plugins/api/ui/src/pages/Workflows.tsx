import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {useWorkflow, useWorkflows} from '../hooks/useWorkflows.tsx';
import {Loading} from '../components/Loading.tsx';
import {Link, useParams} from 'react-router-dom';
import {UpdatingTime} from '../components/UpdatingTime.tsx';
import WorkflowChart from './WorkflowChart.tsx';
import {ReactFlowProvider} from '@xyflow/react';
import {useSlidePanels} from '../components/SlidePanelContext.tsx';
import {Job} from './Jobs.tsx';
import {StateBadge} from '../components/StateBadge.tsx';


export function Workflow() {
  const { url } = useServerConfiguration();
  const { workflow_id } = useParams<{workflow_id: string}>();
  const { data: workflow, isLoading } = useWorkflow({ url, workflow_id, options: {refetchInterval: 5000 } });
  const { openPanel } = useSlidePanels();
  
  const handleJobClick = (jobId: string, e: React.MouseEvent) => {
    e.preventDefault();
    openPanel({
      title: "Job Details",
      content: <Job jobId={jobId} inPanel={true} />
    });
  };

  if (isLoading) return <Loading />;

  if (!workflow) {
    return (
      <div className={"container-fluid"}>
        <h2 className={"mb-4"}>Workflow - {workflow_id}</h2>
        <div className={"alert alert-danger"}>Workflow not found.</div>
      </div>
    );
  }

  return (
    <div className={"container-fluid"}>
      <h2 className={"mb-4"}>Workflow - {workflow_id}</h2>
      <table className={"table table-hover border mb-0"}>
        <tbody>
        <tr>
          <th>Name</th>
          <td>{workflow.name}</td>
        </tr>
        <tr>
          <th>State</th>
          <td>
            <StateBadge state={workflow.state} />
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
      {workflow.steps && (
        <>
          <div className="card mt-4">
            <div className="card-header">
              Workflow Visualization
            </div>
            <div className="card-body">
              <ReactFlowProvider>
                <WorkflowChart workflow={workflow}/>
              </ReactFlowProvider>
            </div>
          </div>
          <h3 className="mt-4">Steps</h3>
          <table className={'table table-striped table-hover border mb-0'}>
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
                  {step.state && <StateBadge state={step.state} /> || "-"}
                </td>
                <td>
                  {step.job_id ? (
                    <Link to={`/jobs/${step.job_id}`} onClick={(e) => handleJobClick(step.job_id, e)}>
                      {step.job_id}
                    </Link>
                  ) : (
                    <em className="text-muted">
                      Waiting for dependencies to be met.
                    </em>
                  )}
                </td>
              </tr>
            ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

export function Workflows() {
  const {url} = useServerConfiguration();
  const {data: workflows, isLoading} = useWorkflows({url});

  if (isLoading) return <Loading />;

  return (
    <div className={'container-fluid'}>
      <h2 className={"mb-4"}>Workflows</h2>
      <table className={"table table-hover mb-0"}>
        <thead>
        <tr>
          <th className={"w-100"}>Name</th>
          <th className={"text-center text-nowrap"}>State</th>
          <th className={"text-center text-nowrap"}>Created</th>
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
              <StateBadge state={workflow.state} />
            </td>
            <td className={"text-center text-nowrap"}>
              <UpdatingTime date={workflow.created_at} />
            </td>
          </tr>
        ))}
        </tbody>
      </table>
    </div>
  );
}