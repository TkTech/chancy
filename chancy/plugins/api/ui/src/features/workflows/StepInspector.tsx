import { useState } from 'react';
import { useWorkflow } from '../../hooks/useWorkflows';
import { useServerConfiguration } from '../../hooks/useServerConfiguration';
import { DetailCard } from '../../components/common/DetailCard';
import { StatusBadge } from '../../components/common/StatusBadge';
import { PackedJobDetails } from '../../components/PackedJobDetails';
import { Loading } from '../../components/Loading';
import { JobDetailsView } from '../jobs/JobDetailsView';
import { dependencyDiagnostics, stepStatus } from './diagnostics';

export function StepInspector({ workflowId, initialStepId }: { workflowId: string; initialStepId: string }) {
  const { url } = useServerConfiguration();
  const { data: workflow, error, isLoading } = useWorkflow({ url, workflow_id: workflowId, options: { refetchInterval: 5000 } });
  const [stepId, setStepId] = useState(initialStepId);
  if (isLoading) return <Loading />;
  if (!workflow) return <div className="alert alert-danger" role="alert">{error?.message || 'Workflow not found.'}</div>;
  const step = workflow.steps[stepId];
  if (!step) return <div className="alert alert-warning">Step {stepId} no longer exists.</div>;
  const { unresolved, failedAncestors } = dependencyDiagnostics(workflow.steps, stepId);

  const stepLink = (id: string) => (
    <li key={id} className="d-flex align-items-center gap-2 mb-1">
      <button className="btn btn-link p-0 text-break text-start" disabled={!workflow.steps[id]} onClick={() => setStepId(id)}>{id}</button>
      <StatusBadge status={stepStatus(workflow.steps[id])} />
    </li>
  );

  return (
    <>
      {error && <div className="alert alert-warning" role="alert">Could not refresh workflow: {error.message}</div>}
      {stepId !== initialStepId && <button className="btn btn-sm btn-outline-secondary mb-3" onClick={() => setStepId(initialStepId)}>Back to {initialStepId}</button>}
      <DetailCard title={`Step: ${stepId}`}>
        <StatusBadge status={stepStatus(step)} />
        {workflow.state === 'failed' && (
          <p className="mt-2 mb-0">This workflow has failed and will not schedule further steps. Jobs already queued or running can still finish. Retrying a job alone does not resume the workflow.</p>
        )}
        {!step.job_id && (
          <p className="mt-2 mb-0">
            {unresolved.length > 0
              ? 'This step has not been queued because its dependencies have not all succeeded.'
              : workflow.state === 'pending' || workflow.state === 'running'
                ? 'Dependencies are satisfied. This step is waiting for the workflow scheduler to queue it.'
                : 'This step has not been queued. The workflow is no longer scheduling steps.'}
          </p>
        )}
        {step.job_id && step.state === null && <p className="mt-2 mb-0">The associated job is missing. Its outcome cannot be determined.</p>}
        {step.state === 'pending' && step.job_id && <p className="mt-2 mb-0">This step is queued and waiting for a worker.</p>}
      </DetailCard>
      <DetailCard title="Unresolved Dependencies">
        {unresolved.length > 0 ? <ul className="list-unstyled mb-0">{unresolved.map(stepLink)}</ul> : 'None'}
      </DetailCard>
      {failedAncestors.length > 0 && (
        <DetailCard title="Failed Ancestors">
          <ul className="list-unstyled mb-0">{failedAncestors.map(stepLink)}</ul>
        </DetailCard>
      )}
      {step.job_id && step.state !== null ? <JobDetailsView key={step.job_id} job_id={step.job_id} /> : (
        <DetailCard title="Job Definition" flush><PackedJobDetails job={step.job} /></DetailCard>
      )}
    </>
  );
}
