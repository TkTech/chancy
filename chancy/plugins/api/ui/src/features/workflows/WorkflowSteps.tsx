import { useId, useState } from 'react';
import { ReactFlowProvider } from '@xyflow/react';
import type { Workflow } from '../../services/schemas';
import { useDrawer } from '../../components/common/DrawerContext';
import { DetailCard } from '../../components/common/DetailCard';
import { StatusBadge } from '../../components/common/StatusBadge';
import { DataTable } from '../../components/common/DataTable';
import WorkflowChart from '../../pages/WorkflowChart';
import { extractFunctionName } from '../../utils';
import { StepInspector } from './StepInspector';
import { stepStatus } from './diagnostics';

export function WorkflowSteps({ workflow }: { workflow: Workflow }) {
  const drawer = useDrawer();
  const searchId = useId();
  const [search, setSearch] = useState('');
  const [failedOnly, setFailedOnly] = useState(false);
  const steps = Object.entries(workflow.steps);
  const failedCount = steps.filter(([, step]) => step.state === 'failed').length;
  const query = search.trim().toLowerCase();
  const matches = steps.filter(([id, step]) => (
    (!failedOnly || step.state === 'failed') &&
    (!query || [id, step.job.func, step.job.queue].some(value => value.toLowerCase().includes(query)))
  ));
  const inspect = (stepId: string) => drawer.open(
    <StepInspector key={`${workflow.id}/${stepId}`} workflowId={workflow.id} initialStepId={stepId} />,
    { title: 'Workflow Step' },
  );

  return (
    <>
      <div className="d-flex flex-wrap align-items-end gap-2 mb-3">
        <div className="flex-grow-1">
          <label htmlFor={searchId} className="form-label">Find steps</label>
          <input id={searchId} type="search" className="form-control" placeholder="Step ID, function, or queue" value={search} onChange={event => setSearch(event.target.value)} />
        </div>
        <button className={`btn ${failedOnly ? 'btn-danger' : 'btn-outline-danger'}`} aria-pressed={failedOnly} onClick={() => setFailedOnly(value => !value)}>Failed steps ({failedCount})</button>
      </div>
      <p className="small text-muted" role="status">{matches.length} of {steps.length} steps</p>
      <DetailCard title="Workflow Visualization">
        <ReactFlowProvider>
          <WorkflowChart workflow={workflow} onStepClick={inspect} matchingStepIds={query || failedOnly ? matches.map(([id]) => id) : undefined} />
        </ReactFlowProvider>
      </DetailCard>
      <h3 className="mt-4">Steps</h3>
      <DataTable>
        <thead><tr><th>Step ID</th><th>Function</th><th>Queue</th><th>Dependencies</th><th>State</th><th>Job ID</th></tr></thead>
        <tbody>
          {matches.length === 0 && <tr><td colSpan={6}>No matching steps.</td></tr>}
          {matches.map(([id, step]) => (
            <tr key={id}>
              <td><button className="btn btn-link p-0 text-break text-start" onClick={() => inspect(id)}>{id}</button></td>
              <td><code title={step.job.func}>{extractFunctionName(step.job.func)}</code></td>
              <td>{step.job.queue}</td>
              <td>
                {step.dependencies.length > 0 ? <div className="d-flex flex-wrap gap-2">
                  {step.dependencies.map(dep => <button key={dep} className="btn btn-link p-0 text-break text-start" disabled={!workflow.steps[dep]} onClick={() => inspect(dep)}>{dep}</button>)}
                </div> : 'None'}
              </td>
              <td><StatusBadge status={stepStatus(step)} /></td>
              <td className="text-break">{step.job_id ?? 'Not queued'}</td>
            </tr>
          ))}
        </tbody>
      </DataTable>
    </>
  );
}
