import type { Step, Workflow } from '../../services/schemas.ts';

export function stepStatus(step: Step | undefined): string {
  if (!step) return 'missing';
  if (!step.job_id) return 'waiting';
  return step.state ?? 'missing job';
}

export function dependencyDiagnostics(steps: Workflow['steps'], stepId: string) {
  const dependencies = steps[stepId]?.dependencies ?? [];
  const unresolved = dependencies.filter(id => steps[id]?.state !== 'succeeded');
  const failedAncestors: string[] = [];
  const visited = new Set([stepId]);
  const pending = [...dependencies];
  while (pending.length > 0) {
    const id = pending.pop()!;
    if (visited.has(id)) continue;
    visited.add(id);
    const step = steps[id];
    if (!step) continue;
    if (step.state === 'failed') failedAncestors.push(id);
    pending.push(...step.dependencies);
  }
  return { unresolved, failedAncestors: failedAncestors.sort() };
}
