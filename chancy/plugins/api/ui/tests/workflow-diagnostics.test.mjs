import assert from 'node:assert/strict';
import { test } from 'node:test';
import { dependencyDiagnostics, stepStatus } from '../src/features/workflows/diagnostics.ts';

const step = (state, dependencies = [], job_id = 'job') => ({ state, dependencies, job_id });

test('distinguishes dependency waiting, queued jobs, and deleted jobs', () => {
  assert.equal(stepStatus(step(null, [], null)), 'waiting');
  assert.equal(stepStatus(step('pending')), 'pending');
  assert.equal(stepStatus(step(null)), 'missing job');
  assert.equal(stepStatus(undefined), 'missing');
});

test('finds direct blockers and deduplicates failed ancestors across branches', () => {
  const steps = {
    failed: step('failed'),
    left: step(null, ['failed'], null),
    right: step(null, ['failed'], null),
    done: step('succeeded'),
    running: step('running'),
    retrying: step('retrying'),
    target: step(null, ['left', 'right', 'done', 'running', 'retrying', 'missing'], null),
  };
  assert.deepEqual(dependencyDiagnostics(steps, 'target'), {
    unresolved: ['left', 'right', 'running', 'retrying', 'missing'],
    failedAncestors: ['failed'],
  });
  steps.failed.state = 'succeeded';
  assert.deepEqual(dependencyDiagnostics(steps, 'left'), { unresolved: [], failedAncestors: [] });
});

test('handles missing steps, cycles, and deep workflows without recursion', () => {
  assert.deepEqual(dependencyDiagnostics({}, 'missing'), { unresolved: [], failedAncestors: [] });
  const steps = { root: step('failed', ['target']), target: step(null, ['root'], null) };
  assert.deepEqual(dependencyDiagnostics(steps, 'target'), { unresolved: ['root'], failedAncestors: ['root'] });
  steps.root.dependencies = [];
  for (let i = 0; i < 10000; i++) steps[`step-${i}`] = step(null, [i ? `step-${i - 1}` : 'root'], null);
  assert.deepEqual(dependencyDiagnostics(steps, 'step-9999'), { unresolved: ['step-9998'], failedAncestors: ['root'] });
});
