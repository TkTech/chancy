import assert from 'node:assert/strict';
import { test } from 'node:test';
import { QueryClient } from '@tanstack/react-query';
import { ChancyApi } from '../src/services/chancy.ts';
import { queryKeys } from '../src/services/queryKeys.ts';
import { ApiError } from '../src/services/http.ts';
import { ZodError } from 'zod';

const timestamp = '2026-10-08T12:00:00+00:00';
const definition = {
  func: 'example.task', queue: 'default', kwargs: {}, priority: 0,
  max_attempts: 1, scheduled_at: timestamp, unique_key: null, meta: {},
  limits: [{ type_: 'time', value: 30 }, { type_: 'memory', value: 256 }],
};
const job = {
  ...definition, id: '00000000-0000-0000-0000-000000000003', state: 'pending',
  attempts: 0, taken_by: null, created_at: timestamp, started_at: null,
  completed_at: null, errors: [], claim_id: null,
  limits: [{ t: 'time', v: 30 }, { t: 'memory', v: 256 }],
};
const queue = {
  name: 'default', concurrency: null, tags: ['.*'], state: 'active',
  executor: 'chancy.executors.process.ProcessExecutor', executor_options: {},
  polling_interval: 5, rate_limit: null, rate_limit_window: null,
  resume_at: null, eager_polling: false,
};
const summary = {
  id: job.id, name: 'example', state: 'pending', created_at: timestamp,
  updated_at: timestamp, total_steps: 1, waiting_steps: 1, pending_steps: 0,
  running_steps: 0, succeeded_steps: 0, failed_steps: 0, retrying_steps: 0,
};
const workflow = {
  id: job.id, name: 'example', state: 'pending', created_at: timestamp,
  updated_at: timestamp,
  steps: { first: { step_id: 'first', state: null, job_id: null, dependencies: [], job: definition } },
};
const cron = { unique_key: 'cron', cron: '* * * * *', timezone: 'Etc/UTC', last_run: null, next_run: timestamp, job: definition };
const page = item => ({ items: [item], next_cursor: null, has_more: false });
const expectedLimits = [{ type: 'time', value: 30 }, { type: 'memory', value: 256 }];
const baseUrl = 'https://example.test/chancy';
const api = ChancyApi(baseUrl);
function respond(t, payload, status = 200) {
  return t.mock.method(globalThis, 'fetch', async () => Response.json(payload, { status }));
}

test('normalizes queued, cron and workflow limits, preserving nullable fields', async t => {
  const fetch = respond(t, job);
  const queued = await api.getJob(job.id);
  assert.deepEqual(queued.limits, expectedLimits);
  assert.equal(queued.started_at, null);
  assert.equal(queued.unique_key, null);
  fetch.mock.mockImplementation(async () => Response.json([cron]));
  const [scheduled] = await api.listCrons();
  assert.deepEqual(scheduled.job.limits, expectedLimits);
  assert.equal(scheduled.last_run, null);
  fetch.mock.mockImplementation(async () => Response.json(workflow));
  const result = await api.getWorkflow(job.id);
  assert.deepEqual(result.steps.first.job.limits, expectedLimits);
  assert.equal(result.steps.first.state, null);
  assert.equal(result.steps.first.job_id, null);
});

test('rejects malformed limits and missing required fields before caching', async t => {
  const fetch = respond(t, page(job));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  t.after(() => client.clear());
  for (const invalid of [
    { ...job, limits: [{ key: 'time', value: 30 }] },
    { ...job, limits: [{ t: 'time', v: '30' }] },
    { ...job, limits: [{ t: 'unknown', v: 30 }] },
    { ...job, limits: undefined },
    { ...job, errors: undefined },
    { ...job, taken_by: undefined },
  ]) {
    fetch.mock.mockImplementation(async () => Response.json(page(invalid)));
    const key = queryKeys.jobPage(baseUrl, {});
    await assert.rejects(client.fetchQuery({ queryKey: key, queryFn: () => api.listJobs() }), ZodError);
    assert.equal(client.getQueryData(key), undefined);
  }
});

test('page requests preserve cursor, filters and envelope', async t => {
  const response = { items: [job], next_cursor: job.id, has_more: true };
  const fetch = respond(t, response);
  const filters = [['queue', '=', 'special & queue']];
  const result = await api.listJobs({ before: job.id, filters, limit: 2 });
  const url = new URL(fetch.mock.calls[0].arguments[0]);
  assert.equal(url.pathname, '/chancy/api/v1/jobs');
  assert.equal(url.searchParams.get('pagination'), 'true');
  assert.equal(url.searchParams.get('before'), job.id);
  assert.deepEqual(JSON.parse(url.searchParams.get('filters')), filters);
  assert.equal(result.next_cursor, job.id);
  assert.equal(result.has_more, true);
  fetch.mock.mockImplementation(async () => Response.json(page(summary)));
  assert.deepEqual((await api.listWorkflows({ before: job.id })).items, [summary]);
  fetch.mock.mockImplementation(async () => Response.json([job]));
  await assert.rejects(api.listJobs(), ZodError);
});

test('every read validates responses and forwards its AbortSignal', async t => {
  const fetch = respond(t, null);
  const controller = new AbortController();
  const signal = controller.signal;
  const cases = [
    [() => api.listJobs({}, signal), page(job)],
    [() => api.getJob(job.id, signal), job],
    [() => api.listFunctions(signal), ['example.task']],
    [() => api.listQueues(signal), [queue]],
    [() => api.listWorkers(signal), [{ worker_id: 'worker', tags: [], queues: [], last_seen: timestamp, expires_at: timestamp, is_leader: false }]],
    [() => api.listCrons(signal), [cron]],
    [() => api.listWorkflows({}, signal), page(summary)],
    [() => api.getWorkflow(job.id, signal), workflow],
    [() => api.getConfiguration(signal), { plugins: [] }],
    [() => api.getSystem(signal), { chancy_version: '0.26.0', database: { version: null, prefix: 'chancy_' } }],
    [() => api.listPlugins(signal), [{ identifier: 'test', tables: [], migrate_key: null, migrate_package: null, api_plugin: null, dependencies: [], scope: 'worker' }]],
    [() => api.getMetricsOverview(signal), { categories: { queue: ['execution_time'] }, count: 1 }],
    [() => api.getMetricDetail('queue:execution_time', { resolution: '5min', limit: 60 }, signal), {
      duration: { type: 'gauge', data: [{ timestamp, value: 0.5 }] },
      states: { type: 'histogram', data: [{ timestamp, value: { pending: 3 } }] },
    }],
  ];
  for (const [read, payload] of cases) {
    fetch.mock.mockImplementation(async () => Response.json(payload));
    await read();
    assert.equal(fetch.mock.calls.at(-1).arguments[1].signal, signal);
    fetch.mock.mockImplementation(async () => Response.json(null));
    await assert.rejects(read(), ZodError);
  }
});

test('HTTP and cancellation errors retain their identity', async t => {
  const fetch = respond(t, { title: 'Unauthenticated' }, 401);
  await assert.rejects(api.listQueues(), error => error instanceof ApiError && error.status === 401);
  const error = new DOMException('Aborted', 'AbortError');
  fetch.mock.mockImplementation(async () => { throw error; });
  await assert.rejects(api.listQueues(), actual => actual === error);
});

test('mutations validate results and preserve per-item outcomes', async t => {
  const result = { ok: false, results: [
    { id: '1', status: 'completed' },
    { id: '2', status: 'skipped', message: 'Running jobs cannot be retried' },
    { id: '3', status: 'failed', message: 'Action failed' },
  ] };
  const fetch = respond(t, result);
  assert.deepEqual(await api.batchJobs(['1', '2', '3'], 'retry'), result);
  assert.deepEqual(JSON.parse(fetch.mock.calls[0].arguments[1].body), { action: 'retry', ids: ['1', '2', '3'] });
  fetch.mock.mockImplementation(async () => Response.json({ ok: true }));
  await assert.rejects(api.batchJobs(['1'], 'purge'), ZodError);
  for (const mutate of [() => api.createQueue({ name: 'default' }), () => api.updateQueue('default', { tags: [] })]) {
    fetch.mock.mockImplementation(async () => Response.json({ ...queue, tags: [] }));
    assert.deepEqual((await mutate()).tags, []);
    fetch.mock.mockImplementation(async () => Response.json({ ...queue, tags: undefined }));
    await assert.rejects(mutate(), ZodError);
  }
});

test('invalidation covers matching server pages without affecting other servers', async () => {
  const client = new QueryClient();
  try {
    const first = queryKeys.jobPage(baseUrl, {});
    const older = queryKeys.jobPage(baseUrl, { before: job.id });
    const other = queryKeys.jobPage('https://other.test', {});
    const detail = queryKeys.job(baseUrl, job.id);
    for (const key of [first, older, other, detail]) client.setQueryData(key, 'cached');
    await client.invalidateQueries({ queryKey: queryKeys.jobs(baseUrl) });
    for (const key of [first, older]) assert.equal(client.getQueryState(key).isInvalidated, true);
    for (const key of [other, detail]) assert.equal(client.getQueryState(key).isInvalidated, false);
    await client.invalidateQueries({ queryKey: queryKeys.jobDetails(baseUrl) });
    assert.equal(client.getQueryState(detail).isInvalidated, true);
  } finally { client.clear(); }
});
