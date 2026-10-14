import { z } from 'zod';
import { request, type RequestOptions } from './http.ts';
import * as schemas from './schemas.ts';
import type { Queue } from './schemas.ts';
export type { Job, Queue, BatchJobResult, BatchJobResponse } from './schemas.ts';

export type FilterTriple = [string, string, string];
export type BatchJobAction = 'retry' | 'purge' | 'cancel';
export interface PageParams { filters?: FilterTriple[]; limit?: number; before?: string; }
export interface JobParams extends PageParams { state?: string; queue?: string; func?: string; }
export interface MetricParams { resolution: string; range?: number; limit?: number; start?: string; end?: string; worker_id?: string; }

function queryString(params: object): string {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) qs.set(key, Array.isArray(value) ? JSON.stringify(value) : String(value));
  }
  return qs.toString();
}

export const ChancyApi = (baseUrl: string) => {
  const read = async <T extends z.ZodType>(path: string, schema: T, options: RequestOptions = {}): Promise<z.output<T>> =>
    schema.parse(await request(baseUrl, `/api/v1${path}`, options));
  const action = (path: string, options: RequestOptions) => read(path, schemas.ActionResponseSchema, options);
  return {
    listJobs: (params: JobParams = {}, signal?: AbortSignal) =>
      read(`/jobs?${queryString({ ...params, pagination: true })}`, schemas.JobPageSchema, { signal }),
    getJob: (id: string, signal?: AbortSignal) => read(`/jobs/${encodeURIComponent(id)}`, schemas.JobSchema, { signal }),
    listFunctions: (signal?: AbortSignal) => read('/jobs/functions', z.array(z.string()), { signal }),
    retryJob: (id: string) => action(`/jobs/${encodeURIComponent(id)}/retry`, { method: 'POST' }),
    cancelJob: (id: string) => action(`/jobs/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
    purgeJob: (id: string) => action(`/jobs/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    batchJobs: (ids: string[], action: BatchJobAction) => read('/jobs', schemas.BatchJobResponseSchema, { method: 'POST', body: { action, ids } }),
    listQueues: (signal?: AbortSignal) => read('/queues', z.array(schemas.QueueSchema), { signal }),
    createQueue: (payload: Partial<Queue> & { name: string }) => read('/queues', schemas.QueueSchema, { method: 'POST', body: payload }),
    updateQueue: (name: string, payload: Partial<Queue>) => read(`/queues/${encodeURIComponent(name)}`, schemas.QueueSchema, { method: 'PATCH', body: payload }),
    pauseQueue: (name: string, resume_at?: string) => action(`/queues/${encodeURIComponent(name)}/pause`, { method: 'POST', body: resume_at ? { resume_at } : {} }),
    resumeQueue: (name: string) => action(`/queues/${encodeURIComponent(name)}/resume`, { method: 'POST' }),
    deleteQueue: (name: string, purge_jobs: boolean) => action(`/queues/${encodeURIComponent(name)}?purge_jobs=${purge_jobs}`, { method: 'DELETE' }),
    listWorkers: (signal?: AbortSignal) => read('/workers', z.array(schemas.WorkerSchema), { signal }),
    listCrons: (signal?: AbortSignal) => read('/crons', z.array(schemas.CronSchema), { signal }),
    listWorkflows: (params: PageParams = {}, signal?: AbortSignal) => read(`/workflows?${queryString({ ...params, pagination: true })}`, schemas.WorkflowPageSchema, { signal }),
    getWorkflow: (id: string, signal?: AbortSignal) => read(`/workflows/${encodeURIComponent(id)}`, schemas.WorkflowSchema, { signal }),
    getConfiguration: (signal?: AbortSignal) => read('/configuration', schemas.ConfigurationSchema, { signal }),
    getSystem: (signal?: AbortSignal) => read('/system', schemas.SystemSchema, { signal }),
    listPlugins: (signal?: AbortSignal) => read('/plugins', z.array(schemas.PluginSchema), { signal }),
    getMetricsOverview: (signal?: AbortSignal) => read('/metrics', schemas.MetricsOverviewSchema, { signal }),
    getMetricDetail: (key: string, params: MetricParams, signal?: AbortSignal) => read(`/metrics/${encodeURIComponent(key)}?${queryString(params)}`, schemas.MetricDetailSchema, { signal }),
  };
};
