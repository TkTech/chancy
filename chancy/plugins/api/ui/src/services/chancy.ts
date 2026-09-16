import { z } from 'zod';
import { request, parseWith } from './http';

// Shared schemas
const JobSchema = z.object({
  id: z.string(),
  queue: z.string(),
  func: z.string(),
  kwargs: z.record(z.any()),
  limits: z.array(z.object({ key: z.string().optional(), value: z.number().optional() }).passthrough()).optional().default([]),
  meta: z.record(z.any()).optional().default({}),
  state: z.string(),
  priority: z.number(),
  attempts: z.number(),
  max_attempts: z.number(),
  taken_by: z.string().nullable().optional(),
  created_at: z.string().optional(),
  started_at: z.string().nullable().optional(),
  completed_at: z.string().nullable().optional(),
  scheduled_at: z.string().nullable().optional(),
  unique_key: z.string().nullable().optional(),
  errors: z.array(z.object({
    traceback: z.string(),
    attempt: z.number(),
  })).optional().default([]),
}).passthrough();

export type Job = z.infer<typeof JobSchema>;

const QueueSchema = z.object({
  name: z.string(),
  concurrency: z.number().nullable().optional(),
  tags: z.array(z.string()),
  state: z.string(),
  executor: z.string(),
  executor_options: z.record(z.any()).optional().default({}),
  polling_interval: z.number(),
  rate_limit: z.number().nullable().optional(),
  rate_limit_window: z.number().nullable().optional(),
  resume_at: z.string().nullable().optional(),
  eager_polling: z.boolean().optional().default(false),
}).passthrough();

export type Queue = z.infer<typeof QueueSchema>;

type FilterTriple = [string, string, string];

export const ChancyApi = (baseUrl: string) => ({
  // Jobs
  listJobs: async (params: { state?: string; queue?: string; func?: string; filters?: FilterTriple[]; limit?: number; before?: string } = {}) => {
    const qs = new URLSearchParams();
    if (params.state) qs.set('state', params.state);
    if (params.queue) qs.set('queue', params.queue);
    if (params.func) qs.set('func', params.func);
    if (params.filters) qs.set('filters', JSON.stringify(params.filters));
    if (params.limit) qs.set('limit', String(params.limit));
    if (params.before) qs.set('before', params.before);
    const data = await request<unknown[]>(baseUrl, `/api/v1/jobs?${qs.toString()}`);
    return data.map(d => parseWith(JobSchema, d));
  },
  listFunctions: async () => {
    return await request<string[]>(baseUrl, `/api/v1/jobs/functions`);
  },
  getJob: async (id: string) => {
    const data = await request<unknown>(baseUrl, `/api/v1/jobs/${id}`);
    return parseWith(JobSchema, data);
  },
  retryJob: async (id: string) => request(baseUrl, `/api/v1/jobs/${id}/retry`, { method: 'POST' }),
  cancelJob: async (id: string) => request(baseUrl, `/api/v1/jobs/${id}/cancel`, { method: 'POST' }),
  purgeJob: async (id: string) => request(baseUrl, `/api/v1/jobs/${id}`, { method: 'DELETE' }),
  batchJobs: async (ids: string[], action: 'retry' | 'purge') => request(baseUrl, `/api/v1/jobs`, { method: 'POST', body: { action, ids } }),

  // Queues
  listQueues: async () => {
    const data = await request<unknown[]>(baseUrl, `/api/v1/queues`);
    return data.map(d => parseWith(QueueSchema, d));
  },
  createQueue: async (payload: Partial<Queue> & { name: string }) => {
    const data = await request<unknown>(baseUrl, `/api/v1/queues`, { method: 'POST', body: payload });
    return parseWith(QueueSchema, data);
  },
  updateQueue: async (name: string, payload: Partial<Queue>) => {
    const data = await request<unknown>(baseUrl, `/api/v1/queues/${encodeURIComponent(name)}`, { method: 'PATCH', body: payload });
    return parseWith(QueueSchema, data);
  },
  pauseQueue: async (name: string, resume_at?: string) => request(baseUrl, `/api/v1/queues/${encodeURIComponent(name)}/pause`, { method: 'POST', body: resume_at ? { resume_at } : {} }),
  resumeQueue: async (name: string) => request(baseUrl, `/api/v1/queues/${encodeURIComponent(name)}/resume`, { method: 'POST' }),
  deleteQueue: async (name: string, purge_jobs: boolean) => request(baseUrl, `/api/v1/queues/${encodeURIComponent(name)}?purge_jobs=${purge_jobs ? 'true' : 'false'}`, { method: 'DELETE' }),
});

export type ChancyApiType = ReturnType<typeof ChancyApi>;

