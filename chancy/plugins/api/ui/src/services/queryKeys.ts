import type { JobParams, PageParams, MetricParams } from './chancy';

export const queryKeys = {
  jobs: (url: string | null) => ['jobs', url] as const,
  jobPage: (url: string | null, params: JobParams) => [...queryKeys.jobs(url), params] as const,
  jobDetails: (url: string | null) => ['job', url] as const,
  job: (url: string | null, id?: string) => [...queryKeys.jobDetails(url), id] as const,
  queues: (url: string | null) => ['queues', url] as const,
  workers: (url: string | null) => ['workers', url] as const,
  crons: (url: string | null) => ['crons', url] as const,
  functions: (url: string | null) => ['functions', url] as const,
  workflows: (url: string | null, params: PageParams) => ['workflows', url, params] as const,
  workflow: (url: string | null, id?: string) => ['workflow', url, id] as const,
  configuration: (url: string | null) => ['configuration', url] as const,
  system: (url: string | null) => ['system', url] as const,
  plugins: (url: string | null) => ['plugins', url] as const,
  metricsOverview: (url: string | null) => ['metrics-overview', url] as const,
  metricDetail: (url: string | null, key: string, params: MetricParams) => ['metric-detail', url, key, params] as const,
};
