import { z } from 'zod';

const JsonObject = z.record(z.string(), z.unknown());
const LimitType = z.enum(['time', 'memory']);
const StoredLimit = z.object({ t: LimitType, v: z.number() })
  .transform(({ t, v }) => ({ type: t, value: v }));
const DefinitionLimit = z.object({ type_: LimitType, value: z.number() })
  .transform(({ type_, value }) => ({ type: type_, value }));

export const JobDefinitionSchema = z.object({
  func: z.string(),
  queue: z.string(),
  kwargs: JsonObject.nullable(),
  priority: z.number(),
  max_attempts: z.number(),
  scheduled_at: z.string(),
  limits: z.array(DefinitionLimit),
  unique_key: z.string().nullable(),
  meta: JsonObject,
});
export const JobSchema = JobDefinitionSchema.extend({
  id: z.string(),
  kwargs: JsonObject,
  limits: z.array(StoredLimit),
  state: z.string(),
  attempts: z.number(),
  taken_by: z.string().nullable(),
  claim_id: z.string().nullish(),
  created_at: z.string(),
  started_at: z.string().nullable(),
  completed_at: z.string().nullable(),
  errors: z.array(z.object({ traceback: z.string(), attempt: z.number() })),
});
export const QueueSchema = z.object({
  name: z.string(),
  concurrency: z.number().nullable(),
  tags: z.array(z.string()),
  state: z.string(),
  executor: z.string(),
  executor_options: JsonObject,
  polling_interval: z.number(),
  rate_limit: z.number().nullable(),
  rate_limit_window: z.number().nullable(),
  resume_at: z.string().nullable(),
  eager_polling: z.boolean(),
});
export const WorkerSchema = z.object({
  worker_id: z.string(),
  tags: z.array(z.string()),
  queues: z.array(z.string()),
  last_seen: z.string(),
  expires_at: z.string(),
  is_leader: z.boolean(),
});
export const CronSchema = z.object({
  unique_key: z.string(),
  cron: z.string(),
  timezone: z.string(),
  last_run: z.string().nullable(),
  next_run: z.string(),
  job: JobDefinitionSchema,
});
export const StepSchema = z.object({
  step_id: z.string(),
  state: z.string().nullable(),
  job_id: z.string().nullable(),
  dependencies: z.array(z.string()),
  job: JobDefinitionSchema,
});
const WorkflowBaseSchema = z.object({
  id: z.string(),
  name: z.string(),
  state: z.string(),
  created_at: z.string().nullable(),
  updated_at: z.string().nullable(),
});
export const WorkflowSchema = WorkflowBaseSchema.extend({
  steps: z.record(z.string(), StepSchema),
});
export const WorkflowSummarySchema = WorkflowBaseSchema.extend({
  total_steps: z.number(),
  waiting_steps: z.number(),
  pending_steps: z.number(),
  running_steps: z.number(),
  succeeded_steps: z.number(),
  failed_steps: z.number(),
  retrying_steps: z.number(),
});
const page = <T extends z.ZodType>(item: T) => z.object({
  items: z.array(item), next_cursor: z.string().nullable(), has_more: z.boolean(),
});
export const JobPageSchema = page(JobSchema);
export const WorkflowPageSchema = page(WorkflowSummarySchema);
export const BatchJobResultSchema = z.object({
  id: z.string(), status: z.enum(['completed', 'skipped', 'failed']), message: z.string().optional(),
});
export const BatchJobResponseSchema = z.object({ ok: z.boolean(), results: z.array(BatchJobResultSchema) });
export const ActionResponseSchema = z.object({ ok: z.boolean() });
export const ConfigurationSchema = z.object({ plugins: z.array(z.string()) });
export const SystemSchema = z.object({
  chancy_version: z.string(), database: z.object({ version: z.string().nullable(), prefix: z.string() }),
});
export const PluginSchema = z.object({
  identifier: z.string(), tables: z.array(z.string()), migrate_key: z.string().nullable(),
  migrate_package: z.string().nullable(), api_plugin: z.string().nullable(),
  dependencies: z.array(z.string()), scope: z.string(),
});
export const MetricsOverviewSchema = z.object({
  categories: z.record(z.string(), z.array(z.string())), count: z.number(),
});
const HistogramSummarySchema = z.object({
  count: z.number().int().positive(), sum: z.number(), min: z.number(), max: z.number(), avg: z.number(),
});
const MetricValueSchema = z.union([z.number(), HistogramSummarySchema]);
export const MetricPointSchema = z.object({
  timestamp: z.iso.datetime({ offset: true }), sampled_at: z.iso.datetime({ offset: true }), value: MetricValueSchema,
});
export const MetricDataSchema = z.object({
  data: z.array(MetricPointSchema), type: z.enum(['counter', 'gauge', 'histogram']),
  unit: z.string(), aggregation: z.enum(['sum', 'last', 'summary']),
  sampled_at: z.iso.datetime({ offset: true }).nullable(), summary: MetricValueSchema.nullable(),
}).superRefine((metric, ctx) => {
  const histogram = metric.type === 'histogram';
  if (metric.aggregation !== ({counter: 'sum', gauge: 'last', histogram: 'summary'}[metric.type]) ||
      metric.data.some(point => (typeof point.value === 'object') !== histogram) ||
      (metric.summary !== null && (typeof metric.summary === 'object') !== histogram)) {
    ctx.addIssue({ code: 'custom', message: 'Metric values and aggregation must match the metric type' });
  }
});
export const MetricDetailSchema = z.object({
  start: z.iso.datetime({ offset: true }), end: z.iso.datetime({ offset: true }),
  generated_at: z.iso.datetime({ offset: true }), resolution: z.enum(['1min', '5min', '1hour', '1day']),
  series: z.record(z.string(), MetricDataSchema),
});
export type MetricDetail = z.infer<typeof MetricDetailSchema>;

export type JobDefinition = z.infer<typeof JobDefinitionSchema>;
export type Job = z.infer<typeof JobSchema>;
export type JobPage = z.infer<typeof JobPageSchema>;
export type Queue = z.infer<typeof QueueSchema>;
export type Worker = z.infer<typeof WorkerSchema>;
export type Cron = z.infer<typeof CronSchema>;
export type Step = z.infer<typeof StepSchema>;
export type Workflow = z.infer<typeof WorkflowSchema>;
export type WorkflowSummary = z.infer<typeof WorkflowSummarySchema>;
export type WorkflowPage = z.infer<typeof WorkflowPageSchema>;
export type BatchJobResult = z.infer<typeof BatchJobResultSchema>;
export type BatchJobResponse = z.infer<typeof BatchJobResponseSchema>;
export type MetricPoint = z.infer<typeof MetricPointSchema>;
export type MetricData = z.infer<typeof MetricDataSchema>;
export type MetricType = MetricData['type'];
export type MetricsOverview = z.infer<typeof MetricsOverviewSchema>;
