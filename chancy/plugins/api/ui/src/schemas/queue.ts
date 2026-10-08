import { z } from 'zod';
import type { Queue } from '../services/chancy';

/**
 * Form input schema for Queue editing/creation.
 * Handles string inputs from forms and coerces to proper types.
 */
export const QueueFormSchema = z.object({
  name: z.string().trim().min(1, 'Name is required'),
  concurrency: z.union([
    z.string().transform(val => val.trim() === '' ? null : Number(val)),
    z.number(),
    z.null()
  ]).pipe(z.number().int().positive('Must be positive').nullable()).optional(),
  polling_interval: z.union([
    z.string().transform(val => Number(val)),
    z.number()
  ]).pipe(z.number().int().positive('Must be positive')),
  eager_polling: z.boolean().default(false),
  rate_limit: z.union([
    z.string().transform(val => val.trim() === '' ? null : Number(val)),
    z.number(),
    z.null()
  ]).pipe(z.number().int().positive('Must be positive').nullable()).optional(),
  rate_limit_window: z.union([
    z.string().transform(val => val.trim() === '' ? null : Number(val)),
    z.number(),
    z.null()
  ]).pipe(z.number().int().positive('Must be positive').nullable()).optional(),
  tags: z.array(z.string()).default(['.*']),
  executor_options: z.union([
    z.string().transform((val, ctx) => {
      if (val.trim() === '') return {};
      try { return JSON.parse(val) as unknown; }
      catch {
        ctx.addIssue({ code: 'custom', message: 'Enter valid JSON' });
        return z.NEVER;
      }
    }).pipe(z.record(z.string(), z.unknown())),
    z.record(z.string(), z.unknown())
  ]).default({}),
});

export type QueueFormInput = z.input<typeof QueueFormSchema>;
export type QueueFormOutput = z.output<typeof QueueFormSchema>;

/**
 * Default values for creating a new queue
 */
export const defaultQueueValues: QueueFormInput = {
  name: '',
  concurrency: '',
  polling_interval: '5',
  eager_polling: false,
  rate_limit: '',
  rate_limit_window: '',
  tags: ['.*'],
  executor_options: '{}',
};

/**
 * Converts a Queue API response to form input values
 */
export function queueToFormValues(queue: Queue): QueueFormInput {
  return {
    name: queue.name ?? '',
    concurrency: queue.concurrency != null ? String(queue.concurrency) : '',
    polling_interval: String(queue.polling_interval),
    eager_polling: queue.eager_polling ?? false,
    rate_limit: queue.rate_limit != null ? String(queue.rate_limit) : '',
    rate_limit_window: queue.rate_limit_window != null ? String(queue.rate_limit_window) : '',
    tags: [...(queue.tags ?? ['.*'])],
    executor_options: JSON.stringify(queue.executor_options || {}, null, 2),
  };
}
