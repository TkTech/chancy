import { z } from 'zod';

/**
 * Form input schema for Queue editing/creation.
 * Handles string inputs from forms and coerces to proper types.
 */
export const QueueFormSchema = z.object({
  name: z.string().min(1, 'Name is required'),
  concurrency: z.union([
    z.string().transform(val => val.trim() === '' ? null : parseInt(val, 10)),
    z.number(),
    z.null()
  ]).optional(),
  polling_interval: z.union([
    z.string().transform(val => parseInt(val, 10)),
    z.number()
  ]).pipe(z.number().positive('Must be positive')),
  eager_polling: z.boolean().default(false),
  rate_limit: z.union([
    z.string().transform(val => val.trim() === '' ? null : parseInt(val, 10)),
    z.number(),
    z.null()
  ]).optional(),
  rate_limit_window: z.union([
    z.string().transform(val => val.trim() === '' ? null : parseInt(val, 10)),
    z.number(),
    z.null()
  ]).optional(),
  tags: z.array(z.string()).default([]),
  executor_options: z.union([
    z.string().transform(val => {
      if (val.trim() === '') return {};
      return JSON.parse(val);
    }),
    z.record(z.any())
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
  tags: [],
  executor_options: '{}',
};

/**
 * Converts a Queue API response to form input values
 */
export function queueToFormValues(queue: any): QueueFormInput {
  return {
    name: queue.name,
    concurrency: queue.concurrency != null ? String(queue.concurrency) : '',
    polling_interval: String(queue.polling_interval),
    eager_polling: queue.eager_polling ?? false,
    rate_limit: queue.rate_limit != null ? String(queue.rate_limit) : '',
    rate_limit_window: queue.rate_limit_window != null ? String(queue.rate_limit_window) : '',
    tags: [...queue.tags],
    executor_options: JSON.stringify(queue.executor_options || {}, null, 2),
  };
}
