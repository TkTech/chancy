import assert from 'node:assert/strict';
import { test } from 'node:test';
import { QueueFormSchema, defaultQueueValues, queueToFormValues } from '../src/schemas/queue.ts';

const values = { ...defaultQueueValues, name: 'example' };

test('create form preserves defaults and deliberate empty tags', () => {
  const parsed = QueueFormSchema.parse(values);
  assert.equal(parsed.concurrency, null);
  assert.equal(parsed.polling_interval, 5);
  assert.deepEqual(parsed.executor_options, {});
  assert.deepEqual(parsed.tags, ['.*']);
  assert.deepEqual(QueueFormSchema.parse({ ...values, tags: [] }).tags, []);
});

test('invalid JSON becomes a field validation error; only objects are accepted', () => {
  for (const executor_options of ['{', 'null', '[]', '1', '"text"']) {
    const result = QueueFormSchema.safeParse({ ...values, executor_options });
    assert.equal(result.success, false);
    assert.deepEqual(result.error.issues[0].path, ['executor_options']);
  }
  assert.deepEqual(QueueFormSchema.parse({ ...values, executor_options: '{"threads": 2}' }).executor_options, { threads: 2 });
});

test('numeric settings reject invalid values instead of truncating them', () => {
  for (const field of ['concurrency', 'polling_interval', 'rate_limit', 'rate_limit_window']) {
    for (const value of ['-1', '0', '1.5', '2workers']) {
      const result = QueueFormSchema.safeParse({ ...values, [field]: value });
      assert.equal(result.success, false, `${field}: ${value}`);
      assert.deepEqual(result.error.issues[0].path, [field]);
    }
    assert.equal(QueueFormSchema.parse({ ...values, [field]: '2' })[field], 2);
  }
});

test('edit values round-trip the editable queue fields without losing nulls or empty tags', () => {
  const editable = {
    name: 'example', concurrency: null, polling_interval: 10, eager_polling: true,
    rate_limit: 3, rate_limit_window: 60, tags: [], executor_options: { threads: 2 },
  };
  const queue = { ...editable, state: 'paused', executor: 'custom.Executor', resume_at: null };
  assert.deepEqual(QueueFormSchema.parse(queueToFormValues(queue)), editable);
});
