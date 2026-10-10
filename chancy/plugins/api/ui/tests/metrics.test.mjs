import assert from 'node:assert/strict';
import test from 'node:test';
import { MetricDetailSchema } from '../src/services/schemas.ts';
import { metricTimeline, metricNumber, formatMetricTimestamp, metricStatus } from '../src/services/metrics.ts';

const start = '2026-10-08T12:00:00Z';
const end = '2026-10-08T12:15:00Z';
const metric = {
  type: 'counter', unit: 'count', aggregation: 'sum', sampled_at: '2026-10-08T12:11:00Z', summary: 3,
  data: [
    { timestamp: '2026-10-08T12:10:00Z', sampled_at: '2026-10-08T12:11:00Z', value: 3 },
    { timestamp: start, sampled_at: start, value: 0 },
    { timestamp: '2026-10-07T00:00:00Z', sampled_at: '2026-10-07T00:00:00Z', value: 999 },
  ],
};
const window = { start, end, generated_at: '2026-10-08T12:12:00Z', resolution: '5min', series: { count: metric } };

test('timeline orders and bounds samples, retaining zero and missing as distinct values', () => {
  const points = metricTimeline(window, metric);
  assert.deepEqual(points.map(point => point.value), [0, null, 3]);
  assert.deepEqual(points.map(point => point.timestamp), [0, 5, 10].map(minutes => Date.parse(start) + minutes * 60000));
  assert.equal(metricTimeline(window, undefined).every(point => point.value === null), true);
  const moved = { ...window, start: '2026-10-08T12:15:00Z', end: '2026-10-08T12:30:00Z' };
  assert.equal(metricTimeline(moved, metric).every(point => point.value === null), true);
});

test('histogram summary is taken from the weighted server summary, not the latest bucket', () => {
  assert.equal(metricNumber({ count: 4, sum: 32, min: 2, max: 10, avg: 8 }), 8);
  assert.equal(metricNumber(null), null);
  assert.equal(metricNumber(0), 0);
});

test('daily axes show dates and stale observations retain their sample time', () => {
  assert.equal(formatMetricTimestamp(Date.parse(start), '1day'), '2026-10-08');
  assert.notEqual(formatMetricTimestamp(Date.parse(end), '5min'), '00:00');
  assert.match(metricStatus({ ...window, generated_at: '2026-10-09T00:00:00Z' }, metric, false), /No recent observations/);
  assert.match(metricStatus(window, metric, true), /Could not refresh/);
  assert.match(metricStatus(window, undefined, false), /No observations/);
});

test('wire schema rejects mismatched metric types, reducers and missing sample metadata', () => {
  MetricDetailSchema.parse(window);
  for (const change of [{ type: 'histogram' }, { aggregation: 'last' }, { unit: undefined }, { sampled_at: undefined }]) {
    assert.equal(MetricDetailSchema.safeParse({ ...window, series: { count: { ...metric, ...change } } }).success, false);
  }
  assert.equal(MetricDetailSchema.safeParse({ ...window, series: { count: { ...metric, summary: Infinity } } }).success, false);
});
