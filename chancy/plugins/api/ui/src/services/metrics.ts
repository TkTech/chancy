import type { MetricData, MetricDetail, MetricPoint } from './schemas.ts';

export const RESOLUTION_SECONDS: Record<string, number> = { '1min': 60, '5min': 300, '1hour': 3600, '1day': 86400 };
export const RETENTION_SECONDS: Record<string, number> = { '1min': 3600, '5min': 86400, '1hour': 604800, '1day': 7776000 };
export const defaultMetricRange = (resolution: string) => Math.min(86400, RETENTION_SECONDS[resolution]);

export function metricNumber(value: MetricPoint['value'] | null | undefined, stat: 'avg' | 'min' | 'max' = 'avg'): number | null {
  return value == null ? null : typeof value === 'number' ? value : value[stat];
}

/** Keep unobserved buckets null, using the server's window rather than the browser clock. */
export function metricTimeline(window: MetricDetail | undefined, metric: MetricData | undefined) {
  if (!window) return [];
  const points = new Map(metric?.data.map(point => [Date.parse(point.timestamp), point.value]));
  const data = [];
  const interval = RESOLUTION_SECONDS[window.resolution] * 1000;
  for (let timestamp = Date.parse(window.start); timestamp < Date.parse(window.end) && data.length < 1000; timestamp += interval) {
    const value = points.get(timestamp);
    data.push({ timestamp, value: metricNumber(value),
      avg: metricNumber(value), min: metricNumber(value, 'min'), max: metricNumber(value, 'max') });
  }
  return data;
}

export function metricStatus(window: MetricDetail | undefined, metric: MetricData | undefined, error: boolean) {
  if (error) return 'Could not refresh metrics';
  if (!metric?.sampled_at || !window) return 'No observations in this range';
  const sampled = new Date(metric.sampled_at);
  const stale = Date.parse(window.generated_at) - sampled.getTime() > Math.max(120, RESOLUTION_SECONDS[window.resolution] * 2) * 1000;
  return `${stale ? 'No recent observations · ' : ''}Sampled ${sampled.toLocaleString()}`;
}

export function formatMetricTimestamp(timestamp: number, resolution: string) {
  const iso = new Date(timestamp).toISOString();
  return resolution === '1day' ? iso.slice(0, 10) : `${iso.slice(5, 10)} ${iso.slice(11, 16)}`;
}
