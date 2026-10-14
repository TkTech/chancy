import { useMetricSeries } from '../../hooks/useMetrics';
import { defaultMetricRange, metricNumber } from '../../services/metrics';
import { MetricCard } from './MetricCard';

export function MetricSuccessRateCard({ title, succeededKey, failedKey, url, resolution = '5min', workerId }: {
  title: string; succeededKey: string; failedKey: string; url: string; resolution?: string; workerId?: string;
}) {
  const { data, isLoading, isError } = useMetricSeries({ url, metricKey: succeededKey, resolution,
    range: defaultMetricRange(resolution), worker_id: workerId });
  const succeeded = metricNumber(data?.series[succeededKey]?.summary);
  const failed = metricNumber(data?.series[failedKey]?.summary);
  const rate = succeeded !== null && failed !== null && succeeded + failed > 0
    ? `${(100 * succeeded / (succeeded + failed)).toFixed(1)}%` : '—';
  return <MetricCard title={title} value={rate} loading={isLoading}
    subtitle={isError ? 'Could not refresh metrics' : rate === '—' ? 'Insufficient observations' : 'Observed terminal updates'} />;
}
