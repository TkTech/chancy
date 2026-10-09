import { useMetricSeries } from '../../hooks/useMetrics';
import { defaultMetricRange, metricNumber, metricStatus, metricTimeline } from '../../services/metrics';
import { MetricCard } from './MetricCard';
import { MiniSparkline } from './MiniSparkline';

export interface MetricStatCardProps {
  title: string; metricKey: string; url: string; resolution?: string; subtitle?: string;
  formatValue?: (value: number) => string; showSparkline?: boolean; sparklineColor?: string;
  trend?: 'up' | 'down' | 'neutral'; workerId?: string; stat?: 'avg' | 'min' | 'max';
}

export function MetricStatCard({ title, metricKey, url, resolution = '5min', subtitle,
  formatValue = String, showSparkline = false, sparklineColor = '#3b82f6', trend, workerId, stat = 'avg',
}: MetricStatCardProps) {
  const { data, isLoading, isError } = useMetricSeries({ url, metricKey, resolution,
    range: defaultMetricRange(resolution), worker_id: workerId });
  const metric = data?.series[metricKey];
  const value = metricNumber(metric?.summary, stat);
  const status = metricStatus(data, metric, isError);
  const points = metricTimeline(data, metric).map(point => ({ ...point, value: point[stat] }));
  return <div title={status}>
    <MetricCard title={title} value={value === null ? '—' : formatValue(value)} loading={isLoading}
      subtitle={isError || value === null ? status : subtitle ?? 'Observed in selected range'} trend={trend}
      graphComponent={showSparkline ? <MiniSparkline data={points} color={sparklineColor} /> : undefined} />
  </div>;
}
