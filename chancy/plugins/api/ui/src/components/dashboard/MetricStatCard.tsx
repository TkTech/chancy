import { useMetricDetail } from '../../hooks/useMetrics';
import { MetricCard } from './MetricCard';
import { MiniSparkline } from './MiniSparkline';

interface MetricStatCardProps {
  title: string;
  metricKey: string;
  url: string;
  resolution?: string;
  subtitle?: string;
  formatValue?: (value: number) => string;
  showSparkline?: boolean;
  sparklineColor?: string;
  trend?: 'up' | 'down' | 'neutral';
  workerId?: string;
}

const getLimit = (resolution: string) => {
  return {
    '1min': 60,
    '5min': 60,
    '1hour': 24,
    '1day': 30
  }[resolution] || 60;
};

const getSparklineData = (data: any) => {
  if (!data) {
    return Array(10).fill({ value: 0 });
  }

  const key = Object.keys(data)[0];
  if (!key || !data[key]?.data || data[key].data.length === 0) {
    return Array(10).fill({ value: 0 });
  }

  return data[key].data.map((point: any) => ({
    value: typeof point.value === 'number' ? point.value : 0
  }));
};

const calculateTotal = (data: any) => {
  if (!data) return 0;
  const key = Object.keys(data)[0];
  if (!key || !data[key]?.data) return 0;
  return data[key].data.reduce((sum: number, point: any) =>
    sum + (typeof point.value === 'number' ? point.value : 0), 0
  );
};

/**
 * High-level metric card that fetches and displays a single metric
 */
export function MetricStatCard({
  title,
  metricKey,
  url,
  resolution = '5min',
  subtitle,
  formatValue = (v) => v.toString(),
  showSparkline = false,
  sparklineColor = '#3b82f6',
  trend,
  workerId
}: MetricStatCardProps) {
  const limit = getLimit(resolution);

  const { data, isLoading } = useMetricDetail({
    url,
    key: metricKey,
    resolution,
    limit,
    worker_id: workerId,
  });

  const total = calculateTotal(data);
  const sparklineData = showSparkline ? getSparklineData(data) : undefined;

  return (
    <MetricCard
      title={title}
      value={formatValue(total)}
      subtitle={subtitle}
      loading={isLoading}
      trend={trend}
      graphComponent={
        showSparkline && sparklineData ? (
          <MiniSparkline data={sparklineData} color={sparklineColor} />
        ) : undefined
      }
    />
  );
}
