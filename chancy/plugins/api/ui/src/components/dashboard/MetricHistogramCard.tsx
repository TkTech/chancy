import { useMetricDetail } from '../../hooks/useMetrics';
import { MetricCard } from './MetricCard';
import { MiniSparkline } from './MiniSparkline';

interface MetricHistogramCardProps {
  title: string;
  metricKey: string;
  url: string;
  resolution?: string;
  sparklineColor?: string;
  stat?: 'avg' | 'min' | 'max';
  formatValue?: (value: number) => string;
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

const getLatestValue = (data: any, stat: 'avg' | 'min' | 'max' = 'avg') => {
  if (!data) return 0;
  const key = Object.keys(data)[0];
  if (!key || !data[key]?.data || data[key].data.length === 0) return 0;

  const latestPoint = data[key].data[data[key].data.length - 1];

  // For histogram metrics, use the specified stat
  if (data[key].type === 'histogram' && latestPoint?.value?.[stat] !== undefined) {
    return latestPoint.value[stat];
  }

  return 0;
};

const getSparklineData = (data: any, stat: 'avg' | 'min' | 'max' = 'avg') => {
  if (!data) {
    return Array(10).fill({ value: 0 });
  }

  const key = Object.keys(data)[0];
  if (!key || !data[key]?.data || data[key].data.length === 0) {
    return Array(10).fill({ value: 0 });
  }

  // For histogram metrics, extract the specified stat
  const isHistogram = data[key].type === 'histogram';

  return data[key].data.map((point: any) => ({
    value: isHistogram && point.value?.[stat] !== undefined ? point.value[stat] : 0
  }));
};

/**
 * High-level card that displays histogram metric data (avg, min, max) with sparkline
 */
export function MetricHistogramCard({
  title,
  metricKey,
  url,
  resolution = '5min',
  sparklineColor = '#3b82f6',
  stat = 'avg',
  formatValue = (v) => v.toString(),
  workerId
}: MetricHistogramCardProps) {
  const limit = getLimit(resolution);

  const { data, isLoading } = useMetricDetail({
    url,
    key: metricKey,
    resolution,
    limit,
    worker_id: workerId,
  });

  const latestValue = getLatestValue(data, stat);
  const sparklineData = getSparklineData(data, stat);

  return (
    <MetricCard
      title={title}
      value={formatValue(latestValue)}
      loading={isLoading}
      graphComponent={
        <MiniSparkline data={sparklineData} color={sparklineColor} />
      }
    />
  );
}
