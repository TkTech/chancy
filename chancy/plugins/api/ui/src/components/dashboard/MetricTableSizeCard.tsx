import { useMetricDetail } from '../../hooks/useMetrics';
import { MetricCard } from './MetricCard';
import { MiniSparkline } from './MiniSparkline';

interface MetricTableSizeCardProps {
  title: string;
  tableName: string;
  url: string;
  resolution?: string;
  sparklineColor?: string;
  selector?: 'total_size_bytes' | 'table_size_bytes' | 'index_size_bytes';
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

  // For histogram metrics, extract the avg value
  const isHistogram = data[key].type === 'histogram';

  return data[key].data.map((point: any) => ({
    value: isHistogram && point.value?.avg ? point.value.avg : 0
  }));
};

const getLatestSize = (data: any) => {
  if (!data) return 0;
  const key = Object.keys(data)[0];
  if (!key || !data[key]?.data || data[key].data.length === 0) return 0;

  const latestPoint = data[key].data[data[key].data.length - 1];

  // For histogram metrics, use the avg value
  if (data[key].type === 'histogram' && latestPoint?.value?.avg) {
    return latestPoint.value.avg;
  }

  return 0;
};

const formatBytes = (bytes: number) => {
  if (bytes === 0) return '0 B';
  if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(1)} GB`;
  if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
};

/**
 * High-level card that displays database table size with sparkline
 */
export function MetricTableSizeCard({
  title,
  tableName,
  url,
  resolution = '5min',
  sparklineColor = '#3b82f6',
  selector = 'total_size_bytes'
}: MetricTableSizeCardProps) {
  const limit = getLimit(resolution);

  const { data, isLoading } = useMetricDetail({
    url,
    key: `table:${tableName}:${selector}`,
    resolution,
    limit,
  });

  const latestSize = getLatestSize(data);
  const sparklineData = getSparklineData(data);

  return (
    <MetricCard
      title={title}
      value={formatBytes(latestSize)}
      loading={isLoading}
      graphComponent={
        <MiniSparkline data={sparklineData} color={sparklineColor} />
      }
    />
  );
}
