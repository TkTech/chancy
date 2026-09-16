import { useMetricDetail } from '../../hooks/useMetrics';
import { MetricCard } from './MetricCard';

interface MetricSuccessRateCardProps {
  title: string;
  succeededKey: string;
  failedKey: string;
  url: string;
  resolution?: string;
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

const calculateTotal = (data: any) => {
  if (!data) return 0;
  const key = Object.keys(data)[0];
  if (!key || !data[key]?.data) return 0;
  return data[key].data.reduce((sum: number, point: any) =>
    sum + (typeof point.value === 'number' ? point.value : 0), 0
  );
};

/**
 * High-level card that calculates and displays success rate from two metrics
 */
export function MetricSuccessRateCard({
  title,
  succeededKey,
  failedKey,
  url,
  resolution = '5min',
  workerId
}: MetricSuccessRateCardProps) {
  const limit = getLimit(resolution);

  const { data: succeededData, isLoading: succeededLoading } = useMetricDetail({
    url,
    key: succeededKey,
    resolution,
    limit,
    worker_id: workerId,
  });

  const { data: failedData, isLoading: failedLoading } = useMetricDetail({
    url,
    key: failedKey,
    resolution,
    limit,
    worker_id: workerId,
  });

  const succeededTotal = calculateTotal(succeededData);
  const failedTotal = calculateTotal(failedData);

  const successRate = succeededTotal + failedTotal > 0
    ? ((succeededTotal / (succeededTotal + failedTotal)) * 100).toFixed(1)
    : '0.0';

  const trend = parseFloat(successRate) >= 95 ? 'up' : parseFloat(successRate) >= 85 ? 'neutral' : 'down';

  return (
    <MetricCard
      title={title}
      value={`${successRate}%`}
      loading={succeededLoading || failedLoading}
      trend={trend}
    />
  );
}
