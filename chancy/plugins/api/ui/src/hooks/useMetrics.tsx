import { useQuery } from '@tanstack/react-query';
import { ChancyApi } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';

export type { MetricType, MetricPoint, MetricData, MetricsOverview } from '../services/schemas';

export function useMetricsOverview({ url }: { url: string | null }) {
  return useQuery({
    queryKey: queryKeys.metricsOverview(url),
    queryFn: ({ signal }) => ChancyApi(url!).getMetricsOverview(signal),
    enabled: url !== null,
    refetchInterval: 10000,
  });
}

export function useMetricDetail({ 
  url, 
  key,
  resolution = '5min',
  limit = 60,
  enabled = true,
  worker_id = undefined
}: { 
  url: string | null;
  key: string;
  resolution?: string;
  limit?: number;
  enabled?: boolean;
  worker_id?: string;
}) {
  const params = { resolution, limit, worker_id };
  return useQuery({
    queryKey: queryKeys.metricDetail(url, key, params),
    queryFn: ({ signal }) => ChancyApi(url!).getMetricDetail(key, params, signal),
    enabled: url !== null && enabled,
    refetchInterval: 10000,
  });
}
