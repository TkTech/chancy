import { useQuery } from '@tanstack/react-query';
import { request } from '../services/http';

export type MetricType = 'counter' | 'gauge' | 'histogram';

export interface MetricPoint {
  timestamp: string;
  value: number | { [key: string]: number };
}

export interface MetricData {
  data: MetricPoint[];
  type: MetricType;
}

export interface MetricsOverview {
  categories: {
    [category: string]: string[];
  };
  count: number;
}


export function useMetricsOverview({ url }: { url: string | null }) {
  return useQuery<MetricsOverview>({
    queryKey: ['metrics-overview', url],
    queryFn: async () => {
      return await request<MetricsOverview>(url as string, `/api/v1/metrics`);
    },
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
  return useQuery<Record<string, MetricData>>({
    queryKey: ['metric-detail', url, key, resolution, limit, worker_id],
    queryFn: async () => {
      const params = new URLSearchParams({
        resolution,
        limit: limit.toString()
      });
      
      if (worker_id) {
        params.append('worker_id', worker_id);
      }
      
      return await request<Record<string, MetricData>>(url as string, `/api/v1/metrics/${key}?${params.toString()}`);
    },
    enabled: enabled,
    refetchInterval: 10000,
  });
}
