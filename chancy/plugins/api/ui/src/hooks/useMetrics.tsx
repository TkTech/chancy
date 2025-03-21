import { useQuery } from '@tanstack/react-query';

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
      const response = await fetch(`${url}/api/v1/metrics`);
      return response.json();
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
      
      const response = await fetch(`${url}/api/v1/metrics/${key}?${params.toString()}`);
      return response.json();
    },
    enabled: enabled,
    refetchInterval: 10000,
  });
}

