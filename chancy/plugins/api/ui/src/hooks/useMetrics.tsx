import { useQuery } from '@tanstack/react-query';

export interface MetricPoint {
  timestamp: string;
  value: number | { [key: string]: number };
}

export interface MetricsOverview {
  types: {
    [type: string]: string[];
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
  type,
  name,
  resolution = '5min',
  limit = 60
}: { 
  url: string | null;
  type: string;
  name: string;
  resolution?: string;
  limit?: number;
}) {
  return useQuery<Record<string, MetricPoint[]>>({
    queryKey: ['metric-detail', url, type, name, resolution, limit],
    queryFn: async () => {
      const params = new URLSearchParams({
        resolution,
        limit: limit.toString()
      });
      const response = await fetch(`${url}/api/v1/metrics/${type}/${name}?${params.toString()}`);
      return response.json();
    },
    enabled: url !== null && type !== '' && name !== '',
    refetchInterval: 10000,
  });
}