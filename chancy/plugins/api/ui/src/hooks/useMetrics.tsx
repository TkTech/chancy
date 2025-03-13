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
  limit = 60,
  enabled = true,
  worker_id = undefined
}: { 
  url: string | null;
  type: string;
  name: string;
  resolution?: string;
  limit?: number;
  enabled?: boolean;
  worker_id?: string;
}) {
  return useQuery<Record<string, MetricPoint[]>>({
    queryKey: ['metric-detail', url, type, name, resolution, limit, worker_id],
    queryFn: async () => {
      const params = new URLSearchParams({
        resolution,
        limit: limit.toString()
      });
      
      if (worker_id) {
        params.append('worker_id', worker_id);
      }
      
      const response = await fetch(`${url}/api/v1/metrics/${type}/${name}?${params.toString()}`);
      return response.json();
    },
    enabled: enabled && url !== null && type !== '' && name !== '',
    refetchInterval: 10000,
  });
}

// Hook to get throughput metrics for a queue (used for sparklines in queue list)
export function useQueueThroughput({ 
  url, 
  queueName,
  resolution = '5min',
  limit = 20,
  enabled = true
}: { 
  url: string | null;
  queueName: string;
  resolution?: string;
  limit?: number;
  enabled?: boolean;
}) {
  return useQuery<MetricPoint[]>({
    queryKey: ['queue-throughput', url, queueName, resolution, limit],
    queryFn: async () => {
      const params = new URLSearchParams({
        resolution,
        limit: limit.toString()
      });
      
      const response = await fetch(`${url}/api/v1/metrics/queue/${queueName}:throughput?${params.toString()}`);
      const data = await response.json();
      return data.default || [];
    },
    enabled: enabled && url !== null && queueName !== '',
    refetchInterval: 30000,
    staleTime: 20000
  });
}

