import {useQuery} from '@tanstack/react-query';

interface Queue {
  name: string;
  concurrency: number;
  tags: string[];
  state: string;
  executor: string;
  executor_options: Record<string, unknown>;
  polling_interval: number;
  rate_limit: number | null;
  rate_limit_window: number | null;
  resume_at: string | null;
}

export function useQueues(url: string | null) {
  return useQuery<Queue[]>({
    queryKey: ['queues', url],
    queryFn: async () => {
      const response = await fetch(`${url}/api/v1/queues`);
      return await response.json();
    },
    enabled: url !== null
  });
}