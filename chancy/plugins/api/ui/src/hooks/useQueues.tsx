import {useQuery} from '@tanstack/react-query';
import { request } from '../services/http';

interface Queue {
  name: string;
  concurrency: number;
  tags: string[];
  state: string;
  executor: string;
  executor_options: Record<string, unknown>;
  polling_interval: number;
  eager_polling: boolean;
  rate_limit: number | null;
  rate_limit_window: number | null;
  resume_at: string | null;
}

export function useQueues(url: string | null) {
  return useQuery<Queue[]>({
    queryKey: ['queues', url],
    queryFn: async () => {
      return await request<Queue[]>(url as string, `/api/v1/queues`);
    },
    enabled: url !== null,
    refetchInterval: 10000
  });
}
