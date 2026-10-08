import {useQuery} from '@tanstack/react-query';
import { request } from '../services/http';
import type { Queue } from '../services/chancy';

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
