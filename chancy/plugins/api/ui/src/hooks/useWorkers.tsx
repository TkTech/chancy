import {useQuery} from '@tanstack/react-query';
import { request } from '../services/http';

export interface Worker {
  worker_id: string;
  tags: string[];
  queues: string[];
  last_seen: string;
  expires_at: string;
  is_leader: boolean;
}

export function useWorkers(url: string | null) {
  return useQuery<Worker[]>({
    queryKey: ['workers', url],
    queryFn: async () => {
      return await request<Worker[]>(url as string, `/api/v1/workers`);
    },
    refetchInterval: 10000,
    enabled: url !== null
  });
}
