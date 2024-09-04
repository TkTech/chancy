import {useQuery} from '@tanstack/react-query';

interface Worker {
  worker_id: string;
  tags: string[];
  queues: string[];
  last_seen: string;
  expires_at: string;
}

export function useWorkers(url: string | null) {
  return useQuery<Worker[]>({
    queryKey: ['workers', url],
    queryFn: async () => {
      const response = await fetch(`${url}/api/v1/workers`);
      return await response.json();
    },
    enabled: url !== null
  });
}