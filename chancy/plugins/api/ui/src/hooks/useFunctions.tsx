import { useQuery } from '@tanstack/react-query';
import { request } from '../services/http';

export function useFunctions(url: string | null) {
  return useQuery<string[]>({
    queryKey: ['functions', url],
    queryFn: async () => {
      if (!url) throw new Error('URL is required');
      return await request<string[]>(url, `/api/v1/jobs/functions`);
    },
    enabled: url !== null,
    staleTime: 60_000, // 60 seconds, matching backend cache
    refetchOnWindowFocus: false,
  });
}
