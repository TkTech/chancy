import {useQuery} from '@tanstack/react-query';
import { request } from '../services/http';

interface Cron {
  unique_key: string;
  cron: string,
  last_run: string,
  next_run: string,
  job: {
    func: string,
    queue: string,
    kwargs: unknown,
    priority: number,
    max_attempts: number,
    limits: {
      key: string,
      value: number
    }[]
  }
}

export function useCrons ({ url }: { url: string | null }) {
  return useQuery<Cron[]>({
    queryKey: ['crons', url],
    queryFn: async () => {
      return await request<Cron[]>(url as string, `/api/v1/crons`);
    },
    enabled: url !== null
  });
}
