import {useQuery} from '@tanstack/react-query';

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
      const response = await fetch(`${url}/api/v1/crons`);
      return await response.json();
    },
    enabled: url !== null
  });
}