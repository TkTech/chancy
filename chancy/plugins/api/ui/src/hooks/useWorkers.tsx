import {useQuery} from '@tanstack/react-query';
import { ChancyApi } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';

export type { Worker } from '../services/schemas';

export function useWorkers(url: string | null) {
  return useQuery({
    queryKey: queryKeys.workers(url),
    queryFn: ({ signal }) => ChancyApi(url!).listWorkers(signal),
    refetchInterval: 10000,
    enabled: url !== null
  });
}
