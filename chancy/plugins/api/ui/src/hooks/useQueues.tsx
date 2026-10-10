import {useQuery} from '@tanstack/react-query';
import { ChancyApi } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';

export type { Queue } from '../services/schemas';

export function useQueues(url: string | null) {
  return useQuery({
    queryKey: queryKeys.queues(url),
    queryFn: ({ signal }) => ChancyApi(url!).listQueues(signal),
    enabled: url !== null,
    refetchInterval: 10000
  });
}
