import {useQuery} from '@tanstack/react-query';
import { ChancyApi } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';

export type { Cron } from '../services/schemas';

export function useCrons ({ url }: { url: string | null }) {
  return useQuery({
    queryKey: queryKeys.crons(url),
    queryFn: ({ signal }) => ChancyApi(url!).listCrons(signal),
    enabled: url !== null
  });
}
