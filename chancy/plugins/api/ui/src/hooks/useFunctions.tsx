import { useQuery } from '@tanstack/react-query';
import { ChancyApi } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';

export function useFunctions(url: string | null) {
  return useQuery({
    queryKey: queryKeys.functions(url),
    queryFn: ({ signal }) => ChancyApi(url!).listFunctions(signal),
    enabled: url !== null,
    staleTime: 60_000, // 60 seconds, matching backend cache
    refetchOnWindowFocus: false,
  });
}
