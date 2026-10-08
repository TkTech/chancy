import {keepPreviousData, useQuery} from '@tanstack/react-query';
import { ChancyApi, type FilterTriple } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';

export type { Job, JobPage } from '../services/schemas';
export type { FilterTriple } from '../services/chancy';

export function useJobs ({
  url,
  state,
  func,
  filters,
  enabled,
  pausePolling = false,
  before,
}: {
  url: string | null,
  state: string | undefined,
  func?: string | undefined,
  filters?: FilterTriple[],
  enabled?: boolean,
  pausePolling?: boolean,
  before?: string,
}) {
  const params = { state, func, filters, before };
  return useQuery({
    queryKey: queryKeys.jobPage(url, params),
    queryFn: ({ signal }) => ChancyApi(url!).listJobs(params, signal),
    enabled: url !== null && (enabled ?? true),
    // Reduce flicker by avoiding focus refetches and keeping data "warm"
    refetchOnWindowFocus: false,
    refetchOnReconnect: !before && !pausePolling,
    staleTime: 0,
    refetchInterval: !before && !pausePolling ? 5000 : false,
    placeholderData: keepPreviousData,
  });
}

export function useJob ({
  url,
  job_id
}: {
  url: string | null,
  job_id: string | undefined
}) {
  const query = useQuery({
    queryKey: queryKeys.job(url, job_id),
    queryFn: ({ signal }) => ChancyApi(url!).getJob(job_id!, signal),
    enabled: url !== null && job_id !== undefined,
    refetchInterval: (query) => {
      // Only refetch if the job is not in a terminal state
      const job = query.state.data;
      if (!job) return false;
      const terminalStates = ['succeeded', 'failed'];
      return terminalStates.includes(job.state) ? false : 5000;
    }
  });

  return query;
}
