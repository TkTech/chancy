import {keepPreviousData, useQuery} from '@tanstack/react-query';
import {useMemo} from 'react';
import { request } from '../services/http';

export interface Job {
  id: string,
  queue: string,
  func: string,
  kwargs: Record<string, unknown>,
  limits: {
    key: string,
    value: number
  }[],
  meta: Record<string, unknown>,
  state: string,
  priority: number,
  attempts: number,
  max_attempts: number,
  taken_by: string,
  created_at: string,
  started_at: string,
  completed_at: string,
  scheduled_at: string,
  unique_key: string,
  errors: {
    traceback: string,
    attempt: number
  }[]
}

export type FilterTriple = [string, string, string];

export interface JobPage {
  items: Job[];
  next_cursor: string | null;
  has_more: boolean;
}

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
  const fullUrl = useMemo(() => {
    const params = new URLSearchParams();
    params.set('pagination', 'true');
    if (before) params.set('before', before);
    if (state) {
      params.append('state', state);
    }
    if (func) {
      params.append('func', func);
    }
    if (filters && filters.length > 0) {
      params.append('filters', JSON.stringify(filters));
    }
    return `${url}/api/v1/jobs?${params.toString()}`;
  }, [url, state, func, filters, before]);

  return useQuery<JobPage>({
    queryKey: ['jobs', fullUrl],
    queryFn: async ({ signal }) => {
      // Use our request helper to attach token automatically
      const urlBase = url as string;
      const path = `/api/v1/jobs?${new URL(fullUrl).searchParams.toString()}`;
      return await request<JobPage>(urlBase, path, { signal });
    },
    enabled: enabled ?? (url !== null),
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
  const query = useQuery<Job>({
    queryKey: ['job', url, job_id],
    queryFn: async () => {
      return await request<Job>(url as string, `/api/v1/jobs/${job_id}`);
    },
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
