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

export function useJobs ({
  url,
  state,
  func,
  filters,
  enabled,
}: {
  url: string | null,
  state: string | undefined,
  func?: string | undefined,
  filters?: FilterTriple[],
  enabled?: boolean,
}) {
  const fullUrl = useMemo(() => {
    const params = new URLSearchParams();
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
  }, [url, state, func, filters]);

  return useQuery<Job[]>({
    queryKey: ['jobs', fullUrl],
    queryFn: async () => {
      // Use our request helper to attach token automatically
      const urlBase = url as string;
      const path = `/api/v1/jobs?${new URL(fullUrl).searchParams.toString()}`;
      return await request<Job[]>(urlBase, path);
    },
    enabled: enabled ?? (url !== null),
    // Reduce flicker by avoiding focus refetches and keeping data "warm"
    refetchOnWindowFocus: false,
    staleTime: 5_000,
    refetchInterval: (enabled ?? (url !== null)) ? 5000 : false,
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
  return useQuery<Job>({
    queryKey: ['job', url, job_id],
    queryFn: async () => {
      return await request<Job>(url as string, `/api/v1/jobs/${job_id}`);
    },
    enabled: url !== null && job_id !== undefined
  });
}
