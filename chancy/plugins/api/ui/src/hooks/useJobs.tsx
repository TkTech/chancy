import {keepPreviousData, useQuery} from '@tanstack/react-query';
import {useMemo} from 'react';

export interface Job {
  id: string,
  queue: string,
  payload: {
    func: string,
    kwargs: any,
    limits: {
      key: string,
      value: number
    }[],
  },
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

export function useJobs ({
  url,
  state
}: {
  url: string | null,
  state: string | undefined
}) {
  const fullUrl = useMemo(() => {
    const params = new URLSearchParams();
    if (state) {
      params.append('state', state);
    }
    return `${url}/api/v1/jobs?${params.toString()}`;
  }, [url, state]);

  return useQuery<Job[]>({
    queryKey: ['jobs', fullUrl],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (state) {
        params.append('state', state);
      }

      const response = await fetch(fullUrl);
      return response.json();
    },
    enabled: url !== null,
    refetchInterval: 5000,
    placeholderData: keepPreviousData
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
      const response = await fetch(`${url}/api/v1/jobs/${job_id}`);
      return response.json();
    },
    enabled: url !== null && job_id !== undefined
  });
}