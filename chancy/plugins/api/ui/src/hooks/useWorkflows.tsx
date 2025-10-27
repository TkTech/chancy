import React from 'react';
import {useQuery} from '@tanstack/react-query';
import { request } from '../services/http';

export interface Step {
  step_id: string;
  state: string;
  job_id: string;
  dependencies: string[];
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

export interface Workflow {
  id: string,
  name: string,
  state: string,
  created_at: string,
  updated_at: string,
  pending_steps?: number,
  running_steps?: number,
  succeeded_steps?: number,
  failed_steps?: number,
  retrying_steps?: number,
  steps? : {
    [key: string]: Step
  }
}

export function useWorkflow ({
  url,
  workflow_id,
  options = {}
}: {
  url: string | null,
  workflow_id: string | undefined,
  options?: {
    refetchInterval?: number,
  }
}) {
  return useQuery<Workflow>({
    queryKey: ['workflow', url, workflow_id],
    queryFn: async () => {
      return await request<Workflow>(url as string, `/api/v1/workflows/${workflow_id}`);
    },
    enabled: url !== null && workflow_id !== undefined,
    ...options
  });
}

export type FilterTriple = [string, string, string];

export function useWorkflows ({
  url,
  filters
}: {
  url: string | null,
  filters?: FilterTriple[]
}) {
  const fullUrl = React.useMemo(() => {
    const params = new URLSearchParams();
    if (filters && filters.length > 0) {
      params.append('filters', JSON.stringify(filters));
    }
    return `${url}/api/v1/workflows?${params.toString()}`;
  }, [url, filters]);

  return useQuery<Workflow[]>({
    queryKey: ['workflows', fullUrl],
    queryFn: async () => {
      const urlBase = url as string;
      const path = `/api/v1/workflows?${new URL(fullUrl).searchParams.toString()}`;
      return await request<Workflow[]>(urlBase, path);
    },
    enabled: url !== null,
    refetchInterval: 5000,
  });
}
