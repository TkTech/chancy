import {useQuery} from '@tanstack/react-query';

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
  steps : {
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
      const response = await fetch(`${url}/api/v1/workflows/${workflow_id}`, {
        credentials: 'include'
      });
      
      if (!response.ok) {
        throw new Error('Failed to fetch workflow details');
      }
      
      return await response.json();
    },
    enabled: url !== null && workflow_id !== undefined,
    ...options
  });
}

export function useWorkflows ({
  url
}: {
  url: string | null
}) {
  return useQuery<Workflow[]>({
    queryKey: ['workflows', url],
    queryFn: async () => {
      const response = await fetch(`${url}/api/v1/workflows`, {
        credentials: 'include'
      });
      
      if (!response.ok) {
        throw new Error('Failed to fetch workflows');
      }
      
      return await response.json();
    },
    enabled: url !== null
  });
}