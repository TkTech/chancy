import {useQuery} from '@tanstack/react-query';

interface Workflow {
  id: string,
  name: string,
  state: string,
  created_at: string,
  updated_at: string,
  steps? : {
    [key: string]: {
      step_id: string,
      state: string,
      job_id: string
    }
  }
}

export function useWorkflow ({
  url,
  workflow_id
}: {
  url: string | null,
  workflow_id: string | undefined
}) {
  return useQuery<Workflow>({
    queryKey: ['workflow', url, workflow_id],
    queryFn: async () => {
      const response = await fetch(`${url}/api/v1/workflows/${workflow_id}`);
      return await response.json();
    },
    enabled: url !== null && workflow_id !== undefined
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
      const response = await fetch(`${url}/api/v1/workflows`);
      return await response.json();
    },
    enabled: url !== null
  });
}