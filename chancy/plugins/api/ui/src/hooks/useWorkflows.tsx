import {keepPreviousData, useQuery} from '@tanstack/react-query';
import { ChancyApi, type FilterTriple } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';

export type { Workflow, WorkflowSummary, WorkflowPage, Step } from '../services/schemas';
export type { FilterTriple } from '../services/chancy';

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
  return useQuery({
    queryKey: queryKeys.workflow(url, workflow_id),
    queryFn: ({ signal }) => ChancyApi(url!).getWorkflow(workflow_id!, signal),
    enabled: url !== null && workflow_id !== undefined,
    ...options
  });
}

export function useWorkflows ({
  url,
  filters,
  before,
}: {
  url: string | null,
  filters?: FilterTriple[],
  before?: string,
}) {
  const params = { filters, before };
  return useQuery({
    queryKey: queryKeys.workflows(url, params),
    queryFn: ({ signal }) => ChancyApi(url!).listWorkflows(params, signal),
    enabled: url !== null,
    refetchOnWindowFocus: false,
    refetchOnReconnect: !before,
    staleTime: 0,
    refetchInterval: !before ? 5000 : false,
    placeholderData: keepPreviousData,
  });
}
