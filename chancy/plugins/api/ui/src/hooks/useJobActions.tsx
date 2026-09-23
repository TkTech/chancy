import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ChancyApi } from '../services/chancy';
import { useServerConfiguration } from './useServerConfiguration';
import { useToast } from '../components/common/ToastProvider';

export function useJobActions() {
  const { url } = useServerConfiguration();
  const api = url ? ChancyApi(url) : null;
  const toast = useToast();
  const qc = useQueryClient();

  const invalidate = async () => {
    await Promise.all([
      qc.invalidateQueries({ queryKey: ['jobs'] }),
      qc.invalidateQueries({ queryKey: ['job'] }),
    ]);
  };

  const retry = useMutation({
    mutationFn: async (id: string | string[]) => {
      if (!api) throw new Error('No API URL');
      if (Array.isArray(id)) return api.batchJobs(id, 'retry');
      return api.retryJob(id);
    },
    onSuccess: async () => {
      toast.show('Job retry scheduled', 'success');
      await invalidate();
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to retry job', 'error'),
  });

  const cancel = useMutation({
    mutationFn: async (id: string) => {
      if (!api) throw new Error('No API URL');
      return api.cancelJob(id);
    },
    onSuccess: async () => {
      toast.show('Job cancelled', 'success');
      await invalidate();
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to cancel job', 'error'),
  });

  const purge = useMutation({
    mutationFn: async (id: string | string[]) => {
      if (!api) throw new Error('No API URL');
      if (Array.isArray(id)) return api.batchJobs(id, 'purge');
      return api.purgeJob(id);
    },
    onSuccess: async (_data, id) => {
      toast.show('Job(s) purged', 'success');
      // The purged jobs no longer exist; refetching their detail queries
      // would only 404 and retry, so drop them instead.
      for (const jobId of Array.isArray(id) ? id : [id]) {
        qc.removeQueries({ queryKey: ['job', url, jobId] });
      }
      await qc.invalidateQueries({ queryKey: ['jobs'] });
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to purge job(s)', 'error'),
  });

  return { retry, cancel, purge } as const;
}
