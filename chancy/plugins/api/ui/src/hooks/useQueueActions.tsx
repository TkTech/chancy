import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ChancyApi, Queue } from '../services/chancy';
import { useServerConfiguration } from './useServerConfiguration';
import { useToast } from '../components/common/ToastProvider';

export function useQueueActions() {
  const { url } = useServerConfiguration();
  const api = url ? ChancyApi(url) : null;
  const toast = useToast();
  const qc = useQueryClient();

  const invalidateQueues = async () => {
    await Promise.all([
      qc.invalidateQueries({ queryKey: ['queues'] }),
      qc.invalidateQueries({ queryKey: ['queue'] }),
    ]);
  };

  const create = useMutation({
    mutationFn: async (payload: Partial<Queue> & { name: string }) => {
      if (!api) throw new Error('No API URL');
      return api.createQueue(payload);
    },
    onSuccess: async () => {
      toast.show('Queue created', 'success');
      await invalidateQueues();
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to create queue', 'error'),
  });

  const update = useMutation({
    mutationFn: async ({ name, payload }: { name: string; payload: Partial<Queue> }) => {
      if (!api) throw new Error('No API URL');
      return api.updateQueue(name, payload);
    },
    onSuccess: async () => {
      toast.show('Queue updated', 'success');
      await invalidateQueues();
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to update queue', 'error'),
  });

  const pause = useMutation({
    mutationFn: async ({ name, resume_at }: { name: string; resume_at?: string }) => {
      if (!api) throw new Error('No API URL');
      return api.pauseQueue(name, resume_at);
    },
    onSuccess: async () => {
      toast.show('Queue paused', 'success');
      await invalidateQueues();
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to pause queue', 'error'),
  });

  const resume = useMutation({
    mutationFn: async (name: string) => {
      if (!api) throw new Error('No API URL');
      return api.resumeQueue(name);
    },
    onSuccess: async () => {
      toast.show('Queue resumed', 'success');
      await invalidateQueues();
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to resume queue', 'error'),
  });

  const remove = useMutation({
    mutationFn: async ({ name, purge_jobs }: { name: string; purge_jobs: boolean }) => {
      if (!api) throw new Error('No API URL');
      return api.deleteQueue(name, purge_jobs);
    },
    onSuccess: async () => {
      toast.show('Queue deleted', 'success');
      await invalidateQueues();
    },
    onError: (e: any) => toast.show(e?.message || 'Failed to delete queue', 'error'),
  });

  return { create, update, pause, resume, remove } as const;
}
