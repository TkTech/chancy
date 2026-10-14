import { useForm, useStore } from '@tanstack/react-form';
import { QueueFormSchema, type QueueFormInput, type QueueFormOutput } from '../../schemas/queue';

export interface QueueFormMutation {
  mutateAsync: (payload: QueueFormOutput) => Promise<unknown>;
  isPending: boolean;
  error: Error | null;
}

export function useQueueForm({ defaultValues, mutation, onSaved }: {
  defaultValues: QueueFormInput;
  mutation: QueueFormMutation;
  onSaved: () => void;
}) {
  const form = useForm({
    defaultValues,
    validators: { onChange: QueueFormSchema },
    onSubmit: async ({ value }) => {
      const payload = QueueFormSchema.parse(value);
      try {
        await mutation.mutateAsync(payload);
      } catch {
        // The existing mutation exposes request failures through mutation.error.
        return;
      }
      form.reset(value);
      onSaved();
    },
  });
  const isSubmitting = useStore(form.store, state => state.isSubmitting) || mutation.isPending;
  const isDirty = useStore(form.store, state => !state.isDefaultValue);
  return { form, isSubmitting, isDirty, error: mutation.error };
}

export type QueueFormApi = ReturnType<typeof useQueueForm>['form'];
