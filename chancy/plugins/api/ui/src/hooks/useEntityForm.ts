import { useForm } from '@tanstack/react-form';
import { UseMutationResult } from '@tanstack/react-query';
import { z } from 'zod';

interface UseEntityFormOptions<TSchema extends z.ZodType, TMutationData, TError = Error> {
  schema: TSchema;
  defaultValues: z.input<TSchema>;
  mutation: UseMutationResult<TMutationData, TError, z.output<TSchema>>;
  onSuccess?: () => void;
}

/**
 * Generic hook for creating entity forms with validation and mutation handling.
 * Integrates @tanstack/react-form with React Query mutations and Zod schemas.
 *
 * @example
 * const form = useEntityForm({
 *   schema: QueueFormSchema,
 *   defaultValues: queueToFormValues(queue),
 *   mutation: update,
 *   onSuccess: () => setIsEditing(false)
 * });
 */
export function useEntityForm<TSchema extends z.ZodType, TMutationData, TError = Error>({
  schema,
  defaultValues,
  mutation,
  onSuccess,
}: UseEntityFormOptions<TSchema, TMutationData, TError>) {
  const form = useForm({
    defaultValues,
    validators: {
      // Zod schemas implement Standard Schema, which react-form accepts at
      // runtime, but the generic TSchema can't be narrowed through useForm's
      // inference here.
      onChange: schema as never,
    },
    onSubmit: async ({ value }) => {
      try {
        await mutation.mutateAsync(value as z.output<TSchema>);
        onSuccess?.();
      } catch (error) {
        // Error handling is done by the mutation's onError callback
        console.error('Form submission error:', error);
      }
    },
  });

  return {
    form,
    isSubmitting: mutation.isPending || form.state.isSubmitting,
    error: mutation.error,
  };
}
