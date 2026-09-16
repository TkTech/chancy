import { useEntityForm } from '../../hooks/useEntityForm';
import { QueueFormSchema, defaultQueueValues, queueToFormValues } from '../../schemas/queue';
import { FormField } from '../../components/forms/FormField';
import { FormInput } from '../../components/forms/FormInput';
import { FormCheckbox } from '../../components/forms/FormCheckbox';
import { FormTagInput } from '../../components/forms/FormTagInput';
import { FormJsonEditor } from '../../components/forms/FormJsonEditor';
import { Queue } from '../../services/chancy';
import { UseMutationResult } from '@tanstack/react-query';
import { z } from 'zod';

interface QueueFormProps {
  mode: 'create' | 'edit';
  initial?: Partial<Queue>;
  onSubmit: (payload: z.output<typeof QueueFormSchema>) => Promise<void>;
  onCancel: () => void;
}

/**
 * Modal form for creating or editing queues.
 * Uses the unified form system with automatic validation and type coercion.
 */
export function QueueForm({ mode, initial, onSubmit, onCancel }: QueueFormProps) {
  const mockMutation = {
    mutateAsync: onSubmit,
    isPending: false,
    error: null,
  } as UseMutationResult<void, Error, z.output<typeof QueueFormSchema>>;

  const { form, isSubmitting, error } = useEntityForm({
    schema: QueueFormSchema,
    defaultValues: mode === 'create' ? defaultQueueValues : queueToFormValues(initial || {}),
    mutation: mockMutation,
    onSuccess: onCancel,
  });

  return (
    <>
      <div className="modal-backdrop fade show"></div>
      <div className="modal d-block" tabIndex={-1}>
        <div className="modal-dialog modal-dialog-centered">
          <div className="modal-content">
            <div className="modal-header">
              <h5 className="modal-title fw-semibold">
                {mode === 'create' ? 'Create New Queue' : 'Edit Queue'}
              </h5>
              <button className="btn-close" onClick={onCancel} aria-label="Close"></button>
            </div>
            <div className="modal-body">
              {error && <div className="alert alert-danger">{error.message}</div>}

              <form.Field name="name">
                {(field) => (
                  <FormField label="Name" error={field.state.meta.errors.join(', ')} required>
                    <FormInput field={field} type="text" placeholder="my-queue" />
                  </FormField>
                )}
              </form.Field>

              <div className="row">
                <div className="col">
                  <form.Field name="concurrency">
                    {(field) => (
                      <FormField label="Concurrency" error={field.state.meta.errors.join(', ')}>
                        <FormInput field={field} type="number" placeholder="Default" />
                      </FormField>
                    )}
                  </form.Field>
                </div>
                <div className="col">
                  <form.Field name="polling_interval">
                    {(field) => (
                      <FormField label="Polling Interval" error={field.state.meta.errors.join(', ')} required>
                        <FormInput field={field} type="number" unit="s" />
                      </FormField>
                    )}
                  </form.Field>
                </div>
              </div>

              <div className="row">
                <div className="col">
                  <form.Field name="rate_limit">
                    {(field) => (
                      <FormField label="Rate Limit" error={field.state.meta.errors.join(', ')}>
                        <FormInput field={field} type="number" placeholder="No limit" />
                      </FormField>
                    )}
                  </form.Field>
                </div>
                <div className="col">
                  <form.Field name="rate_limit_window">
                    {(field) => (
                      <FormField label="Rate Limit Window" error={field.state.meta.errors.join(', ')}>
                        <FormInput field={field} type="number" unit="s" />
                      </FormField>
                    )}
                  </form.Field>
                </div>
              </div>

              <form.Field name="eager_polling">
                {(field) => (
                  <FormField label="Eager Polling">
                    <FormCheckbox field={field} label="Enabled" id="eagerPollingModal" />
                  </FormField>
                )}
              </form.Field>

              <form.Field name="tags">
                {(field) => (
                  <FormField label="Tags">
                    <FormTagInput field={field} />
                  </FormField>
                )}
              </form.Field>

              <form.Field name="executor_options">
                {(field) => (
                  <FormField label="Executor Options (JSON)" error={field.state.meta.errors.join(', ')}>
                    <FormJsonEditor field={field} rows={6} />
                  </FormField>
                )}
              </form.Field>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" disabled={isSubmitting} onClick={onCancel}>
                Cancel
              </button>
              <button
                className="btn btn-primary"
                disabled={isSubmitting}
                onClick={() => form.handleSubmit()}
              >
                {isSubmitting ? 'Saving...' : (mode === 'create' ? 'Create' : 'Save')}
              </button>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
