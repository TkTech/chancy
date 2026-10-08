import type { QueueFormApi } from './useQueueForm';
import { FormField } from '../../components/forms/FormField';
import { FormInput } from '../../components/forms/FormInput';
import { FormCheckbox } from '../../components/forms/FormCheckbox';
import { FormTagInput } from '../../components/forms/FormTagInput';
import { FormJsonEditor } from '../../components/forms/FormJsonEditor';

export function QueueFields({ form, mode }: { form: QueueFormApi; mode: 'create' | 'edit' }) {
  return <>
    <form.Field name="name">
      {(field) => (
        <FormField label="Name" errors={field.state.meta.errors} required>
          {(control) => <FormInput {...control} field={field} readOnly={mode === 'edit'} type="text" placeholder="my-queue" />}
        </FormField>
      )}
    </form.Field>

    <div className="row">
      <div className="col-12 col-sm-6">
        <form.Field name="concurrency">
          {(field) => (
            <FormField label="Concurrency" errors={field.state.meta.errors}>
              {(control) => <FormInput {...control} field={field} type="number" placeholder="Default" />}
            </FormField>
          )}
        </form.Field>
      </div>
      <div className="col-12 col-sm-6">
        <form.Field name="polling_interval">
          {(field) => (
            <FormField label="Polling Interval" errors={field.state.meta.errors} required>
              {(control) => <FormInput {...control} field={field} type="number" unit="s" />}
            </FormField>
          )}
        </form.Field>
      </div>
    </div>

    <div className="row">
      <div className="col-12 col-sm-6">
        <form.Field name="rate_limit">
          {(field) => (
            <FormField label="Rate Limit" errors={field.state.meta.errors}>
              {(control) => <FormInput {...control} field={field} type="number" placeholder="No limit" />}
            </FormField>
          )}
        </form.Field>
      </div>
      <div className="col-12 col-sm-6">
        <form.Field name="rate_limit_window">
          {(field) => (
            <FormField label="Rate Limit Window" errors={field.state.meta.errors}>
              {(control) => <FormInput {...control} field={field} type="number" unit="s" />}
            </FormField>
          )}
        </form.Field>
      </div>
    </div>

    <form.Field name="eager_polling">
      {(field) => (
        <FormField label="Eager Polling">
          {(control) => <FormCheckbox {...control} field={field} />}
        </FormField>
      )}
    </form.Field>

    <form.Field name="tags">
      {(field) => (
        <FormField label="Tags" help="Use .* to match all workers. No tags leaves the queue unassigned.">
          {(control) => <FormTagInput {...control} field={field} />}
        </FormField>
      )}
    </form.Field>

    <form.Field name="executor_options">
      {(field) => (
        <FormField label="Executor Options (JSON)" errors={field.state.meta.errors}>
          {(control) => <FormJsonEditor {...control} field={field} rows={6} />}
        </FormField>
      )}
    </form.Field>
  </>;
}
