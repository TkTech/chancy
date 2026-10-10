import { useId, useState } from 'react';
import { Dialog } from '../../components/common/Dialog';
import { DetailCard } from '../../components/common/DetailCard';
import { FormExitGuard } from '../../components/forms/FormExitGuard';
import { useQueueForm, type QueueFormMutation } from './useQueueForm';
import { defaultQueueValues, queueToFormValues } from '../../schemas/queue';
import type { Queue } from '../../services/chancy';
import { QueueFields } from './QueueFields';

type QueueFormProps = {
  layout?: 'dialog' | 'inline';
  mutation: QueueFormMutation;
  onClose: () => void;
} & ({ mode: 'create'; initial?: never } | { mode: 'edit'; initial: Queue });

export function QueueForm(props: QueueFormProps) {
  const { mode, layout = 'dialog', mutation, onClose } = props;
  const formId = useId();
  const { form, isSubmitting, isDirty, error } = useQueueForm({
    defaultValues: props.mode === 'create' ? defaultQueueValues : queueToFormValues(props.initial),
    mutation,
    onSaved: onClose,
  });
  const [cancelRequested, setCancelRequested] = useState(false);
  const close = () => {
    if (isSubmitting) return;
    if (isDirty) setCancelRequested(true);
    else onClose();
  };
  const title = mode === 'create' ? 'Create New Queue' : 'Edit Queue';
  const actions = <>
    <button type="button" className="btn btn-secondary" disabled={isSubmitting} onClick={close}>Cancel</button>
    <button type="submit" form={formId} className="btn btn-primary" disabled={isSubmitting}>
      {isSubmitting ? 'Saving...' : (mode === 'create' ? 'Create' : 'Save Changes')}
    </button>
  </>;
  const contents = <form id={formId} noValidate onSubmit={async event => {
    event.preventDefault();
    const element = event.currentTarget;
    if (!form.state.isSubmitting && !mutation.isPending) {
      await form.handleSubmit();
      element.querySelector<HTMLElement>('[aria-invalid="true"]')?.focus();
    }
  }}>
    {error && <div className="alert alert-danger" role="alert">{error.message}</div>}
    <fieldset disabled={isSubmitting}>
      <QueueFields form={form} mode={mode} />
    </fieldset>
  </form>;
  return <>
    {layout === 'dialog' ? <Dialog title={title} onClose={close} dismissible={!isSubmitting} initialFocus="input" footer={actions}>
      {contents}
    </Dialog> : <DetailCard title={title}>
      {contents}
      <div className="d-flex flex-wrap gap-2 justify-content-end">{actions}</div>
    </DetailCard>}
    <FormExitGuard isDirty={isDirty} isSubmitting={isSubmitting} cancelRequested={cancelRequested}
      onStay={() => setCancelRequested(false)} onDiscard={onClose} />
  </>;
}
