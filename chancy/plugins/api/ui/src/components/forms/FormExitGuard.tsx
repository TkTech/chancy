import { useId } from 'react';
import { useBeforeUnload, useBlocker } from 'react-router';
import { Dialog } from '../common/Dialog';

interface FormExitGuardProps {
  isDirty: boolean;
  isSubmitting: boolean;
  cancelRequested: boolean;
  onStay: () => void;
  onDiscard: () => void;
}

export function FormExitGuard({ isDirty, isSubmitting, cancelRequested, onStay, onDiscard }: FormExitGuardProps) {
  const messageId = useId();
  const blocker = useBlocker(isDirty || isSubmitting);
  useBeforeUnload(event => {
    if (isDirty || isSubmitting) {
      event.preventDefault();
      event.returnValue = '';
    }
  });
  if (!cancelRequested && blocker.state !== 'blocked') return null;
  const stay = () => {
    if (blocker.state === 'blocked') blocker.reset();
    onStay();
  };
  return <Dialog
    title={isSubmitting ? 'Saving Changes' : 'Discard Changes?'}
    onClose={stay}
    describedBy={messageId}
    initialFocus="[data-keep-editing]"
    footer={<>
      <button type="button" className="btn btn-secondary" data-keep-editing onClick={stay}>Keep Editing</button>
      <button type="button" className="btn btn-danger" disabled={isSubmitting} onClick={() => {
        if (blocker.state === 'blocked') {
          const proceed = blocker.proceed;
          onDiscard();
          proceed();
        } else onDiscard();
      }}>Discard Changes</button>
    </>}
  >
    <p id={messageId} className="mb-0">{isSubmitting ? 'Wait for the save to finish before leaving.' : 'Your unsaved changes will be lost.'}</p>
  </Dialog>;
}
