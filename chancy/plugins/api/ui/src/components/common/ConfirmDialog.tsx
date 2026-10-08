import React from 'react';
import { Dialog } from './Dialog';

interface ConfirmOptions {
  title?: string;
  message?: string;
  confirmText?: string;
  cancelText?: string;
}

export function useConfirm() {
  const [opts, setOpts] = React.useState<(ConfirmOptions & { returnFocus: HTMLElement | null }) | null>(null);
  const resolver = React.useRef<((v: boolean) => void) | null>(null);
  const messageId = React.useId();

  React.useEffect(() => () => {
    resolver.current?.(false);
    resolver.current = null;
  }, []);

  const confirm = React.useCallback((options: ConfirmOptions = {}) => {
    resolver.current?.(false);
    setOpts({ ...options, returnFocus: document.activeElement instanceof HTMLElement ? document.activeElement : null });
    return new Promise<boolean>(resolve => { resolver.current = resolve; });
  }, []);

  const onClose = (result: boolean) => {
    setOpts(null);
    resolver.current?.(result);
    resolver.current = null;
  };

  const dialog = (
    <>
      {opts && (
        <Dialog
          title={opts.title || 'Confirm'}
          onClose={() => onClose(false)}
          describedBy={messageId}
          initialFocus="[data-confirm-cancel]"
          returnFocus={opts.returnFocus}
          footer={<>
            <button type="button" className="btn btn-secondary" data-confirm-cancel onClick={() => onClose(false)}>{opts.cancelText || 'Cancel'}</button>
            <button type="button" className="btn btn-danger" onClick={() => onClose(true)}>{opts.confirmText || 'Confirm'}</button>
          </>}
        >
          <p id={messageId} className="mb-0">{opts.message || 'Are you sure?'}</p>
        </Dialog>
      )}
    </>
  );

  return { confirm, dialog } as const;
}
