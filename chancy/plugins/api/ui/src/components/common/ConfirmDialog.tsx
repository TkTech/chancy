import React from 'react';

interface ConfirmOptions {
  title?: string;
  message?: string;
  confirmText?: string;
  cancelText?: string;
}

export function useConfirm() {
  const [opts, setOpts] = React.useState<ConfirmOptions | null>(null);
  const [resolver, setResolver] = React.useState<((v: boolean) => void) | null>(null);

  const confirm = React.useCallback((options: ConfirmOptions = {}) => {
    setOpts(options);
    return new Promise<boolean>(resolve => setResolver(() => resolve));
  }, []);

  const onClose = (result: boolean) => {
    setOpts(null);
    resolver?.(result);
    setResolver(null);
  };

  const dialog = (
    <>
      {opts && (
        <div className="modal d-block" tabIndex={-1}>
          <div className="modal-dialog">
            <div className="modal-content">
              <div className="modal-header">
                <h5 className="modal-title">{opts.title || 'Confirm'}</h5>
                <button type="button" className="btn-close" aria-label="Close" onClick={() => onClose(false)}></button>
              </div>
              <div className="modal-body">
                <p>{opts.message || 'Are you sure?'}</p>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-secondary" onClick={() => onClose(false)}>{opts.cancelText || 'Cancel'}</button>
                <button type="button" className="btn btn-danger" onClick={() => onClose(true)}>{opts.confirmText || 'Confirm'}</button>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );

  return { confirm, dialog } as const;
}

