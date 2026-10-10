import { CSSProperties, ReactNode, useId, useLayoutEffect, useRef } from 'react';
import { createPortal } from 'react-dom';

interface DialogProps {
  title: string;
  children: ReactNode;
  onClose: () => void;
  footer?: ReactNode;
  variant?: 'modal' | 'drawer';
  width?: number;
  dismissible?: boolean;
  describedBy?: string;
  initialFocus?: string;
  returnFocus?: HTMLElement | null;
}

/** Mount only while open. Native modal dialogs isolate and stack their contents. */
export function Dialog({
  title, children, onClose, footer, variant = 'modal', width,
  dismissible = true, describedBy, initialFocus, returnFocus,
}: DialogProps) {
  const titleId = useId();
  const dialogRef = useRef<HTMLDialogElement>(null);
  const backdropPress = useRef(false);

  useLayoutEffect(() => {
    const dialog = dialogRef.current!;
    const opener = returnFocus ?? document.activeElement;
    dialog.showModal();
    const focusTarget = initialFocus ? dialog.querySelector<HTMLElement>(initialFocus) : null;
    (focusTarget ?? dialog.querySelector<HTMLElement>('[data-dialog-title]'))?.focus();
    return () => {
      dialog.close();
      // Wait for disabled triggers to be re-enabled and removed rows to settle.
      requestAnimationFrame(() => {
        if (dialog.isConnected && dialog.open) return;
        if (opener instanceof HTMLElement && opener.isConnected && !opener.matches(':disabled')) {
          opener.focus({ preventScroll: true });
        } else {
          document.querySelector<HTMLElement>('main')?.focus({ preventScroll: true });
        }
      });
    };
  }, [initialFocus, returnFocus]);

  const outside = (x: number, y: number) => {
    const rect = dialogRef.current!.getBoundingClientRect();
    return x < rect.left || x > rect.right || y < rect.top || y > rect.bottom;
  };

  return createPortal(
    <dialog
      ref={dialogRef}
      className={`modal app-dialog app-dialog--${variant}`}
      style={{ '--dialog-width': `${width ?? (variant === 'drawer' ? 720 : 500)}px` } as CSSProperties}
      aria-labelledby={titleId}
      aria-describedby={describedBy}
      onCancel={event => {
        event.preventDefault();
        event.stopPropagation();
        if (dismissible) onClose();
      }}
      onKeyDown={event => {
        if (event.key !== 'Tab' || event.defaultPrevented) return;
        event.stopPropagation();
        const controls = Array.from(event.currentTarget.querySelectorAll<HTMLElement>(
          'a[href], button, input, select, textarea, [tabindex], [contenteditable="true"], summary',
        )).filter(node => node.tabIndex >= 0 && !node.matches(':disabled') &&
          !node.closest('[inert]') && node.getClientRects().length > 0 && getComputedStyle(node).visibility !== 'hidden');
        const first = controls[0];
        const last = controls[controls.length - 1];
        const active = document.activeElement;
        if (!first) {
          event.preventDefault();
          event.currentTarget.querySelector<HTMLElement>('[data-dialog-title]')?.focus();
        } else if (event.shiftKey && (active === first || !controls.includes(active as HTMLElement))) {
          event.preventDefault();
          last?.focus();
        } else if (!event.shiftKey && active === last) {
          event.preventDefault();
          first.focus();
        }
      }}
      onPointerDown={event => {
        backdropPress.current = event.target === event.currentTarget && outside(event.clientX, event.clientY);
      }}
      onPointerCancel={() => { backdropPress.current = false; }}
      onClick={event => {
        event.stopPropagation();
        if (dismissible && backdropPress.current && event.target === event.currentTarget && outside(event.clientX, event.clientY)) onClose();
        backdropPress.current = false;
      }}
    >
      <div className="modal-content">
        <div className="modal-header">
          <h5 id={titleId} className="modal-title" tabIndex={-1} data-dialog-title>{title}</h5>
          <button type="button" className="btn-close" aria-label="Close" disabled={!dismissible} onClick={onClose} />
        </div>
        <div className="modal-body">{children}</div>
        {footer && <div className="modal-footer">{footer}</div>}
      </div>
    </dialog>,
    document.body,
  );
}
