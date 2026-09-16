import React from 'react';

type ToastKind = 'success' | 'error' | 'info';

export interface Toast {
  id: number;
  kind: ToastKind;
  message: string;
}

interface ToastContextValue {
  show: (message: string, kind?: ToastKind, timeoutMs?: number) => void;
}

const ToastContext = React.createContext<ToastContextValue | null>(null);

export function useToast() {
  const ctx = React.useContext(ToastContext);
  if (!ctx) throw new Error('useToast must be used within ToastProvider');
  return ctx;
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = React.useState<Toast[]>([]);
  const counter = React.useRef(1);

  const remove = (id: number) => setToasts(ts => ts.filter(t => t.id !== id));

  const show = (message: string, kind: ToastKind = 'info', timeoutMs = 3000) => {
    const id = counter.current++;
    setToasts(ts => [...ts, { id, kind, message }]);
    if (timeoutMs > 0) {
      setTimeout(() => remove(id), timeoutMs);
    }
  };

  return (
    <ToastContext.Provider value={{ show }}>
      {children}
      <div className="toast-container position-fixed bottom-0 end-0 p-3" style={{ zIndex: 2000 }}>
        {toasts.map(t => (
          <div key={t.id} className={`toast align-items-center text-bg-${t.kind === 'error' ? 'danger' : t.kind === 'success' ? 'success' : 'secondary'} show`} role="alert" aria-live="assertive" aria-atomic="true">
            <div className="d-flex">
              <div className="toast-body">{t.message}</div>
              <button type="button" className="btn-close btn-close-white me-2 m-auto" aria-label="Close" onClick={() => remove(t.id)} />
            </div>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

