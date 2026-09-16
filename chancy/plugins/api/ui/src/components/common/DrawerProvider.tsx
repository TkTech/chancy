import React from 'react';

type DrawerOptions = {
  title?: string;
  width?: number; // in px
  onClose?: () => void;
};

type DrawerContextValue = {
  open: (content: React.ReactNode, opts?: DrawerOptions) => void;
  close: () => void;
};

const DrawerContext = React.createContext<DrawerContextValue | null>(null);

export function useDrawer() {
  const ctx = React.useContext(DrawerContext);
  if (!ctx) throw new Error('useDrawer must be used within DrawerProvider');
  return ctx;
}

export function DrawerProvider({ children }: { children: React.ReactNode }) {
  const [isOpen, setOpen] = React.useState(false);
  const [content, setContent] = React.useState<React.ReactNode>(null);
  const [title, setTitle] = React.useState<string | undefined>(undefined);
  const [width, setWidth] = React.useState<number>(720);
  const onCloseRef = React.useRef<(() => void) | undefined>(undefined);

  const open = (node: React.ReactNode, opts?: DrawerOptions) => {
    setContent(node);
    setTitle(opts?.title);
    setWidth(opts?.width ?? 720);
    onCloseRef.current = opts?.onClose;
    setOpen(true);
  };

  const close = () => {
    try {
      onCloseRef.current?.();
    } finally {
      setOpen(false);
    }
    // allow exit animation before clearing (optional)
    setTimeout(() => setContent(null), 200);
  };

  // prevent background scroll when drawer is open
  React.useEffect(() => {
    if (isOpen) {
      const prev = document.body.style.overflow;
      document.body.style.overflow = 'hidden';
      return () => {
        document.body.style.overflow = prev;
      };
    }
  }, [isOpen]);

  return (
    <DrawerContext.Provider value={{ open, close }}>
      {children}
      {/* overlay */}
      <div
        role="presentation"
        onClick={close}
        style={{
          position: 'fixed', inset: 0, background: isOpen ? 'rgba(0,0,0,0.35)' : 'transparent',
          pointerEvents: isOpen ? 'auto' : 'none', transition: 'background 0.2s ease', zIndex: 2049,
        }}
      />
      {/* drawer */}
      <aside
        aria-hidden={!isOpen}
        style={{
          position: 'fixed', top: 0, right: 0, height: '100vh', width,
          // Use Bootstrap body colors so the drawer matches the app theme
          background: 'var(--bs-body-bg)',
          color: 'var(--bs-body-color)',
          boxShadow: '0 0 20px rgba(0,0,0,0.15)',
          transform: `translateX(${isOpen ? 0 : width}px)`, transition: 'transform 0.2s ease', zIndex: 2050,
          display: 'flex', flexDirection: 'column',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="d-flex align-items-center border-bottom bg-body-tertiary px-3 py-2">
          <strong className="me-auto">{title || 'Details'}</strong>
          <button className="btn-close" aria-label="Close" onClick={close} />
        </div>
        <div className="flex-grow-1 overflow-auto p-3 bg-body">
          {content}
        </div>
      </aside>
    </DrawerContext.Provider>
  );
}
