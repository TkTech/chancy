import React from 'react';
import { DrawerContext, DrawerOptions } from './DrawerContext';
import { Dialog } from './Dialog';

export function DrawerProvider({ children }: { children: React.ReactNode }) {
  const [drawer, setDrawer] = React.useState<{
    content: React.ReactNode;
    options?: DrawerOptions;
  } | null>(null);

  const open = (content: React.ReactNode, options?: DrawerOptions) => {
    setDrawer({ content, options });
  };

  const close = () => {
    setDrawer(null);
    drawer?.options?.onClose?.();
  };

  return (
    <DrawerContext.Provider value={{ open, close }}>
      {children}
      {drawer && (
        <Dialog
          title={drawer.options?.title || 'Details'}
          variant="drawer"
          width={drawer.options?.width}
          onClose={close}
        >
          {drawer.content}
        </Dialog>
      )}
    </DrawerContext.Provider>
  );
}
