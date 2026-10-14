import React from 'react';

export type DrawerOptions = {
  title?: string;
  width?: number; // in px
  onClose?: () => void;
};

type DrawerContextValue = {
  open: (content: React.ReactNode, opts?: DrawerOptions) => void;
  close: () => void;
};

export const DrawerContext = React.createContext<DrawerContextValue | null>(null);

export function useDrawer() {
  const ctx = React.useContext(DrawerContext);
  if (!ctx) throw new Error('useDrawer must be used within DrawerProvider');
  return ctx;
}

