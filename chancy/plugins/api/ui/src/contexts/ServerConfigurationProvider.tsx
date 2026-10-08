import type React from 'react';
import { ServerContext, useServerSettings } from '../hooks/useServerConfiguration';

export function ServerConfigurationProvider({children}: {children: React.ReactNode}) {
  const value = useServerSettings();

  return (
    <ServerContext.Provider value={value}>
      {children}
    </ServerContext.Provider>
  )
}

