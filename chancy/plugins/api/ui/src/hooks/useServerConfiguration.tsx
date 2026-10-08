import {useLocalStorage} from './useLocalStorage.tsx';
import {useQuery} from '@tanstack/react-query';
import { request, ApiError } from '../services/http';
import React, {useMemo} from 'react';
import {dashboardBasePath, dashboardUrl, normalizeServerUrl} from '../config.ts';

interface ServerConfiguration {
  plugins: string[],
}

const ServerContext = React.createContext<ReturnType<typeof useServerSettings> | null>(null);

function useServerSettings() {
  // Keep overrides separate for dashboards mounted on the same origin.
  const [serverUrl, setServerUrl] = useLocalStorage<string>(
    `settings.serverUrl:${dashboardBasePath}`,
    dashboardUrl.href.replace(/\/+$/, ''),
  );
  const url = useMemo(() => normalizeServerUrl(serverUrl), [serverUrl]);

  const { data, isLoading, refetch } = useQuery<ServerConfiguration | null>({
    queryKey: ['configuration', url],
    queryFn: async () => {
      if (!url) return null;
      try {
        return await request<ServerConfiguration>(url, `/api/v1/configuration`);
      } catch (e: unknown) {
        if (e instanceof ApiError && (e.status === 401 || e.status === 403)) {
          return null; // not authenticated
        }
        throw e;
      }
    },
    enabled: url !== null,
    staleTime: Infinity,
  });

  return {
    configuration: data ?? null,
    isLoading,
    serverUrl,
    setServerUrl,
    url,
    refetch,
  }
}

export function ServerConfigurationProvider({children}: {children: React.ReactNode}) {
  const value = useServerSettings();

  return (
    <ServerContext.Provider value={value}>
      {children}
    </ServerContext.Provider>
  )
}

export function useServerConfiguration() {
  const context = React.useContext(ServerContext);
  if (!context) {
    throw new Error('useServerConfiguration must be used within ServerConfigurationProvider');
  }
  return context;
}

export function useServer() {
  return useServerConfiguration().configuration;
}
