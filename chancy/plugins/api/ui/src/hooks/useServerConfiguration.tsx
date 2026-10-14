import { ChancyApi } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';
import {useLocalStorage} from './useLocalStorage.tsx';
import {useQuery} from '@tanstack/react-query';
import { ApiError } from '../services/http';
import React, {useMemo} from 'react';
import {dashboardBasePath, dashboardUrl, normalizeServerUrl} from '../config.ts';

export const ServerContext = React.createContext<ReturnType<typeof useServerSettings> | null>(null);

export function useServerSettings() {
  // Keep overrides separate for dashboards mounted on the same origin.
  const [serverUrl, setServerUrl] = useLocalStorage<string>(
    `settings.serverUrl:${dashboardBasePath}`,
    dashboardUrl.href.replace(/\/+$/, ''),
  );
  const url = useMemo(() => normalizeServerUrl(serverUrl), [serverUrl]);

  const { data, isLoading, refetch } = useQuery({
    queryKey: queryKeys.configuration(url),
    queryFn: async ({ signal }) => {
      if (!url) return null;
      try {
        return await ChancyApi(url).getConfiguration(signal);
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
