import {useLocalStorage} from './useLocalStorage.tsx';
import {useQuery} from '@tanstack/react-query';
import { request, ApiError } from '../services/http';
import React, {useMemo} from 'react';

interface ServerConfiguration {
  plugins: string[],
}

const ServerContext = React.createContext<ServerConfiguration | null>(null);

export function useServerConfiguration() {
  const [host, setHost] = useLocalStorage<string>('settings.host', "http://localhost");
  const [port, setPort] = useLocalStorage<number>('settings.port', 8000);

  const { data, isLoading, refetch } = useQuery<ServerConfiguration | null>({
    queryKey: ['configuration'],
    queryFn: async () => {
      try {
        return await request<ServerConfiguration>(`${host}:${port}`, `/api/v1/configuration`);
      } catch (e: any) {
        if (e instanceof ApiError && (e.status === 401 || e.status === 403)) {
          return null; // not authenticated
        }
        throw e;
      }
    },
    enabled: false, // Never auto-run, only via explicit refetch() calls
    staleTime: Infinity,
  });

  const url = useMemo(() => {
    if (!host || !port) {
      return null;
    }

    return `${host}:${port}`;
  }, [host, port]);

  return {
    configuration: data ?? null,
    isLoading,
    setHost,
    setPort,
    host,
    port,
    url,
    refetch,
  }
}

export function ServerConfigurationProvider({children}: {children: React.ReactNode}) {
  const value = useServerConfiguration();

  return (
    <ServerContext.Provider value={value.configuration}>
      {children}
    </ServerContext.Provider>
  )
}

export function useServer() {
  return React.useContext(ServerContext);
}
