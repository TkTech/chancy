import {useSessionStorage} from './useSessionStorage.tsx';
import {useQuery} from '@tanstack/react-query';
import React, {useMemo} from 'react';

interface ServerConfiguration {
  plugins: string[],
}

const ServerContext = React.createContext<ServerConfiguration | null>(null);

export function useServerConfiguration() {
  const [host, setHost] = useSessionStorage<string>('settings.host', "http://localhost");
  const [port, setPort] = useSessionStorage<number>('settings.port', 8000);

  const { data, isLoading } = useQuery<ServerConfiguration>({
    queryKey: ['configuration', host, port],
    queryFn: async () => {
      const response = await fetch(`${host}:${port}/api/v1/configuration`);
      return await response.json();
    },
    enabled: host !== null,
    retry: false
  });

  const url = useMemo(() => {
    if (!host || !port) {
      return null;
    }

    return `${host}:${port}`;
  }, [host, port]);

  return {
    configuration: data || null,
    isLoading,
    setHost,
    setPort,
    host,
    port,
    url
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