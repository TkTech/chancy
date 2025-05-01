import { useSessionStorage } from './useSessionStorage';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import React, { createContext, useContext, useMemo } from 'react';

interface ServerSettings {
  host: string;
  port: number;
}

interface ServerConfiguration {
  plugins: string[];
}

interface AuthState {
  isAuthenticated: boolean;
  error: string | null;
  isLoading: boolean;
}

interface AppState {
  serverSettings: ServerSettings;
  updateServerSettings: (settings: Partial<ServerSettings>) => void;
  serverUrl: string | null;
  auth: AuthState;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
  configuration: ServerConfiguration | null;
  isLoading: boolean;
  error: Error | null;
  refetch: () => Promise<any>;
}

const AppContext = createContext<AppState | null>(null);

export function AppProvider({ children }: { children: React.ReactNode }) {
  const queryClient = useQueryClient();
  
  const [serverSettings, setServerSettings] = useSessionStorage<ServerSettings>('server-settings', {
    host: "http://localhost",
    port: 8000,
  });
  
  const serverUrl = useMemo(() => {
    if (!serverSettings.host || !serverSettings.port) return null;
    return `${serverSettings.host}:${serverSettings.port}`;
  }, [serverSettings.host, serverSettings.port]);
  
  const updateServerSettings = (newSettings: Partial<ServerSettings>) => {
    setServerSettings({ ...serverSettings, ...newSettings });
    queryClient.invalidateQueries();
  };

  const [auth, setAuth] = useSessionStorage<AuthState>('auth-state', {
    isAuthenticated: false,
    error: null,
    isLoading: false,
  });
  
  const login = async (username: string, password: string) => {
    if (!serverUrl) {
      setAuth({ isAuthenticated: false, error: "Server URL is not configured", isLoading: false });
      return;
    }

    try {
      setAuth({ ...auth, isLoading: true, error: null });

      const response = await fetch(`${serverUrl}/api/v1/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
        credentials: 'include',
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || "Invalid credentials");
      }

      setAuth({ isAuthenticated: true, isLoading: false, error: null });
      queryClient.removeQueries();
    } catch (error) {
      setAuth({
        isAuthenticated: false, 
        isLoading: false,
        error: (error as Error).message || "Authentication failed",
      });
      throw error;
    }
  };

  const logout = () => {
    setAuth({ isAuthenticated: false, error: null, isLoading: false });
    queryClient.clear();
  };

  const { 
    data: configuration,
    isLoading,
    error,
    refetch
  } = useQuery<ServerConfiguration>({
    queryKey: ['configuration', serverUrl],
    queryFn: async () => {
      if (!serverUrl) throw new Error("Server URL is not configured");

      const response = await fetch(`${serverUrl}/api/v1/configuration`);
      if (response.status === 403) {
        setAuth({ isAuthenticated: false, error: "Session expired", isLoading: false });
        throw new Error("Please log in again");
      }
      return await response.json();
    },
    enabled: !!serverUrl && auth.isAuthenticated,
    retry: false,
    staleTime: 30000,
    refetchOnWindowFocus: false,
  });

  return (
    <AppContext.Provider value={{
      serverSettings,
      updateServerSettings,
      serverUrl,
      auth,
      login,
      logout,
      configuration: configuration || null,
      isLoading,
      error,
      refetch
    }}>
      {children}
    </AppContext.Provider>
  );
}

export function useApp() {
  const context = useContext(AppContext);
  if (!context) throw new Error("useApp must be used within an AppProvider");
  return context;
}