import {NavLink, Outlet} from 'react-router-dom';
import {useServerConfiguration} from './hooks/useServerConfiguration.tsx';
import {useMutation, useQueryClient} from '@tanstack/react-query';
import {Loading, Spinner} from './components/Loading.tsx';
import React, {useState, ReactNode} from 'react';
import { DrawerProvider } from './components/common/DrawerProvider';
import { setToken, clearToken } from './services/auth';
import { useWebSocket } from './contexts/WebSocketContext';
import { useTheme } from './contexts/ThemeContext';
import DashboardIcon from './assets/icons/dashboard.svg?react';
import JobsIcon from './assets/icons/jobs.svg?react';
import SchedulesIcon from './assets/icons/schedules.svg?react';
import WorkflowsIcon from './assets/icons/workflows.svg?react';
import GossipIcon from './assets/icons/gossip.svg?react';
import MetricsIcon from './assets/icons/metrics.svg?react';
import QueuesIcon from './assets/icons/queues.svg?react';
import WorkersIcon from './assets/icons/workers.svg?react';
import SystemIcon from './assets/icons/system.svg?react';

function Layout() {
  const {configuration, isLoading, setHost, setPort, host, port, url, refetch} = useServerConfiguration();
  const { connected } = useWebSocket();
  const { theme, toggleTheme } = useTheme();
  // Drawer sync is handled by a nested component within DrawerProvider
  const [formUsername, setFormUsername] = useState("");
  const [formPassword, setFormPassword] = useState("");
  const queryClient = useQueryClient();
  const [checkingSession, setCheckingSession] = useState(true);

  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    const saved = localStorage.getItem('sidebarCollapsed');
    return saved ? JSON.parse(saved) : false;
  });

  React.useEffect(() => {
    localStorage.setItem('sidebarCollapsed', JSON.stringify(sidebarCollapsed));
  }, [sidebarCollapsed]);

  const loginMutation = useMutation({
    mutationFn: async (
      {username, password}: { username: string, password: string }
    ) => {
      const response = await fetch(`${url}/api/v1/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        credentials: 'include',
        body: JSON.stringify({username, password}),
      });

      if (!response.ok || response.status !== 200) {
        throw new Error("Invalid credentials");
      }

      const data = await response.json();
      if (data?.token) setToken(data.token);
      await queryClient.invalidateQueries();
      await refetch();
      return data;
    },
    onSuccess: () => {
      // Clear sensitive form fields after a successful login
      setFormUsername('');
      setFormPassword('');
    }
  });

  // Attempt to restore session on load.
  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      setCheckingSession(true);
      try {
        await refetch();
      } finally {
        if (!cancelled) setCheckingSession(false);
      }
    })();
    return () => { cancelled = true };
  }, [refetch]);

  if (isLoading || checkingSession) {
    return (
      <div className={"p-4"}>
        <Loading />
      </div>
    );
  }

  if (!configuration) {
    return (
      <DrawerProvider>
      <div className={"h-100 w-100 d-flex align-items-center justify-content-center bg-body"}>
        <div className={"w-100 px-3 login-form-container"}>
          <div className="card shadow-lg">
            <div className="card-body p-4">
              <div className="text-center mb-4">
                <img src="/logo_small.png" alt="Chancy Logo" width={"128px"} className="mb-3" />
              </div>

              {loginMutation.isError && (
                <div className={"alert alert-danger mb-3"}>{loginMutation.error.message}</div>
              )}

              <form onSubmit={(e) => {
                e.preventDefault();
                loginMutation.mutate({
                  username: formUsername,
                  password: formPassword
                });
              }}>
                <h6 className="text-uppercase text-muted small fw-semibold mb-3">User Credentials</h6>
                <div className={"form-floating mb-2"}>
                  <input
                    className={"form-control"}
                    type={"text"}
                    id={"username"}
                    value={formUsername}
                    onChange={(e) => setFormUsername(e.target.value)}
                    autoFocus={true}
                    autoComplete={'username'}
                    placeholder="Username"
                  />
                  <label htmlFor={"username"}>Username</label>
                </div>
                <div className={"form-floating mb-4"}>
                  <input
                    className={"form-control"}
                    type={"password"}
                    id={"password"}
                    value={formPassword}
                    onChange={(e) => setFormPassword(e.target.value)}
                    autoComplete={'current-password'}
                    placeholder="Password"
                  />
                  <label htmlFor={"password"}>Password</label>
                </div>

                <h6 className="text-uppercase text-muted small fw-semibold mb-3">Server Connection</h6>
                <div className={"form-floating mb-2"}>
                  <input
                    className={"form-control"}
                    type={"text"}
                    id={"host"}
                    placeholder={"http://localhost"}
                    value={host}
                    onChange={(e) => setHost(e.target.value)}
                  />
                  <label htmlFor={"host"}>Host</label>
                </div>
                <div className={"form-floating mb-4"}>
                  <input
                    className={"form-control"}
                    type={"number"}
                    id={"port"}
                    placeholder={"8000"}
                    value={port}
                    onChange={(e) => setPort(parseInt(e.target.value))}
                  />
                  <label htmlFor={"port"}>Port</label>
                </div>

                <button
                  type="submit"
                  className={"btn btn-primary w-100"}
                  disabled={loginMutation.isPending}
                >
                  {loginMutation.isPending ? <Spinner size={16} /> : "Connect"}
                </button>
              </form>
            </div>
          </div>
        </div>
      </div>
      </DrawerProvider>
    )
  }

    function navLink(link: {to: string, text: ReactNode, icon?: React.ComponentType<React.SVGProps<SVGSVGElement>>, needs?: string[], subLinks?: {to: string, text: ReactNode}[]}) {
    if (link.needs && configuration && !link.needs.every(need => configuration.plugins.includes(need))) {
      return null;
    }

    const Icon = link.icon;

    return (
      <li className="nav-item w-100 mb-2">
        <NavLink
          to={link.to}
          end={!!link.subLinks}
          className={({isActive}) => `nav-link ${isActive ? 'active' : ''}`}
          style={{
            justifyContent: sidebarCollapsed ? 'center' : 'flex-start',
            display: 'flex',
            alignItems: 'center',
            fontSize: '0.875rem'
          }}
          title={sidebarCollapsed ? String(link.text) : ''}
        >
          {Icon && (
            <Icon
              width={32}
              height={32}
              className={sidebarCollapsed ? '' : 'me-2'}
              style={{
                verticalAlign: 'middle',
                marginTop: '-2px',
                fill: 'currentColor',
                color: 'currentColor'
              }}
            />
          )}
          {!sidebarCollapsed && link.text}
        </NavLink>
        {!sidebarCollapsed && link.subLinks && (
          <ul className="nav flex-column ms-3 mt-1">
            {link.subLinks.map(subLink => (
              <li key={subLink.to} className="nav-item">
                <NavLink
                  to={subLink.to}
                  className={({isActive}) => `nav-link py-1 ${isActive ? 'active' : ''}`}
                >
                  {subLink.text}
                </NavLink>
              </li>
            ))}
          </ul>
        )}
      </li>
    );
  }

  // DrawerRouteSync removed: drawer is opened directly from links to avoid complex background routing.

  return (
    <DrawerProvider>
    <div className="d-flex">
      <div
        id="sidebar"
        className="sidebar flex-shrink-0 vh-100 border-end d-flex flex-column position-relative"
        style={{
          width: sidebarCollapsed ? '80px' : '280px',
          transition: 'width 0.3s ease'
        }}
      >
        <button
          className="btn btn-light position-absolute"
          style={{
            right: '-16px',
            top: '24px',
            width: '32px',
            height: '32px',
            padding: 0,
            borderRadius: '50%',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '1rem',
            boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
            zIndex: 1000,
            border: '1px solid var(--bs-border-color)'
          }}
          onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {sidebarCollapsed ? '›' : '‹'}
        </button>
        <div className="d-flex align-items-center justify-content-center px-3 py-3 border-bottom">
          {!sidebarCollapsed && (
            <>
              <img src="/logo_small.png" alt="Chancy Logo" width="48" height="48" />
              <h5 className="ms-3 mb-0 fw-semibold flex-grow-1">Chancy</h5>
              <span title={connected ? 'Live updates connected' : 'Live updates disconnected'}>
                <span className={`badge rounded-pill bg-${connected ? 'success' : 'secondary'} connection-badge`}></span>
              </span>
            </>
          )}
          {sidebarCollapsed && (
            <img src="/logo_small.png" alt="Chancy Logo" width="48" height="48" />
          )}
        </div>
        <ul className="nav nav-pills flex-column flex-grow-1 px-3 py-3 sidebar-scrollable">
          {navLink({to: "/dashboard", text: "Dashboard", icon: DashboardIcon})}
          {navLink({to: "/jobs", text: "Jobs", icon: JobsIcon})}
          {navLink({to: "/queues", text: "Queues", icon: QueuesIcon})}
          {navLink({to: "/workers", text: "Workers", icon: WorkersIcon})}
          {navLink({to: "/crons", text: "Scheduled Jobs", icon: SchedulesIcon, needs: ["Cron"]})}
          {navLink({to: "/workflows", text: "Workflows", icon: WorkflowsIcon, needs: ["WorkflowPlugin"]})}
          {navLink({to: "/metrics", text: "Metrics", icon: MetricsIcon, needs: ["Metrics"]})}
          {navLink({to: "/gossip", text: "Gossip", icon: GossipIcon})}
          {navLink({to: "/system", text: "System", icon: SystemIcon})}
        </ul>
        <div className="border-top px-3 py-3">
          <button
            className="btn btn-outline-secondary btn-sm w-100 mb-2"
            onClick={toggleTheme}
            title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
          >
            {sidebarCollapsed ? (theme === 'dark' ? '☀️' : '🌙') : (theme === 'dark' ? '☀️ Light Theme' : '🌙 Dark Theme')}
          </button>
          <button
            className="btn btn-outline-secondary btn-sm w-100"
            onClick={async () => {
              try {
                await fetch(`${url}/api/v1/logout`, { method: 'POST' });
                clearToken();
                await queryClient.invalidateQueries();
                await refetch();
                // Clear any sensitive data from inputs on logout
                setFormUsername('');
                setFormPassword('');
              } catch {/* ignore */}
            }}
            title={sidebarCollapsed ? 'Logout' : ''}
          >
            {sidebarCollapsed ? '⎋' : 'Logout'}
          </button>
        </div>
      </div>
      <div className="flex-grow-1 overflow-x-scroll vh-100 p-4">
          <Outlet/>
      </div>
    </div>
    </DrawerProvider>
  );
}

export default Layout;
