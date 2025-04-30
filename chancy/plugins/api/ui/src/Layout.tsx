import {NavLink, Outlet} from 'react-router-dom';
import {useApp} from './hooks/useServerConfiguration.tsx';
import {SlidePanelProvider} from './components/SlidePanelContext';
import {Login} from './pages/Login.tsx';
import {LoadingWithMessage} from './components/Loading.tsx';

function Layout() {
  const { auth, configuration, isLoading } = useApp();

  if (!auth.isAuthenticated ) {
    return <Login />;
  }

  if (!configuration || isLoading) {
    return <LoadingWithMessage message={"Loading configuration..."} />;
  }

  return (
    <SlidePanelProvider>
      <div>
        <header className="d-flex align-items-center p-3">
          <img src="/logo_small.png" alt="Chancy" width={"48"} title={"Adorable, no?"} />
          <ul className="nav nav-pills ms-2">
            {[
              {to: "/jobs", label: "Jobs"},
              {to: "/queues", label: "Queues"},
              {to: "/workers", label: "Workers"},
              {to: "/crons", label: "Crons", needs: ["Cron"]},
              {to: "/workflows", label: "Workflows", needs: ["WorkflowPlugin"]},
              {to: "/metrics", label: "Metrics", needs: ["Metrics"]},
            ].map(({ to, label, needs = [] }: {to: string, label: string, needs?: string[]}) => {
              if (needs.length > 0 && needs.every((need) => !configuration.plugins.includes(need))) {
                return null;
              }
              return (
                <li className="nav-item" key={to}>
                  <NavLink
                    to={to}
                    className={
                      ({isActive}) => `nav-link ${isActive ? 'active' : ''}`
                    }
                  >
                    {label}
                  </NavLink>
                </li>
              );
            })}
          </ul>
        </header>
        <div className={"p-3"}>
          <Outlet/>
        </div>
      </div>
    </SlidePanelProvider>
  );
}

export default Layout;