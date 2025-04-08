import {NavLink, Outlet} from 'react-router-dom';
import {useServerConfiguration} from './hooks/useServerConfiguration.tsx';
import {useMutation, useQueryClient} from '@tanstack/react-query';
import {Loading} from './components/Loading.tsx';
import {SparklineChart} from './components/MetricCharts.tsx';
import {useState, ReactNode} from 'react';
import {useMetricDetail} from './hooks/useMetrics.tsx';

function StatusLink({ status, text }: { status: string, text: string }) {
  const { url } = useServerConfiguration();

  const key = `global:status:${status}`;
  const { data } = useMetricDetail({
    url: url,
    key: key,
    enabled: !!url && !!status
  });

  return (
    <div className="d-flex align-items-center w-100">
      <span className="flex-grow-1">{text}</span>
      {data && data[key] && (
        <div className="ms-2">
          <SparklineChart points={data[key].data} height={20} width={60} />
        </div>
      )}
    </div>
  );
}

function Layout() {
  const {configuration, isLoading, setHost, setPort, host, port, url, refetch} = useServerConfiguration();
  const [formUsername, setFormUsername] = useState("");
  const [formPassword, setFormPassword] = useState("");
  const queryClient = useQueryClient();

  const loginMutation = useMutation({
    mutationFn: async (
      {username, password}: { username: string, password: string }
    ) => {
      const response = await fetch(`${url}/api/v1/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({username, password}),
      });

      if (!response.ok || response.status !== 200) {
        throw new Error("Invalid credentials");
      }

      await queryClient.invalidateQueries();
      await refetch();
      return await response.json();
    }
  });

  if (isLoading) {
    return (
      <div className={"p-4"}>
        <Loading />
      </div>
    );
  }

  if (!configuration) {
    return (
      <div className={"h-100 w-100 d-flex align-items-center justify-content-center"}>
        <div className={"w-100"} style={{maxWidth: "400px"}}>
          <div className="text-center mb-4">
            <img src="/logo_small.png" alt="Chancy Logo" width={"128"} />
            <h1 className="ms-2 mb-0">Chancy</h1>
          </div>
          <div className={"d-flex align-items-center my-2"}>
            <div className={"me-2"}>
              User
            </div>
            <hr className={"flex-grow-1"} />
          </div>
          {loginMutation.isError && (
            <div className={"alert alert-danger"}>{loginMutation.error.message}</div>
          )}
          <div className={"form-floating"}>
            <input
              className={"form-control"}
              type={"text"}
              id={"username"}
              value={formUsername}
              onChange={(e) => setFormUsername(e.target.value)}
              autoFocus={true}
            />
            <label htmlFor={"username"}>Username</label>
          </div>
          <div className={"form-floating"}>
            <input
              className={"form-control"}
              type={"password"}
              id={"password"}
              value={formPassword}
              onChange={(e) => setFormPassword(e.target.value)}
            />
            <label htmlFor={"password"}>Password</label>
          </div>
          <div className={"d-flex align-items-center my-2"}>
            <div className={"me-2"}>
              Connection
            </div>
            <hr className={"flex-grow-1"} />
          </div>
          <div className={"form-floating"}>
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
          <div className={"form-floating"}>
            <input
              className={"form-control"}
              type={"number"}
              id={"port"}
              placeholder={"8000"}
              value={port}
              onChange={(e) => setPort(parseInt(e.target.value))}/>
            <label htmlFor={"port"}>Port</label>
          </div>
          <button
            className={"btn btn-primary w-100 mt-4"}
            onClick={() => {
              loginMutation.mutate({
                username: formUsername,
                password: formPassword
              })
            }}
            disabled={loginMutation.isPending}
          >
            {loginMutation.isPending ? <Loading /> : "Connect"}
          </button>
        </div>
      </div>
    )
  }
  
    function navLink(link: {to: string, text: ReactNode, needs?: string[], subLinks?: {to: string, text: ReactNode}[]}) {
    if (link.needs && configuration && !link.needs.every(need => configuration.plugins.includes(need))) {
      return null;
    }

    return (
      <li className="nav-item w-100 mb-2">
        <NavLink
          to={link.to}
          end={!!link.subLinks}
          className={({isActive}) => `nav-link ${isActive ? 'active' : ''}`}
        >
          {link.text}
        </NavLink>
        {link.subLinks && (
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

  return (
    <div className="d-flex">
      <div id="sidebar" className="flex-shrink-0 vh-100 border-end" style={{width: "280px"}}>
        <div className="d-flex align-items-center px-3 pt-3">
          <img src="/logo_small.png" alt="Chancy Logo" width="40" height="40" />
          <h4 className="ms-2 mb-0">Chancy</h4>
        </div>
        <ul className="nav nav-pills flex-column mb-auto px-3">
          {navLink({
            to: "/jobs",
            text: "Jobs",
            subLinks: [
              { to: "/jobs/pending", text: <StatusLink status="pending" text="Pending" /> },
              { to: "/jobs/running", text: <StatusLink status="running" text="Running" /> },
              { to: "/jobs/succeeded", text: <StatusLink status="succeeded" text="Succeeded" /> },
              { to: "/jobs/failed", text: <StatusLink status="failed" text="Failed" /> },
              { to: "/jobs/retrying", text: <StatusLink status="retrying" text="Retrying" /> },
              { to: "/jobs/expired", text: <StatusLink status="expired" text="Expired" /> },
            ]
          })}
          {navLink({to: "/queues", text: "Queues"})}
          {navLink({to: "/workers", text: "Workers"})}
          {navLink({to: "/crons", text: "Cron", needs: ["Cron"]})}
          {navLink({to: "/workflows", text: "Workflows", needs: ["WorkflowPlugin"]})}
          {navLink({to: "/metrics", text: "Metrics", needs: ["Metrics"]})}
        </ul>
      </div>
      <div className="flex-grow-1 overflow-x-scroll vh-100 p-3">
          <Outlet/>
      </div>
    </div>
  );
}

export default Layout;