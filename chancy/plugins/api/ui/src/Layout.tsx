import {NavLink, Outlet} from 'react-router-dom';
import {useServerConfiguration} from './hooks/useServerConfiguration.tsx';
import {useQueryClient} from '@tanstack/react-query';
import {Loading} from './components/Loading.tsx';
import {useCallback, useState} from 'react';

function Layout() {
  const {configuration, isLoading, setHost, setPort, host, port} = useServerConfiguration();
  const [formHost, setFormHost] = useState(host);
  const [formPort, setFormPort] = useState(port);
  const queryClient = useQueryClient();

  const connect = useCallback(() => {
    setHost(formHost);
    setPort(formPort);
    queryClient.invalidateQueries();
  }, [formHost, formPort, setHost, setPort, queryClient]);

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
        <div>
          <h1 className={"text-center mb-4"}>Chancy</h1>
          <p></p>
          <div className={"mb-3"}>
            <label htmlFor={"host"} className={"form-label"}>Host</label>
            <input
              className={"form-control"}
              type={"text"}
              id={"host"}
              placeholder={"http://localhost"}
              value={formHost}
              onChange={(e) => setFormHost(e.target.value)}
            />
            <small className={"form-text text-muted"}>The host of the Chancy API to connect to.</small>
          </div>
          <div className={"mb-3"}>
            <label htmlFor={"port"} className={"form-label"}>Port</label>
            <input
              className={"form-control"}
              type={"number"}
              id={"port"}
              placeholder={"8000"}
              value={formPort}
              onChange={(e) => setFormPort(parseInt(e.target.value))}/>
            <small className={"form-text text-muted"}>The port of the Chancy API to connect to.</small>
          </div>
          <button className={"btn btn-primary w-100"} onClick={connect}>
            Connect
          </button>
        </div>
      </div>
    )
  }

  function navLink(link: {to: string, text: string, needs?: string[]}) {
    if (link.needs && configuration && !link.needs.every(need => configuration.plugins.includes(need))) {
      return null;
    }

    return (
      <li className="nav-item">
        <NavLink
          to={link.to}
          className={({isActive}) => `nav-link ${isActive ? 'active' : ''}`}
        >
          {link.text}
        </NavLink>
      </li>
    );
  }

  return (
    <>
      <header className={"bg-body-tertiary mb-4 py-3 d-flex flex-wrap justify-content-center"}>
          <ul className="nav nav-pills">
            {navLink({to: "/queues", text: "Queues"})}
            {navLink({to: "/workers", text: "Workers"})}
            {navLink({to: "/jobs", text: "Jobs"})}
            {navLink({to: "/crons", text: "Cron", needs: ["Cron"]})}
            {navLink({to: "/workflows", text: "Workflows", needs: ["WorkflowPlugin"]})}
          </ul>
      </header>
      <Outlet/>
    </>
  );
}

export default Layout;