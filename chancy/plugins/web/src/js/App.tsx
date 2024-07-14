import {useEffect} from 'react';
import {NavLink, Outlet, useMatches} from 'react-router-dom';

const logoURL = new URL(
  "../img/logo_small.png",
  import.meta.url
);

const querySubscriptions = () => {
  useEffect(() => {
    let url = new URL("/ws", window.location.href);
    url.protocol = url.protocol.replace("http", "ws");
    const websocket = new WebSocket(url.href);
    websocket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      console.log(data);
    }

    return () => {
      websocket.close();
    }
  }, []);
}

export function App() {
  const matches = useMatches();
  const crumbs = matches.filter((match) => Boolean(match.handle?.crumb))
    .map((match) => match.handle.crumb(match))

  return (
    <div className={"container is-fluid"}>
      <nav className={'navbar mb-3'} role={'navigation'} aria-label={'main navigation'}>
        <div className={'navbar-brand'}>
          <a className={'navbar-item'} href={'/'}>
            <img src={logoURL} alt={'Chancy Logo'}/>
            Chancy
          </a>
        </div>
        <NavLink
          to={"/jobs"}
          className={({ isActive, isPending }) =>
            `navbar-item ${isActive ? 'is-active' : ''}`
          }>Jobs</NavLink>
        <NavLink
          to={"/workers"}
          className={({ isActive, isPending }) =>
            `navbar-item ${isActive ? 'is-active' : ''}`
          }>Workers</NavLink>
        <NavLink
          to={"/gossip"}
          className={({ isActive, isPending }) =>
            `navbar-item ${isActive ? 'is-active' : ''}`
          }>Gossip</NavLink>
      </nav>
      {/*<nav className={'breadcrumb has-arrow-separator my-4'} aria-label={'breadcrumbs'}>
        <ul>
          {crumbs.map((crumb, index) => (
            <li key={index}>
              {crumb}
            </li>
          ))}
        </ul>
      </nav>*/}
      <Outlet />
    </div>
  );
}