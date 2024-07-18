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

  const menu = [
    {to: '/jobs', label: 'Jobs'},
    {to: '/workers', label: 'Workers'},
    {to: '/queues', label: 'Queues'},
    {to: '/gossip', label: 'Gossip'}
  ];

  return (
    <div className={"container h-100"}>
      <header className={"d-flex justify-content-center py-3"}>
        <ul className={"nav nav-pills"}>
          <a className={"navbar-brand"} href={'/'}>
            <img src={logoURL} alt={'Chancy Logo'} height={48}/>
          </a>
          {menu.map((item) => (
            <li key={item.to} className={"nav-item"}>
              <NavLink
                to={item.to}
                className={({ isActive }) => (
                  `nav-link ${isActive ? 'active' : ''}`
                )}>{item.label}</NavLink>
            </li>
          ))}
        </ul>
      </header>
      <nav aria-label={'breadcrumbs'}>
        <ol className={"breadcrumb bg-body-tertiary rounded-3 p-3"}>
          {crumbs.map((crumb, index) => (
            <li className={"breadcrumb-item"} key={index}>
              {crumb}
            </li>
          ))}
        </ol>
      </nav>
      <Outlet />
    </div>
  );
}