import {useRouteError} from 'react-router-dom';

interface RouteError {
  statusText?: string;
  message?: string;
}

export default function ErrorPage() {
  const error = useRouteError() as RouteError;

  return (
    <section className={"hero"}>
      <div className={"hero-body"}>
        <p className={"title"}>Oops!</p>
        <p className={"subtitle"}>{error.statusText || error.message}</p>
      </div>
    </section>
  );
}
