import {useRouteError} from 'react-router-dom';

interface RouteError {
  statusText?: string;
  message?: string;
}

export default function ErrorPage() {
  const error = useRouteError() as RouteError;

  const logoURL = new URL(
    "../img/logo_small.png",
    import.meta.url
  );

  return (
    <div className={"container h-100 d-flex justify-content-center"}>
      <div className={"p-5 text-center bg-body-tertiary rounded-3 my-auto"}>
        <img src={logoURL} alt={'Chancy Logo'} height={128} />
        <h1 className={"display-1"}>Oops!</h1>
        <p className={"lead"}>{error.statusText || error.message}</p>
      </div>
    </div>
  );
}
