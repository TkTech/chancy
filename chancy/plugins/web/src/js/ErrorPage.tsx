import {useRouteError} from 'react-router-dom';

interface RouteError {
  statusText?: string;
  message?: string;
}

export default function ErrorPage() {
  const error = useRouteError() as RouteError;

  return (
    <div className={"container h-100 d-flex justify-content-center"}>
      <div className={"p-5 text-center bg-body-tertiary rounded-3 my-auto"}>
        <h1 className={"display-1"}>Oops!</h1>
        <p className={"lead"}>{error.statusText || error.message}</p>
      </div>
    </div>
  );
}
