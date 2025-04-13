import {useMutation, useQueryClient} from '@tanstack/react-query';
import {useServerConfiguration} from '../hooks/useServerConfiguration.tsx';
import {Loading, LoadingWithMessage} from '../components/Loading.tsx';
import {useState} from 'react';

export function Login() {
  const {isLoading, setHost, setPort, host, port, url, refetch} = useServerConfiguration();
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
        <LoadingWithMessage message={`Attempting to connect to ${host}:${port}...`}/>
      </div>
    );
  }

  return (
    <div className={"h-100 w-100 d-flex align-items-center justify-content-center"}>
      <div className={"w-100"} style={{maxWidth: "400px"}}>
        <div className="text-center mb-4">
          <img src="/logo_small.png" alt="Chancy Logo" width={"128"} />
          <h1 className="ms-2 mb-0">Chancy</h1>
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            loginMutation.mutate({
              username: formUsername,
              password: formPassword
            })
          }}
        >
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
            type={"submit"}
            disabled={loginMutation.isPending}
          >
            {loginMutation.isPending ? <Loading /> : "Connect"}
          </button>
        </form>
      </div>
    </div>
    )
}