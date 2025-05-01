import { useState } from 'react';
import { useApp } from '../hooks/useServerConfiguration.tsx';
import { Loading, LoadingWithMessage } from '../components/Loading.tsx';

export function Login() {
  const { serverSettings, updateServerSettings, auth, login } = useApp();
  
  const [formUsername, setFormUsername] = useState("");
  const [formPassword, setFormPassword] = useState("");
  const [formHost, setFormHost] = useState(serverSettings.host);
  const [formPort, setFormPort] = useState(serverSettings.port);
  const [connectionError, setConnectionError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setConnectionError(null);
    
    updateServerSettings({
      host: formHost,
      port: formPort,
    });
    await login(formUsername, formPassword);
  };

  // Show loading state during authentication or form submission
  if (auth?.isLoading) {
    return (
      <div className={"p-4"}>
        <LoadingWithMessage message={`Attempting to connect to ${formHost}:${formPort}...`}/>
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
        <form onSubmit={handleSubmit}>
          <div className={"d-flex align-items-center my-2"}>
            <div className={"me-2"}>
              User
            </div>
            <hr className={"flex-grow-1"} />
          </div>
          
          {(auth?.error || connectionError) && (
            <div className={"alert alert-danger"}>
              {auth?.error || connectionError}
            </div>
          )}
          
          <div className={"form-floating"}>
            <input
              className={"form-control"}
              type={"text"}
              id={"username"}
              value={formUsername}
              onChange={(e) => setFormUsername(e.target.value)}
              autoFocus={true}
              required
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
              required
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
              value={formHost}
              onChange={(e) => setFormHost(e.target.value)}
              required
            />
            <label htmlFor={"host"}>Host</label>
          </div>
          <div className={"form-floating"}>
            <input
              className={"form-control"}
              type={"number"}
              id={"port"}
              placeholder={"8000"}
              value={formPort}
              onChange={(e) => {
                const value = e.target.value;
                setFormPort(value ? parseInt(value) : 0);
              }}
              required
            />
            <label htmlFor={"port"}>Port</label>
          </div>
          <button
            className={"btn btn-primary w-100 mt-4"}
            type={"submit"}
            disabled={auth.isLoading}
          >
            {auth.isLoading ? <Loading /> : "Connect"}
          </button>
        </form>
      </div>
    </div>
  );
}