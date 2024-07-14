import { createRoot } from "react-dom/client";
import {createBrowserRouter, Link, RouterProvider} from "react-router-dom";
import { StrictMode } from "react";
import ErrorPage from "./ErrorPage";
import {QueryClient, QueryClientProvider} from "@tanstack/react-query";

import { App } from "./App";
import { Jobs } from "./Jobs";
import { Workers } from "./Workers";
import { Gossip } from "./Gossip";
import {Job} from "./Job";

const router = createBrowserRouter([
  {
    path: "/",
    element: <App />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <Jobs />
      },
      {
        path: "jobs",
        handle: {
          crumb: () => <Link to={"/jobs"}>Jobs</Link>
        },
        children: [
          {
            index: true,
            element: <Jobs />
          },
          {
            path: ":jobId",
            element: <Job />,
            handle: {
              crumb: ({params}) => <Link to={`/jobs/${params.jobId}`}>Job {params.jobId}</Link>
            }
          }
        ]
      },
      {
        path: "workers",
        element: <Workers />
      },
      {
        path: "Gossip",
        element: <Gossip />
      }
    ]
  }
]);

const queryClient = new QueryClient();

createRoot(
  document.getElementById("app")
).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>
)