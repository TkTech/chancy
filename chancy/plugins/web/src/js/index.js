import * as bootstrap from 'bootstrap';

import { createRoot } from "react-dom/client";
import {createBrowserRouter, Link, Navigate, RouterProvider} from "react-router-dom";
import { StrictMode } from "react";
import ErrorPage from "./ErrorPage";
import {QueryClient, QueryClientProvider} from "@tanstack/react-query";

import { App } from "./App";
import { Jobs } from "./Jobs";
import { Workers } from "./Workers";
import { Gossip } from "./Gossip";
import {Job} from "./Job";
import {Queues} from "./Queues";
import {Queue} from "./Queue";

const router = createBrowserRouter([
  {
    path: "/",
    element: <App />,
    errorElement: <ErrorPage />,
    children: [
      {
        index: true,
        element: <Navigate to={"/jobs"} replace />
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
        element: <Workers />,
        handle: {
          crumb: () => <Link to={"/workers"}>Workers</Link>
        },
      },
      {
        path: "Gossip",
        element: <Gossip />,
        handle: {
          crumb: () => <Link to={"/gossip"}>Gossip</Link>
        },
      },
      {
        path: "queues",
        handle: {
          crumb: () => <Link to={"/queues"}>Queues</Link>
        },
        children: [
          {
            index: true,
            element: <Queues />,
          },
          {
            path: ":queueName",
            element: <Queue />,
            handle: {
              crumb: ({params}) => <Link to={`/queues/${params.queueName}`}>{params.queueName}</Link>
            },
          }
        ]
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