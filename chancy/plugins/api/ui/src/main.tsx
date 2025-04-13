import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import {QueryClient, QueryClientProvider} from '@tanstack/react-query';
import {
  createBrowserRouter,
  redirect,
  RouterProvider
} from 'react-router-dom';

import Layout from './Layout.tsx'
import './index.scss'
// @ts-expect-error We need to import this for the side effects
import * as bootstrap from 'bootstrap'; // eslint-disable-line
import {ServerConfigurationProvider} from './hooks/useServerConfiguration.tsx';
import {Queue, Queues} from './pages/Queues.tsx';
import {WorkerDetails, Workers} from './pages/Workers.tsx';
import {Job, Jobs} from './pages/Jobs.tsx';
import {Cron, Crons} from './pages/Crons.tsx';
import {Workflow, Workflows} from './pages/Workflows.tsx';
import {Metrics, MetricDetail} from './pages/Metrics.tsx';

const queryClient = new QueryClient();

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: "/", loader: () => redirect("/jobs") },
      { path: "/queues", element: <Queues /> },
      { path: "/queues/:name", element: <Queue /> },
      { path: "/workers",  element: <Workers /> },
      { path: "/workers/:worker_id",  element: <WorkerDetails /> },
      { path: "/jobs", element: <Jobs /> },
      { path: "/jobs/:job_id", element: <Job />},
      { path: "/crons", element: <Crons />},
      { path: "/crons/:cron_id", element: <Cron />},
      { path: "/workflows", element: <Workflows />},
      { path: "/workflows/:workflow_id", element: <Workflow />},
      { path: "/metrics", element: <Metrics />},
      { path: "/metrics/:metricKey", element: <MetricDetail />},
    ]
  }
]);

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <ServerConfigurationProvider>
        <RouterProvider router={router} />
      </ServerConfigurationProvider>
    </QueryClientProvider>
  </StrictMode>,
)
