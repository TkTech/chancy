# Chancy

![Chancy Logo](misc/logo_small.png)

A postgres-backed task queue for Python.

## Key Features

- Fully-featured jobs support priorities, retries, timeouts, memory limits,
  future scheduling, and more.
- Completed jobs stick around in the database for easy debugging and job
  tracking, with configurable retention policies. Inspect your jobs with plain
  old SQL, an HTTP API, or the dashboard.
- Core is dependency-free except for psycopg3, but plugins can add additional
  dependencies.
- Optional transactional job queueing - only queue a job if your transaction
  commits successfully.
- asyncio-based worker, can be run in-process with your web server on small
  projects or as a standalone worker on larger projects.
- Statically declare your queues or create and manage them on-the-fly,
  assigning them to workers based on tags.

## Documentation

Checkout the getting-started guide and the API documentation at
https://tkte.ch/chancy/.

## Similar Work

Many similar projects exist. Some of them are:

- https://worker.graphile.org/ (Node.js)
- https://riverqueue.com/ (Go)
- https://github.com/acaloiaro/neoq (Go)
- https://github.com/contribsys/faktory (Go)
- https://github.com/sorentwo/oban (Elixir)
- https://github.com/procrastinate-org/procrastinate (Python)